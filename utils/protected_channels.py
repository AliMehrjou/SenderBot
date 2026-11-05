# -*- coding: utf-8 -*-
"""
🛡 (خواسته کارفرما) کانال‌های حفاظت‌شده‌ی ورکرها
================================================
دو کانال اختصاصی سیستم (@linkdoonifun و @robotsfunlink) که:

  ۱. هر ورکر (اکانت ارسال) به‌محض آنلاین شدن باید عضو آن‌ها باشد
     → ensure_worker_in_protected_channels()
  ۲. هیچ عملیات «پاکسازی» حق خارج‌کردن اکانت‌ها را از آن‌ها ندارد:
     - خروج دسته‌جمعی از گروه‌ها/کانال‌ها (cleanup_handlers)
     - پاکسازی تاریخچه چت‌ها (cleanup_handlers)
     - leave-sweep سفارش‌های terminal (task_queue)
     → is_protected_chat()  در همه‌ی نقاط خروج چک می‌شود.

منبع کانال‌ها: config.WORKER_PROTECTED_CHANNELS
(عمداً جدای از FORCE_JOIN_CHANNELS است؛ آن کلید مخصوص جوین اجباریِ «کاربران ربات»
است و اپراتور ممکن است آن را تغییر دهد — این دو کانال باید مستقل بمانند.)
"""

import asyncio
import logging
import random
import time
from typing import Dict, Optional, Set

from config import config

logger = logging.getLogger(__name__)

# هر ورکر حداکثر با این فاصله یک‌بار join-check می‌شود (به‌عنوان членو باقی می‌ماند
# حتی وقتی ری‌کانکت می‌شود؛ FloodWait به‌عنوان خطا در نظر گرفته می‌شود).
RECHECK_TTL_SECONDS = 6 * 3600


# 🧊 FIX (dead-channel-retry): کانال‌های مرده (حذف‌شده توسط تلگرام —
# USERNAME_NOT_OCCUPIED / CHANNEL_PRIVATE) برای این مدت (ساعت) دیگر resolve/JOIN
# نمی‌شوند. در لاگ bot_19 همین دو کانالِ ازقبل‌حذف‌شده ۷۰ بار تلاش بیهوده
# (و ۷۰ فراخوانی contacts.ResolveUsername) داشتند.
DEAD_CHANNEL_CACHE_HOURS = 24


def _dead_channel_cfg_hours() -> int:
    try:
        from config import config
        return max(1, int(getattr(config, "DEAD_CHANNEL_CACHE_HOURS", DEAD_CHANNEL_CACHE_HOURS)))
    except (TypeError, ValueError, AttributeError):
        return DEAD_CHANNEL_CACHE_HOURS


async def _is_dead_channel(name: str) -> bool:
    """آیا این کانال اخیراً مرده/حذف‌شده شناسایی شده؟ (کش منفی Redis — best-effort)"""
    try:
        from workers.sender import _get_redis
        return bool(await _get_redis().get(f"dead_ch:{name}"))
    except Exception:
        return False


async def _mark_dead_channel(name: str) -> None:
    try:
        from workers.sender import _get_redis
        await _get_redis().set(f"dead_ch:{name}", "1", ex=_dead_channel_cfg_hours() * 3600)
    except Exception:
        pass

# آینه‌ی محلی برای مقایسه‌ی سریع (بدون فراخوانی تلگرام)
_usernames: Set[str] = set()
_resolved_ids: Set[int] = set()
_resolve_lock: Optional[asyncio.Lock] = None
_last_ensure_at: Dict[int, float] = {}


def _get_resolve_lock() -> asyncio.Lock:
    global _resolve_lock
    if _resolve_lock is None:
        _resolve_lock = asyncio.Lock()
    return _resolve_lock


def _normalize_channel(value: str) -> Optional[str]:
    """@username / https://t.me/username/ ... → usernameِ حروف‌کوچک؛ None یعنی نامعتبر."""
    if not value:
        return None
    raw = str(value).strip()
    low = raw.lower()
    for prefix in (
        "https://t.me/",
        "http://t.me/",
        "https://telegram.me/",
        "http://telegram.me/",
        "www.t.me/",
        "t.me/",
    ):
        if low.startswith(prefix):
            raw = raw[len(prefix):]
            break
    raw = raw.split("?")[0].split("#")[0].strip("/ ")
    raw = raw.lstrip("@").strip()
    return raw.lower() or None


def _load_usernames() -> Set[str]:
    """خواندن کانال‌های حفاظت‌شده از کانفیگ (idempotent)."""
    global _usernames
    if _usernames:
        return _usernames
    channels = []
    raw = getattr(config, "WORKER_PROTECTED_CHANNELS", "") or ""
    for part in str(raw).replace(";", ",").split(","):
        name = _normalize_channel(part)
        if name:
            channels.append(name)
    _usernames = set(channels)
    return _usernames


def protected_channel_usernames() -> Set[str]:
    """لیست یوزرنیم‌های کانال‌های حفاظت‌شده (نرمال‌شده)."""
    return set(_load_usernames())


def protected_channel_handles() -> Set[str]:
    """لیست کانال‌ها به شکل @username — برای نمایش به کاربر."""
    return {f"@{u}" for u in _load_usernames()}


def is_protected_chat(
    chat_id: Optional[int] = None,
    username: Optional[str] = None,
    link: Optional[str] = None,
) -> bool:
    """
    آیا این چت یکی از کانال‌های حفاظت‌شده است؟
    ترکیب سه روش (هر کدام که در دسترس است):
      - تطبیق مستقیم chat_id با آیدی‌های resolve شده
      - تطبیق username
      - استخراج username از لینک (t.me/...)
    """
    if chat_id is not None:
        try:
            if int(chat_id) in _resolved_ids:
                return True
        except (TypeError, ValueError):
            pass
    names = _load_usernames()
    if not names:
        return False
    for candidate in (username, link):
        if not candidate:
            continue
        normalized = _normalize_channel(candidate)
        if normalized and normalized in names:
            return True
    return False


async def resolve_protected_channel_ids(client) -> Set[int]:
    """
    Resolve‌کردن یوزرنیم‌های کانال‌های حفاظت‌شده → chat_id (تا در مقایسه‌های
    بعدی بدون فراخوانی تلگرام چِک شود). نتیجه برای همیشه کش می‌شود؛
    best-effort است و هرگز exception بالا نمی‌برد.
    """
    names = _load_usernames()
    pending = [n for n in names if n not in _known_username_to_id]
    if not pending:
        return set(_resolved_ids)

    lock = _get_resolve_lock()
    if lock.locked():
        # یک resolve دیگر در جریان است؛ منتظرش نمی‌مانیم (best-effort)
        return set(_resolved_ids)
    async with lock:
        for name in pending:
            if name in _known_username_to_id:
                continue
            try:
                chat = await asyncio.wait_for(client.get_chat(name), timeout=15)
                chat_id = getattr(chat, "id", None)
                if chat_id is not None:
                    _resolved_ids.add(int(chat_id))
                    _known_username_to_id[name] = int(chat_id)
                    logger.info(f"Protected channels: @{name} resolved to chat_id={chat_id}")
            except Exception as e:
                logger.debug(f"Protected channels: could not resolve @{name} yet: {e}")
    return set(_resolved_ids)


# آینه‌ی username → chat_id (پرشده توسط resolve_protected_channel_ids)
_known_username_to_id: Dict[str, int] = {}


async def ensure_worker_in_protected_channels(
    client,
    account_id: Optional[int] = None,
    force: bool = False,
) -> Dict[str, str]:
    """
    عضویت ورکر در کانال‌های حفاظت‌شده را تضمین می‌کند.
    idempotent و best-effort است؛ هرگز exception بالا نمی‌برد.

    برگرداندن: گزارش {username: status} برای لاگ/نوتیفیکیشن.
      status ∈ joined | already | pending | flood | error
    """
    report: Dict[str, str] = {}
    names = _load_usernames()
    if not names:
        return report

    # 🧊 FIX (frozen-join): اکانت فریز اجازه‌ی جوین ندارد — FROZEN_METHOD_INVALID
    # روی channels.JoinChannel در bot_18 دقیقاً همین‌جا رخ می‌داد (۳۵ بار در استارت).
    if account_id is not None:
        try:
            from utils.freeze_guard import is_account_frozen
            if await is_account_frozen(account_id):
                logger.info(
                    f"Protected channels: worker {account_id} is FROZEN — skipping join check."
                )
                return report
        except Exception:
            pass

    # 🧊 FIX (dead-channel-retry): کانال‌های مرده را همین‌جا فیلتر می‌کنیم
    alive_names = []
    for n in names:
        if await _is_dead_channel(n):
            logger.debug(f"Protected channels: @{n} is cached DEAD — skipping.")
            report[n] = "dead"
        else:
            alive_names.append(n)
    names = alive_names
    if not names:
        return report

    # throttle per-account ( reconnectها هر ۶۰ ثانیه صدا زده می‌شوند؛
    # نباید هر بار join-check بزنیم )
    if account_id is not None and not force:
        last = _last_ensure_at.get(account_id)
        if last is not None and (time.monotonic() - last) < RECHECK_TTL_SECONDS:
            return report
        _last_ensure_at[account_id] = time.monotonic()
    elif account_id is not None:
        _last_ensure_at[account_id] = time.monotonic()

    from pyrogram.errors import FloodWait, UserAlreadyParticipant

    try:
        if not getattr(client, "is_connected", False):
            logger.debug(
                f"Protected channels: worker {account_id} not connected; skipping join check."
            )
            return report
    except Exception:
        return report

    for name in names:
        status = "error"
        try:
            # 🧊 رفتار انسانی: مکث تصادفی قبل از هر resolve/join — قبلاً هر دو کانال
            # برای همه‌ی ورکرها در همان ثانیه‌ی استارت‌آپ پشت‌سرهم زده می‌شد.
            await asyncio.sleep(random.uniform(1.0, 4.0))
            # ۱. resolve و کش‌کردن chat_id (برای محافظت‌های بعدی در leave-sweep)
            try:
                chat = await asyncio.wait_for(client.get_chat(name), timeout=15)
                chat_id = getattr(chat, "id", None)
                if chat_id is not None and int(chat_id) not in _resolved_ids:
                    _resolved_ids.add(int(chat_id))
                    _known_username_to_id[name] = int(chat_id)
            except Exception as e:
                _err_name = type(e).__name__
                if _err_name in ("UsernameNotOccupied", "ChannelPrivate", "UsernameInvalid"):
                    # کانال حذف/خصوصی شده — کش منفی تا حلقه‌های بعدی مزاحم نباشد
                    await _mark_dead_channel(name)
                    logger.warning(
                        f"Protected channels: @{name} is GONE ({_err_name}) — "
                        f"cached as dead for {_dead_channel_cfg_hours()}h."
                    )
                else:
                    logger.debug(f"Protected channels: get_chat(@{name}) failed: {e}")

            # ۲. join (اگر از قبل عضو باشد UserAlreadyParticipant می‌گیریم)
            await asyncio.wait_for(client.join_chat(name), timeout=30)
            status = "joined"
        except UserAlreadyParticipant:
            status = "already"
        except FloodWait as e:
            # منتظر نمی‌مانیم؛ حلقه‌ی ری‌کانکت بعد از TTL دوباره تلاش می‌کند
            status = "flood"
            logger.warning(
                f"Protected channels: FloodWait {e.value}s while joining @{name} "
                f"(worker {account_id}); will retry later."
            )
        except Exception as e:
            # InviteRequestSent (کانال تایيیدی) و خطاهاي گذرا اينجا مي‌افتند
            _err_name = type(e).__name__
            if _err_name in ("UsernameNotOccupied", "ChannelPrivate", "UsernameInvalid"):
                await _mark_dead_channel(name)
                status = "dead"
            else:
                status = "pending" if "InviteRequestSent" in _err_name else "error"
            # فریز: ثبت و قطع براي همين اکانت (تا موج فريز هم ديده شود)
            if "FROZEN_METHOD_INVALID" in str(e).upper():
                try:
                    from utils.freeze_guard import mark_account_frozen
                    await mark_account_frozen(account_id, reason=f"join @{name}: {e}")
                except Exception:
                    pass
                status = "frozen"
            logger.warning(
                f"Protected channels: join @{name} failed for worker {account_id}: "
                f"{_err_name}: {e}"
            )
        report[name] = status

    joined_now = [n for n, s in report.items() if s == "joined"]
    if joined_now:
        logger.info(
            f"Protected channels: worker {account_id} joined {joined_now} "
            f"({', '.join(protected_channel_handles())})."
        )
    return report
