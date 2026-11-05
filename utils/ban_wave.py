# -*- coding: utf-8 -*-
"""
🛑 ANTI-CHAIN-BAN GUARD (گارد ضد بنِ زنجیره‌ای)
=============================================

چرا این ماژول وجود دارد؟
------------------------
فاجعه‌ی ۳۴ بن از ۴۰ ورکر دقیقاً از یک «حلقه‌ی بازخورد» تغذیه می‌شد:

  ورکر ۱ بن می‌شود → تارگت‌های ارسال‌نشده بلافاصله به ورکر ۲ سپرده می‌شد
  (که احتمالاً روی همان پروکسی/IP سوخته بود) → بن → ورکر ۳ → … تا نابودی ناوگان.

سه کانال اصلی انتقال بن زنجیره‌ای در این سیستم:
  1) **IP/پروکسی مشترک** (MAX_ACCOUNTS_PER_PROXY قبلاً ۲۰ بود!) — تلگرام
     اکانت‌های روی یک IP را یک «خوشه» می‌بیند؛ سوختن IP یعنی سوختن خوشه.
  2) **سوییچ فوری ورکر بعد از بن** — بدون هیچ مکث/بررسی، همان محتوا و همان
     تارگت‌ها با اکانت بعدی (روی همان IP) ادامه می‌یافت.
  3) **نبود شناسایی موج** — سیستم هیچ واکنشی به «بن‌های متوالی در بازه‌ی کوتاه»
     نداشت؛ مدارقطع‌کنِ قدیمی فقط سرعت را ۳۰ ثانیه پایین می‌آورد و فقط وقتی
     سرعتِ غیر-safe فعال بود.

این ماژول سه لایه‌ی دفاعی اضافه می‌کند (همه با Redis + DB، بدون تغییر معماری):

  لایه ۱ — «فورنزیک بن»: هر بن واقعی/PeerFlood همراه با پروکسیِ لحظه‌ی بن
           ثبت می‌شود (WorkerEvent + کلید Redis `last_ban_proxy:{account_id}`)
           تا ادمین دقیقاً بداند بن روی کدام پروکسی رخ داده و چند اکانت دیگر
           روی همان پروکسی‌اند.

  لایه ۲ — «قرنطینه‌ی پروکسی سوخته»: اگر روی یک پروکسی ≥ آستانه بن/PeerFlood
           رخ دهد، پروکسی BURNED می‌شود (is_active=False + health_state='BURNED'
           + cooldown بلند claim) تا اکانتِ جدیدی هرگز روی آن قرار نگیرد.

  لایه ۳ — «فریز موج بن»: اگر در پنجره‌ی زمانی کوتاه ≥ آستانه بن سراسری رخ
           دهد، کل دیسپچر به‌مدت قابل تنظیم فریز می‌شود (مثل global slowdown
           ولی قطعِ واقعی، نه فقط کاهش سرعت) و به ادمین‌ها هشدار می‌رود.

🧊 رویداد «freeze» (FROZEN_METHOD_INVALID):
  تحلیل لاگ‌های bot_18/bot_19 نشان داد موج‌های بنِ واقعی از «موج فریز» شروع
  می‌شوند: چند اکانت همزمان FROZEN_METHOD_INVALID می‌گیرند و سیستمِ قبلی —
  که این خطا را هندل نمی‌کرد — به جای توقف، همان عملیات را بی‌وقفه تکرار
  می‌کرد (حلقه‌ی بی‌نهایت photos.GetUserPhotos). حالا freeze به‌عنوان نوع
  رویدادِ درجه‌یک (مثل ban/peer_flood) ثبت می‌شود: شمارنده‌ی سراسری خودش را
  دارد و از آستانه‌ی BAN_WAVE_FROZEN_GLOBAL_THRESHOLD عبور کند، فریز سراسری
  دیسپچر فعال می‌شود — پیش از آنکه موج فریز به موج بن تبدیل شود.

همه‌ی آستانه‌ها از config.py / متغیرهای محیطی قابل تنظیم‌اند و همه‌ی مسیرها
fail-open هستند (خطای Redis/DB هرگز جریان ارسال را نمی‌شکند).
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import select, update

logger = logging.getLogger(__name__)

# ==========================================
# کلیدهای Redis (قرارداد)
# ==========================================
# banwave:global:ban        → شمارنده‌ی بن‌های واقعی در پنجره‌ی موج (TTL = پنجره)
# banwave:global:peer_flood → شمارنده‌ی PeerFloodها در پنجره‌ی موج
# banwave:global:freeze     → شمارنده‌ی فریزهای FROZEN_METHOD_INVALID (TTL = پنجره)
# banwave:freeze_until      → epoch پایان فریز سراسری دیسپچ (TTL = مدت فریز)
# proxy_bans:{proxy}        → شمارنده‌ی بن‌های واقعی روی این پروکسی (TTL = 24h)
# proxy_pf:{proxy}          → شمارنده‌ی PeerFloodهای این پروکسی (TTL = 24h)
# last_ban_proxy:{acc_id}   → پروکسیِ لحظه‌ی بنِ این اکانت (TTL = 7 روز، فورنزیک)
# proxy_cd:{proxy}          → (کلید موجودِ claim cooldown) — بعد از بن، بلند می‌شود
_BANWAVE_GLOBAL_BAN_KEY = "banwave:global:ban"
_BANWAVE_GLOBAL_PF_KEY = "banwave:global:peer_flood"
_BANWAVE_GLOBAL_FREEZE_KEY = "banwave:global:freeze"
_BANWAVE_FREEZE_KEY = "banwave:freeze_until"
_PROXY_BAN_KEY = "proxy_bans:{proxy}"
_PROXY_PF_KEY = "proxy_pf:{proxy}"
_LAST_BAN_PROXY_KEY = "last_ban_proxy:{account_id}"

# رویدادهای معتبر ورودی — freeze هم اکنون درجه‌یک است (utils/freeze_guard.py)
_VALID_EVENT_TYPES = ("ban", "peer_flood", "freeze")

# رجیستری سبکِ instance بات برای هشدار به ادمین‌ها (بدون وابستگی حلقوی)
_admin_bot = None


def set_admin_bot(bot) -> None:
    """ثبت instance بات aiogram برای ارسال هشدارهای گارد (از main.py صدا زده می‌شود)."""
    global _admin_bot
    _admin_bot = bot


def _redis():
    """کلاینت Redisِ سندر (lazy import برای پرهیز از وابستگی حلقوی در زمان import)."""
    from workers.sender import _get_redis
    return _get_redis()


def _cfg(name: str, default) -> object:
    """خواندن امن تنظیمات کانفیگ با fallback."""
    from config import config
    try:
        return int(getattr(config, name, default))
    except (TypeError, ValueError):
        return default


def _proxy_display(proxy_string: Optional[str]) -> str:
    """
    نمایش امنِ پروکسی برای پیام‌های ادمین: فقط host:port
    (بدون افشای user:pass در لاگ‌ها/پیام‌ها). رشته‌های بدون '@'
    یا نامعتبر دست‌نخورده برمی‌گردند.
    """
    if not proxy_string:
        return "بدون پروکسی (دایرکت!)"
    s = str(proxy_string).strip()
    if "@" in s:
        # فرمت user:pass@host:port → فقط host:port
        tail = s.rsplit("@", 1)[-1]
        return tail or s
    # حذف scheme اگر بود
    if "://" in s:
        s = s.split("://", 1)[-1]
    return s


async def _alert_admins(text: str) -> None:
    """ارسال هشدار HTML به ادمین‌ها (اگر bot ثبت شده باشد) + لاگ همیشگی."""
    logger.critical(text.replace("<b>", "").replace("</b>", "").replace("<code>", "").replace("</code>", ""))
    bot = _admin_bot
    if bot is None:
        return
    try:
        from utils.admin_broadcast import broadcast_to_admins
        await broadcast_to_admins(bot, text=text)
    except Exception as e:
        logger.warning(f"ban_wave admin alert failed: {e}")


def _spawn(coro) -> None:
    """اجرای fire-and-forget امن (بداینجکت در لاگ، بدون بلاکِ جریان اصلی)."""
    try:
        task = asyncio.create_task(coro)
        task.add_done_callback(lambda t: t.exception() if not t.cancelled() else None)
    except Exception as e:
        logger.debug(f"ban_wave spawn failed: {e}")


# ==========================================
# 🧊 لایه ۳: فریز موج بن (سراسری)
# ==========================================

async def is_banwave_freeze_active() -> bool:
    """آیا فریز سراسریِ دیسپچ (به‌دلیل موج بن) فعال است؟ fail-open."""
    try:
        val = await _redis().get(_BANWAVE_FREEZE_KEY)
        if val and float(val) > datetime.now(timezone.utc).timestamp():
            return True
    except Exception as e:
        logger.debug(f"banwave freeze check failed (fail-open): {e}")
    return False


async def banwave_freeze_remaining_seconds() -> int:
    """ثانیه‌های باقی‌مانده تا پایان فریز موج بن (۰ = فعال نیست)."""
    try:
        val = await _redis().get(_BANWAVE_FREEZE_KEY)
        if val:
            remaining = int(float(val) - datetime.now(timezone.utc).timestamp())
            return max(0, remaining)
    except Exception as e:
        logger.debug(f"banwave freeze remaining failed: {e}")
    return 0


async def _trigger_global_freeze(ban_count: int, pf_count: int, window_minutes: int) -> None:
    """فعال‌سازی/تمدید فریز سراسری دیسپچر پس از عبور از آستانه‌ی موج."""
    freeze_minutes = max(1, _cfg("BAN_WAVE_FREEZE_MINUTES", 60))
    until_epoch = datetime.now(timezone.utc).timestamp() + freeze_minutes * 60
    try:
        await _redis().set(_BANWAVE_FREEZE_KEY, str(int(until_epoch)), ex=freeze_minutes * 60 + 60)
    except Exception as e:
        logger.warning(f"banwave freeze SET failed: {e}")
        return

    # ترمز سراسری کلاسیک را هم فعال می‌کنیم تا بعد از پایان فریز، ادامه با سرعت امن باشد
    try:
        from workers.sender import mark_global_slowdown
        await mark_global_slowdown(freeze_minutes * 60)
    except Exception:
        pass

    _spawn(_alert_admins(
        "🛑 <b>فریز موج بن فعال شد!</b>\n\n"
        f"در {window_minutes} دقیقه‌ی اخیر: <b>{ban_count}</b> بن واقعی و <b>{pf_count}</b> PeerFlood ثبت شده است.\n"
        f"کل دیسپچِ سفارش‌ها به‌مدت <b>{freeze_minutes} دقیقه</b> متوقف شد تا زنجیره‌ی بن قطع شود.\n\n"
        "🔍 <i>اقدام پیشنهادی فوری: پروکسی‌های قرمز‌شده در پیام‌های قبلی را بررسی/جایگزین کنید و "
        "قبل از رفع دستی فریز، علت موج (پروکسی سوخته / بچ شماره / محتوای پرچم‌شده) را رفع کنید.</i>\n"
        "<i>فریز خودکار منقضی می‌شود؛ نیازی به ری‌استارت نیست.</i>"
    ))


# ==========================================
# 🔥 لایه ۲: قرنطینه‌ی پروکسی سوخته
# ==========================================

async def _quarantine_burned_proxy(proxy_string: str, account_id: int, event_type: str, count: int) -> None:
    """
    قرنطینه‌ی پروکسی سوخته:
      - is_active=False + health_state='BURNED' در DB (ادمین در پنل می‌بیند)
      - cooldown بلند claim در Redis (حتی اگر ردیف DB جا افتاد، claim جدید نمی‌گیرد)
      - WorkerEvent برای ممیزی
      - هشدار ادمین با لیست اکانت‌های آسیب‌پذیر روی همان پروکسی
    نکته: اکانت‌های موجود روی پروکسی به‌طور خودکار جابه‌جا نمی‌شوند (sticky) —
    جابه‌جایی خودسرانه‌ی IP اکانتِ بالغ خودش ریسک است؛ ادمین تصمیم می‌گیرد.
    """
    quarantine_hours = max(1, _cfg("BAN_PROXY_QUARANTINE_HOURS", 72))
    now_utc = datetime.now(timezone.utc)

    affected = []  # [(account_id, phone_masked)]
    try:
        from database.engine import async_session
        from database.models import Account, Proxy, WorkerEvent
        from utils.crypto import mask_phone

        async with async_session() as session:
            proxy = await session.scalar(select(Proxy).where(Proxy.proxy_string == proxy_string))
            already_burned = bool(proxy and proxy.health_state == "BURNED")
            if proxy and not already_burned:
                proxy.is_active = False
                proxy.health_state = "BURNED"
                proxy.last_state_changed_at = now_utc
                await session.commit()

            rows = (await session.execute(
                select(Account.id, Account.phone_number).where(
                    Account.proxy_string == proxy_string,
                    Account.is_banned == False,  # noqa: E712
                )
            )).all()
            affected = [(r.id, mask_phone(r.phone_number)) for r in rows]

            session.add(WorkerEvent(
                account_id=account_id,
                old_status="PROXY_HEALTH",
                new_status="PROXY_BURNED",
                reason=f"{event_type} x{count} on proxy — quarantined {quarantine_hours}h",
                error_details=f"Proxy: {proxy_string}",
            ))
            await session.commit()
    except Exception as e:
        logger.warning(f"ban_wave quarantine DB update failed for {proxy_string}: {e}")

    # cooldown بلند claim (قرارداد موجود claim_proxy_for_account → proxy_cd:*)
    try:
        await _redis().set(f"proxy_cd:{proxy_string}", "1", ex=quarantine_hours * 3600)
    except Exception as e:
        logger.warning(f"ban_wave proxy claim-cooldown failed for {proxy_string}: {e}")

    affected_txt = ""
    if affected:
        lines = [f"▫️ user_{aid}/ (<code>{phone}</code>)" for aid, phone in affected[:10]]
        more = f"\n▫️ … و {len(affected) - 10} اکانت دیگر" if len(affected) > 10 else ""
        affected_txt = (
            f"\n\n⚠️ <b>{len(affected)} اکانت زنده دیگر هنوز روی همین پروکسی‌اند</b> "
            "(جابه‌جایی خودکار انجام نمی‌شود؛ در پنل، انتقال دستی یا جایگزینی پروکسی را انجام دهید):\n"
            + "\n".join(lines) + more
        )

    _spawn(_alert_admins(
        f"🔥 <b>پروکسی سوخته قرنطینه شد!</b>\n\n"
        f"پروکسی: <code>{_proxy_display(proxy_string)}</code>\n"
        f"علت: {count}× {event_type} روی همین پروکسی در ۲۴ ساعت اخیر.\n"
        f"اقدام: قرنطینه‌ی {quarantine_hours} ساعته — <b>هیچ اکانت جدیدی روی این پروکسی قرار نمی‌گیرد</b>."
        f"{affected_txt}"
    ))


# ==========================================
# 🧾 لایه ۱: ثبت رویداد بن + ارزیابی آستانه‌ها
# ==========================================

async def register_ban_event(
    account_id: int,
    proxy_string: Optional[str],
    order_id: Optional[int] = None,
    event_type: str = "ban",   # "ban" | "peer_flood"
) -> None:
    """
    نقطه‌ی ورود واحدِ همه‌ی بن‌ها/PeerFloodها/فریزها
    (از utils/limit_handler.py و utils/freeze_guard.py صدا زده می‌شود).

    کارها (تماماً best-effort / fail-open):
      ۱) شمارنده‌ی سراسری پنجره‌ی موج (ban / peer_flood / freeze جدا)
      ۲) شمارنده‌ی per-proxy (۲۴ ساعت)
      ۳) کلید فورنزیک last_ban_proxy:{account_id}
      ۴) WorkerEvent ممیزی با پروکسیِ لحظه‌ی رخداد
      ۵) cooldown claim کوتاه برای پروکسی (فاصله‌گذاری حتی زیر آستانه)
      ۶) ارزیابی آستانه‌ها → فریز سراسری / قرنطینه‌ی پروکسی
      ۷) هشدار ادمین با جزئیات پروکسی (فقط بن واقعی)
    """
    if account_id is None:
        return
    if event_type not in _VALID_EVENT_TYPES:
        event_type = "ban"

    window_minutes = max(5, _cfg("BAN_WAVE_WINDOW_MINUTES", 60))
    window_seconds = window_minutes * 60

    # --- ۱) شمارنده‌های سراسری پنجره‌ی موج (مسیر ساده و مقاوم) ---
    global_freeze_count = 0
    try:
        redis = _redis()
        global_key = {
            "peer_flood": _BANWAVE_GLOBAL_PF_KEY,
            "freeze": _BANWAVE_GLOBAL_FREEZE_KEY,
        }.get(event_type, _BANWAVE_GLOBAL_BAN_KEY)
        cur = await redis.incr(global_key)
        if cur == 1 or await redis.ttl(global_key) == -1:
            await redis.expire(global_key, window_seconds)
        g_bans = await redis.get(_BANWAVE_GLOBAL_BAN_KEY)
        g_pfs = await redis.get(_BANWAVE_GLOBAL_PF_KEY)
        g_frz = await redis.get(_BANWAVE_GLOBAL_FREEZE_KEY)
        global_ban_count = int(g_bans) if g_bans else 0
        global_pf_count = int(g_pfs) if g_pfs else 0
        global_freeze_count = int(g_frz) if g_frz else 0
    except Exception as e:
        logger.warning(f"ban_wave global counters failed (fail-open): {e}")
        global_ban_count, global_pf_count = 0, 0

    # فریزِ تلگرام همان‌قدر خطرناک است که بن — شمارنده‌ی بن را هم می‌شمارد
    # تا آستانه‌ی موج بن (فریز سراسری دیسپچ) زودتر فعال شود.
    if event_type == "freeze":
        global_ban_count += global_freeze_count

    proxy_ban_count = 0
    proxy_pf_count = 0

    if proxy_string:
        try:
            redis = _redis()
            # --- ۲) شمارنده‌های per-proxy (۲۴ ساعت) ---
            # فریز هم مثل بن، شمارنده‌ی proxy_bans را زیاد می‌کند (خوشه‌ی IP مشترک)
            p_key = _PROXY_PF_KEY.format(proxy=proxy_string) if event_type == "peer_flood" \
                else _PROXY_BAN_KEY.format(proxy=proxy_string)
            p_count = await redis.incr(p_key)
            if p_count == 1 or await redis.ttl(p_key) == -1:
                await redis.expire(p_key, 24 * 3600)

            pb = await redis.get(_PROXY_BAN_KEY.format(proxy=proxy_string))
            pp = await redis.get(_PROXY_PF_KEY.format(proxy=proxy_string))
            proxy_ban_count = int(pb) if pb else 0
            proxy_pf_count = int(pp) if pp else 0

            # --- ۳) فورنزیک: پروکسیِ لحظه‌ی بن ---
            await redis.set(
                _LAST_BAN_PROXY_KEY.format(account_id=account_id),
                str(proxy_string),
                ex=7 * 24 * 3600,
            )

            # --- ۵) فاصله‌گذاری claim حتی زیر آستانه (بن/فریز: ۲۴h، PeerFlood: ۶h) ---
            claim_cd_hours = _cfg("BAN_PROXY_CLAIM_COOLDOWN_HOURS", 24) \
                if event_type in ("ban", "freeze") else 6
            try:
                await redis.set(f"proxy_cd:{proxy_string}", "1", ex=claim_cd_hours * 3600)
            except Exception:
                pass
        except Exception as e:
            logger.warning(f"ban_wave proxy counters failed for {proxy_string} (fail-open): {e}")

    # --- ۴) WorkerEvent ممیزی ---
    try:
        from database.engine import async_session
        from database.models import WorkerEvent
        async with async_session() as session:
            session.add(WorkerEvent(
                account_id=account_id,
                old_status="SENDING",
                new_status=f"BANWAVE_{event_type.upper()}",
                reason=(
                    f"{event_type} recorded; proxy_bans={proxy_ban_count}, "
                    f"proxy_pf={proxy_pf_count}, global_bans={global_ban_count}, "
                    f"global_pf={global_pf_count}"
                ),
                error_details=(
                    f"Proxy: {proxy_string or 'DIRECT'}"
                    + (f" | Order: {order_id}" if order_id else "")
                ),
            ))
            await session.commit()
    except Exception as e:
        logger.debug(f"ban_wave WorkerEvent audit failed: {e}")

    # --- ۶) ارزیابی آستانه‌ها ---
    # ۶-الف) فریز سراسری موج بن
    #     آستانه‌ی فریز: موجِ FROZEN_METHOD_INVALID خودش (پیش‌درآمدِ بن) دیسپچ را
    #     متوقف می‌کند — BAN_WAVE_FROZEN_GLOBAL_THRESHOLD (پیش‌فرض ۳ در پنجره).
    try:
        global_ban_threshold = max(1, _cfg("BAN_WAVE_GLOBAL_THRESHOLD", 3))
        global_pf_threshold = max(1, _cfg("BAN_WAVE_GLOBAL_PF_THRESHOLD", 5))
        global_freeze_threshold = max(1, _cfg("BAN_WAVE_FROZEN_GLOBAL_THRESHOLD", 3))
        should_freeze = (
            (global_ban_count >= global_ban_threshold)
            or (global_pf_count >= global_pf_threshold)
            or (global_freeze_count >= global_freeze_threshold)
        )
        if should_freeze:
            await _trigger_global_freeze(global_ban_count, global_pf_count, window_minutes)
    except Exception as e:
        logger.warning(f"ban_wave freeze evaluation failed: {e}")

    # ۶-ب) قرنطینه‌ی پروکسی سوخته
    if proxy_string:
        try:
            proxy_ban_threshold = max(1, _cfg("BAN_WAVE_PROXY_BAN_THRESHOLD", 2))
            proxy_pf_threshold = max(1, _cfg("BAN_WAVE_PROXY_PF_THRESHOLD", 3))
            if (event_type == "ban" and proxy_ban_count >= proxy_ban_threshold) or \
               (event_type == "peer_flood" and proxy_pf_count >= proxy_pf_threshold):
                await _quarantine_burned_proxy(
                    proxy_string, account_id, event_type,
                    proxy_ban_count if event_type == "ban" else proxy_pf_count,
                )
        except Exception as e:
            logger.warning(f"ban_wave quarantine evaluation failed: {e}")

    # --- ۷) هشدار ادمین برای هر بن واقعی (فورنزیک پروکسی) ---
    if event_type == "ban":
        shared_count = None
        try:
            from database.engine import async_session
            from database.models import Account
            from sqlalchemy import func as sa_func
            async with async_session() as session:
                shared_count = await session.scalar(
                    select(sa_func.count()).select_from(Account).where(
                        Account.proxy_string == proxy_string,
                        Account.is_banned == False,  # noqa: E712
                    )
                )
        except Exception:
            shared_count = None

        shared_txt = ""
        if shared_count is not None and shared_count > 0:
            shared_txt = (
                f"\n\n⚠️ <b>{shared_count} اکانت دیگر هنوز روی همین پروکسی‌اند</b> — "
                "اگر بن‌ها ادامه یافت، پروکسی به‌طور خودکار قرنطینه می‌شود."
            )

        _spawn(_alert_admins(
            f"⛔️ <b>بن اکانت ثبت شد</b>\n\n"
            f"اکانت: <code>user_{account_id}/</code>"
            + (f"\nسفارش: <code>#{order_id}</code>" if order_id else "")
            + f"\nپروکسی لحظه‌ی بن: <code>{_proxy_display(proxy_string)}</code>"
            + f"\nبن روی این پروکسی (۲۴h): <b>{proxy_ban_count}</b>"
            + shared_txt
        ))


async def get_last_ban_proxy(account_id: int) -> Optional[str]:
    """
    فورنزیک برای ادمین: آخرین پروکسی‌ای که روی آن بن ثبت شده
    (کلید Redis `last_ban_proxy:{account_id}` با TTL هفت روز).
    """
    try:
        val = await _redis().get(_LAST_BAN_PROXY_KEY.format(account_id=account_id))
        return str(val) if val else None
    except Exception as e:
        logger.debug(f"get_last_ban_proxy failed: {e}")
        return None


async def get_banwave_snapshot() -> dict:
    """
    عکسِ لحظه‌ای وضعیت گارد (برای /status ها و پنل ادمین):
      freeze_active, freeze_remaining, global_bans, global_pfs
    """
    snap = {
        "freeze_active": await is_banwave_freeze_active(),
        "freeze_remaining": await banwave_freeze_remaining_seconds(),
        "global_bans": 0,
        "global_pfs": 0,
        "global_freezes": 0,
    }
    try:
        redis = _redis()
        b = await redis.get(_BANWAVE_GLOBAL_BAN_KEY)
        p = await redis.get(_BANWAVE_GLOBAL_PF_KEY)
        f = await redis.get(_BANWAVE_GLOBAL_FREEZE_KEY)
        snap["global_bans"] = int(b) if b else 0
        snap["global_pfs"] = int(p) if p else 0
        snap["global_freezes"] = int(f) if f else 0
    except Exception:
        pass
    return snap
