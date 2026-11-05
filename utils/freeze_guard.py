# -*- coding: utf-8 -*-
"""
🧊 FREEZE GUARD (گارد فریزِ اکانت — FROZEN_METHOD_INVALID)
==========================================================

چرا این ماژول وجود دارد؟
------------------------
در لاگ‌های bot_18 و bot_19، ریشه‌ی اصلی بن/فریز دسته‌ای اکانت‌ها خطای
`[420 FROZEN_METHOD_INVALID]` بود که در «هیچ جای پروژه» هندل نمی‌شد:

  • تلگرام اکانتِ مشکوک (معمولاً تازه/بای) را «فریز» می‌کند: اتصال برقرار
    می‌ماند، ولی متدهای حساس (JoinChannel، UploadProfilePhoto،
    DeleteProfilePhotos، SendMessage به غیرمخاطب و…) با FROZEN_METHOD_INVALID
    رد می‌شوند.
  • کد قبلی این خطا را مثل هر Exception دیگری «بی‌صدا» قورت می‌داد و همان
    عملیات را بی‌وقفه تکرار می‌کرد ← حلقه‌ی بی‌نهایت photos.GetUserPhotos
    (هر ~۱۱ ثانیه، ۲۴/۷، برای ساعت‌ها؛ ۱۱٬۰۹۰ فراخوانی فقط در یک لاگ!).
    همین چکش‌کاریِ مداوم، خودش قوی‌ترین سیگنال اسپم برای تلگرام است و
    فریز را به بن دائم تبدیل می‌کند.
  • گارد موج بن (utils/ban_wave.py) هم فقط UserDeactivatedBan / PeerFloodِ
    «مسیر ارسال» را می‌شمرد؛ فریز هرگز به آن گزارش نمی‌شد ← «اسکریپت
    BAN WAVE اثر نکرده» چون کور بود نسبت به این نوع رخداد.

این ماژول نقطه‌ی واحد تشخیص و واکنش است:

  1) is_frozen_error(exc)      → تشخیص رشته‌ی خطا (pyrofork آن را با کلاس
                                  عمومی Flood/420 می‌آورد، نه کلاس اختصاصی)
  2) mark_account_frozen(...)  → کول‌داون بلند قابل‌تنظیم + پرچم Redis +
                                  WorkerEvent + هشدار throttle‌شده‌ی ادمین +
                                  ثبت رویداد در گارد موج بن (event_type=freeze)
  3) is_account_frozen(id)     → چک ارزان Redis قبل از هر فعالیت غیرضروری
                                  (چرخش عکس، جوین کانال، warm-up و…)

همه‌ی مسیرها fail-open هستند: خطای Redis/DB هرگز جریان اصلی را نمی‌شکند.
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

logger = logging.getLogger(__name__)

# پرچم Redis: frozen:{account_id} → "1" با TTL = طول کول‌داون
_FROZEN_KEY = "frozen:{account_id}"
# هشدار ادمین هر اکانت حداکثر یک‌بار در این بازه (ثانیه) — جلوگیری از اسپم پیام
_FROZEN_ALERT_THROTTLE_KEY = "frozen_alert:{account_id}"
_FROZEN_ALERT_THROTTLE_SECONDS = 3600


def _cfg_float(name: str, default: float) -> float:
    from config import config
    try:
        return float(getattr(config, name, default))
    except (TypeError, ValueError, AttributeError):
        return default


def _frozen_cooldown_hours() -> float:
    """طول کول‌داون فریز (ساعت) — کلید FROZEN_COOLDOWN_HOURS، پیش‌فرض ۲۴."""
    return max(1.0, _cfg_float("FROZEN_COOLDOWN_HOURS", 24.0))


def is_frozen_error(exc: BaseException) -> bool:
    """
    آیا این استثنا خطای FROZEN_METHOD_INVALID تلگرام است؟

    pyrofork خطای 420 بدون FloodWait را با کلاس عمومی «Flood» می‌آورد:
      Flood: Telegram says: [420 Flood] - [420 FROZEN_METHOD_INVALID] (caused by "...")
    بنابراین تشخیصِ مطمئن، تطبیق رشته‌ایِ «FROZEN_METHOD_INVALID» است.
    """
    try:
        return "FROZEN_METHOD_INVALID" in str(exc).upper()
    except Exception:
        return False


async def is_account_frozen(account_id: Optional[int]) -> bool:
    """
    آیا این اکانت الان در وضعیت فریز/کول‌داونِ فریز است؟ (چک ارزان Redis)

    fail-open: اگر Redis در دسترس نبود False برمی‌گردد (جریان ادامه می‌یابد)؛
    لایه‌های دیگر (وضعیت cooldown در DB) همچنان محافظت می‌کنند.
    """
    if account_id is None:
        return False
    try:
        from workers.sender import _get_redis
        val = await _get_redis().get(_FROZEN_KEY.format(account_id=account_id))
        return bool(val)
    except Exception as e:
        logger.debug(f"is_account_frozen({account_id}) check failed (fail-open): {e}")
        return False


async def mark_account_frozen(
    account_id: int,
    proxy_string: Optional[str] = None,
    reason: str = "",
    order_id: Optional[int] = None,
) -> None:
    """
    اکانت را در وضعیت «فریز» می‌گذارد:

      ۱) پرچم Redis با TTL = FROZEN_COOLDOWN_HOURS (اسکیپ فعالیت‌های غیرضروری)
      ۲) DB: status=cooldown + expected_return_time + last_limit_type=frozen
         (+ flood_wait_until تا preflight دیسپچ هم متوقفش کند)
      ۳) WorkerEvent ممیزی
      ۴) ثبت رویداد freeze در گارد موج بن ← آستانه‌ی «موج فریز» فریز سراسری
         دیسپچ را فعال می‌کند (فریزِ دسته‌ای = پیش‌درآمد بنِ دسته‌ای)
      ۵) هشدار ادمین (throttle‌شده — هر اکانت حداکثر ۱ پیام در ساعت)

    بن دائم نیست: بعد از پایان کول‌داون، حلقه‌ی Reconnect (بخش ۰
    health_checker) اکانت را خودکار به چرخه برمی‌گرداند.
    """
    if account_id is None:
        return

    cooldown_hours = _frozen_cooldown_hours()
    now_utc = datetime.now(timezone.utc)
    return_time = now_utc + timedelta(hours=cooldown_hours)

    # --- ۱) پرچم Redis ---
    try:
        from workers.sender import _get_redis
        await _get_redis().set(
            _FROZEN_KEY.format(account_id=account_id), "1",
            ex=int(cooldown_hours * 3600),
        )
    except Exception as e:
        logger.warning(f"freeze_guard: redis flag failed for user_{account_id}/ (fail-open): {e}")

    # --- ۲) وضعیت DB (تراکنش مستقل) ---
    try:
        from database.engine import async_session
        from database.models import Account, AccountStatus, WorkerEvent
        from sqlalchemy import update, select

        async with async_session() as session:
            old_status_val = await session.scalar(
                select(Account.status).where(Account.id == account_id)
            )
            old_status = (
                old_status_val.value if hasattr(old_status_val, "value") else str(old_status_val)
                if old_status_val else "active"
            )
            await session.execute(
                update(Account).where(Account.id == account_id).values(
                    status=AccountStatus.cooldown.value,
                    status_reason=f"Frozen by Telegram (FROZEN_METHOD_INVALID) — {int(cooldown_hours)}h cooldown",
                    expected_return_time=return_time,
                    flood_wait_until=return_time,
                    last_limit_type="frozen",
                    consecutive_errors=0,
                )
            )
            session.add(WorkerEvent(
                account_id=account_id,
                old_status=old_status,
                new_status=AccountStatus.cooldown.value,
                reason="telegram-freeze",
                error_details=(reason or "FROZEN_METHOD_INVALID")[:500],
            ))
            await session.commit()
    except Exception as e:
        logger.warning(f"freeze_guard: DB cooldown failed for user_{account_id}/ (fail-open): {e}")

    # --- ۴) ثبت در گارد موج بن (event_type=freeze) ---
    try:
        from utils import ban_wave
        await ban_wave.register_ban_event(
            account_id=account_id,
            proxy_string=proxy_string,
            order_id=order_id,
            event_type="freeze",
        )
    except Exception as e:
        logger.warning(f"freeze_guard: ban_wave register failed for user_{account_id}/ (fail-open): {e}")

    # --- ۵) هشدار ادمین (throttle‌شده) ---
    try:
        from workers.sender import _get_redis
        redis = _get_redis()
        alert_key = _FROZEN_ALERT_THROTTLE_KEY.format(account_id=account_id)
        if not await redis.set(alert_key, "1", nx=True, ex=_FROZEN_ALERT_THROTTLE_SECONDS):
            return  # همین اخیراً هشدار داده شده
    except Exception:
        pass  # throttle best-effort — پیام باز هم ارسال می‌شود

    try:
        from utils.admin_broadcast import broadcast_to_admins
        from utils.ban_wave import _admin_bot
        bot = _admin_bot
        if bot is not None:
            await broadcast_to_admins(
                bot,
                text=(
                    f"🧊 <b>اکانت توسط تلگرام فریز شد</b>\n\n"
                    f"اکانت: <code>user_{account_id}/</code>\n"
                    f"علت: FROZEN_METHOD_INVALID (اکنتِ فریز‌شده متدهای حساس را رد می‌کند)\n"
                    + (f"جزئیات: <code>{reason[:150]}</code>\n" if reason else "")
                    + f"اقدام: {int(cooldown_hours)} ساعت استراحت کامل (بدون هیچ فراخوانی غیرضروری)؛ "
                    "بعد از آن خودکار به چرخه برمی‌گردد."
                ),
            )
    except Exception as e:
        logger.debug(f"freeze_guard: admin alert failed for user_{account_id}/: {e}")

    logger.warning(
        f"freeze_guard: user_{account_id}/ marked FROZEN for {int(cooldown_hours)}h"
        + (f" — {reason[:120]}" if reason else "")
    )


async def handle_frozen_exception(
    exc: BaseException,
    account_id: Optional[int],
    proxy_string: Optional[str] = None,
    context: str = "",
) -> bool:
    """
    هلپر یک‌خطی برای بلوک‌های except:
    اگر استثنا خطای فریز بود، اکانت را mark می‌کند و True برمی‌گرداند
    (فراخواننده باید عملیات را همان‌جا قطع کند). در غیر این صورت False.
    """
    if not is_frozen_error(exc):
        return False
    if account_id is not None:
        await mark_account_frozen(account_id, proxy_string=proxy_string, reason=f"{context}: {exc}"[:400])
    else:
        logger.warning(f"freeze_guard: frozen error without account_id — {context}: {exc}")
    return True
