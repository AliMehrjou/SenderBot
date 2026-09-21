import asyncio
import logging
from datetime import datetime, timezone
from pyrogram import Client
from sqlalchemy import update, select
from sqlalchemy.ext.asyncio import AsyncSession

from database.models import Account, GlobalSettings
from utils.error_aggregator import report_admin_error
from utils.anti_ban import apply_adaptive_flood_wait
from workers.sender import mark_global_slowdown

logger = logging.getLogger(__name__)

LIMIT_FLOOD_WAIT = "flood_wait"

async def register_account_limit(
    session: AsyncSession,
    account_id: int,
    client: Client,
    limit_type: str,
    wait_seconds: int = 0,
    is_banned: bool = False
) -> None:
    """
    مدیریت متمرکز لیمیت‌های تلگرام: ثبت استراحت در دیتابیس، کاهش بار سراسری، 
    ارسال گزارش تجمیعی، و اجرای خودکار ربات اسپم.
    🟢 تغییرات: استقلال سشن جهت جلوگیری از تداخل تراکنش با حلقه ارسال دیسپچر.
    """
    from database.engine import async_session  # وارد کردن در سطح تابع برای اطمینان
    account_tag = f"user_{account_id}/"
    error_msg = ""
    
    # --- گرفتن تنظیمات جهانی با سشن مستقل ---
    try:
        async with async_session() as indep_session:
            gs = await indep_session.scalar(select(GlobalSettings).limit(1))
            smart_anti_ban_enabled = getattr(gs, 'smart_anti_ban', True) if gs else True
            val = getattr(gs, 'spam_penalty_days', None) if gs else None
            user_penalty_days = max(int(val) if val is not None else 3, 1) # حداقل ۱ روز گارانتی می‌شود
    except Exception as e:
        logger.error(f"Failed to fetch GlobalSettings in limit_handler: {e}")
        smart_anti_ban_enabled = True
        user_penalty_days = 3

    # 🛡 فاز ۹: Circuit Breaker - ایمن‌سازی TTLها (آیتم ۶)
    if smart_anti_ban_enabled:
        from workers.sender import _get_redis
        try:
            redis = _get_redis()
            current_speed = await redis.get("settings:speed_mode")
            current_speed = current_speed.decode("utf-8") if isinstance(current_speed, bytes) else (current_speed or "safe")

            if current_speed != "safe":
                if is_banned:
                    bans = await redis.incr("cb:bans")
                    if bans == 1 or await redis.ttl("cb:bans") == -1: 
                        await redis.expire("cb:bans", 600) # 10 دقیقه
                    if bans >= 2:
                        await redis.set("settings:speed_mode", "safe", ex=30)
                        await report_admin_error("⚠️ <b>مدار قطع‌کن (Circuit Breaker) فعال شد!</b>\n۲ بن واقعی در ۱۰ دقیقه تشخیص داده شد. سرعت سیستم به‌طور خودکار به حالت `safe` (ایمن) کاهش یافت.")
                
                # استفاده از نام متغیر ثابت LIMIT_FLOOD_WAIT یا رشته اصلی
                if limit_type in ("flood_wait", getattr(globals(), "LIMIT_FLOOD_WAIT", "flood_wait")):
                    consec = await redis.incr(f"cb:consec_fw:{account_id}")
                    if consec == 1 or await redis.ttl(f"cb:consec_fw:{account_id}") == -1: 
                        await redis.expire(f"cb:consec_fw:{account_id}", 3600)
                    if consec >= 3:
                        await redis.set("settings:speed_mode", "safe", ex=30)
                        logger.warning(f"Circuit Breaker: Worker {account_id} hit 3 consec FloodWaits. System dropped to safe.")
                    
                    if wait_seconds > 300:
                        fw300 = await redis.incr("cb:fw_300")
                        if fw300 == 1 or await redis.ttl("cb:fw_300") == -1: 
                            await redis.expire("cb:fw_300", 3600)
                        if fw300 >= 2:
                            await redis.set("settings:speed_mode", "safe", ex=30)
                            await report_admin_error("⚠️ <b>مدار قطع‌کن فعال شد!</b>\n۲ توقف FloodWait بیش از ۳۰۰ ثانیه در ۱ ساعت. سرعت به‌طور خودکار به حالت `safe` (ایمن) کاهش یافت.")
        except Exception as cb_err:
            logger.error(f"Circuit breaker failed to evaluate: {cb_err}")
    else:
        logger.info(f"Smart Anti-Ban is DISABLED. Circuit breaker bypassed for {account_tag}.")

    # ۱. بخش حیاتی: ثبت جریمه اصلی (استفاده از سشن مستقل - آیتم ۵)
    try:
        async with async_session() as indep_session:
            if is_banned or limit_type == "banned":
                await indep_session.execute(
                    update(Account).where(Account.id == account_id).values(is_banned=True)
                )
                error_msg = f"⛔️ <b>بن اکانت!</b>\nاکانت <code>{account_tag}</code> بن یا دی‌اکتیو شده است."

            elif limit_type in ("flood_wait", getattr(globals(), "LIMIT_FLOOD_WAIT", "flood_wait")):
                await apply_adaptive_flood_wait(indep_session, account_id, wait_seconds)
                error_msg = f"⏳ <b>توقف FloodWait</b>\nاکانت <code>{account_tag}</code> به مدت {wait_seconds} ثانیه محدود شد."

            elif limit_type == "peer_flood":
                penalty_seconds = user_penalty_days * 86400
                await apply_adaptive_flood_wait(indep_session, account_id, penalty_seconds)
                
                if smart_anti_ban_enabled:
                    await mark_global_slowdown(300)
                    error_msg = f"🚫 <b>محدودیت اسپم (PeerFlood)</b>\nاکانت <code>{account_tag}</code> اسپم شد! استراحت: {user_penalty_days} روز.\nدر حال بررسی با @spambot..."
                else:
                    error_msg = f"🚫 <b>محدودیت اسپم (PeerFlood)</b>\nاکانت <code>{account_tag}</code> اسپم شد! استراحت: {user_penalty_days} روز (ترمز سراسری اعمال نشد)."

            # کامیت قطعی صرفاً روی سشن مستقل
            await indep_session.commit()
    except Exception as e:
        logger.error(f"Critical Error in register_account_limit for {account_tag}: {e}")
        return  # بدون آسیب زدن به سشن اصلی دیسپچر، خارج می‌شویم

    # ۲. بخش متادیتا: ثبت last_limit_type در تراکنش مستقل مجزا
    try:
        async with async_session() as indep_session:
            limit_val = "banned" if is_banned else limit_type
            await indep_session.execute(
                update(Account).where(Account.id == account_id).values(last_limit_type=limit_val)
            )
            await indep_session.commit()
    except Exception as e:
        logger.warning(f"Metadata update failed in register_account_limit for {account_tag}: {e}")

    # ۳. تریگر چک وضعیت اسپم بات در بک‌گراند و ارسال گزارش
    if limit_type == "peer_flood" and client:
        asyncio.create_task(_run_spambot_check(client, account_id))

    if error_msg:
        await report_admin_error(error_msg)

async def _run_spambot_check(client: Client, account_id: int):
    """اجرای چک اسپم‌بات و ذخیره نتیجه در دیتابیس با مدیریت مجزای تراکنش."""
    from database.engine import async_session
    from utils.advanced_anti_ban import check_spambot_status
    
    status = await check_spambot_status(client, account_id)
    if status:
        # آپدیت متادیتای اسپم بات با try/except محافظت‌شده
        try:
            async with async_session() as session:
                update_vals = {
                    "spambot_report": status["text"],
                    "spambot_checked_at": datetime.now(timezone.utc)
                }
                
                if status["until"]:
                    update_vals["restricted_until"] = status["until"]
                    # 🛡 همگام‌سازی: اعمال محدودیت زمانی اسپم‌بات روی flood_wait_until تا دیسپچر متوقف بماند
                    update_vals["flood_wait_until"] = status["until"]
                elif not status.get("restricted", False):
                    update_vals["restricted_until"] = None
                    update_vals["flood_wait_until"] = None
                    update_vals["last_limit_type"] = None
                    update_vals["status"] = "active"
                    update_vals["status_reason"] = "SpamBot clear"
                    
                    old_status_val = await session.scalar(select(Account.status).where(Account.id == account_id))
                    old_status = old_status_val.value if hasattr(old_status_val, 'value') else str(old_status_val) if old_status_val else "blocked"
                    
                    from database.models import WorkerEvent
                    session.add(WorkerEvent(
                        account_id=account_id, old_status=old_status, new_status="active", reason="spambot-clear"
                    ))
                    
                await session.execute(
                    update(Account).where(Account.id == account_id).values(**update_vals)
                )
                await session.commit()
        except Exception as e:
            logger.warning(f"Failed to update spambot metadata for user_{account_id}/: {e}")

        try:
            report_txt = (
                f"🤖 <b>نتیجه SpamBot برای <code>user_{account_id}/</code>:</b>\n"
                f"وضعیت محدودیت: {'فعال 🔴' if status['restricted'] else 'آزاد 🟢'}\n"
                f"متن پیام:\n<pre>{status['text'][:150]}...</pre>"
            )
            await report_admin_error(report_txt)
        except Exception as e:
            logger.error(f"Failed to send SpamBot report for user_{account_id}/: {e}")