# utils/account_display.py

import logging
from datetime import datetime, timezone
from sqlalchemy import select
from database.models import Account, AccountStatus
from utils.timezone_helpers import to_tehran_time

logger = logging.getLogger(__name__)

def _as_utc(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def get_account_display_status(acc: Account, is_connected: bool, has_redis_cooldown: bool, now_utc: datetime) -> dict:
    """
    تابع مرجع: ترکیب تمام منابع وضعیت اکانت و تولید یک خروجی استاندارد و واحد برای همه‌ی نماها.
    """
    if not acc.session_string:
        return {"cat": "NOT_REG", "badge": "⛔️ ثبت‌نام نشده", "desc": "ثبت‌نام نشده"}
        
    if acc.is_banned:
        return {"cat": "BANNED", "badge": "🚫 مسدود شده", "desc": "مسدود شده (بن دائم)"}
        
    if acc.status == AccountStatus.blocked:
        return {"cat": "BLOCKED", "badge": "🚫 مسدود (سیستم)", "desc": "مسدود در دیتابیس (نیاز به بررسی)"}
        
    if acc.restricted_until and _as_utc(acc.restricted_until) > now_utc:
        t = to_tehran_time(acc.restricted_until, "%H:%M")
        return {"cat": "RESTRICTED", "badge": "🚫 محدود اسپم", "desc": f"محدودیت اسپم (تا ساعت {t})"}
        
    if acc.flood_wait_until and _as_utc(acc.flood_wait_until) > now_utc:
        t = to_tehran_time(acc.flood_wait_until, "%H:%M")
        return {"cat": "LIMITED", "badge": "❌ محدود (Flood)", "desc": f"محدود شده موقت (تا ساعت {t})"}
        
    if acc.status == AccountStatus.cooldown:
        if acc.expected_return_time and _as_utc(acc.expected_return_time) > now_utc:
            t = to_tehran_time(acc.expected_return_time, "%H:%M")
            return {"cat": "COOLDOWN_DB", "badge": "💤 استراحت (DB)", "desc": f"استراحت موقت (تا ساعت {t})"}
        return {"cat": "COOLDOWN_DB", "badge": "💤 استراحت (DB)", "desc": "استراحت برنامه‌ریزی‌شده"}
        
    if has_redis_cooldown:
        return {"cat": "COOLDOWN_REDIS", "badge": "💤 استراحت (Redis)", "desc": "استراحت موقت (محافظت اسپم)"}
        
    if acc.status == AccountStatus.disabled:
        return {"cat": "DISABLED", "badge": "⚪️ غیرفعال", "desc": "غیرفعال شده"}
        
    if not is_connected:
        return {"cat": "DISCONNECTED", "badge": "⚠️ قطع اتصال", "desc": "آفلاین (قطع از سرور تلگرام)"}
        
    return {"cat": "READY", "badge": "✅ آماده ارسال", "desc": "متصل و آماده ارسال"}

async def get_all_accounts_stats(session, redis_client, worker_pool) -> tuple[dict, dict]:
    """
    محاسبه‌ی یک‌جای تمام شمارنده‌ها. تضمین می‌کند جمع شمارنده‌ها دقیقاً برابر با کل اکانت‌ها باشد.
    """
    now_utc = datetime.now(timezone.utc)
    stmt = select(Account)
    accounts = (await session.scalars(stmt)).all()
    
    pipe = redis_client.pipeline()
    for acc in accounts:
        pipe.exists(f"chunk_cooldown:{acc.id}")
    redis_results = await pipe.execute()
    
    stats = {
        "TOTAL": len(accounts),
        "NOT_REG": 0,
        "BLOCKED": 0,    
        "COOLDOWN": 0,
        "DISABLED": 0,
        "DISCONNECTED": 0,
        "READY": 0
    }
    
    account_categories = {}
    for idx, acc in enumerate(accounts):
        is_conn = acc.id in worker_pool and getattr(worker_pool[acc.id], "is_connected", False)
        has_redis = bool(redis_results[idx])
        disp = get_account_display_status(acc, is_conn, has_redis, now_utc)
        
        cat = disp["cat"]
        # یکپارچه‌سازی برای نماهای کلی داشبورد
        if cat in ("BANNED", "BLOCKED", "RESTRICTED", "LIMITED"):
            cat = "BLOCKED"
        elif cat in ("COOLDOWN_DB", "COOLDOWN_REDIS"):
            cat = "COOLDOWN"
            
        stats[cat] = stats.get(cat, 0) + 1
        account_categories[acc.id] = cat
        
    return stats, account_categories