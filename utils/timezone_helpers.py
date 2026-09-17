# utils/timezone_helpers.py

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

def to_tehran_time(utc_dt: datetime, format_str: str = "%Y-%m-%d %H:%M:%S") -> str:
    """
    تبدیل آبجکت datetime (از نوع UTC که معمولاً در دیتابیس ذخیره می‌شود) 
    به زمان محلی ایران (Asia/Tehran) همراه با فرمت‌دهی دلخواه.
    
    مثال استفاده:
    tehran_time_str = to_tehran_time(account.warmed_up_at)
    """
    if utc_dt is None:
        return "نامشخص"
        
    # اگر datetime تگ timezone نداشت، فرض می‌کنیم UTC است
    if utc_dt.tzinfo is None:
        utc_dt = utc_dt.replace(tzinfo=timezone.utc)
        
    # تبدیل به تایم‌زون تهران
    tehran_dt = utc_dt.astimezone(ZoneInfo("Asia/Tehran"))
    
    # خروجی به فرمت رشته‌ای خوانا
    return tehran_dt.strftime(format_str)

def get_current_tehran_time() -> datetime:
    """دریافت زمان و تاریخ دقیق همین لحظه به وقت تهران"""
    return datetime.now(ZoneInfo("Asia/Tehran"))