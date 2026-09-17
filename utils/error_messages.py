# -*- coding: utf-8 -*-
"""
🛡 مدیریت خطا — فاز ۱ از ۵
==========================
utils/error_messages.py

منبع واحد (Single Source of Truth) پیام‌های خطای کاربرپسند ربات.
هدف: رفع «مشکل ۱» (پیام‌های عمومی «خطای دیتابیس») + پیام‌های استاندارد
برای رفع مشکلات ۲ و ۴ در فازهای بعد.

قواعد طلایی این ماژول:
    ۱. هیچ‌گاه str(e) یا جزئیات فنی (متن کوئری، مسیر فایل، توکن و...) به
       کاربر نمایش داده نمی‌شود — جزئیات فقط در logger ثبت می‌شود.
    ۲. هر پیام خطا باید به دو سؤال جواب بدهد:
        الف) چه چیزی fail شد؟
        ب) کاربر الان چه کاری باید بکند؟ (تلاش مجدد / تغییر ورودی / صبر)
    ۳. همه پیام‌ها فارسی هستند.

قرارداد پارامتر action:
    نام «موجودیت» به فارسی — نه جملهٔ کامل عملیات — تا در همه قالب‌ها طبیعی بخواند:
        ✅ «دسته‌بندی»، «API»، «سفارش»، «اکانت»، «ادمین»، «تنظیمات»
        ❌ «افزودن دسته‌بندی» (این یک جمله است، نه موجودیت)
"""

import logging
from typing import Optional

from sqlalchemy.exc import IntegrityError, InterfaceError, OperationalError

logger = logging.getLogger(__name__)


# ==========================================================
# 🛡 فاز ۱ — دسته‌بندی خطاهای دیتابیس (تشخیص دو لایه‌ای)
# ==========================================================

# الگوهای متنی رایج در پیام خطای درایورهای دیتابیس (SQLite / Postgres / ...)
_DUP_PATTERNS = ("unique", "duplicate", "already exists", "primary key")
_FK_PATTERNS = ("foreign key", "still referenced", "violates foreign key")
_CONN_PATTERNS = (
    "connection", "timeout", "timed out", "could not connect",
    "connection refused", "server closed", "network is unreachable",
    "reset by peer", "broken pipe", "too many connections",
)
_LOCK_PATTERNS = ("database is locked", "database table is locked", "deadlock")


def _classify_db_error(error: Exception) -> str:
    """
    تشخیص دستهٔ خطای دیتابیس.

    خروجی یکی از این کلیدهاست:
        duplicate | foreign_key | connection | locked | unknown

    تشخیص دو لایه دارد:
        لایه ۱) نوع استثنای SQLAlchemy (مطمئن‌ترین راه)
        لایه ۲) الگوهای متنی پیام خطا (برای استثناهای خامِ درایور)
    """
    # متن خطای درایور (orig) معمولاً دقیق‌تر و کوتاه‌تر از کل استثناست
    orig = getattr(error, "orig", None)
    error_str = (str(orig) or str(error)).lower()

    # ۱) قفل / بن‌بست — قبل از بقیه؛ چون در قالب OperationalError می‌آید
    if any(p in error_str for p in _LOCK_PATTERNS):
        return "locked"

    # ۲) قطع اتصال / تایم‌اوت — قبل از بقیه؛ چون وقتی دیتابیس در دسترس نیست،
    #    دسته‌بندی‌های دیگر گمراه‌کننده‌اند
    if isinstance(error, (OperationalError, InterfaceError)):
        return "connection"
    if any(p in error_str for p in _CONN_PATTERNS):
        return "connection"

    # ۳) وابستگی کلید خارجی — قبل از «یکتایی»؛ چون هر دو IntegrityError هستند
    if any(p in error_str for p in _FK_PATTERNS):
        return "foreign_key"

    # ۴) تکراری بودن رکورد (محدودیت یکتایی)
    if isinstance(error, IntegrityError) or any(p in error_str for p in _DUP_PATTERNS):
        return "duplicate"

    return "unknown"


# ==========================================================
# 🛡 فاز ۱ — قالب پیام‌های خطای دیتابیس (همه فارسی)
# ==========================================================

_DB_ERROR_MESSAGES = {
    "duplicate": (
        "⚠️ <b>رکورد تکراری!</b>\n"
        "این {action} پیش از این در سیستم ثبت شده است. لطفاً مقدار دیگری وارد کنید."
    ),
    "foreign_key": (
        "🔗 <b>خطای وابستگی داده‌ها</b>\n"
        "این {action} به بخش‌های دیگری از سیستم متصل است و قابل حذف/ویرایش نیست.\n"
        "💡 <i>راهنمایی: لطفاً ابتدا موارد مرتبط (مثل اکانت‌ها یا سفارش‌های متصل به آن) را حذف کنید.</i>"
    ),
    "connection": (
        "🔌 <b>ارتباط با سرور قطع شد</b>\n"
        "در حال حاضر ارتباط با پایگاه داده برقرار نیست. لطفاً چند لحظه دیگر مجدداً تلاش کنید."
    ),
    "locked": (
        "⏳ <b>ترافیک بالای سیستم</b>\n"
        "دیتابیس در حال پردازش دستورات قبلی است. لطفاً چند ثانیه صبر کرده و دوباره امتحان کنید."
    ),
    "unknown": (
        "❌ <b>خطای ناشناخته</b>\n"
        "عملیات روی {action} با مشکل مواجه شد. لطفاً دوباره تلاش کنید یا کد خطای زیر را به پشتیبانی ارسال نمایید."
    ),
}


def get_user_friendly_db_error(action: str, error: Exception) -> str:
    """
    تولید پیام خطای کاربرپسند بر اساس نوع خطای دیتابیس (رفع مشکل ۱).

    Args:
        action: نام موجودیت به فارسی — مثلاً «دسته‌بندی»، «API»، «سفارش».
        error:  استثنای دریافتی (ترجیحاً استثنای SQLAlchemy).

    Returns:
        پیام فارسی آمادهٔ ارسال به کاربر — بدون هیچ جزئیات فنی.
    """
    category = _classify_db_error(error)
    return _DB_ERROR_MESSAGES[category].format(action=action)


def report_db_error(action: str, error: Exception, log: Optional[logging.Logger] = None) -> str:
    """
    ثبت کامل خطای دیتابیس در لاگ (با stack trace) + بازگرداندن پیام کاربرپسند با کد رهگیری.
    """
    import uuid
    err_code = f"ERR-{uuid.uuid4().hex[:8].upper()}"
    category = _classify_db_error(error)
    
    (log or logger).error(
        "Database error [%s] in '%s' [category=%s]: %s",
        err_code, action, category, error,
        exc_info=error,
    )
    
    base_msg = get_user_friendly_db_error(action, error)
    return f"{base_msg}\n\nکد خطا: <code>{err_code}</code>\nلطفاً این کد را به پشتیبانی گزارش دهید."


# ==========================================================
# 🛡 فاز ۱ — پیام‌های خطای عمومی (غیر دیتابیس)
# ==========================================================

def get_generic_error_message() -> str:
    """
    پیام استاندارد «خطای پیش‌بینی نشده» — جایگزین امنِ نمایش str(e) (رفع مشکل ۲).
    برای خطاهایی که علت مشخص و قابل ارائه به کاربر ندارند.
    """
    return (
        "🛠 <b>بروز خطای سیستمی</b>\n"
        "متأسفانه خطای پیش‌بینی نشده‌ای رخ داد. سیستم در حال بررسی است.\n\n"
        "💡 <i>برای خروج از این وضعیت، لطفاً دستور /cancel را ارسال کنید و یا به منوی اصلی برگردید.</i>"
    )


def get_telegram_api_error_message() -> str:
    """خطای ارتباط با سرورهای تلگرام (خطاهای RPC در pyrogram)."""
    return (
        "🌐 <b>اختلال در API تلگرام</b>\n"
        "ارتباط سرور ما با تلگرام موقتاً دچار اختلال شده است (ممکن است به دلیل فیلترینگ یا قطعی خود تلگرام باشد).\n"
        "لطفاً چند دقیقه دیگر دوباره تلاش کنید."
    )


def get_floodwait_message(seconds) -> str:
    """تأخیر اجباری تلگرام (FloodWait) — استفاده در فاز ۳."""
    try:
        seconds = int(seconds)
    except (TypeError, ValueError):
        seconds = "چند"
    return (
        "⏳ <b>محدودیت موقت تلگرام (FloodWait)</b>\n"
        f"تلگرام برای جلوگیری از اسپم، ربات/اکانت را موقتاً محدود کرده است. لطفاً حدود <b>{seconds} ثانیه</b> استراحت کنید و سپس مجدداً تلاش نمایید."
    )


def get_download_error_message() -> str:
    """خطای دانلود فایل از سرور تلگرام (رفع مشکل ۴)."""
    return (
        "📥 <b>خطا در دریافت فایل</b>\n"
        "متأسفانه دانلود فایل از سرور تلگرام با مشکل مواجه شد. لطفاً دوباره تلاش کنید یا فایل دیگری ارسال نمایید."
    )


def get_file_too_large_message(limit_mb: int = 20) -> str:
    """حجم فایل بیش از حد مجاز است (رفع مشکل ۴)."""
    return (
        "📦 <b>حجم فایل بیش از حد مجاز</b>\n"
        f"حجم فایل ارسال شده بیشتر از مقدار مجاز (<b>{limit_mb} مگابایت</b>) است. لطفاً فایل کوچکتری ارسال کنید."
    )
