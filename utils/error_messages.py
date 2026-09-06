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

طرح استفاده در هندلرها (از فاز ۲ اعمال می‌شود):

    except Exception as e:
        await session.rollback()
        await message.answer(report_db_error("دسته‌بندی", e))
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
        "❌ این {action} قبلاً ثبت شده است.\n"
        "لطفاً یک مورد دیگر را امتحان کنید."
    ),
    "foreign_key": (
        "❌ {action} به داده‌های دیگری وابسته است و این عملیات روی آن انجام نمی‌شود.\n"
        "لطفاً ابتدا موارد مرتبط (مثل اکانت‌ها یا سفارش‌های متصل) را حذف کنید."
    ),
    "connection": (
        "❌ خطای اتصال به دیتابیس.\n"
        "لطفاً چند لحظه دیگر دوباره تلاش کنید."
    ),
    "locked": (
        "❌ دیتابیس در حال حاضر مشغول است.\n"
        "لطفاً چند لحظه دیگر دوباره تلاش کنید."
    ),
    "unknown": (
        "❌ عملیات روی {action} با خطا مواجه شد.\n"
        "لطفاً دوباره تلاش کنید. اگر مشکل ادامه یافت، با پشتیبانی تماس بگیرید."
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
    ثبت کامل خطای دیتابیس در لاگ (با stack trace) + بازگرداندن پیام کاربرپسند.

    فقط برای حذف تکرارِ این دو خط از همهٔ هندلرهاست:

        logger.error(f"DB error in {action}: {e}", exc_info=True)
        user_msg = get_user_friendly_db_error(action, e)
    """
    (log or logger).error(
        "Database error in '%s' [category=%s]: %s",
        action, _classify_db_error(error), error,
        exc_info=error,
    )
    return get_user_friendly_db_error(action, error)


# ==========================================================
# 🛡 فاز ۱ — پیام‌های خطای عمومی (غیر دیتابیس)
# ==========================================================

def get_generic_error_message() -> str:
    """
    پیام استاندارد «خطای پیش‌بینی نشده» — جایگزین امنِ نمایش str(e) (رفع مشکل ۲).
    برای خطاهایی که علت مشخص و قابل ارائه به کاربر ندارند.
    """
    return (
        "❌ خطای پیش‌بینی نشده‌ای رخ داد.\n"
        "لطفاً دوباره تلاش کنید.\n"
        "اگر مشکل ادامه یافت، /cancel را ارسال کنید."
    )


def get_telegram_api_error_message() -> str:
    """خطای ارتباط با سرورهای تلگرام (خطاهای RPC در pyrogram)."""
    return (
        "❌ خطا در ارتباط با سرورهای تلگرام.\n"
        "لطفاً چند لحظه دیگر دوباره تلاش کنید."
    )


def get_floodwait_message(seconds) -> str:
    """تأخیر اجباری تلگرام (FloodWait) — استفاده در فاز ۳."""
    try:
        seconds = int(seconds)
    except (TypeError, ValueError):
        seconds = "چند"
    return f"⏳ تلگرام این درخواست را موقتاً محدود کرده است. لطفاً حدود {seconds} ثانیه صبر کنید."


def get_download_error_message() -> str:
    """خطای دانلود فایل از سرور تلگرام (رفع مشکل ۴)."""
    return (
        "❌ خطا در دانلود فایل.\n"
        "لطفاً دوباره تلاش کنید یا فایل دیگری ارسال کنید."
    )


def get_file_too_large_message(limit_mb: int = 20) -> str:
    """حجم فایل بیش از حد مجاز است (رفع مشکل ۴)."""
    return (
        "⚠️ فایل بیش از حد بزرگ است.\n"
        f"حداکثر حجم مجاز: {limit_mb} مگابایت."
    )


# ==========================================================
# اجرای مستقیم فایل = تست دستی خروجی پیام‌ها (بدون نیاز به ربات):
#     python -m utils.error_messages
# ==========================================================
if __name__ == "__main__":
    from sqlalchemy.exc import IntegrityError as _IE, OperationalError as _OE

    samples = (
        ("دسته‌بندی", _IE("INSERT ...", {}, Exception("UNIQUE constraint failed: categories.name"))),
        ("دسته‌بندی", _IE("DELETE ...", {}, Exception("FOREIGN KEY constraint failed"))),
        ("اکانت", _OE("SELECT ...", {}, Exception("connection refused"))),
        ("سفارش", _OE("UPDATE ...", {}, Exception("database is locked"))),
        ("سفارش", RuntimeError("some weird internal failure")),
    )
    for action, err in samples:
        print(f"--- {type(err).__name__} (action={action}) ---")
        print(get_user_friendly_db_error(action, err))
        print()

    print("--- پیام‌های عمومی ---")
    print(get_generic_error_message())
    print()
    print(get_floodwait_message(30))
    print()
    print(get_file_too_large_message(20))