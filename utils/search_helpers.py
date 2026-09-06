"""
🔍 زیرساخت مشترک جستجو — فاز ۱

stateها و توابع کمکی استاندارد جستجوی لیست‌ها. این ماژول از الان به عنوان
زیرساخت ایجاد شده و در فاز ۴ (جستجوی اکانت با شماره موبایل و جستجوی
سفارش با شناسه) استفاده خواهد شد.

امکانات:
    - SearchStates          : گروه stateهای استاندارد جستجو
    - normalize_digits      : تبدیل ارقام فارسی/عربی به لاتین
    - normalize_phone_query : نرمال‌سازی ورودی جستجوی شماره موبایل
    - build_like_pattern    : ساخت الگوی LIKE امن برای جستجوی جزئی

⚠️ نکتهٔ مهم: کاربر فارسی‌زبان معمولاً ارقام را با کیبورد فارسی تایپ می‌کند
(۰۹۱۲…). بدون نرمال‌سازی، جستجوی «۰۹۱۲» هیچ نتیجه‌ای نمی‌دهد چون در
دیتابیس ارقام لاتین ذخیره شده‌اند.
"""

import re

from aiogram.fsm.state import State, StatesGroup


class SearchStates(StatesGroup):
    """stateهای استاندارد جستجو در لیست‌ها (الگوی مشترک پروژه)."""

    #: جستجوی اکانت با شماره موبایل (یا بخشی از آن)
    waiting_for_phone_query = State()

    #: جستجوی سفارش با شناسه / کد رهگیری
    waiting_for_order_id_query = State()


# ──────────────────────────────────────────────
# نرمال‌سازی ورودی جستجو
# ──────────────────────────────────────────────

_PERSIAN_DIGITS = "۰۱۲۳۴۵۶۷۸۹"
_ARABIC_DIGITS = "٠١٢٣٤٥٦٧٨٩"
_DIGIT_TRANSLATION = str.maketrans(
    _PERSIAN_DIGITS + _ARABIC_DIGITS,
    "01234567890123456789",
)

#: کاراکترهای جداساز رایج در تایپ شماره موبایل (+ فاصله - پرانتز)
_PHONE_SEPARATORS = r"[+\s\-\(\)]"


def normalize_digits(text: str) -> str:
    """تبدیل ارقام فارسی (۰-۹) و عربی (٠-٩) به معادل لاتین."""
    return (text or "").translate(_DIGIT_TRANSLATION)


def normalize_phone_query(raw_query: str) -> str:
    """
    نرمال‌سازی ورودی جستجوی شماره موبایل:
        ۱) ارقام فارسی/عربی → لاتین
        ۲) حذف +، فاصله، خط تیره و پرانتز

    مثال: "۰۹۱۲ ۳۴۵-۶۷۸۹" → "09123456789"
    """
    normalized = normalize_digits(raw_query or "")
    return re.sub(_PHONE_SEPARATORS, "", normalized).strip()


def build_like_pattern(
    raw_query: str,
    escape_wildcards: bool = True,
    escape_char: str = "\\",
) -> str:
    """
    ساخت الگوی LIKE برای جستجوی جزئی (contains): "98912" → "%98912%"

    اگر escape_wildcards فعال باشد، کاراکترهای خاص LIKE (٪ _ و خودِ
    escape_char) در ورودی کاربر escape می‌شوند تا جستجو امن باشد.

    نحوهٔ استفاده در کوئری:

        pattern = build_like_pattern(query)
        stmt = select(Account).where(
            Account.phone_number.like(pattern, escape="\\\\")
        )

    ⚠️ برای query خالی، الگوی "%%" (تطابق با همه) برمی‌گردد —
    هندلرها باید ورودی خالی را قبل از جستجو رد کنند.
    """
    query = normalize_digits(raw_query or "").strip()

    if escape_wildcards:
        query = (
            query.replace(escape_char, escape_char * 2)
            .replace("%", escape_char + "%")
            .replace("_", escape_char + "_")
        )

    return f"%{query}%"