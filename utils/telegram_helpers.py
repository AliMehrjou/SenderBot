# -*- coding: utf-8 -*-
"""
🛡 مدیریت خطا — فاز ۱ از ۵
==========================
utils/telegram_helpers.py

هندلرهای امن برای عملیات پرتکرار Bot API تلگرام (رفع «مشکل ۶»).

هدف: یکسان‌سازی رفتار همه هندلرها در برابر خطاهای رایج تلگرام:

    خطا                           |  رفتار
    ------------------------------|------------------------------------------
    message is not modified       |  نادیده گرفته می‌شود  → return False
    پیام حذف‌شده/غیرقابل ویرایش  |  fallback به ارسال پیام جدید → True
    پیام مدیا (بدون متن)         |  کپشن ویرایش می‌شود → return True
    پاسخ دیرهنگام به callback    |  نادیده گرفته می‌شود  → return False
    سایر خطاها                    |  مجدداً raise می‌شوند (هندلر تصمیم می‌گیرد)
"""

import logging
from typing import Optional

from aiogram.exceptions import TelegramBadRequest
from aiogram.types import CallbackQuery, InlineKeyboardMarkup, Message

logger = logging.getLogger(__name__)

# متن انتظار استاندارد برای عملیات طولانی
# (قید پروژه: همهٔ long-running operationها باید loading message داشته باشند)
LOADING_TEXT = "⏳ در حال پردازش..."

# بخش‌های متنی خطاهای رایج TelegramBadRequest (مقایسه case-insensitive)
_MSG_NOT_MODIFIED = "message is not modified"
_MSG_NOT_FOUND = "message to edit not found"
_MSG_CANT_EDIT = "message can't be edited"
_MSG_NO_TEXT = "there is no text in the message to edit"
_MSG_QUERY_INVALID = "query id invalid"


def _err_text(e: TelegramBadRequest) -> str:
    """متن خطا به حروف کوچک — برای مقایسه با الگوها."""
    return str(e).lower()


async def safe_edit_message(
    message: Message,
    text: str,
    reply_markup: Optional[InlineKeyboardMarkup] = None,
    **kwargs,
) -> bool:
    """
    ویرایش امن متن پیام — جایگزین الگوهای ناهماهنگ «مشکل ۶».

    رفتار:
        ✅ ویرایش موفق                       → True
        🟡 محتوا تغییری نکرده (NotModified)  → False  (خطا نیست؛ مثلاً دوبار
                                               کلیک روی «🔄 بروزرسانی»)
        🟡 پیام حذف/غیرقابل ویرایش شده      → پیام جدید ارسال می‌شود → True
        🟡 پیام مدیا (بدون متن)             → کپشن ویرایش می‌شود      → True
        ❌ هر خطای دیگر                      → مجدداً raise می‌شود

    kwargs اضافی (مثل disable_web_page_preview) مستقیم به edit_text پاس داده
    می‌شود.
    """
    try:
        await message.edit_text(text, reply_markup=reply_markup, **kwargs)
        return True

    except TelegramBadRequest as e:
        err = _err_text(e)

        # ۱) محتوا یکسان است — کاملاً طبیعی، خطا محسوب نمی‌شود
        if _MSG_NOT_MODIFIED in err:
            return False

        # ۲) پیام مدیا است و متن ندارد → کپشن ویرایش می‌شود
        if _MSG_NO_TEXT in err:
            try:
                await message.edit_caption(caption=text, reply_markup=reply_markup, **kwargs)
                return True
            except TelegramBadRequest as e2:
                if _MSG_NOT_MODIFIED in _err_text(e2):
                    return False
                raise

        # ۳) پیام قدیمی حذف شده یا قابل ویرایش نیست → ارسال پیام جدید
        if _MSG_NOT_FOUND in err or _MSG_CANT_EDIT in err:
            await message.answer(text, reply_markup=reply_markup, **kwargs)
            return True

        # ۴) خطای ناشناخته — تصمیم با هندلر است
        raise


async def safe_callback_answer(
    callback: CallbackQuery,
    text: Optional[str] = None,
    show_alert: bool = False,
) -> bool:
    """
    پاسخ امن به CallbackQuery.

    چرا لازم است؟ در مسیرهای خطای هندلرها — که ممکن است دیرتر از مهلت
    ~۱۵ ثانیه‌ای تلگرام اجرا شوند — فراخوانی callback.answer خطای
    QUERY_ID_INVALID می‌دهد و «هندلرِ خطا» را خودش کرش می‌کند!
    این خطا (که خطای واقعی نیست) بی‌صدا نادیده گرفته می‌شود.

    Returns:
        True  = پاسخ به تلگرام رسید
        False = مهلت پاسخ گذشته بود / قبلاً پاسخ داده شده بود
    """
    try:
        await callback.answer(text, show_alert=show_alert)
        return True
    except TelegramBadRequest as e:
        if _MSG_QUERY_INVALID in _err_text(e):
            logger.debug("callback.answer skipped (already answered or expired).")
            return False
        raise


async def send_loading_message(message: Message, text: str = LOADING_TEXT) -> Message:
    """
    ارسال پیام انتظار استاندارد برای عملیات طولانی (قید پروژه).

    الگوی مصرف در هندلرها:

        wait_msg = await send_loading_message(message, "⏳ در حال پاکسازی...")
        try:
            ...  # عملیات طولانی
            await safe_edit_message(wait_msg, "✅ انجام شد")
        except Exception as e:
            logger.error(..., exc_info=True)
            await safe_edit_message(wait_msg, error_text)
    """
    return await message.answer(text)

async def answer_callback_error(
    callback: CallbackQuery,
    error_text: str,
    fallback_markup: Optional[InlineKeyboardMarkup] = None,
) -> None:
    """
    🛡 فاز ۲ — نمایش متن خطا به کاربر از مسیر CallbackQuery.

    اگر callback هنوز پاسخ داده نشده باشد → خطا به صورت alert نمایش داده می‌شود.
    اگر قبلاً پاسخ داده شده باشد (فراخوانی داخلی هندلرها با skip_answer=True) →
    یک پیام جدید با کیبورد fallback ارسال می‌شود تا کاربر بدون بازخورد نماند.

    نکته: alert تلگرام محدودیت ~۲۰۰ کاراکتر دارد؛ همهٔ پیام‌های
    utils/error_messages.py کوتاه‌تر از این حد هستند.
    """
    answered = await safe_callback_answer(callback, error_text, show_alert=True)
    if not answered:
        try:
            await callback.message.answer(error_text, reply_markup=fallback_markup)
        except Exception:
            # تحویل پیام خطا best-effort است؛ نباید خودش باعث کرش شود
            logger.error("Failed to deliver error message to user.", exc_info=True)


import re as _re

RE_PUBLIC_LINK = _re.compile(r"^(?:https?://)?(?:t|telegram)\.me/[A-Za-z0-9_]{4,64}/?$", _re.IGNORECASE)
RE_INVITE_LINK = _re.compile(r"^(?:https?://)?(?:t|telegram)\.me/(?:\+|joinchat/)[A-Za-z0-9_\-]{8,}/?$", _re.IGNORECASE)
RE_USERNAME = _re.compile(r"^@?[A-Za-z0-9_]{4,64}$")

def normalize_target_line(line: str) -> Optional[str]:
    """@username → لینک t.me؛ خود لینک‌ها بدون تغییر؛ None یعنی خط نامعتبر."""
    if RE_USERNAME.fullmatch(line):
        return f"https://t.me/{line.lstrip('@')}"
    if RE_PUBLIC_LINK.fullmatch(line) or RE_INVITE_LINK.fullmatch(line):
        return line
    return None

def parse_target_links(text: str) -> "tuple[list, list]":
    """متن چندخطی → (لینک‌های نرمال‌شدهٔ معتبر، [(شماره خط، متن خام خطوط نامعتبر)])"""
    valid_lines = []
    invalid_lines = []
    for i, line in enumerate(text.strip().split('\n'), start=1):
        token = line.strip()
        if not token:
            continue
        normalized = normalize_target_line(token)
        if normalized:
            valid_lines.append(normalized)
        else:
            invalid_lines.append((i, token))
    return valid_lines, invalid_lines