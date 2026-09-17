# utils/safe_edit.py
"""
🟣 فاز ۲ (رفع بن‌بست FSM): ابزار مشترک ویرایش امن پیام‌ها

🟣 فاز ۶ (به‌روزرسانی): خطای «message is not modified» به صورت بی‌صدا نادیده
گرفته می‌شود. این خطا یعنی محتوای فعلی پیام دقیقاً همان محتوای درخواستی است
(مثلاً toggle دوباره روی همان دسته‌بندی در فلوی ثبت سفارش) — یعنی پیام از قبل
درست نمایش داده شده و ارسال پیام جدید فقط باعث UI تکراری می‌شد.
"""
import logging
from contextlib import suppress

from aiogram import types
from aiogram.exceptions import TelegramBadRequest

logger = logging.getLogger(__name__)


async def safe_edit_or_answer(
    message: types.Message,
    text: str,
    reply_markup=None,
    **kwargs,
) -> None:
    """
    ویرایش متن پیام با fallback به ارسال پیام جدید.

    :param message: پیام هدف (معمولاً callback.message)
    :param text: متن جدید
    :param reply_markup: کیبورد inline اختیاری
    :param kwargs: پارامترهای مشترک edit_text/answer (مثل disable_web_page_preview)
    """
    try:
        await message.edit_text(text, reply_markup=reply_markup, **kwargs)
    except Exception as e:
        if is_not_modified_error(e):
            # خطا کاملاً بی‌خطر است، نادیده گرفتن قطعی بدون اکسپشن
            return
            
        err_msg = str(e).lower()
        if "there is no text in the message to edit" in err_msg:
            try:
                await message.edit_caption(caption=text, reply_markup=reply_markup, **kwargs)
                return
            except Exception as e2:
                if is_not_modified_error(e2):
                    return
                logger.warning(f"safe_edit_or_answer: edit_caption failed ({e2}); falling back to answer.")
                
        elif is_message_gone_error(e):
            logger.debug(f"safe_edit_or_answer: message not editable ({e}); falling back to answer.")
            
        else:
            logger.warning(f"safe_edit_or_answer: Unexpected error ({e}); falling back to answer.")
            
        with suppress(Exception):
            await message.answer(text, reply_markup=reply_markup, **kwargs)


def is_not_modified_error(exc: Exception) -> bool:
    """
    True when an edit attempt was a no-op (identical content) — safe to swallow.
    Shared with utils/progress_reporter.py so both use one classification.
    """
    return "message is not modified" in str(exc).lower()


def is_message_gone_error(exc: Exception) -> bool:
    """
    True when the target message is deleted or too old to be edited.
    Shared with utils/progress_reporter.py so both use one classification.
    """
    msg = str(exc).lower()
    return "message to edit not found" in msg or "message can't be edited" in msg