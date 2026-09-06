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
    except TelegramBadRequest as e:
        err_msg = str(e).lower()
        # ۱. محتوای تکراری — نیازی به تغییر یا فال‌بک نیست
        if "message is not modified" in err_msg:
            return
            
        # ۲. پیام مدیا است و متن ندارد — باید کپشن ویرایش شود
        if "there is no text in the message to edit" in err_msg:
            try:
                await message.edit_caption(caption=text, reply_markup=reply_markup, **kwargs)
                return
            except TelegramBadRequest as e2:
                if "message is not modified" in str(e2).lower():
                    return
                # هر خطای دیگری حین ویرایش کپشن رخ داد، لاگ و فال‌بک می‌شود
                logger.warning(f"safe_edit_or_answer: edit_caption failed ({e2}); falling back to answer.")
                
        # ۳. پیام حذف شده یا قدیمی (غیرقابل ویرایش) — فال‌بک به answer
        elif "message to edit not found" in err_msg or "message can't be edited" in err_msg:
            # اینجا فقط لاگِ دیباگ یا info می‌زنیم تا هشدار لاگ بیش از حد پر نشود
            logger.debug(f"safe_edit_or_answer: message not editable ({e}); falling back to answer.")
            
        else:
            # سایر خطاهای BadRequest
            logger.warning(f"safe_edit_or_answer: TelegramBadRequest ({e}); falling back to answer.")
            
        with suppress(Exception):
            await message.answer(text, reply_markup=reply_markup, **kwargs)
            
    except Exception as edit_error:
        logger.warning(f"safe_edit_or_answer: Unexpected error ({edit_error}); falling back to answer.")
        with suppress(Exception):
            await message.answer(text, reply_markup=reply_markup, **kwargs)