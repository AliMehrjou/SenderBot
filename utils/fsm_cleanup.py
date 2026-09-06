# utils/fsm_cleanup.py
"""
🟣 فاز ۱ (رفع بن‌بست FSM): ابزار مشترک پاکسازی فایل‌های موقت FSM

هنگام لغو یا ترک یک فلوی FSM (مثل فلوی ثبت سفارش)، فایل‌هایی که کاربر
آپلود کرده (لیست تارگت‌ها، مدیاها) ممکن است روی هارد سرور باقی بمانند.
این ماژول به صورت «عمومی» کلیدهای استاندارد state را بررسی و هر فایل
موقتی را به‌صورت امن حذف می‌کند.
"""
import logging
import os
from contextlib import suppress

from aiogram.fsm.context import FSMContext

logger = logging.getLogger(__name__)


async def cleanup_fsm_temp_files(state: FSMContext) -> int:
    """
    پاکسازی امن فایل‌های موقت ذخیره‌شده در داده‌های FSM.

    کلیدهای بررسی‌شده (استاندارد فلوی ثبت سفارش):
    - target_data:    مسیر فایل لیست تارگت‌ها (فقط مسیرهای داخل downloads/)
    - order_messages: لیست دیکشنری پیام‌ها با کلید media_path

    اگر کلیدی وجود نداشته باشد بی‌صدا رد می‌شود؛ بنابراین فراخوانی این تابع
    برای هر state‌ای (حتی کاملاً غیرمرتبط) امن است.

    خروجی: تعداد فایل‌هایی که حذف شدند.
    """
    fsm_data = await state.get_data()
    deleted_count = 0

    # ۱) فایل لیست تارگت‌های سفارش (نوع list)
    target_data = fsm_data.get("target_data")
    if (
        isinstance(target_data, str)
        and target_data.startswith("downloads/")
        and os.path.exists(target_data)
    ):
        with suppress(Exception):
            os.remove(target_data)
            deleted_count += 1
            logger.info(f"FSM Cleanup: removed target list file {target_data}")

    # ۲) فایل‌های مدیای حلقه پیام‌های سفارش
    order_messages = fsm_data.get("order_messages") or []
    for msg in order_messages:
        media_path = msg.get("media_path") if isinstance(msg, dict) else None
        if media_path and os.path.exists(media_path):
            with suppress(Exception):
                os.remove(media_path)
                deleted_count += 1
                logger.info(f"FSM Cleanup: removed media file {media_path}")

    return deleted_count