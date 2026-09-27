import logging
from aiogram import Bot

logger = logging.getLogger(__name__)

async def report_admin_error(error_text: str):
    """
    ثبت پیام خطا فقط در لاگ‌های سرور.
    هیچ پیامی در ردیس ذخیره نمی‌شود و چیزی برای ارسال تجمیعی آماده نمی‌گردد.
    """
    # 🛡 لیست سیاه: خطاهایی که حتی نیازی به لاگ شدن در سرور هم ندارند
    ignored_errors = [
        "query is too old and response timeout expired",
        "query ID is invalid",
        "message is not modified"
    ]
    
    if any(ignored in error_text for ignored in ignored_errors):
        return

    # فقط پرینت/لاگ در کنسول سرور برای دیباگ خودت
    logger.error(f"Buffered Error (Disabled): {error_text}")


async def error_aggregator_loop(bot: Bot):
    """
    تسک پس‌زمینه خنثی شده.
    اگر این تابع از جای دیگری استارت بخورد، بدون هیچ حلقه‌ای بلافاصله تمام می‌شود 
    تا منابع CPU و RAM سرور درگیر نشوند.
    """
    logger.info("Error Aggregator Loop is disabled. Exiting task.")
    return