import asyncio
import logging
from aiogram import Bot
from config import config
from workers.sender import _get_redis  # استفاده از کلاینت ردیس موجود

logger = logging.getLogger(__name__)


async def report_admin_error(error_text: str):
    """ثبت پیام خطا در بافر ردیس برای ارسال گروهی"""
    
    # 🛡 لیست سیاه خطاها: عباراتی که نمی‌خواهیم به ادمین گزارش شوند
    ignored_errors = [
        "query is too old and response timeout expired",
        "query ID is invalid",
        "message is not modified" # این هم یکی از خطاهای رایج و بی‌خطر تلگرام است
    ]
    
    # اگر متن خطا شامل یکی از عبارات لیست سیاه بود، بی‌صدا خارج شو
    if any(ignored in error_text for ignored in ignored_errors):
        return

    try:
        redis = _get_redis()
        # کلید admin_errors:buffer به عنوان یک دیکشنری در ردیس عمل می‌کند
        # و در صورت تکرار یک خطا، فقط شمارنده آن بالا می‌رود.
        await redis.hincrby("admin_errors:buffer", error_text, 1)
        await redis.expire("admin_errors:buffer", 86400)  # TTL معادل ۲۴ ساعت
    except Exception as e:
        logger.error(f"Failed to buffer admin error: {e}")


async def error_aggregator_loop(bot: Bot):
    """تسک پس‌زمینه: هر ۵ دقیقه خطاها را خوانده و یکجا به ادمین ارسال می‌کند"""
    logger.info("Error Aggregator Loop started.")
    while True:
        try:
            await asyncio.sleep(300)  # ۵ دقیقه تاخیر
            
            redis = _get_redis()
            errors = await redis.hgetall("admin_errors:buffer")
            
            if not errors:
                continue
            
            # پاکسازی بافر فعلی به صورت اتمیک
            await redis.delete("admin_errors:buffer")
            
            report_lines = ["🚨 <b>گزارش تجمیعی سیستم (۵ دقیقه اخیر)</b>\n"]
            for error_msg, count in errors.items():
                report_lines.append(f"🔸 <b>{count} بار تکرار:</b>\n{error_msg}\n")
            
            final_msg = "\n".join(report_lines)
            
            # جلوگیری از خطای طولانی‌شدن پیام تلگرام (محدودیت ۴۰۹۶ کاراکتر)
            if len(final_msg) > 4000:
                final_msg = final_msg[:4000] + "\n... (پیام به دلیل محدودیت طول کوتاه شد)"
                
            from utils.admin_broadcast import broadcast_to_admins
            await broadcast_to_admins(bot, text=final_msg)
            
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Error Aggregator crashed: {e}")