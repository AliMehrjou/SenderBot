import asyncio
import logging
from pyrogram import Client
from aiogram import Bot
from config import config

logger = logging.getLogger(__name__)

async def auto_health_check_loop(worker_pool: dict, bot: Bot) -> None:
    """تسک پس‌زمینه برای بررسی سلامت روزانه پراکسی‌ها و کلاینت‌ها"""
    if not config.ADMIN_ID or config.ADMIN_ID == 0:
        logger.warning("ADMIN_ID is not set. Auto Health Checker is disabled.")
        return
        
    logger.info("Auto Health Checker Loop started.")
    
    while True:
        try:
            # هر ۲۴ ساعت یک‌بار اجرا می‌شود
            await asyncio.sleep(24 * 3600)
            
            total_workers = len(worker_pool)
            connected_workers = sum(1 for c in worker_pool.values() if c.is_connected)
            disconnected = total_workers - connected_workers
            
            report_text = (
                "🩺 <b>گزارش روزانه سلامت موتور سندر</b>\n\n"
                f"🟢 <b>ورکرهای آنلاین و سالم:</b> <code>{connected_workers}</code>\n"
                f"🔴 <b>ورکرهای قطع یا بن شده:</b> <code>{disconnected}</code>\n"
                f"🌐 <b>کل اکانت‌های در استخر:</b> <code>{total_workers}</code>\n\n"
                "<i>💡 برای جزئیات بیشتر می‌توانید از منوی اصلی وارد بخش «📈 آمار» شوید.</i>"
            )
            
            await bot.send_message(chat_id=config.ADMIN_ID, text=report_text)
            logger.info("Daily Health Check report sent to Admin.")
            
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Error in Health Check loop: {e}")