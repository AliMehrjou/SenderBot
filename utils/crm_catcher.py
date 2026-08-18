import logging
import os
import aiohttp
from pyrogram import Client
from pyrogram.types import Message
from config import config

logger = logging.getLogger(__name__)

async def incoming_message_handler(client: Client, message: Message) -> None:
    """
    هندلر دریافت پیام CRM (آپدیت فاز ۵):
    رفع نشت حافظه (Memory Leak) با ارسال درخواست مستقیم REST به جای ساختن مداوم آبجکت Bot.
    """
    admin_id = config.ADMIN_ID
    if not admin_id or admin_id == 0:
        return

    # نادیده گرفتن پیام‌های خود اکانت، ربات‌ها یا پیام‌های سرویس
    if not message.from_user or message.from_user.is_self or message.from_user.is_bot:
        return

    bot_token = os.getenv("BOT_TOKEN")
    if not bot_token:
        logger.error("BOT_TOKEN is missing in env. CRM Catcher cannot notify admin.")
        return

    try:
        sender_name = message.from_user.first_name or "کاربر"
        sender_username = f"(@{message.from_user.username})" if message.from_user.username else ""
        worker_id = client.name.replace("worker_acc_", "")
        
        msg_text = message.text or message.caption or "<i>[پیام حاوی مدیا/فایل است. برای مشاهده به اکانت ورکر مراجعه کنید]</i>"

        info_text = (
            "📩 <b>پیام جدید از تارگت (سیستم CRM)</b>\n\n"
            f"👤 <b>فرستنده:</b> {sender_name} {sender_username}\n"
            f"🆔 <b>آیدی فرستنده:</b> <code>{message.from_user.id}</code>\n"
            f"🤖 <b>دریافت شده در ورکر:</b> <code>{worker_id}</code>\n\n"
            f"💬 <b>متن پیام:</b>\n{msg_text}"
        )
        
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = {
            "chat_id": admin_id,
            "text": info_text,
            "parse_mode": "HTML"
        }
        
        # استفاده از aiohttp برای یک درخواست سبک، سریع و ایزوله
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload) as response:
                if response.status == 200:
                    logger.info(f"CRM: Successfully notified Admin about message from {message.from_user.id}")
                else:
                    logger.error(f"CRM API Error: HTTP {response.status}")
                    
    except Exception as e:
        logger.error(f"CRM Catcher failed to notify admin: {e}")