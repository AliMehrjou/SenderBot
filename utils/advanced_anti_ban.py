import asyncio
import random
import logging
from pyrogram import Client

logger = logging.getLogger(__name__)

# مخزن داده‌های هویتی (می‌تواند در آینده به دیتابیس متصل شود)
FIRST_NAMES = ["علی", "محمد", "سارا", "زهرا", "رضا", "مریم", "امیر", "فاطمه", "مهدی", "نیلوفر"]
LAST_NAMES = ["احمدی", "حسینی", "محمدی", "رضایی", "کریمی", "موسوی", "جعفری", "جلالی"]
BIOS = [
    "روزهای خوب در راهند 🌟",
    "تلاش برای اهداف 💻",
    "عاشق طبیعت 🌿",
    "پشتکار و امید!",
    "کارآفرین",
    "Life is beautiful.",
    "Carpe Diem ☀️",
    "" # بیوگرافی خالی برای طبیعی‌تر شدن
]

async def randomize_profile(client: Client, account_id: int) -> None:
    """
    تغییر تصادفی پروفایل برای جلوگیری از تشخیص الگوریتمی (Pattern Recognition).
    با احتمال ۲۰ درصد در هر بار فراخوانی اجرا می‌شود تا رفتار غیرطبیعی ایجاد نکند.
    """
    if random.random() > 0.2:
        return

    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    bio = random.choice(BIOS)
    
    try:
        await client.update_profile(first_name=first, last_name=last, bio=bio)
        logger.info(f"Worker user_{account_id}/ profile randomized: {first} {last}")
    except Exception as e:
        logger.debug(f"Worker user_{account_id}/ failed to randomize profile: {e}")

async def perform_warmup_cycle(client: Client, account_id: int) -> None:
    """
    شبیه‌سازی رفتار انسانی با اسکن کردن لیست چت‌ها و سین کردن پیام‌های خوانده نشده.
    """
    try:
        count = 0
        # دریافت حداکثر ۱۵ چت اخیر
        async for dialog in client.get_dialogs(limit=15):
            if dialog.unread_messages_count > 0:
                await client.read_chat_history(dialog.chat.id)
                count += 1
                # تاخیر انسانی بین سین کردن هر چت
                await asyncio.sleep(random.uniform(2, 6)) 
        
        if count > 0:
            logger.info(f"Worker user_{account_id}/ warm-up complete. Read {count} chats.")
    except Exception as e:
        logger.debug(f"Worker user_{account_id}/ warm-up error: {e}")

from pyrogram.errors import PeerIdInvalid, UserRestricted

async def appeal_to_spambot(client: Client, account_id: int) -> None:
    """
    ارسال دستورات متوالی به ربات اسپم‌بات برای ثبت درخواست رفع محدودیت.
    این تابع مراحل پاسخ‌دهی به اسپم‌بات را شبیه‌سازی می‌کند.
    """
    try:
        # مرحله اول: استارت
        await client.send_message("spambot", "/start")
        logger.info(f"Worker user_{account_id}/ sent /start to @spambot.")
        await asyncio.sleep(random.uniform(3, 6))
        
        # مرحله دوم: اعتراض به محدودیت
        await client.send_message("spambot", "This is a mistake")
        await asyncio.sleep(random.uniform(2, 4))
        
        # مرحله سوم: تایید شکایت
        await client.send_message("spambot", "Yes")
        await asyncio.sleep(random.uniform(2, 4))
        
        # مرحله چهارم: تایید عدم ارسال اسپم
        await client.send_message("spambot", "No! Never did that!")
        await asyncio.sleep(random.uniform(3, 5))
        
        # مرحله پنجم: ارسال متن دفاعیه
        appeal_text = "I think my account was restricted by mistake. I just send messages to my friends. Please remove the limitation."
        await client.send_message("spambot", appeal_text)
        
        logger.info(f"Worker user_{account_id}/ successfully submitted full SpamBot appeal.")
        
    except Exception as e:
        logger.debug(f"Worker user_{account_id}/ failed to contact @spambot: {e}")