import asyncio
import os
import random
import logging
from typing import TYPE_CHECKING, Optional

from pyrogram import Client
from pyrogram.errors import PeerIdInvalid, UserRestricted
from pyrogram.raw.functions.auth import ResetAuthorizations
from pyrogram.errors import FreshResetAuthorisationForbidden

# ایمپورت تابع اسپینتکس برای داینامیک کردن متن‌ها
from utils.anti_ban import parse_spintax

# 🎭 مدیریت پیشرفته پروفایل‌ها: فقط برای type-hint ایمپورت می‌شود (TYPE_CHECKING)
# تا وابستگی runtime بین لایه utils و database ایجاد نشود و ریسک circular import صفر بماند.
if TYPE_CHECKING:
    from database.models import GlobalSettings, ProfilePhotoPackage

logger = logging.getLogger(__name__)

# مخزن داده‌های هویتی
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
    "" 
]

async def randomize_profile(
    client: Client,
    account_id: int,
    force: bool = False,
    settings: Optional["GlobalSettings"] = None,
) -> None:
    """
    🎭 مدیریت پیشرفته پروفایل‌ها — تغییر پروفایل اکانت بر اساس سوئیچ‌های مستقل.

    منطق:
      • فقط اگر settings.auto_set_name روشن باشد → first_name / last_name آپدیت می‌شود.
      • فقط اگر settings.auto_set_bio روشن باشد → bio آپدیت می‌شود.
      • عکس پروفایل در این تابع کاری انجام نمی‌دهد — چرخش عکس توسط تابع مستقل
        rotate_profile_photos (پکیج عکس) انجام می‌شود که از session_manager و
        جدا از این تسک اجرا می‌شود.
      • اگر settings پاس داده نشود (None) یا هر دو سوئیچ خاموش باشند، تابع بدون
        هیچ تماس اضافه با تلگرام برمی‌گردد.

    برای جلوگیری از تشخیص الگوریتمی، فیلدی که از قبل پر شده باشد (مگر با force)
    تغییر نمی‌خورد تا رفتار اکانت طبیعی به نظر برسد.
    """
    set_name = bool(getattr(settings, "auto_set_name", False))
    set_bio = bool(getattr(settings, "auto_set_bio", False))

    if not set_name and not set_bio:
        return

    try:
        if not force:
            me = await client.get_me()
            # اگر فیلدی از قبل پر باشد یعنی قبلاً تنظیم شده و نیازی به تغییر مجدد نیست
            # (بررسی per-field: فقط فیلدهایی که قرار است آپدیت شوند چک می‌شوند)
            if set_name and me.last_name:
                set_name = False
            if set_bio and me.bio:
                set_bio = False
    except Exception as e:
        logger.debug(f"Worker user_{account_id}/ could not fetch profile: {e}")

    # پارامتری که به update_profile پاس نشود روی سرور بدون تغییر می‌ماند؛ بنابراین
    # فقط فیلدهای دارای سوئیچ روشن ارسال می‌شوند.
    update_kwargs: dict = {}
    if set_name:
        update_kwargs["first_name"] = random.choice(FIRST_NAMES)
        update_kwargs["last_name"] = random.choice(LAST_NAMES)
    if set_bio:
        update_kwargs["bio"] = random.choice(BIOS)

    if not update_kwargs:
        return

    try:
        await client.update_profile(**update_kwargs)
        changed_fields = ", ".join(update_kwargs.keys())
        logger.info(f"Worker user_{account_id}/ profile randomized (fields: {changed_fields})")
    except Exception as e:
        logger.debug(f"Worker user_{account_id}/ failed to randomize profile: {e}")


async def rotate_profile_photos(client: Client, package: "ProfilePhotoPackage") -> None:
    """
    🖼 پکیج‌های ۳ تایی عکس پروفایل — جایگزینی عکس‌های پروفایل با عکس‌های پکیج.

    مراحل (مطابق طراحی):
      ۱) photos = await client.get_profile_photos("me")
      ۲) حذف همه‌ی عکس‌ها به‌جز آخری:
         await client.delete_profile_photos([p.file_id for p in photos[:-1]])
         (تلگرام اجازه‌ی حذف تک‌عکسِ باقی‌مانده را نمی‌دهد؛ عکس جدید جایگزینش می‌شود)
      ۳) آپلود ۳ عکس پکیج به ترتیب position با تاخیر تصادفی انسانی.

    نکات:
      • ترتیب package.photos توسط relationship (order_by=position) تضمین می‌شود؛
        نقاط فراخوانی باید آن را با selectinload لود کرده باشند (session_manager همین کار را می‌کند).
      • تمام خطاها داخل همین تابع مهار می‌شوند تا شکست عکس هرگز آپدیت نام/بیو
        (randomize_profile) را خراب نکند.
      • گیتِ auto_set_photo در session_manager است؛ این تابع خودش تنظیمات را چک نمی‌کند.
    """
    if not package or not package.photos:
        return

    # --- مرحله ۱ و ۲: حذف عکس‌های قدیمی (به‌جز آخری) ---
    try:
        photos = await client.get_profile_photos("me")
        old_ids = [p.file_id for p in photos[:-1]]
        if old_ids:
            await client.delete_profile_photos(old_ids)
            logger.info(
                f"Worker {client.name}: {len(old_ids)} old profile photo(s) deleted "
                f"(package «{package.name}»)."
            )
            await asyncio.sleep(random.uniform(2, 5))
    except Exception as e:
        logger.warning(f"Worker {client.name}: failed to delete old profile photos: {e}")

    # --- مرحله ۳: آپلود عکس‌های پکیج به ترتیب position ---
    for photo in package.photos:
        try:
            if not os.path.exists(photo.file_path):
                logger.warning(
                    f"Worker {client.name}: package photo missing on disk: {photo.file_path}"
                )
                continue
            await client.set_profile_photo(photo=photo.file_path)
        except Exception as e:
            logger.warning(
                f"Worker {client.name}: failed to set profile photo (pos={photo.position}): {e}"
            )
        await asyncio.sleep(random.uniform(4, 10))

    logger.info(
        f"Worker {client.name}: profile photo rotation for package «{package.name}» finished."
    )

async def perform_warmup_cycle(client: Client, account_id: int) -> None:
    """
    شبیه‌سازی رفتار انسانی با اسکن کردن لیست چت‌ها و سین کردن پیام‌های خوانده نشده.
    (اضافه شدن منطق خروج از فریز برای اکانت‌های صفر)
    """
    try:
        dialogs_count = 0
        read_count = 0
        
        async for dialog in client.get_dialogs(limit=15):
            dialogs_count += 1
            if dialog.unread_messages_count > 0:
                await client.read_chat_history(dialog.chat.id)
                read_count += 1
                await asyncio.sleep(random.uniform(2, 6)) 
        
        # --- منطق فاز ۳: بیدار کردن اکانت‌های خام ---
        if dialogs_count == 0:
            logger.info(f"Worker user_{account_id}/ is completely raw. Starting anti-freeze routine.")
            
            # انتخاب تصادفی یک ربات معتبر و رسمی تلگرام
            safe_bots = ["gif", "pic", "sticker", "bing", "youtube"]
            target_bot = random.choice(safe_bots)
            
            # مرحله اول: استارت ربات
            await client.send_message(target_bot, "/start")
            await asyncio.sleep(random.uniform(3, 6))
            
            # مرحله دوم: یک سرچ یا تعامل ساده انسانی
            queries = ["cat", "hello", "smile", "car", "nature", "funny"]
            await client.send_message(target_bot, random.choice(queries))
            
            logger.info(f"Worker user_{account_id}/ successfully interacted with @{target_bot}.")
            
        elif read_count > 0:
            logger.info(f"Worker user_{account_id}/ warm-up complete. Read {read_count} chats.")
            
    except Exception as e:
        logger.debug(f"Worker user_{account_id}/ warm-up error: {e}")


async def appeal_to_spambot(client: Client, account_id: int) -> None:
    """
    ارسال دستورات متوالی به ربات اسپم‌بات برای ثبت درخواست رفع محدودیت.
    با استفاده از Spintax، متن ارسالی هر اکانت کاملاً یونیک خواهد بود.
    """
    try:
        await client.send_message("spambot", "/start")
        logger.info(f"Worker user_{account_id}/ sent /start to @spambot.")
        await asyncio.sleep(random.uniform(3, 6))
        
        msg1 = parse_spintax("{This is a mistake|I think there is a mistake|It's a mistake|Please check this mistake}")
        await client.send_message("spambot", msg1)
        await asyncio.sleep(random.uniform(2, 4))
        
        msg2 = parse_spintax("{Yes|Yeah|Yes please|Yes, it is}")
        await client.send_message("spambot", msg2)
        await asyncio.sleep(random.uniform(2, 4))
        
        msg3 = parse_spintax("{No! Never did that!|No, I didn't|I never did anything wrong|No|Never!}")
        await client.send_message("spambot", msg3)
        await asyncio.sleep(random.uniform(3, 5))
        
        appeal_text = parse_spintax(
            "{I think my account was restricted by mistake.|My account got limited for no reason.|Please remove the limitation.} "
            "{I just send messages to my friends.|I only chat with my contacts.|I am a normal user.} "
            "{Please fix this.|Please remove the limitation.|Thanks in advance.}"
        )
        await client.send_message("spambot", appeal_text)
        
        logger.info(f"Worker user_{account_id}/ successfully submitted unique SpamBot appeal.")
        
    except Exception as e:
        logger.debug(f"Worker user_{account_id}/ failed to contact @spambot: {e}")

async def terminate_other_sessions(client: Client) -> bool:
    """
    تلاش برای پایان دادن به تمام نشست‌های فعال تلگرام به جز نشست فعلی (ربات).
    """
    try:
        await client.invoke(ResetAuthorizations())
        logger.info(f"✅ تمامی نشست‌های دیگر برای اکانت {client.name} با موفقیت ترمینیت شدند.")
        return True
        
    except FreshResetAuthorisationForbidden:
        logger.warning(f"⚠️ اکانت {client.name} تازه لاگین شده است. تلگرام اجازه خروج فوری را نمی‌دهد (نیاز به گذشت 24 ساعت).")
        return False
        
    except Exception as e:
        logger.error(f"❌ خطای ناشناخته در بستن نشست‌ها برای {client.name}: {e}")
        return False