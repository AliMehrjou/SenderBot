import asyncio
import os
import random
import logging
from typing import TYPE_CHECKING, Optional

from pyrogram import Client
from pyrogram import raw
from pyrogram.errors import PeerIdInvalid, UserRestricted
from pyrogram.raw.functions.auth import ResetAuthorizations
from pyrogram.errors import FreshResetAuthorisationForbidden
import re
from datetime import datetime, timezone
from pyrogram.errors import YouBlockedUser

# ایمپورت تابع اسپینتکس برای داینامیک کردن متن‌ها
from utils.anti_ban import parse_spintax

# 🎭 مدیریت پیشرفته پروفایل‌ها: فقط برای type-hint ایمپورت می‌شود (TYPE_CHECKING)
# تا وابستگی runtime بین لایه utils و database ایجاد نشود و ریسک circular import صفر بماند.
if TYPE_CHECKING:
    from database.models import GlobalSettings, ProfilePhotoPackage

logger = logging.getLogger(__name__)
PHOTO_ROTATION_SEMAPHORE = asyncio.Semaphore(2)

# 🩹 FIX (photo-race): قفل انحصاری به ازای هر «اکانت» — جلوی اجرای همزمانِ
# دو چرخش عکس (rotate) یا چرخش + پاک‌سازی (clear) روی یک اکانت را می‌گیرد.
# بدون این قفل، دو تسک همزمان روی یک پروفایل، عکس‌های همدیگر را پاک/تکرار می‌کنند
# و نتیجه‌ی نهایی گالری (۱ یا ۵ عکس به‌جای ۳) غیرقطعی می‌شود.
_PHOTO_ACCOUNT_LOCKS = {}


def get_photo_account_lock(account_id):
    """قفل سراسری عکس‌پروفایلِ یک اکانت (بین session_manager و این ماژول مشترک است)."""
    lock = _PHOTO_ACCOUNT_LOCKS.get(account_id)
    if lock is None:
        lock = asyncio.Lock()
        _PHOTO_ACCOUNT_LOCKS[account_id] = lock
    return lock

import json

# مسیردهی جدید: پیدا کردن پوشه json_files در کنار پوشه utils
# مقدار __file__ مسیر همین فایل پایتون را نشان می‌دهد و دو بار dirname ما را به ریشه پروژه می‌رساند.
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
JSON_FILE_PATH = os.path.join(BASE_DIR, 'json_files', 'profiles.json')

# مقادیر پیش‌فرض (در صورتی که فایل جیسون در دسترس نباشد یا پاک شده باشد)
FIRST_NAMES = ["کاربر"]
LAST_NAMES = [""]
BIOS = [""]

# استخراج داده‌ها از فایل JSON با پشتیبانی از UTF-8
try:
    with open(JSON_FILE_PATH, 'r', encoding='utf-8') as f:
        data = json.load(f)
        FIRST_NAMES = data.get("first_names", FIRST_NAMES)
        LAST_NAMES = data.get("last_names", LAST_NAMES)
        BIOS = data.get("bios", BIOS)
except FileNotFoundError:
    logger.error(f"❌ فایل {JSON_FILE_PATH} پیدا نشد! از مقادیر پیش‌فرض استفاده می‌شود.")
except json.JSONDecodeError:
    logger.error("❌ فایل profiles.json مشکل سینتکسی دارد! از مقادیر پیش‌فرض استفاده می‌شود.")
except Exception as e:
    logger.error(f"❌ خطای ناشناخته در خواندن profiles.json: {e}")

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


async def rotate_profile_photos(client: Client, package: "ProfilePhotoPackage", account_id=None) -> bool:
    """
    🖼 پکیج‌های عکس پروفایل — جایگزینی عکس‌های پروفایل.
    استراتژی: حذف عکس‌های فعلی (محدود و قطعی) -> آپلود عکس‌های جدید به صورت معکوس

    🩹 FIX (photo-race + silent-failure):
      • قفل per-account: چرخش‌های همزمان روی یک اکانت دیگر باعث خرابی گالری نمی‌شوند.
        اگر account_id پاس نشود، از الگوی نامِ ورکر (worker_acc_N) استخراج می‌شود تا
        همان قفلی که clear_profile_photos_now می‌گیرد، اینجا هم گرفته شود.
      • خطاهای آپلود دیگر «بی‌صدا» نیستند؛ با ERROR لاگ می‌شوند و خروجی False است.
        راستی‌آزمایی با «کلِ» عکس‌های پکیج مقایسه می‌شود (نه فقط فایل‌های روی دیسک)
        تا نبودِ فایل عکس ۲/۳ دیگر حالتِ موفقِ کاذب نسازد.

    🧊 FIX (infinite-hammer — ریشه‌ی اصلی بن‌ها در bot_18/bot_19):
      حلقه‌ی قبلیِ «while True» وقتی اکانت FROZEN بود و حذف عکس همیشه شکست
      می‌خورد، هرگز خارج نمی‌شد ← photos.GetUserPhotos هر ~۱۱ ثانیه، ۲۴/۷
      (۱۱٬۰۹۰ بار فقط در یک لاگ!). الان:
        • چکِ فریز بودن اکانت قبل از شروع (utils/freeze_guard.py)
        • سقف دورِ پاک‌سازی (PHOTO_WIPE_MAX_PASSES) + خروج فوری اگر در یک دور
          «هیچ» عکسی حذف نشد (حذف ممکن نیست = فریز/محدودیت → نه چکش‌کاری)
        • خطای FROZEN در هر مرحله → mark_account_frozen + قطع فوری
        • بودجه‌ی زمانی کلی (PHOTO_ROTATION_TIMEOUT_SECONDS) تا اسلات سمافور
          و قفلِ اکانت هرگز برای همیشه اشغال نشود
        • چک اتصال قبل از آپلود + مکث بین تلاش‌ها (قبلاً ۳ خطای پشت‌سرهمِ
          «no active connection» در همان ثانیه لاگ می‌شد)
    """
    if not package or not package.photos:
        return False

    from pyrogram.errors import FloodWait
    import asyncio
    import time as _time

    lock_key = account_id
    if lock_key is None:
        _m = re.search(r"worker_acc_(\d+)", str(getattr(client, "name", "") or ""))
        lock_key = int(_m.group(1)) if _m else str(getattr(client, "name", "?"))

    # 🧊 گارد فریز: اکانتِ فریز‌شده اصلاً وارد چرخش نمی‌شود
    from utils.freeze_guard import is_account_frozen, is_frozen_error, mark_account_frozen
    if account_id is not None and await is_account_frozen(account_id):
        logger.warning(
            f"Worker {client.name}: photo rotation SKIPPED — account is FROZEN "
            "(cooldown active; no Telegram calls will be made)."
        )
        return False

    def _photo_cfg_int(name: str, default: int) -> int:
        from config import config
        try:
            return max(1, int(getattr(config, name, default)))
        except (TypeError, ValueError):
            return default

    max_passes = _photo_cfg_int("PHOTO_WIPE_MAX_PASSES", 4)
    timeout_seconds = _photo_cfg_int("PHOTO_ROTATION_TIMEOUT_SECONDS", 900)
    deadline = _time.monotonic() + timeout_seconds

    async with PHOTO_ROTATION_SEMAPHORE:
        async with get_photo_account_lock(lock_key):
            # --- مرحله ۱: حذف عکس‌های قدیمی (محدود و قطعی — بدون حلقه‌ی بی‌نهایت) ---
            deleted_count = 0
            try:
                for pass_no in range(1, max_passes + 1):
                    if _time.monotonic() > deadline:
                        logger.warning(
                            f"Worker {client.name}: photo wipe exceeded time budget "
                            f"({timeout_seconds}s) — stopping to avoid API hammering."
                        )
                        break

                    old_photos = [p async for p in client.get_chat_photos("me", limit=5)]
                    if not old_photos:
                        break

                    deleted_this_pass = 0
                    for photo in old_photos:
                        try:
                            await client.delete_profile_photos([photo.file_id])
                            deleted_count += 1
                            deleted_this_pass += 1
                            await asyncio.sleep(1.5)
                        except FloodWait as e:
                            await asyncio.sleep(int(getattr(e, "value", 0) or 0) + 1)
                            try:
                                await client.delete_profile_photos([photo.file_id])
                                deleted_count += 1
                                deleted_this_pass += 1
                            except Exception as retry_err:
                                if is_frozen_error(retry_err):
                                    await mark_account_frozen(
                                        account_id, reason=f"delete_profile_photos: {retry_err}"
                                    )
                                    return False
                        except Exception as del_err:
                            # 🧊 فریز = قطع فوری؛ بقیه‌ی خطاها فقط لاگ می‌شوند
                            if is_frozen_error(del_err):
                                await mark_account_frozen(
                                    account_id, reason=f"delete_profile_photos: {del_err}"
                                )
                                return False
                            logger.warning(
                                f"Worker {client.name}: failed to delete a photo: {del_err}"
                            )

                    # 🛑 پیشرفتِ صفر در یک دورِ کامل یعنی حذف ممکن نیست
                    # (اکانت فریز/محدود است یا عکس‌ها قابل حذف نیستند) —
                    # تکرارِ بیشتر فقط چکش‌کاریِ API و پرچمِ اسپم است.
                    if deleted_this_pass == 0:
                        logger.warning(
                            f"Worker {client.name}: photo wipe pass {pass_no} deleted 0 "
                            "photos (delete not permitted — likely frozen/restricted); "
                            "aborting wipe to prevent API hammering."
                        )
                        break
            except Exception as e:
                if is_frozen_error(e):
                    await mark_account_frozen(account_id, reason=f"photo wipe: {e}")
                    return False
                logger.warning(f"Worker {client.name}: error during old photo wipe: {e}")

            if deleted_count > 0:
                logger.info(f"Worker {client.name}: {deleted_count} old profile photo(s) wiped.")

            # --- مرحله ۲: آپلود عکس‌های جدید به صورت معکوس (عکس اول، آواتار اصلی بماند) ---
            successful_uploads = 0
            failed_uploads = []
            missing_files = []
            sorted_photos = sorted(package.photos, key=lambda p: getattr(p, "position", 0), reverse=True)

            for photo in sorted_photos:
                if _time.monotonic() > deadline:
                    logger.warning(
                        f"Worker {client.name}: photo upload exceeded time budget — stopping."
                    )
                    break

                if not os.path.exists(photo.file_path):
                    missing_files.append(photo.file_path)
                    logger.error(
                        f"Worker {client.name}: package photo file MISSING on disk: {photo.file_path} "
                        f"(package={getattr(package, 'id', '?')} position={getattr(photo, 'position', '?')}) — skipped"
                    )
                    continue

                uploaded = False
                for attempt in range(3):
                    # 🩹 چک اتصال قبل از هر تلاش — قبلاً ۳ خطای پشت‌سرهمِ
                    # «no active connection» در همان ثانیه زده می‌شد
                    if not getattr(client, "is_connected", False):
                        logger.warning(
                            f"Worker {client.name}: client disconnected before upload "
                            f"(attempt {attempt + 1}/3) — waiting 5s..."
                        )
                        await asyncio.sleep(5)
                        if not getattr(client, "is_connected", False):
                            break
                    try:
                        await client.set_profile_photo(photo=photo.file_path)
                        successful_uploads += 1
                        uploaded = True
                        break
                    except FloodWait as e:
                        wait_s = int(getattr(e, "value", 0) or 0)
                        logger.warning(f"Worker {client.name}: hit FloodWait ({wait_s}s) during photo upload. Retrying...")
                        await asyncio.sleep(wait_s + 1)
                    except Exception as e:
                        # 🧊 فریز → قطع فوری (آپلودِ مجدد روی اکانت فریز بی‌معناست)
                        if is_frozen_error(e):
                            await mark_account_frozen(account_id, reason=f"set_profile_photo: {e}")
                            return False
                        logger.error(
                            f"Worker {client.name}: set_profile_photo FAILED for "
                            f"{photo.file_path} (attempt {attempt + 1}/3): {e}"
                        )
                        await asyncio.sleep(2)
                if not uploaded:
                    failed_uploads.append(photo.file_path)

                await asyncio.sleep(random.uniform(5, 9))

            # --- مرحله ۳: راستی‌آزمایی (در برابر «کلِ» عکس‌های پکیج) ---
            expected_count = len(package.photos)
            try:
                final_photos = [p async for p in client.get_chat_photos("me")]

                if len(final_photos) >= expected_count and not failed_uploads and not missing_files:
                    logger.info(f"Worker {client.name}: profile photo rotation verified successfully.")
                    return True

                logger.error(
                    f"Worker {client.name}: profile photo rotation INCOMPLETE — "
                    f"expected {expected_count}, found {len(final_photos)} on Telegram; "
                    f"failed_uploads={failed_uploads}; missing_files={missing_files}"
                )
                return False
            except Exception:
                return False


async def perform_warmup_cycle(client: Client, account_id: int) -> None:
    """
    شبیه‌سازی رفتار انسانی با اسکن کردن لیست چت‌ها و سین کردن پیام‌های خوانده نشده.
    (اضافه شدن منطق خروج از فریز برای اکانت‌های صفر)
    """
    try:
        from utils.freeze_guard import is_account_frozen
        if await is_account_frozen(account_id):
            logger.info(f"Worker user_{account_id}/ warm-up SKIPPED — account is FROZEN.")
            return

        dialogs_count = 0
        read_count = 0
        
        async for dialog in client.get_dialogs(limit=15):
            dialogs_count += 1
            if dialog.unread_messages_count > 0:
                await client.read_chat_history(dialog.chat.id)
                read_count += 1
                await asyncio.sleep(random.uniform(1, 3)) 
        
        # --- منطق فاز ۳: بیدار کردن اکانت‌های خام ---
        if dialogs_count == 0:
            logger.info(f"Worker user_{account_id}/ is completely raw. Starting anti-freeze routine.")
            
            # انتخاب تصادفی یک ربات معتبر و رسمی تلگرام
            safe_bots = ["gif", "pic", "sticker", "bing", "youtube"]
            target_bot = random.choice(safe_bots)
            
            # مرحله اول: استارت ربات
            await client.send_message(target_bot, "/start")
            await asyncio.sleep(random.uniform(2, 4))
            
            # مرحله دوم: یک سرچ یا تعامل ساده انسانی
            queries = ["cat", "hello", "smile", "car", "nature", "funny"]
            await client.send_message(target_bot, random.choice(queries))
            
            logger.info(f"Worker user_{account_id}/ successfully interacted with @{target_bot}.")
            
        elif read_count > 0:
            logger.info(f"Worker user_{account_id}/ warm-up complete. Read {read_count} chats.")

        # [FIX-online-24h] بعد از warm-up صراحتاً آفلاین شو تا فلگ آنلاین چسبیده باقی نماند
        await go_offline(client, account_id)
            
    except Exception as e:
        logger.debug(f"Worker user_{account_id}/ warm-up error: {e}")

# ==========================================
# 🩹 FIX (وضعیت اشتباه «مسدود») — تشخیص پیام‌های @SpamBot
# قبلاً بعد از ارسال /start فقط ۲.۵ ثانیه صبر می‌شد و history[0] خوانده می‌شد؛
# اگر جواب اسپم‌بات دیر می‌رسید، «پیام /start خودِ اکانت» به‌عنوان پاسخ تفسیر
# می‌شد و چون «Good news» نداشت، اکانت به‌اشتباه محدود/مسدود ثبت می‌شد
# (در حالی که ورکر کاملاً سالم بود و می‌توانست به بقیه پیام بدهد).
# ==========================================
async def _latest_spambot_reply(
    client: Client,
    after_message_id: int,
    timeout_seconds: float = 12.0,
    poll_interval: float = 1.5,
):
    """
    جدیدترین پیامِ «واقعیِ» @SpamBot با شناسه‌ی بزرگ‌تر از after_message_id.
    - پیام‌های خروجی (outgoing) خودِ اکانت هرگز در نظر گرفته نمی‌شوند.
    - تا timeout_seconds به‌صورت پله‌ای poll می‌شود تا پاسخ دیررس اسپم‌بات هم گرفته شود.
    - اگر هیچ پاسخی نرسید None برمی‌گرداند (فراخوان‌دهنده باید آن را «بررسی ناموفق»
      بپندارد، نه «محدود شده»).
    """
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout_seconds
    best = None
    while True:
        try:
            async for msg in client.get_chat_history("spambot", limit=8):
                if getattr(msg, "outgoing", False):
                    continue
                if after_message_id and msg.id <= after_message_id:
                    continue
                if best is None or msg.id > best.id:
                    best = msg
        except Exception as e:
            logger.debug(f"spambot history read failed: {e}")
        if best is not None:
            return best
        if loop.time() >= deadline:
            return None
        await asyncio.sleep(poll_interval)


# نشانه‌های پیام «آزاد بودن» اکانت در پاسخ @SpamBot (کوچک/بزرگ غیرحساس)
_SPAMBOT_FREE_MARKERS = (
    "good news",
    "free as a bird",
    "free from any limitations",
    "no limits are currently applied",
    "هیچ محدودیتی",
)

# الگوهای مختلف تاریخ انقضای محدودیت در پیام @SpamBot
_SPAMBOT_UNTIL_PATTERNS = (
    (r"until (\d{1,2} [a-zA-Z]+ \d{4}, \d{1,2}:\d{2}:\d{2}) UTC", "%d %b %Y, %H:%M:%S"),
    (r"until (\d{1,2} [a-zA-Z]+ \d{4}, \d{1,2}:\d{2}) UTC", "%d %b %Y, %H:%M"),
    (r"until (\d{1,2} [a-zA-Z]+ \d{4} at \d{1,2}:\d{2})", "%d %b %Y at %H:%M"),
    (r"until (\d{1,2} [a-zA-Z]+ \d{4}, \d{1,2}:\d{2})", "%d %b %Y, %H:%M"),
)


def parse_spambot_until_date(text: str) -> Optional[datetime]:
    """استخراج تاریخ پایان محدودیت از متن پیام @SpamBot (چند فرمت پشتیبانی می‌شود)."""
    for pattern, fmt in _SPAMBOT_UNTIL_PATTERNS:
        date_match = re.search(pattern, text)
        if date_match:
            try:
                parsed = datetime.strptime(date_match.group(1), fmt)
                return parsed.replace(tzinfo=timezone.utc)
            except Exception:
                continue
    return None


def is_spambot_free_text(text: str) -> bool:
    """آیا این متن پیامِ «بدون محدودیت» از @SpamBot است؟"""
    lowered = (text or "").lower()
    return any(marker in lowered for marker in _SPAMBOT_FREE_MARKERS)


async def check_spambot_status(client: Client, account_id: int) -> Optional[dict]:
    """
    بررسی وضعیت محدودیت اکانت از طریق SpamBot.
    ارسال /start، خواندن پاسخِ واقعی ربات (نه اکوی /start خودمان)، فشردن دکمه inline
    (در صورت وجود) برای دیدن جزئیات/تاریخ محدودیت و استخراج تاریخ انقضا.

    خروجی:
      {"text": str, "restricted": bool, "until": Optional[datetime]}
      None یعنی «بررسی ناموفق» — فراخوان‌دهنده نباید وضعیت اکانت را تغییر دهد.
    """
    try:
        try:
            await client.unblock_user("spambot")
        except Exception:
            pass

        sent = await client.send_message("spambot", "/start")
        base_id = sent.id if sent is not None else 0

        reply = await _latest_spambot_reply(client, base_id)
        if reply is None:
            logger.warning(
                f"Worker user_{account_id}/ spambot did not answer in time — check skipped (no state change)."
            )
            return None

        # اگر دکمه‌ای وجود دارد (مثلاً "But I can't message some people!") آن را فشار بده
        # تا جزئیات واقعی و تاریخ پایان محدودیت (در صورت وجود) نمایش داده شود.
        # 🟢 رفع باگ کرش در برخورد با کیبورد معمولی (ReplyKeyboardMarkup) اسپم‌بات
        if reply.reply_markup and getattr(reply.reply_markup, "inline_keyboard", None):
            try:
                callback_data = reply.reply_markup.inline_keyboard[0][0].callback_data
                if callback_data:
                    await client.request_callback_answer(
                        chat_id="spambot",
                        message_id=reply.id,
                        callback_data=callback_data
                    )
                    # رفرش کردن آخرین پیام بعد از تعامل — فقط پیام جدیدتر از پاسخ قبلی
                    follow_up = await _latest_spambot_reply(client, reply.id, timeout_seconds=8.0)
                    if follow_up is not None:
                        reply = follow_up
            except Exception as btn_err:
                logger.debug(f"Worker user_{account_id}/ spambot button press failed: {btn_err}")

        text = reply.text or ""
        is_restricted = not is_spambot_free_text(text)
        until_date = parse_spambot_until_date(text) if is_restricted else None

        # اختیاری: ارسال Appeal اتوماتیک در صورت محدودیت و روشن بودن تنظیمات
        from config import config
        if is_restricted and getattr(config, "SPAMBOT_AUTO_APPEAL", False):
            # ارسال متن رندومایز شده اسپینتکس
            appeal_text = parse_spintax(
        "{Dear administrator|Hello Telegram Support|Hi Support Team},\n"
        "{There is some problem with my telegram account|My account has been limited unfairly|I am unable to send messages to non-contacts}.\n"
        "{Someone reported me wrongly|I believe this is a false positive by the algorithm|I haven't done anything against the terms of service}.\n"
        "{Would you please fix the problem|Please review and remove this limitation|Kindly check my account status}.\n"
        "{I look forward to hearing from you|Thanks for your time|Best regards}."
    )
            await client.send_message("spambot", appeal_text)

        logger.info(f"Worker user_{account_id}/ spambot check complete. Restricted: {is_restricted}")
        return {
            "text": text,
            "restricted": is_restricted,
            "until": until_date
        }
    except Exception as e:
        logger.error(f"Spambot check failed for user_{account_id}/: {e}")
        return None
    

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


# ==========================================
# 🩹 FIX (آخرین بازدید اکانت‌ها): حضور دوره‌ای
# اتصالِ صرفِ کلاینت (Pyrogram) «آنلاین» حساب نمی‌شود؛ بدون فراخوانیِ
# account.updateStatus، آخرین بازدیدِ اکانتِ بیکار در همان زمانِ آخرین
# فعالیتِ واقعی می‌ماند (مثلاً «یک هفته پیش») و ارسال پیام از طرف چنین
# اکانتی پرچم قرمزِ ربات‌بودن برای تلگرام و تارگت‌هاست.
# ==========================================
# [FIX-online-24h] مدیریت حضور (Presence)
# مشکل قبلی: حلقه‌ی presence هر ۲۰ تا ۴۵ دقیقه UpdateStatus(offline=False)
# صدا می‌زد و چون ورکرها ۲۴/۷ متصل بودند، اکانت‌ها همیشه «آنلاین» دیده
# می‌شدند — هیچ انسانی ۲۴ ساعته آنلاین نیست؛ هم برای الگوریتم تلگرام پرچم
# قرمز است و هم تارگت‌ها شک می‌کنند که با ربات طرف هستند.
# راهکار جدید: پیش‌فرض خاموش (PRESENCE_KEEPER_ENABLED=false).
# در صورت روشن‌کردن، فقط «چشمک آنلاین» با چرخه‌ی ۳ تا ۶ ساعته زده می‌شود:
# آنلاین (چند ثانیه) → آفلاین. نتیجه: «آخرین بازدید» تازه می‌ماند، اما
# اکانت دیگر ۲۴ ساعته آنلاین نیست.
# ==========================================
async def keep_presence_alive(client: Client, account_id: int) -> None:
    """بروزرسانی وضعیت آنلاین (last seen) یک ورکر — best-effort."""
    try:
        await client.invoke(raw.functions.account.UpdateStatus(offline=False))
        logger.debug(f"Worker user_{account_id}/ presence refreshed (online).")
    except Exception as e:
        logger.debug(f"Worker user_{account_id}/ presence refresh failed: {e}")


async def go_offline(client: Client, account_id: Optional[int] = None) -> None:
    """
    [FIX-online-24h] وضعیت اکانت را صراحتاً «آفلاین» می‌کند.

    بعد از هر فعالیت واقعی (warm-up / chunk ارسال) صدا زده می‌شود تا فلگِ
    آنلاینِ چسبنده از بین برود و اکانت در پیوی تارگت‌ها «آخرین بازدید اخیر»
    نشان داده شود، نه «آنلاین» دائمی. (مثل گوشی که بعد از قفل‌شدن صفحه
    از آنلاین خارج می‌شود.)
    """
    try:
        await client.invoke(raw.functions.account.UpdateStatus(offline=True))
        who = f"user_{account_id}/" if account_id is not None else str(client.name)
        logger.debug(f"Worker {who} explicitly set offline.")
    except Exception as e:
        logger.debug(f"Worker {getattr(client, 'name', '?')} go_offline failed: {e}")


async def presence_keeper_loop(worker_pool: dict) -> None:
    """
    حلقه‌ی پس‌زمینه (اختیاری — پیش‌فرض خاموش):

    اگر PRESENCE_KEEPER_ENABLED روشن باشد، هر ۳ تا ۶ ساعت برای هر ورکرِ متصل
    یک «چشمک آنلاین» می‌زند: آنلاین (۱۰ تا ۳۰ ثانیه) → آفلاین.
    این کار «آخرین بازدید» را تازه نگه می‌دارد بدون آنلاینِ دائمی.
    اگر فلگ خاموش باشد، حلقه بلافاصله و تمیز خارج می‌شود.
    """
    from config import config

    if not getattr(config, "PRESENCE_KEEPER_ENABLED", False):
        logger.info(
            "Presence Keeper disabled (PRESENCE_KEEPER_ENABLED=false) — "
            "workers will NOT be forced online 24/7."
        )
        return

    logger.info(
        "Presence Keeper Loop started (blip mode: online %s-%ss every %s-%sh)."
        % (
            int(config.PRESENCE_BLIP_MIN_SECONDS),
            int(config.PRESENCE_BLIP_MAX_SECONDS),
            config.PRESENCE_CYCLE_MIN_HOURS,
            config.PRESENCE_CYCLE_MAX_HOURS,
        )
    )
    while True:
        try:
            await asyncio.sleep(
                random.uniform(
                    config.PRESENCE_CYCLE_MIN_HOURS,
                    config.PRESENCE_CYCLE_MAX_HOURS,
                )
                * 3600
            )
            for account_id, client in list(worker_pool.items()):
                if getattr(client, "is_connected", False):
                    # چشمک آنلاین: آنلاین → مکث کوتاه → آفلاین
                    await keep_presence_alive(client, account_id)
                    await asyncio.sleep(
                        random.uniform(
                            config.PRESENCE_BLIP_MIN_SECONDS,
                            config.PRESENCE_BLIP_MAX_SECONDS,
                        )
                    )
                    await go_offline(client, account_id)
                    # پخش‌کردن زمان فراخوانی‌ها بین ورکرها (رفتار انسانی)
                    await asyncio.sleep(random.uniform(3, 15))
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Presence keeper loop error: {e}")
            await asyncio.sleep(60)
