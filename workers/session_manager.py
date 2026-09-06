import asyncio
import logging
import random
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional
from urllib.parse import urlparse, unquote
import asyncio
from typing import Set



from pyrogram import Client
from pyrogram.errors import (
    AuthKeyUnregistered,
    UserDeactivated,
    UserDeactivatedBan,
    Unauthorized
)
from pyrogram.handlers import MessageHandler
from pyrogram import filters

from aiogram import Bot
from sqlalchemy import select, update, func 
from sqlalchemy.orm import selectinload
from sqlalchemy.ext.asyncio import AsyncSession

from database.models import Account, Proxy, APIKey, Admin, GlobalSettings, ProfilePhotoPackage
from database.engine import async_session
from config import config
from utils.advanced_anti_ban import (
    randomize_profile,
    rotate_profile_photos,
    terminate_other_sessions,
)
from utils.crm_catcher import incoming_message_handler
from utils.seen_watcher import attach_seen_listener  # 🧠 جریان هوشمند (Smart Flow): شنود «سین» تارگت
from utils.crypto import decrypt_session 
from workers.sender import _get_redis
logger = logging.getLogger(__name__)
# ==========================================
# CONSTANTS: SPOOFING DATA
# ==========================================
DEVICE_MODELS = [
    "iPhone 13 Pro Max", "iPhone 14 Plus", "iPhone 15 Pro", 
    "Samsung Galaxy S22 Ultra", "Samsung Galaxy S23", 
    "Xiaomi 13 Pro", "OnePlus 11", "Google Pixel 7"
]
SYSTEM_VERSIONS = ["15.0", "16.0", "13.0", "12.0", "14.0", "17.0"]
APP_VERSIONS = ["9.6.5", "9.7.0", "9.5.2", "10.0.1", "10.1.3", "10.2.0"]
_background_tasks: Set[asyncio.Task] = set()
# ==========================================
# 🔥 فاز ۶ (R7): گرم‌شدن اجباری اکانت تازه-لاگین
# ==========================================
def warmup_hours() -> int:
    """
    🔥 فاز ۶ (R7): طول دوره‌ی گرم‌شدن اکانت تازه-لاگین (ساعت).
    کلید config: MIN_WARMUP_HOURS (پیش‌فرض ۲۴).
    (دوقلوی این تابع در workers/task_queue.py::_warmup_hours — برای پرهیز از
    وابستگی import جدید بین دو ماژول در هر دو تعریف شده است.)
    نکته: تمایز ۴۸ ساعته‌ی «اکانت تازه» نیازمند فیلد سن/نوع اکانت در مدل است که
    موجود نیست → فعلاً یک مقدار واحد برای همه‌ی اکانت‌ها (نقطه‌ی الحاق آینده).
    """
    try:
        return max(0, int(config.MIN_WARMUP_HOURS))
    except (AttributeError, TypeError, ValueError):
        return 24

# ==========================================
# GLOBAL WORKER POOL
# ==========================================
worker_pool: Dict[int, Client] = {}


# ==========================================
# UTILITY: PROXY PARSER
# ==========================================
def parse_proxy_string(proxy_string: str) -> Optional[dict]:
    if not proxy_string:
        return None
        
    try:
        parsed = urlparse(proxy_string)
        if not parsed.hostname or not parsed.port:
            logger.error("Missing hostname or port in proxy string.")
            return None
            
        scheme = parsed.scheme.lower()
        if scheme not in ["socks4", "socks5"]:
            logger.error(f"Unsupported proxy scheme '{scheme}'. Pyrogram requires socks4 or socks5.")
            return None
            
        # 🛡 فاز ۵ (BUG-14c): urlparse اجزای userinfo را URL-encoded برمی‌گرداند.
        # پسوردِ حاوی @ : / % و… به‌صورت %40/%3A/%2F در رشته ذخیره می‌شود؛ بدون
        # unquote همان رشته‌ی encode شده به Pyrogram پاس می‌شود و اتصال بی‌دلیل
        # fail می‌شود. گارد «is not None» سمانتیک قبلی را حفظ می‌کند (None→None،
        # ""→"" و از unquote(None) که TypeError می‌دهد جلوگیری می‌کند).
        return {
            "scheme": scheme,
            "hostname": parsed.hostname,
            "port": int(parsed.port),
            "username": unquote(parsed.username) if parsed.username is not None else None,
            "password": unquote(parsed.password) if parsed.password is not None else None
        }
    except Exception as e:
        logger.error(f"Failed to parse proxy string '{proxy_string}': {e}")
        return None


# ==========================================
# 🧲 PROXY CLAIM LOGIC (فاز ۵ — BUG-14a/14b)
# تخصیص sticky + سقف اکانت per proxy + claim اتمیک
# ==========================================
# تعداد کاندیدی که در هر claim امتحان می‌شوند؛ اگر اولی‌ها هم‌زمانی رفته باشند،
# کاندید بعدی امتحان می‌شود (رقابت claim با rowcount حل می‌شود، نه با قفل جدول).
PROXY_CLAIM_CANDIDATES = 10


def _proxy_cap() -> int:
    """سقف اکانت روی هر پراکسی — همیشه >= 1."""
    return max(1, config.MAX_ACCOUNTS_PER_PROXY)


async def release_proxy_slot(session: AsyncSession, proxy_string: str) -> None:
    """
    آزادسازی یک اسلاتِ مصرفِ پراکسی (کاهش in_use با گاردِ عدم منفی شدن).
    🛡 BUG-30: commit نمی‌زند — کنترل تراکنش با caller است.
    """
    stmt = (
        update(Proxy)
        .where(Proxy.proxy_string == proxy_string, Proxy.in_use > 0)
        .values(in_use=Proxy.in_use - 1)
        .execution_options(synchronize_session=False)
    )
    await session.execute(stmt)


async def reconcile_proxy_usage(session: AsyncSession) -> None:
    """
    🧲 فاز ۵ (BUG-14): بازسازی شمارنده‌ی in_use از منبع حقیقت
    (accounts.proxy_string). شمارنده در برابر کرش، rollback گمشده و ویرایش
    دستی DB خودترمیم می‌شود. اکانت‌های شمول: غیر بن‌شده + دارای session
    (دقیقاً همان شرط initialize_workers) که sticky روی پراکسی خودشان هستند.
    ⚠️ commit داخلی دارد؛ باید قبل از هر تغییر pending دیگری روی session
    صدا زده شود (اولین statement در initialize_workers).
    """
    active_account_count = (
        select(func.count())
        .select_from(Account)
        .where(
            Account.proxy_string == Proxy.proxy_string,
            Account.is_banned == False,
            Account.session_string.isnot(None),
        )
        .correlate(Proxy)
        .scalar_subquery()
    )
    stmt = (
        update(Proxy)
        .values(in_use=active_account_count)
        .execution_options(synchronize_session=False)
    )
    await session.execute(stmt)
    await session.commit()

    # هشدار ظرفیتِ ردشده: bindingهای قدیمی sticky می‌مانند (جابه‌جایی خودسرانه
    # یعنی تغییر IP اکانتِ بالغ = ریسک)؛ فقط claimهای جدید از این پراکسی‌ها
    # دوری می‌کنند تا به‌مرور با چرخش طبیعی تخلیه شوند.
    over_stmt = select(Proxy.id, Proxy.in_use).where(
        Proxy.is_active == True, Proxy.in_use > _proxy_cap()
    )
    over = (await session.execute(over_stmt)).all()
    if over:
        logger.warning(
            f"Proxy capacity exceeded (MAX_ACCOUNTS_PER_PROXY={_proxy_cap()}): "
            + ", ".join(f"proxy_id={pid} in_use={used}" for pid, used in over)
            + " — existing bindings are sticky; new claims avoid these proxies."
        )


async def claim_proxy_for_account(
    session: AsyncSession, account_id: Optional[int] = None
) -> Optional[str]:
    """
    🧲 فاز ۵ (BUG-14a/14b) + فاز ۴ (رفع آنتی‌پترن لاگین): تخصیص اتمیکِ پراکسیِ
    با ظرفیت آزاد — دو حالت فراخوانی (با account_id برای چرخش ورکر؛ با
    account_id=None برای جریان لاگین که هنوز Accountی ساخته نشده).

    ۱) sticky (با account_id): اکانت روی پراکسی فعلی‌اش می‌ماند؛ فقط هنگام چرخش، اسلاتِ پراکسی
       قبلی در همین تراکنش آزاد می‌شود.
    ۲) کاندیدها «بدون FOR UPDATE» خوانده می‌شوند — کوئری قبلی
       (ORDER BY RAND() ... FOR UPDATE SKIP LOCKED) عملاً همه‌ی ردیف‌های فعال را
       تا commit قفل می‌کرد (sort روی همه‌ی ردیف‌ها) و رزرو واقعی هم نبود.
    ۳) رزرو واقعی با UPDATE اتمیک روی سطرِ تنها:
       UPDATE proxies SET in_use = in_use + 1 WHERE id = ? AND in_use < cap
       شرط WHERE با current-read (آخرین مقدار commit شده) ارزیابی می‌شود →
       دو claim همزمان هرگز سقف را رد نمی‌کنند. rowcount == 1 یعنی مالِ ما؛
       0 یعنی رقابت/ظرفیت پر → کاندید بعدی.
    ۴) کاندید قبل از claim با parse_proxy_string اعتبارسنجی می‌شود تا اکانت
       هرگز به پراکسی غیرقابل‌پارس bind نشود (حفظ رفتار «جایگزینی فقط با
       parse مجدد» — caller باز هم parse مجدد می‌کند).
    ۵) binding جدید (accounts.proxy_string) در همان تراکنش ثبت می‌شود.
       🛡 BUG-30: commit با caller است؛ rollback اتمیک claim را هم برمی‌گرداند.

    خروجی: proxy_string در صورت موفقیت؛ None یعنی ظرفیت آزادی نیست. در حالت
    account_id ارائه‌شده، اکانت روی binding فعلی‌اش می‌ماند و هیچ تغییری در
    in_use رخ نمی‌دهد. در حالت account_id=None، هیچ تغییر پایداری روی Account
    رخ نداده (فقط in_use + ۱ که caller باید در صورت شکست آزاد کند).
    """
    cap = _proxy_cap()

    # پراکسی فعلی اکانت — فقط در حالت چرخش (account_id ارائه شده) معنا دارد.
    # در جریان لاگین (account_id is None) همیشه None است و آزادسازی رخ نمی‌دهد.
    old_proxy_str: Optional[str] = None
    if account_id is not None:
        old_proxy_str = await session.scalar(
            select(Account.proxy_string).where(Account.id == account_id)
        )

    # کاندیدها: فعال + ظرفیت آزاد + پراکسیِ فعلی خودِ اکانت نیست (در حالت چرخش)
    cand_filters = [Proxy.is_active == True, Proxy.in_use < cap]
    if old_proxy_str:
        cand_filters.append(Proxy.proxy_string != old_proxy_str)
    cand_stmt = (
        select(Proxy.id, Proxy.proxy_string)
        .where(*cand_filters)
        .order_by(func.rand())
        .limit(PROXY_CLAIM_CANDIDATES)
    )
    candidates = (await session.execute(cand_stmt)).all()

    claimed_proxy_str: Optional[str] = None
    for proxy_id, proxy_str in candidates:
        # اعتبارسنجی قبل از claim — پراکسی خراب نه اسلات می‌گیرد نه bind می‌شود
        if not parse_proxy_string(proxy_str):
            logger.error(f"Proxy candidate id={proxy_id} is malformed - marking failed and skipping.")
            try:
                await mark_proxy_failed(session, proxy_str)
            except Exception as mark_err:
                logger.error(f"Failed to mark malformed proxy candidate: {mark_err}")
            continue

        # 🧲 رزرو واقعی — UPDATE شرطیِ اتمیک + چک rowcount
        claim_stmt = (
            update(Proxy)
            .where(Proxy.id == proxy_id, Proxy.in_use < cap)
            .values(in_use=Proxy.in_use + 1)
            .execution_options(synchronize_session=False)
        )
        result = await session.execute(claim_stmt)
        if result.rowcount == 1:
            claimed_proxy_str = proxy_str
            break
        # rowcount == 0 → همزمانی: ظرفیت در لحظه‌ی UPDATE پر بود → کاندید بعدی

    if claimed_proxy_str is None:
        return None

    # آزادسازی اسلات پراکسی قبلی + ثبت binding جدید — فقط در حالت چرخش
    # (account_id ارائه شده). در جریان لاگین (account_id is None) هیچ Accountی
    # برای bind کردن وجود ندارد؛ caller proxy_string را در FSM ذخیره می‌کند
    # و در finalize_login_and_save روی Account تنظیم می‌کند.
    # 🛡 BUG-30: commit با caller است.
    if account_id is not None:
        if old_proxy_str:
            await release_proxy_slot(session, old_proxy_str)
        await session.execute(
            update(Account).where(Account.id == account_id).values(proxy_string=claimed_proxy_str)
        )
    return claimed_proxy_str

async def mark_proxy_failed(session: AsyncSession, proxy_string: str) -> None:
    """
    Increments fail count for a proxy and deactivates it if it fails too often.

    🛡 فاز ۳ (BUG-30): commit درون‌تابعی حذف شد. قبلاً این commit تراکنشِ بازِ
    caller را بی‌صدا می‌بست و تغییرات pending مربوط به caller را زودتر از موعد
    ثبت می‌کرد (الگوی خطرناک SQLAlchemy). کنترل تراکنش حالا با caller است:
    تغییر fail_count/is_active روی همان sessionِ caller می‌ماند و با commit بعدیِ
    خود caller ثبت می‌شود (در چرخش پراکسی، همان commitِ آپدیت proxy_stringِ
    اکانت این تغییر را هم در همان تراکنش ثبت می‌کند).
    """
    stmt = select(Proxy).where(Proxy.proxy_string == proxy_string)
    result = await session.execute(stmt)
    proxy_obj = result.scalar_one_or_none()
    
    if proxy_obj:
        proxy_obj.fail_count += 1
        if proxy_obj.fail_count >= 5:
            proxy_obj.is_active = False
            logger.warning(f"Proxy {proxy_string} marked as inactive due to high failure rate.")
        # 🛡 BUG-30: دیگر اینجا commit نمی‌زنیم — کنترل تراکنش با caller است.


# ==========================================
# 🔒 IP LEAK GUARD (یکدست‌شده در همه‌ی مسیرها)
# ==========================================
def direct_ip_fallback_enabled(account_id: int) -> bool:
    """
    🔒 سیاست امنیتی «قطع به‌جای افشای IP سرور»:
    اتصال بدون پراکسی فقط با فال‌بک صریح FALLBACK_TO_DIRECT_IP=true مجاز است.
    پیش‌فرض False است؛ یعنی هیچ اکانتی هرگز با IP مستقیم سرور اجرا نمی‌شود.
    """
    if not config.FALLBACK_TO_DIRECT_IP:
        return False
    # ⚠️ لاگ هشدار بزرگ: فال‌بک خطرناک فعال است (ریسک Chain Ban همه‌ی اکانت‌ها)
    logger.critical(
        "█" * 62 + "\n"
        f"⚠️  FALLBACK_TO_DIRECT_IP=TRUE → Worker {account_id} بدون پراکسی و با "
        "IP مستقیم سرور اجرا می‌شود!\n"
        "⚠️  ریسک: Chain Ban همه‌ی اکانت‌ها و مسدود شدن IP سرور.\n"
        "⚠️  در پروداکشن حتماً FALLBACK_TO_DIRECT_IP=false باشد.\n"
        + "█" * 62
    )
    return True


# ==========================================
# ⚠️ ADMIN ALERTS (هشدار Realtime به ادمین)
# ==========================================
# جلوگیری از اسپم هشدار تکراری: اولین رخدادِ هر اکانت «بلافاصله» ارسال می‌شود؛
# تکرارهای بعدیِ همان اکانت (مثلاً از حلقه‌ی Reconnect) تا این بازه سرکوب می‌گردند.
NO_PROXY_ALERT_THROTTLE_SECONDS = 30 * 60
_last_no_proxy_alert_at: Dict[int, float] = {}


def _no_proxy_alert_throttled(account_id: int) -> bool:
    """True اگر هشدار «اتمام پراکسی»ی همین اکانت به‌تازگی ارسال شده باشد."""
    now = time.monotonic()
    if now - _last_no_proxy_alert_at.get(account_id, 0.0) < NO_PROXY_ALERT_THROTTLE_SECONDS:
        return True
    _last_no_proxy_alert_at[account_id] = now
    return False


async def notify_admins(bot: Optional[Bot], text: str) -> int:
    """
    ارسال هشدار/گزارش فوری به ادمین اصلی (ADMIN_ID از config) و ساب‌ادمین‌های
    جدول Admin. بهترین تلاش (best-effort) است و خطای ارسال هرگز فلوی اصلی را
    نمی‌شکند. (این تابع اینجا تعریف شده تا health_checker بتواند بدون ایجاد
    import دور، از آن استفاده کند.) خروجی: تعداد ارسال‌های موفق.
    """
    if bot is None:
        return 0

    target_admins = set()
    if config.ADMIN_ID and config.ADMIN_ID != 0:
        target_admins.add(config.ADMIN_ID)

    try:
        async with async_session() as db_session:
            stmt = select(Admin.telegram_id)
            result = await db_session.execute(stmt)
            for admin_id in result.scalars().all():
                target_admins.add(admin_id)
    except Exception as db_err:
        logger.error(f"notify_admins: failed to fetch sub-admins: {db_err}")

    if not target_admins:
        logger.warning(
            "notify_admins: هیچ مقصدی برای هشدار امنیتی پیدا نشد - "
            "ADMIN_ID را در فایل .env تنظیم کنید!"
        )
        return 0

    sent = 0
    for admin_tg_id in target_admins:
        try:
            await bot.send_message(chat_id=admin_tg_id, text=text)
            sent += 1
        except Exception as send_err:
            logger.error(f"notify_admins: failed to notify admin {admin_tg_id}: {send_err}")
    return sent


# ==========================================
# 🏭 WORKER CLIENT FACTORY (منبع واحد ساخت کلاینت)
# ==========================================
async def build_worker_client(
    account: Account,
    session: AsyncSession,
    proxy_dict: Optional[dict],
    prefetched_apis: Optional[Dict[int, APIKey]] = None,
) -> Optional[Client]:
    """
    ساخت کلاینت Pyrogram ورکر در یک نقطه‌ی واحد تا گارد پراکسی در هیچ مسیری
    قابل دور زدن نباشد. منطق رمزنگاری سشن (decrypt_session)، هندلر CRM و
    شنود «سین» دقیقاً همان نسخه‌ی قبل و دست‌نخورده است.
    """
    try:
        # استخراج API اختصاصی اکانت (در صورت نبود، API سراسری سیستم)
        api_obj = None
        if account.api_id:
            if prefetched_apis is not None:
                api_obj = prefetched_apis.get(account.api_id)
            else:
                stmt_linked_api = select(APIKey).where(APIKey.id == account.api_id)
                api_obj = await session.scalar(stmt_linked_api)
        worker_api_id = int(api_obj.api_id) if api_obj else int(config.API_ID)
        worker_api_hash = str(api_obj.api_hash) if api_obj else str(config.API_HASH)

        client = Client(
            name=f"worker_acc_{account.id}",
            session_string=decrypt_session(account.session_string),
            api_id=worker_api_id,
            api_hash=worker_api_hash,
            proxy=proxy_dict,
            in_memory=True,
            device_model=account.device_model or random.choice(DEVICE_MODELS),
            system_version=account.system_version or random.choice(SYSTEM_VERSIONS),
            app_version=account.app_version or random.choice(APP_VERSIONS),
            lang_code="en"
        )

        # اتصال هندلر CRM (منطق CRM دست‌نخورده)
        client.add_handler(
            MessageHandler(
                incoming_message_handler, 
                filters.private & ~filters.me
            )
        )

        # 🧠 جریان هوشمند (Smart Flow): نصب شنود «سین» تارگت
        attach_seen_listener(client)

        return client
    except Exception as e:
        logger.error(f"Failed to build worker client for account {account.id}: {e}")
        return None


# ==========================================
# 🖼 PHOTO PACKAGE: PROFILE PHOTO ROTATION
# ==========================================
async def _get_photo_package_for_account(
    session: AsyncSession, account_id: int
) -> Optional[ProfilePhotoPackage]:
    """
    واکشی پکیج عکس پروفایلِ متصل به اکانت (در صورت وجود) به‌همراه عکس‌های
    مرتب‌شده بر اساس position (selectinload → امن در context async، بدون lazy load).
    """
    try:
        stmt = (
            select(ProfilePhotoPackage)
            .join(Account, Account.photo_package_id == ProfilePhotoPackage.id)
            .where(Account.id == account_id)
            .options(selectinload(ProfilePhotoPackage.photos))
        )
        return await session.scalar(stmt)
    except Exception as e:
        logger.error(f"Failed to fetch photo package for user_{account_id}/: {e}")
        return None


async def _safe_rotate_profile_photos(client: Client, package: ProfilePhotoPackage) -> None:
    """
    try/except جداگانه مطابق الزام: شکست چرخش عکس پروفایل نباید روی آپدیت
    نام/بیو (تسک randomize_profile) اثر بگذارد. خطاهای درون‌گام نیز داخل خود
    rotate_profile_photos مهار می‌شوند؛ این لایه فقط گارد نهایی تسک است.
    """
    try:
        await rotate_profile_photos(client, package)
    except Exception as e:
        logger.warning(f"Worker {client.name}: profile photo rotation failed: {e}")


async def apply_photo_package_now(account_id: int) -> bool:
    """B13: اعمال فوری پکیج عکسِ فعلیِ اکانت روی ورکرِ متصل.
    فقط وقتی auto_set_photo روشن باشد، ورکر متصل باشد و پکیجی متصل شده باشد.
    True یعنی همین حالا اعمال شد."""
    client = worker_pool.get(account_id)
    if not client or not client.is_connected:
        return False
        
    async with async_session() as session:
        global_settings = await session.scalar(select(GlobalSettings).limit(1))
        if not global_settings or not getattr(global_settings, "auto_set_photo", False):
            return False
            
        photo_package = await _get_photo_package_for_account(session, account_id)
        if not photo_package or not photo_package.photos:
            return False
            
        await _safe_rotate_profile_photos(client, photo_package)
        return True


# ==========================================
# WORKER INITIALIZATION (با پشتیبانی از تنظیمات On/Off)
# ==========================================
async def initialize_workers(session: AsyncSession, bot: Optional[Bot] = None) -> None:
    """
    🔒 فاز ۵: گارد نشت IP اینجا هم (یکدست با start_single_worker) اعمال می‌شود:
    اکانتِ بدون proxy_string معتبر skip می‌شود، لیست آن لاگ می‌گردد و از طریق
    نمونه‌ی bot به ادمین گزارش داده می‌شود. تنها استثنا: FALLBACK_TO_DIRECT_IP=true
    """
    # 🧲 فاز ۵ (BUG-14): reconcile شمارنده‌ی in_use از منبع حقیقت
    # (accounts.proxy_string) — خودترمیمی بعد از کرش/ویرایش دستی DB.
    # اولین statement روی session است تا commit داخلی‌اش تراکنشِ بازِ
    # caller را نبندد (الگوی BUG-30).
    await reconcile_proxy_usage(session)

    stmt = select(Account).where(Account.is_banned == False)
    result = await session.execute(stmt)
    accounts = result.scalars().all()

    # 🆔 فاز ۵ (R5): هشدار خوشه‌ی API_ID مشترک — اکانت‌های بدون API اختصاصی
    # همه به config.API_ID سراسری برمی‌گردند؛ بیش از N اکانت روی یک api_id =
    # خوشه تشخیصی بزرگ (ریسک بن دسته‌ای). طبق scope این فاز فقط «هشدار/گزارش»؛
    # پیاده‌سازی per-account API → فاز آینده (گزارش طراحی در همین پاسخ).
    shared_api_accounts = [acc for acc in accounts if acc.session_string and not acc.api_id]
    if len(shared_api_accounts) > config.SHARED_API_ID_WARN_THRESHOLD:
        shared_ids = ", ".join(str(acc.id) for acc in shared_api_accounts)
        logger.warning(
            f"R5 API-cluster: {len(shared_api_accounts)} account(s) fall back to the GLOBAL "
            f"API_ID (config.API_ID) - threshold {config.SHARED_API_ID_WARN_THRESHOLD} exceeded. "
            f"Account IDs: [{shared_ids}]"
        )
        if bot is not None:
            try:
                await notify_admins(
                    bot,
                    "🆔 <b>هشدار: خوشه‌ی API_ID مشترک</b>\n\n"
                    f"تعداد <b>{len(shared_api_accounts)}</b> اکانت API اختصاصی ندارند و همه به "
                    "API_ID سراسری سیستم برمی‌گردند؛ تلگرام این اکانت‌ها را به‌عنوان یک خوشه‌ی "
                    "مرتبط می‌بیند (ریسک بن دسته‌ای).\n\n"
                    "💡 توصیه: به اکانت‌ها (یا گروه‌های کوچک) API_ID/API_HASH اختصاصی تخصیص دهید."
                )
            except Exception as notify_err:
                logger.error(f"Shared-API cluster admin notification failed: {notify_err}")

    stmt_apis = select(APIKey)
    result_apis = await session.execute(stmt_apis)
    all_apis = {api.id: api for api in result_apis.scalars().all()}

    stmt_settings = select(GlobalSettings).limit(1)
    global_settings = await session.scalar(stmt_settings)

    # 🔒 اکانت‌های skipشده به دلیل نبود پراکسی معتبر (برای لاگ + گزارش ادمین)
    skipped_no_proxy: List[Account] = []

    for account in accounts:
        if not account.session_string:
            continue

        # 🔒 IP Leak Guard (همان منطق گارد start_single_worker):
        # بدون proxy_string معتبر، ورکر اصلاً ساخته نمی‌شود تا بعداً با IP
        # مستقیم سرور بالا نیاید (ریسک Chain Ban).
        proxy_dict = parse_proxy_string(account.proxy_string) if account.proxy_string else None
        if not proxy_dict and not direct_ip_fallback_enabled(account.id):
            logger.error(
                f"IP Leak Guard: Skipping account {account.id} "
                f"(phone={account.phone_number}) - no valid proxy_string! "
                "Worker NOT created to prevent server IP leak."
            )
            skipped_no_proxy.append(account)
            continue

        client = await build_worker_client(account, session, proxy_dict, prefetched_apis=all_apis)
        if client is None:
            # خطای ساخت کلاینت (مثلاً شکست رمزگشایی سشن) نباید بقیه‌ی اکانت‌ها را متوقف کند
            continue

        worker_pool[account.id] = client
        
        if global_settings and global_settings.terminate_sessions:
            async def delayed_terminate(c: Client):
                # باگ ۵: رفع مشکل رقابت با استفاده از حلقه انتظار به جای خواب ثابت
                for _ in range(30):
                    if c.is_connected:
                        break
                    await asyncio.sleep(2)
                if c.is_connected:
                    await terminate_other_sessions(c)

            # باگ ۳: ذخیره reference قوی برای task
            task = asyncio.create_task(delayed_terminate(client))
            _background_tasks.add(task)
            task.add_done_callback(_background_tasks.discard)
            
    logger.info(f"Initialized {len(worker_pool)} worker(s) with Specific APIs & Spoofed Devices.")

    # 🔒 لاگ و اطلاع‌رسانی به ادمین: لیست اکانت‌های skipشده به دلیل نبود پراکسی
    if skipped_no_proxy:
        skipped_ids = ", ".join(str(acc.id) for acc in skipped_no_proxy)
        logger.warning(
            f"IP Leak Guard: {len(skipped_no_proxy)} account(s) SKIPPED "
            f"(no valid proxy) → [{skipped_ids}]"
        )
        if bot is not None:
            detail_lines = [
                f"▫️ آیدی {acc.id} (<code>{acc.phone_number if acc.phone_number else '؟'}</code>)"
                for acc in skipped_no_proxy[:30]
            ]
            if len(skipped_no_proxy) > 30:
                detail_lines.append("▫️ ... و موارد دیگر")
            await notify_admins(
                bot,
                "🔒 <b>گارد نشت IP — اکانت‌های بدون پراکسی skip شدند</b>\n\n"
                f"تعداد <b>{len(skipped_no_proxy)}</b> اکانت به دلیل نبود پراکسی معتبر از "
                "استخر ورکرها حذف شدند (سیاست امنیتی: قطع به‌جای افشای IP سرور).\n\n"
                "📋 <b>لیست اکانت‌های skipشده:</b>\n"
                + "\n".join(detail_lines) +
                "\n\n💡 برای فعال‌سازی این اکانت‌ها ابتدا در پنل مدیریت پراکسی سالم ثبت کنید؛ "
                "حلقه‌ی Reconnect خودکار ظرف حداکثر ۵ دقیقه آن‌ها را به استخر برمی‌گرداند."
            )


# ==========================================
# 🛡 فاز ۴ (BUG-23): قرنطینه‌ی اکانت — متمایز از بن/حذف
# AuthKeyUnregistered گاهی گذرا/سمت-سروری است؛ به‌جای is_banned=True +
# session_string=None (نابودی سشن)، اکانت با فلگی جدا قرنطینه می‌شود و
# سشن در DB دست‌نخورده می‌ماند (قابل بازیابی). تا پایان قرنطینه استارت/دیسپچ
# نمی‌گیرد؛ بعد از TTL به‌طور خودکار دوباره امتحان می‌شود.
# بازیابی دستی: DEL quarantine_auth:{account_id} در Redis.
# کلید Redis: quarantine_auth:{account_id} (TTL) + آینه‌ی درون-حافظه‌ای —
# همان الگوی chunk_cooldown در workers/sender.py (fallback قطع Redis).
# ==========================================
AUTH_QUARANTINE_SECONDS = 24 * 3600  # پنجره‌ی قرنطینه: ۲۴ ساعت

_local_quarantine_until: Dict[int, datetime] = {}


def _quarantine_key(account_id: int) -> str:
    return f"quarantine_auth:{account_id}"


async def mark_account_quarantined(account_id: int) -> None:
    """ثبت قرنطینه (بدون حذف داده) — Redis با TTL + آینه‌ی حافظه (fallback)."""
    now_utc = datetime.now(timezone.utc)
    for acc_id in [a for a, until in _local_quarantine_until.items() if until <= now_utc]:
        _local_quarantine_until.pop(acc_id, None)
    _local_quarantine_until[account_id] = now_utc + timedelta(seconds=AUTH_QUARANTINE_SECONDS)
    try:
        await _get_redis().set(_quarantine_key(account_id), "1", ex=AUTH_QUARANTINE_SECONDS)
    except Exception as e:
        logger.warning(
            f"Redis SET failed for {_quarantine_key(account_id)} "
            f"({e.__class__.__name__}: {e}); quarantine kept in-memory only."
        )


async def is_account_quarantined(account_id: int) -> bool:
    """آیا اکانت قرنطینه است؟ (Redis + آینه‌ی حافظه — الگوی is_in_cooldown)"""
    try:
        if await _get_redis().exists(_quarantine_key(account_id)):
            return True
    except Exception as e:
        logger.warning(
            f"Redis EXISTS failed for {_quarantine_key(account_id)} "
            f"({e.__class__.__name__}: {e}); falling back to in-memory quarantine."
        )
    until = _local_quarantine_until.get(account_id)
    if until is not None:
        if until > datetime.now(timezone.utc):
            return True
        _local_quarantine_until.pop(account_id, None)
    return False


# ==========================================
# LIFECYCLE MANAGEMENT
# ==========================================
async def start_all_workers(session: AsyncSession, bot: Optional[Bot] = None) -> None:
    """Starts all clients with retry and proxy rotation mechanisms."""
    logger.info("Starting all initialized workers with Proxy Rotation...")
    
    # واکشی مجدد تنظیمات سیستم
    stmt_settings = select(GlobalSettings).limit(1)
    global_settings = await session.scalar(stmt_settings)
    
    for account_id, client in list(worker_pool.items()):
        # 🔒 گارد دفاعی دوم (Defense-in-Depth): اگر به هر دلیلی کلاینتِ بدون پراکسی
        # داخل استخر باشد و فال‌بک صریح فعال نباشد، هرگز استارت نمی‌شود.
        if getattr(client, "proxy", None) is None and not config.FALLBACK_TO_DIRECT_IP:
            logger.critical(
                f"IP Leak Guard: Worker {account_id} has NO proxy - refusing to start "
                "and removing from pool to prevent server IP leak!"
            )
            worker_pool.pop(account_id, None)
            continue

        try:
            await start_worker_with_rotation(account_id, client, session, global_settings, bot=bot)
        except Exception as e:
            # خطای غیرمنتظره‌ی یک ورکر نباید استارت بقیه را متوقف کند
            logger.error(f"Unexpected error while starting worker {account_id}: {e}")

async def start_worker_with_rotation(
    account_id: int,
    client: Client,
    session: AsyncSession,
    global_settings: Optional[GlobalSettings],
    bot: Optional[Bot] = None,
) -> bool:
    """
    ⚙️ همان منطق retry/چرخش پراکسی که قبلاً داخل start_all_workers بود؛ به‌صورت
    تابع مستقل درآمده تا حلقه‌ی Reconnect خودکار (utils/health_checker.py) هم
    دقیقاً از همین مسیر استفاده کند. خروجی: True در صورت اتصال موفق.
    """
    # 🛡 فاز ۴ (BUG-23): اکانت قرنطینه‌شده (AuthKeyUnregistered مکرر) تا پایان
    # قرنطینه استارت نمی‌گیرد — سشنش در DB محفوظ است و کلاینتش هم از استخر
    # خارج می‌ماند تا دیسپچر chunk ندهد.
    if await is_account_quarantined(account_id):
        logger.info(f"Worker {account_id} is quarantined (repeated AuthKeyUnregistered); skipping start attempt.")
        worker_pool.pop(account_id, None)
        return False

    MAX_RETRIES = 3
    # 🛡 فاز ۴ (BUG-23): شمارش بروز AuthKeyUnregistered در همین چرخه‌ی retry
    auth_key_failures = 0
    
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            await client.start()
            logger.info(f"Worker {account_id} connected successfully.")
            
            # باگ ۱: ثبت کلاینت در pool در صورت موفقیت
            worker_pool[account_id] = client
            
            # 🎭 مدیریت پیشرفته پروفایل‌ها: آبجکت تنظیمات به randomize_profile پاس
            # می‌شود؛ سوئیچ‌های مستقل نام/بیو داخل خود تابع اعمال می‌شوند.
            # باگ ۳: ذخیره reference قوی
            task_profile = asyncio.create_task(
                randomize_profile(client, account_id, settings=global_settings)
            )
            _background_tasks.add(task_profile)
            task_profile.add_done_callback(_background_tasks.discard)

            # 🖼 پکیج عکس پروفایل: فقط اگر auto_set_photo روشن باشد و اکانت
            # پکیج متصل داشته باشد، چرخش عکس در تسکی «کاملاً جداگانه» اجرا
            # می‌شود تا شکست عکس هرگز آپدیت نام/بیو را خراب نکند.
            if global_settings and getattr(global_settings, "auto_set_photo", False):
                photo_package = await _get_photo_package_for_account(session, account_id)
                if photo_package and photo_package.photos:
                    task_photo = asyncio.create_task(
                        _safe_rotate_profile_photos(client, photo_package)
                    )
                    _background_tasks.add(task_photo)
                    task_photo.add_done_callback(_background_tasks.discard)
                    
            return True
            
        except AuthKeyUnregistered as e:
            # 🛡 فاز ۴ (BUG-23): این خانواده‌ی خطا گاهی گذرا/سمت-سروری است؛ قبلاً
            # اولین بروز، بلافاصله is_banned=True + session_string=None می‌شد (نابودی
            # دائمی سشن از DB). فیکس: تا ۲ بار «استارت تازه + چرخش پراکسی»؛ فقط
            # تکرارِ سوم قرنطینه می‌شود (فلگ متمایز از حذف — سشن در DB می‌ماند).
            # ⚠️ این except باید قبل از خانواده‌ی ۴۰۱ بیاید: AuthKeyUnregistered
            # زیرکلاسِ Unauthorized است وگرنه مسیر نابودکننده می‌گیردش.
            auth_key_failures += 1
            if auth_key_failures >= MAX_RETRIES:
                logger.error(
                    f"Worker {account_id}: AuthKeyUnregistered persisted after "
                    f"{auth_key_failures} attempts - quarantining account "
                    f"(session data preserved in DB). Error: {e}"
                )
                worker_pool.pop(account_id, None)
                await mark_account_quarantined(account_id)
                if bot is not None:
                    try:
                        await notify_admins(
                            bot,
                            "🟠 <b>قرنطینه اکانت (AuthKeyUnregistered)</b>\n\n"
                            f"اکانت <code>{account_id}</code> پس از چندین تلاش استارت با خطای "
                            "AuthKeyUnregistered مواجه شد.\n"
                            "سشن اکانت <b>حفظ شده</b> و حذف نشده است؛ اکانت موقتاً قرنطینه شد و تا "
                            "پایان قرنطینه استارت/دیسپچ نمی‌گیرد.\n"
                            "اگر خطا گذرا/سمت-سروری بوده، پس از پایان قرنطینه به‌صورت خودکار دوباره "
                            "تلاش می‌شود؛ در غیر این صورت سشن برای بررسی دستی باقی مانده است."
                        )
                    except Exception as notify_err:
                        logger.error(f"Quarantine admin notification failed for {account_id}: {notify_err}")
                return False

            logger.warning(
                f"Worker {account_id}: AuthKeyUnregistered (possibly transient/server-side) - {e}; "
                f"auth retry {auth_key_failures}/{MAX_RETRIES - 1} with fresh client + rotated proxy."
            )
            # استارت تازه + چرخش پراکسی — همان گاردهای مسیر خطای عمومی (پراکسی
            # تازه باید قابل‌پارس باشد؛ بدون آن چرخشی انجام نمی‌شود). پراکسیِ فعلی
            # fail-mark نمی‌شود چون خطای auth-key تقصیر پراکسی نیست.
            # 🧲 فاز ۵ (BUG-14): claim اتمیک از ظرفیت آزاد — سقف اکانت per proxy
            # رعایت می‌شود و اسلاتِ پراکسیِ فعلی اکانت در همان تراکنش آزاد می‌گردد.
            new_proxy_str = await claim_proxy_for_account(session, account_id)
            new_proxy_dict = parse_proxy_string(new_proxy_str) if new_proxy_str else None
            if new_proxy_str and not new_proxy_dict:
                try:
                    await mark_proxy_failed(session, new_proxy_str)
                except Exception as mark_err:
                    logger.error(f"Failed to mark malformed proxy as failed: {mark_err}")
                new_proxy_dict = None
            if not new_proxy_dict:
                # پراکسی جایگزین معتبری نیست؛ استارتِ تازه ممکن نیست — همان کلاینت
                # در تلاش بعدی دوباره امتحان می‌شود تا شمارنده به قرنطینه برسد.
                await asyncio.sleep(2)
                continue
            try:
                # claim خودش binding (accounts.proxy_string) و in_use را در همین
                # تراکنش نوشته است — اینجا فقط ثبت نهایی (commit) است.
                await session.commit()
            except Exception:
                await session.rollback()
            if client.is_connected:
                try:
                    await client.stop()
                except Exception:
                    pass
            stmt_acc = select(Account).where(Account.id == account_id)
            account_obj = await session.scalar(stmt_acc)
            if not account_obj or not account_obj.session_string:
                logger.error(f"Cannot recreate worker {account_id}: missing session data in DB.")
                worker_pool.pop(account_id, None)
                return False
            new_client = await build_worker_client(account_obj, session, new_proxy_dict)
            if new_client is None:
                logger.error(f"Cannot recreate worker {account_id}: client build failed.")
                worker_pool.pop(account_id, None)
                return False
            worker_pool[account_id] = new_client
            client = new_client

        except (UserDeactivated, UserDeactivatedBan, Unauthorized) as e:
            # 🛡 فاز ۴ (BUG-23): AuthKeyUnregistered از این مسیر جدا شد (بلوک بالا)؛
            # بقیه‌ی خانواده‌ی ۴۰۱ (SessionRevoked و…) همچنان قطعی تلقی و پاکسازی می‌شوند.
            logger.error(f"Worker {account_id} session revoked or banned: {e}")
            worker_pool.pop(account_id, None)
            try:
                # 🧲 فاز ۵ (BUG-14): پراکسیِ اکانتِ بن‌شده آزاد می‌شود تا ظرفیتش
                # برای اکانت سالم قابل claim باشد (in_use یک واحد کم می‌شود).
                banned_proxy_str = await session.scalar(
                    select(Account.proxy_string).where(Account.id == account_id)
                )
                stmt = update(Account).where(Account.id == account_id).values(is_banned=True, session_string=None)
                await session.execute(stmt)
                if banned_proxy_str:
                    await release_proxy_slot(session, banned_proxy_str)
                await session.commit()
                logger.info(f"Account {account_id} flagged as banned and session data cleared from DB.")
            except Exception as db_err:
                await session.rollback()
                logger.error(f"Failed to flag account {account_id} as banned: {db_err}")
            return False
            
        except Exception as e:
            logger.warning(f"Worker {account_id} connection failed (Attempt {attempt}/{MAX_RETRIES}): {e}")
            
            try:
                acc_stmt = select(Account).where(Account.id == account_id)
                failed_acc = await session.scalar(acc_stmt)
                if failed_acc and failed_acc.proxy_string:
                    await mark_proxy_failed(session, failed_acc.proxy_string)
            except Exception as db_err:
                logger.error(f"Error reading original proxy string for account {account_id}: {db_err}")
            
            # 🧲 فاز ۵ (BUG-14): claim اتمیک از ظرفیتِ آزاد (سقف اکانت per proxy)؛
            # اسلاتِ پراکسی قبلی اکانت در همان تراکنش آزاد و binding جدید ثبت می‌شود.
            new_proxy_str = await claim_proxy_for_account(session, account_id)
            if new_proxy_str:
                # 🔒 گارد نشت IP در چرخش: پراکسیِ تازه‌چرخیده باید قابل‌پارس باشد.
                # (در نسخه‌ی قبل اگر رشته‌ی پراکسیِ فعالِ دیتابیس خراب بود، کلاینت
                # با proxy=None و IP مستقیم سرور ساخته می‌شد — نشت IP!)
                new_proxy_dict = parse_proxy_string(new_proxy_str)
                if not new_proxy_dict:
                    logger.error(
                        f"Rotated proxy for worker {account_id} is malformed - marking it "
                        "failed and NOT falling back to direct IP."
                    )
                    try:
                        await mark_proxy_failed(session, new_proxy_str)
                    except Exception as mark_err:
                        logger.error(f"Failed to mark malformed proxy as failed: {mark_err}")
                    await asyncio.sleep(2)
                    continue
                
                logger.info(f"Rotating proxy for worker {account_id}...")
                
                # ۱. commit تراکنشِ claim — binding جدید + شمارنده‌ی in_use
                try:
                    await session.commit()
                except Exception:
                    await session.rollback()
                    
                # 🔴 اصلاح فاز ۳: ساخت مجدد (Re-instantiation) کلاینت به جای تغییر درجای پراکسی
                # توقف امن کلاینت قبلی (در صورت وجود سوکت باز)
                if client.is_connected:
                    try:
                        await client.stop()
                    except Exception:
                        pass
                        
                # واکشی اطلاعات کامل اکانت برای ساخت کلاینت جدید
                stmt_acc = select(Account).where(Account.id == account_id)
                account_obj = await session.scalar(stmt_acc)
                
                if not account_obj or not account_obj.session_string:
                    logger.error(f"Cannot recreate worker {account_id}: missing session data in DB.")
                    worker_pool.pop(account_id, None)
                    return False
                    
                # ساخت کلاینت کاملاً جدید از کارخانه‌ی واحد
                # (CRM و شنود «سین» همان قبل - دست‌نخورده)
                new_client = await build_worker_client(account_obj, session, new_proxy_dict)
                if new_client is None:
                    logger.error(f"Cannot recreate worker {account_id}: client build failed.")
                    worker_pool.pop(account_id, None)
                    return False
                
                # جایگزینی کلاینت جدید در استخر ورکرها و متغیر لوپ فعلی
                worker_pool[account_id] = new_client
                client = new_client
                
            else:
                # 🧲 فاز ۵: claim می‌تواند به‌دلیل «اتمام پراکسی فعال» یا «پُر بودن
                # ظرفیت همه‌ی پراکسی‌ها (MAX_ACCOUNTS_PER_PROXY)» ناموفق باشد.
                logger.critical(
                    f"CRITICAL: No proxy with FREE capacity for Worker {account_id} "
                    f"(MAX_ACCOUNTS_PER_PROXY={config.MAX_ACCOUNTS_PER_PROXY})! Disconnecting."
                )
                worker_pool.pop(account_id, None)
                # ⚠️ هشدار Realtime به ادمین: پراکسی سالم این ورکر تمام شده است.
                # اولین رخداد بلافاصله ارسال می‌شود؛ تکرارهای بعدیِ همان اکانت
                # (از حلقه‌ی Reconnect) برای جلوگیری از اسپم تا ۳۰ دقیقه سرکوب می‌شوند.
                if not _no_proxy_alert_throttled(account_id):
                    await notify_admins(
                        bot,
                        "🚨 <b>هشدار امنیتی: اتمام پراکسی‌های سالم</b>\n\n"
                                                f"🔴 ورکر <code>{account_id}</code> به دلیل اتمام پراکسی‌های سالم یا پُر بودن ظرفیت آن‌ها، "
                        "قطع و از استخر حذف شد.\n\n"
                        "طبق سیاست امنیتی «قطع به‌جای افشای IP سرور»، این اکانت با IP مستقیم "
                        "سرور اجرا نخواهد شد.\n"
                        "💡 لطفاً در پنل مدیریت پراکسی سالم جدید ثبت کنید؛ حلقه‌ی Reconnect "
                        "خودکار ظرف حداکثر ۵ دقیقه ورکر را به استخر برمی‌گرداند."
                    )
                else:
                    logger.info(
                        f"No-proxy alert for worker {account_id} throttled (sent recently)."
                    )
                return False


    logger.error(f"Worker {account_id} failed to connect after {MAX_RETRIES} attempts. Waiting for reconnect loop.")
    worker_pool.pop(account_id, None)
    return False
async def stop_all_workers() -> None:
    """Cleanly disconnects all active Pyrogram clients."""
    logger.info("Stopping all workers...")
    for account_id, client in list(worker_pool.items()):
        try:
            if client.is_connected:
                await client.stop()
            logger.info(f"Worker {account_id} disconnected safely.")
        except Exception as e:
            logger.error(f"Error disconnecting worker {account_id}: {e}")
        finally:
            worker_pool.pop(account_id, None)
            
    # باگ ۳: پاک‌سازی Best-effort برای تسک‌های پس‌زمینه رها شده
    if _background_tasks:
        logger.info(f"Cancelling {len(_background_tasks)} background tasks...")
        for task in list(_background_tasks):
            task.cancel()
        await asyncio.gather(*_background_tasks, return_exceptions=True)
        _background_tasks.clear()
            
    logger.info("All workers have been stopped and removed from the pool.")

# ==========================================
# START SINGLE WORKER (پشتیبانی از لاگین دینامیک)
# ==========================================
async def start_single_worker(account: Account, session: AsyncSession) -> bool:
    if not account.session_string:
        return False

    proxy_dict = parse_proxy_string(account.proxy_string) if account.proxy_string else None

    # 🔴 اصلاح فاز ۴: گارد امنیتی برای جلوگیری از نشت آی‌پی سرور
    # 🔒 فاز ۵: تنها استثنا، فال‌بک صریح FALLBACK_TO_DIRECT_IP=true است
    if not proxy_dict and not direct_ip_fallback_enabled(account.id):
        logger.critical(
            f"CRITICAL: Cannot start Worker {account.id} - No valid proxy found! "
            "Aborting dynamically started worker to prevent IP leak."
        )
        return False

    # ساخت کلاینت از کارخانه‌ی واحد (همان منطق قبلی: API اختصاصی، Spoofing، CRM و «سین»)
    client = await build_worker_client(account, session, proxy_dict)
    if client is None:
        logger.error(f"Failed to build client for dynamically started Worker {account.id}.")
        return False

    try:
        await client.start()
        worker_pool[account.id] = client
        logger.info(f"Dynamically started new Worker {account.id} with API {client.api_id}.")

        # 🔥 فاز ۶ (R7): ثبت شروع دوره‌ی گرم‌شدن برای اکانت تازه-لاگین‌شده — تا
        # MIN_WARMUP_HOURS ساعت بعد از اولین اتصال، دیسپچر به این اکانت chunk
        # نمی‌دهد (دوره honeymoon). تراکنشِ مستقل تا تراکنشِ بازِ caller لمس نشود
        # (الگوی BUG-30)؛ گارد IS NULL یعنی idempotent.
        try:
            async with async_session() as w_session:
                async with w_session.begin():
                    await w_session.execute(
                        update(Account)
                        .where(
                            Account.id == account.id,
                            Account.warmed_up_at.is_(None),
                        )
                        .values(
                            warmed_up_at=datetime.now(timezone.utc)
                            + timedelta(hours=warmup_hours())
                        )
                        .execution_options(synchronize_session=False)
                    )
        except Exception as warm_err:
            logger.warning(f"Warmup registration failed for account {account.id}: {warm_err}")

        stmt_settings = select(GlobalSettings).limit(1)
        global_settings = await session.scalar(stmt_settings)
        
        # 🎭 مدیریت پیشرفته پروفایل‌ها: تنظیمات به randomize_profile پاس می‌شود
        # باگ ۳: ذخیره reference قوی
        task_profile = asyncio.create_task(
            randomize_profile(client, account.id, settings=global_settings)
        )
        _background_tasks.add(task_profile)
        task_profile.add_done_callback(_background_tasks.discard)

        # 🖼 پکیج عکس پروفایل: تسک مستقل — فقط با auto_set_photo روشن و پکیج متصل
        if global_settings and getattr(global_settings, "auto_set_photo", False):
            photo_package = await _get_photo_package_for_account(session, account.id)
            if photo_package and photo_package.photos:
                task_photo = asyncio.create_task(
                    _safe_rotate_profile_photos(client, photo_package)
                )
                _background_tasks.add(task_photo)
                task_photo.add_done_callback(_background_tasks.discard)
            
        return True
    except Exception as e:
        logger.error(f"Failed to start new Worker {account.id} dynamically: {e}")
        # باگ ۲: توقف ایمن کلاینت نیمه راه‌اندازی شده برای جلوگیری از آویزان ماندن سوکت
        try:
            await client.stop()
        except Exception:
            pass
        return False