import asyncio
import logging
import random
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional
from urllib.parse import urlparse, unquote
import asyncio
from typing import Set, Iterable
from pathlib import Path
import sqlite3

from pyrogram import Client
from pyrogram.errors import (
    AuthKeyUnregistered,
    UserDeactivated,
    UserDeactivatedBan,
    Unauthorized,
    AuthKeyDuplicated
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
from utils.join_request_listener import attach_join_request_listener  # 🟢 شنود تأیید/رد عضویت
from utils.crypto import decrypt_session 
from workers.sender import _get_redis
from utils.admin_broadcast import broadcast_to_admins
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

SESSIONS_DIR = Path("sessions")
SESSIONS_DIR.mkdir(parents=True, exist_ok=True)



# ==========================================
# 🟢 فاز ۷: throttle بین warm-upهای ورکرها
# ==========================================
_warmup_lock_key = "warmup_lock:worker_cache"

def direct_ip_fallback_enabled(account_id: int) -> bool:
    return getattr(config, "FALLBACK_TO_DIRECT_IP", True)

async def _acquire_warmup_slot() -> None:
    """
    🟢 فاز ۷: قبل از warm_worker_cache، این تابع صدا زده می‌شود تا مطمئن شویم
    فقط یک ورکر همزمان در حال get_dialogs است.
    """
    from workers.sender import _get_redis
    
    try:
        redis = _get_redis() # 🛡 استفاده از سینگلتون بجای ساختن کانکشن‌پول جدید
        throttle_min = float(getattr(config, "WARMUP_THROTTLE_MIN", 2.0))
        throttle_max = float(getattr(config, "WARMUP_THROTTLE_MAX", 5.0))
        throttle_seconds = random.uniform(throttle_min, throttle_max)
        
        for _ in range(20):
            acquired = await redis.set(
                _warmup_lock_key, "1",
                nx=True,
                px=int((throttle_seconds + 2.0) * 1000),
            )
            if acquired:
                return
            await asyncio.sleep(0.5)
        
        logger.warning("warmup throttle: could not acquire lock after 10s — proceeding anyway.")
    except Exception as e:
        logger.debug(f"warmup throttle skipped (best-effort): {e}")
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

async def process_proxy_queue(session: AsyncSession) -> None:
    """منطق صف FIFO برای تخصیص ظرفیت‌های خالی به اکانت‌های منتظر"""
    stmt = (
        select(Account)
        .where(Account.proxy_status == "WAITING_PROXY")
        .order_by(Account.proxy_queue_joined_at.is_(None), Account.proxy_queue_joined_at.asc())
        .limit(30)
    )
    waiting_accounts = (await session.scalars(stmt)).all()

    for acc in waiting_accounts:
        claimed = await claim_proxy_for_account(session, acc.id)
        if not claimed:
            break # اتمام ظرفیت، خروج از صف
        await log_proxy_event(session, acc.id, "DEQUEUE", claimed, "Assigned from FIFO queue")


async def background_process_proxy_queue() -> None:
    """Wrapper برای اجرای پس‌زمینه صف بدون تداخل با تراکنش caller"""
    try:
        from database.engine import async_session
        async with async_session() as session:
            await process_proxy_queue(session)
            await session.commit()
    except Exception as e:
        logger.error(f"Error in background_process_proxy_queue: {e}")


async def release_proxy_slot(session: AsyncSession, proxy_string: str, account_id: Optional[int] = None) -> None:
    """آزادسازی یک اسلات و بیدار کردن رویدادمحورِ صف انتظار."""
    stmt = (
        update(Proxy)
        .where(Proxy.proxy_string == proxy_string, Proxy.in_use > 0)
        .values(in_use=Proxy.in_use - 1)
        .execution_options(synchronize_session=False)
    )
    res = await session.execute(stmt)
    if res.rowcount > 0 and account_id:
        await log_proxy_event(session, account_id, "RELEASE", proxy_string, "Slot released")

    # 🔄 فاز ۵: اعمال Cooldown برای پروکسیِ تازه رهاشده تا مدتی کاندید نشود
    try:
        from workers.sender import _get_redis
        await _get_redis().set(f"proxy_cd:{proxy_string}", "1", ex=getattr(config, "ROTATE_COOLDOWN_SECONDS", 300))
    except Exception as e:
        logger.debug(f"Failed to set proxy cooldown for {proxy_string}: {e}")

    # بیدار کردن رویدادمحور صف انتظار (Event-Driven)
    asyncio.create_task(background_process_proxy_queue())



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


async def mark_proxy_failed(session: AsyncSession, proxy_string: str) -> None:
    """
    Marks a proxy as failed by feeding it to the health hysteresis machine.
    """
    if not proxy_string:
        return
        
    try:
        from utils.health_checker import report_proxy_result
        # ارسال شکست به ماشین سلامت؛ این تابع خودش commit مستقل دارد و is_healthy را هم سینک می‌کند
        await report_proxy_result(proxy_string, is_success=False)
        logger.info(f"Proxy marked as failed via health machine: {proxy_string}")
    except Exception as e:
        logger.error(f"Failed to mark proxy as failed via health machine: {e}")

# ==========================================
# 🧲 PROXY CLAIM & LOGIN LOGIC (NEW ATOMIC FLOW)
# ==========================================

def login_proxy_dict() -> Optional[dict]:
    """پارس کردن پراکسی مخصوص لاگین (در صورت وجود). اگر نامعتبر باشد لاگ می‌دهد."""
    url = getattr(config, "LOGIN_PROXY_URL", "")
    if not url:
        return None
    
    parsed = parse_proxy_string(url)
    if not parsed:
        logger.warning(f"LOGIN_PROXY_URL is set but invalid: {url}")
        return None
    return parsed


WORKER_PROXY_USAGE = ("sender", "both")


async def log_proxy_event(session: AsyncSession, account_id: int, action: str, proxy_string: Optional[str], reason: str) -> None:
    """ثبت Audit Log رویدادهای تخصیص و صف پروکسی"""
    from database.models import WorkerEvent
    event = WorkerEvent(
        account_id=account_id,
        old_status="PROXY_QUEUE",
        new_status=action,
        reason=reason,
        error_details=f"Proxy: {proxy_string}" if proxy_string else None
    )
    session.add(event)



async def claim_proxy_for_account(
    session: AsyncSession, account_id: Optional[int] = None, ignore_cooldown: bool = False
) -> Optional[str]:
    cap = max(1, config.MAX_ACCOUNTS_PER_PROXY)
    
    cooldown_proxies = []
    if not ignore_cooldown:
        try:
            from workers.sender import _get_redis
            redis = _get_redis()
            # جایگزینی الگوی مسدودکننده KEYS با SCAN (O(N) امن‌تر) برای فاز ۱۱
            async for key in redis.scan_iter(match="proxy_cd:*", count=100):
                k_str = key.decode("utf-8") if isinstance(key, bytes) else key
                cooldown_proxies.append(k_str.split(":", 1)[1])
        except Exception:
            pass

    # فاز ۳/۵: پروکسی HEALTHY با ظرفیت آزاد (Least-Loaded). نادیده گرفتن پروکسی‌های در Cooldown.
    stmt = select(Proxy).where(
        Proxy.is_active == True, 
        Proxy.in_use < cap,
        Proxy.health_state == "HEALTHY",
        Proxy.usage_type.in_(("sender", "both"))
    )
    
    # اعمال فیلتر Cooldown روی کوئری
    if cooldown_proxies:
        stmt = stmt.where(Proxy.proxy_string.notin_(cooldown_proxies))
        
    stmt = stmt.order_by(Proxy.in_use.asc(), Proxy.ping_ms.is_(None), Proxy.ping_ms.asc()).limit(10)
    
    result = await session.execute(stmt)
    candidates = result.scalars().all()

    # 🛡 فاز ۱ (F9): مرحله دوم Claim (Fallback به WEAK)
    allow_weak_fallback = getattr(config, "PROXY_ALLOW_WEAK_FALLBACK", True)
    if not candidates and allow_weak_fallback:
        stmt_weak = select(Proxy).where(
            Proxy.is_active == True,
            Proxy.in_use < cap,
            Proxy.health_state == "WEAK",
            Proxy.usage_type.in_(("sender", "both"))
        )
        if cooldown_proxies:
            stmt_weak = stmt_weak.where(Proxy.proxy_string.notin_(cooldown_proxies))
            
        stmt_weak = stmt_weak.order_by(Proxy.ping_ms.is_(None), Proxy.ping_ms.asc(), Proxy.in_use.asc()).limit(10)
        result_weak = await session.execute(stmt_weak)
        candidates = result_weak.scalars().all()
        if candidates:
            logger.warning(f"No HEALTHY proxies available. Fallback: Claiming from WEAK proxies for account {account_id}.")
    
    if not candidates:
        # باگ ۴: گارد نشت اسلات — اگر Claim شکست خورد و اکانت از قبل پروکسی داشت، آن را آزاد می‌کنیم
        if account_id is not None:
            old_proxy_stmt = select(Account.proxy_string).where(Account.id == account_id)
            old_proxy_str = await session.scalar(old_proxy_stmt)
            if old_proxy_str:
                await release_proxy_slot(session, old_proxy_str, account_id)
                clear_stmt = (
                    update(Account)
                    .where(Account.id == account_id)
                    .values(proxy_string=None, proxy_status="WAITING_PROXY")
                    .execution_options(synchronize_session=False)
                )
                await session.execute(clear_stmt)
        return None
    
    for proxy in candidates:
        # رقابت اتمیک (Atomic Claim): در محیط‌های چندهسته‌ای، فقط ورکری که rowcount > 0 بگیرد برنده است
        update_stmt = (
            update(Proxy)
            .where(Proxy.id == proxy.id, Proxy.is_active == True, Proxy.in_use < cap)
            .values(in_use=Proxy.in_use + 1)
            .execution_options(synchronize_session=False)
        )
        res = await session.execute(update_stmt)
        
        if res.rowcount > 0:
            new_proxy_str = proxy.proxy_string
            
            if account_id is not None:
                old_proxy_stmt = select(Account.proxy_string).where(Account.id == account_id)
                old_proxy_str = await session.scalar(old_proxy_stmt)
                
                # ثبت پروکسی جدید برای اکانت و خروج از صف انتظار
                bind_stmt = (
                    update(Account)
                    .where(Account.id == account_id)
                    .values(
                        proxy_string=new_proxy_str,
                        proxy_status="ASSIGNED",
                        proxy_queue_joined_at=None
                    )
                    .execution_options(synchronize_session=False)
                )
                await session.execute(bind_stmt)
                await log_proxy_event(session, account_id, "ASSIGNED", new_proxy_str, "Claimed new capacity")
                
                # آزادسازی ظرفیت پروکسی قبلی (که خودش Cooldown را برای آن اعمال می‌کند)
                if old_proxy_str and old_proxy_str != new_proxy_str:
                    await release_proxy_slot(session, old_proxy_str, account_id)
                    
            return new_proxy_str
            
    # باگ ۴: گارد نشت اسلات — در صورت شکست تمام کاندیدها (از دست دادن رقابت اتمیک)
    if account_id is not None:
        old_proxy_stmt = select(Account.proxy_string).where(Account.id == account_id)
        old_proxy_str = await session.scalar(old_proxy_stmt)
        if old_proxy_str:
            await release_proxy_slot(session, old_proxy_str, account_id)
            clear_stmt = (
                update(Account)
                .where(Account.id == account_id)
                .values(proxy_string=None, proxy_status="WAITING_PROXY")
                .execution_options(synchronize_session=False)
            )
            await session.execute(clear_stmt)
            
    return None

async def direct_budget_ok(session: AsyncSession) -> bool:
    """
    بررسی بودجه ارسال مستقیم (بدون پراکسی).
    اگر تعداد اکانت‌های بدون پراکسی از سقف MAX_DIRECT_ACCOUNTS عبور کند،
    False برگردانده و هشدار می‌دهد.
    """
    stmt = select(func.count()).select_from(Account).where(Account.proxy_string.is_(None))
    direct_count = await session.scalar(stmt)
    
    if direct_count is not None and direct_count >= config.MAX_DIRECT_ACCOUNTS:
        # استفاده از الگوی هشدار throttle شده (آیدی 0 به عنوان شناسه سیستم رزرو شده)
        if not _no_proxy_alert_throttled(0):
            logger.critical(
                "█" * 62 + "\n"
                f"⚠️ سقف اکانت‌های دایرکت (MAX_DIRECT_ACCOUNTS={config.MAX_DIRECT_ACCOUNTS}) "
                f"پر شده است (فعلی: {direct_count}).\n"
                "⚠️ اکانت‌های جدید بدون پراکسی تا زمان آزاد شدن ظرفیت متصل نخواهند شد.\n"
                + "█" * 62
            )
        return False
    return True


# ==========================================
# 🔄 تابع سوییچ مشترک پروکسی (DRY - فاز ۴ و ۵)
# ==========================================
async def switch_worker_proxy(account_id: int, session: AsyncSession, reason: str, ignore_cooldown: bool = False) -> bool:
    """
    سوییچ پروکسی درجا برای یک ورکرِ فعال در استخر (پس از اتمام چانک یا خطا).
    در صورت نبود کاندید مناسب، False برمی‌گرداند (سوییچ لغو می‌شود).
    """
    from workers.sender import _get_redis
    old_proxy = await session.scalar(select(Account.proxy_string).where(Account.id == account_id))
    
    # گرفتن پروکسی جدید با گارد ظرفیت و سلامت (و اعمال یا نادیده‌گرفتن Cooldown)
    new_proxy_str = await claim_proxy_for_account(session, account_id, ignore_cooldown=ignore_cooldown)
    
    if not new_proxy_str:
        logger.info(f"Rotation skipped for user_{account_id}/ (no healthy candidates) - keeping current proxy.")
        return False
        
    await session.commit()
    
    proxy_dict = parse_proxy_string(new_proxy_str)
    
    # توقف کلاینت قبلی
    client = worker_pool.get(account_id)
    if client and client.is_connected:
        try:
            await client.stop()
        except Exception: pass
        
    # بازسازی و اجرای کلاینت با پروکسی جدید
    account = await session.get(Account, account_id)
    new_client = await build_worker_client(account, session, proxy_dict)
    if not new_client:
        return False
        
    try:
        await asyncio.wait_for(new_client.start(), timeout=45)
        worker_pool[account_id] = new_client
        logger.info(f"Worker user_{account_id}/ rotated proxy from {old_proxy} to {new_proxy_str}. Reason: {reason}")
        
        # ریست کانترهای چرخش
        redis = _get_redis()
        await redis.set(f"worker_chunks:{account_id}", "0")
        await redis.set(f"worker_last_rot:{account_id}", str(time.time()), ex=259200) # انقضای ۳ روزه
        
        return True
    except Exception as e:
        logger.error(f"Failed to start worker {account_id} with new proxy {new_proxy_str}: {e}")
        worker_pool.pop(account_id, None)
        return False


# ==========================================
# ⚠️ ADMIN ALERTS (هشدار Realtime به ادمین)
# ==========================================
# جلوگیری از اسپم هشدار تکراری: اولین رخدادِ هر اکانت «بلافاصله» ارسال می‌شود؛
# تکرارهای بعدیِ همان اکانت (مثلاً از حلقه‌ی Reconnect) تا این بازه سرکوب می‌گردند.
NO_PROXY_ALERT_THROTTLE_SECONDS = 30 * 60
_last_no_proxy_alert_at: Dict[int, float] = {}

_last_disconnect_alert_at: Dict[int, float] = {}

def _disconnect_alert_throttled(account_id: int) -> bool:
    """True اگر هشدار «قطعی عمومی ورکر» برای این اکانت در ۱ ساعت گذشته ارسال شده باشد."""
    now = time.monotonic()
    if now - _last_disconnect_alert_at.get(account_id, 0.0) < 3600:
        return True
    _last_disconnect_alert_at[account_id] = now
    return False

def _no_proxy_alert_throttled(account_id: int) -> bool:
    """True اگر هشدار «اتمام پراکسی»ی همین اکانت به‌تازگی ارسال شده باشد."""
    now = time.monotonic()
    if now - _last_no_proxy_alert_at.get(account_id, 0.0) < NO_PROXY_ALERT_THROTTLE_SECONDS:
        return True
    _last_no_proxy_alert_at[account_id] = now
    return False

async def notify_admins(bot: Optional[Bot], text: str) -> int:
    """
    ارسال هشدار/گزارش فوری به ادمین اصلی و ساب‌ادمین‌ها از طریق helper مرکزی.
    """
    if bot is None:
        return 0
    res = await broadcast_to_admins(bot, text)
    return res.get("sent", 0)

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

        session_name = f"worker_acc_{account.id}"
        session_file = SESSIONS_DIR / f"{session_name}.session"

        client_kwargs = dict(
            name=session_name,
            workdir=SESSIONS_DIR,          # pyrofork FileStorage -> sessions/<name>.session
            api_id=worker_api_id,
            api_hash=worker_api_hash,
            proxy=proxy_dict,
            sleep_threshold=60,            # L-02: auto-sleep on FloodWait <= 60s instead of raising
            device_model=account.device_model or random.choice(DEVICE_MODELS),
            system_version=account.system_version or random.choice(SYSTEM_VERSIONS),
            app_version=account.app_version or random.choice(APP_VERSIONS),
            lang_code="en",
        )

        if session_file.exists():
            # Persistent session: peers + access hashes survive restarts.
            client = Client(**client_kwargs)
        else:
            # First run for this account: bootstrap from the encrypted DB string.
            # (session_string -> MemoryStorage; the file is written by
            #  _persist_memory_session() after a successful start + warmup.)
            client = Client(
                session_string=decrypt_session(account.session_string),
                in_memory=True,
                **client_kwargs,
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
        
        # 🟢 نصب شنودگر تأیید/رد عضویت
        attach_join_request_listener(client)

        return client
    except Exception as e:
        from cryptography.fernet import InvalidToken
        if isinstance(e, InvalidToken):
            logger.critical(f"FATAL: Fernet InvalidToken for account {account.id} - Wrong FERNET_KEY!")
        logger.error(f"Failed to build worker client for account {account.id}: {e}")
        return None
    

_PG_SESSION_DDL = """
CREATE TABLE sessions
(
    dc_id     INTEGER PRIMARY KEY,
    api_id    INTEGER,
    test_mode INTEGER,
    auth_key  BLOB,
    date      INTEGER NOT NULL,
    user_id   INTEGER,
    is_bot    INTEGER
);

CREATE TABLE peers
(
    id             INTEGER PRIMARY KEY,
    access_hash    INTEGER,
    type           INTEGER NOT NULL,
    username       TEXT,
    phone_number   TEXT,
    last_update_on INTEGER NOT NULL DEFAULT (CAST(STRFTIME('%s', 'now') AS INTEGER))
);

CREATE TABLE version
(
    number INTEGER PRIMARY KEY
);

CREATE INDEX idx_peers_id ON peers (id);
CREATE INDEX idx_peers_username ON peers (username);
CREATE INDEX idx_peers_phone_number ON peers (phone_number);
"""


async def _persist_memory_session(client: Client, account_id: int) -> Optional[Path]:
    """
    Copy the ':memory:' SQLite storage of a session_string-bootstrapped
    client into sessions/worker_acc_{account_id}.session, so the NEXT
    restart opens a FileStorage client that already knows the auth key
    AND the peer/access-hash cache. Best-effort: never raises.
    """
    dest_path = SESSIONS_DIR / f"worker_acc_{account_id}.session"
    if dest_path.exists():
        return dest_path
    src = getattr(client.storage, "conn", None)  # aiosqlite ':memory:' conn
    if src is None:
        return None
    try:
        async def _fetch(sql: str) -> list:
            async with src.execute(sql) as cur:
                return await cur.fetchall()

        session_rows = await _fetch(
            "SELECT dc_id, api_id, test_mode, auth_key, date, user_id, is_bot FROM sessions")
        peer_rows = await _fetch(
            "SELECT id, access_hash, type, username, phone_number, last_update_on FROM peers")
        version_rows = await _fetch("SELECT number FROM version")

        def _write() -> None:
            dest = sqlite3.connect(str(dest_path))
            try:
                dest.executescript(_PG_SESSION_DDL)
                dest.executemany(
                    "INSERT OR REPLACE INTO sessions VALUES (?,?,?,?,?,?,?)", session_rows)
                dest.executemany(
                    "INSERT OR REPLACE INTO peers "
                    "(id, access_hash, type, username, phone_number, last_update_on) "
                    "VALUES (?,?,?,?,?,?)", peer_rows)
                dest.executemany("INSERT OR REPLACE INTO version VALUES (?)", version_rows)
                dest.commit()
            finally:
                dest.close()

        await asyncio.to_thread(_write)
        logger.info(f"Persisted session file for account {account_id} -> {dest_path}")
        return dest_path
    except Exception as e:
        logger.warning(f"Could not persist session file for account {account_id}: {e}")
        try:
            dest_path.unlink(missing_ok=True)
        except Exception:
            pass
        return None

async def warm_worker_cache(client: Client, extra_peer_ids: Optional[Iterable[int]] = None) -> None:
    """
    C-01 mitigation: after start, populate the session peer cache so
    resolve_peer() never runs against an empty peers table (which is what
    previously dropped whole update batches). Best-effort: never raises.
    
    🟢 فاز ۷: قبل از get_dialogs، یک throttle lock گرفته می‌شود تا از FloodWait
    ناشی از warm-up همزمان چندین ورکر جلوگیری شود.
    """
    # 🟢 فاز ۷: throttle بین warm-upهای ورکرهای مختلف
    await _acquire_warmup_slot()
    
    try:
        async for _ in client.get_dialogs(limit=500):
            pass
    except Exception as e:
        logger.warning(f"warmup get_dialogs failed for {client.name}: {e}")
    if extra_peer_ids:
        for pid in extra_peer_ids:
            try:
                await client.get_chat(pid)
            except Exception as e:
                logger.debug(f"warmup get_chat({pid}) failed for {client.name}: {e}")


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
# بازنویسی کامل: initialize_workers — workers/session_manager.py
async def get_use_proxy_for_sending(session: AsyncSession) -> bool:
    """خواندن سوییچ ارسال با پروکسی/مستقیم با یک پیش‌فرض واحد (True)"""
    stmt_settings = select(GlobalSettings).limit(1)
    global_settings = await session.scalar(stmt_settings)
    return getattr(global_settings, "use_proxy_for_sending", True) if global_settings else True

async def validate_fernet_key(session: AsyncSession, bot: Optional[Bot]) -> bool:
    """اعتبارسنجی زودهنگام کلید Fernet برای جلوگیری از خواب خاموش استخر ورکرها."""
    stmt = select(Account.session_string).where(Account.session_string.isnot(None)).limit(1)
    sample = await session.scalar(stmt)
    if not sample:
        return True
    try:
        from utils.crypto import decrypt_session
        decrypt_session(sample)
        return True
    except Exception as e:
        msg = "🚨 کلید FERNET_KEY نامعتبر است یا با سشن‌های موجود هم‌خوان نیست! استخر ورکرها متوقف شد."
        logger.critical(msg)
        if bot:
            await notify_admins(bot, f"<b>خطای بحرانی استارتاپ</b>\n\n{msg}")
        return False

async def initialize_workers(session: AsyncSession, bot: Optional[Bot] = None) -> None:
    """
    🔒 مقدار use_proxy_for_sending پیش از راه اندازی خوانده می‌شود.
    در صورت اتصال مستقیم، پروکسی به کلاینت تزریق نخواهد شد (سرعت بالا).
    """
    if not await validate_fernet_key(session, bot):
        logger.critical("Aborting worker initialization due to Invalid Fernet Key.")
        return

    await reconcile_proxy_usage(session)

    # خواندن از تنظیمات سراسری به کمک helper واحد
    use_proxy_for_sending = await get_use_proxy_for_sending(session)
    
    # اصلاح باگ ۱: در حالت Direct (بدون پروکسی)، اکانت‌هایی که در گذشته پروکسی خود را 
    # از دست داده‌اند (WAITING_PROXY) به NO_PROXY تبدیل می‌شوند تا در چرخه Reconnect دیده شوند.
    if not use_proxy_for_sending:
        stmt_fix = (
            update(Account)
            .where(Account.proxy_status == "WAITING_PROXY")
            .values(proxy_status="NO_PROXY")
            .execution_options(synchronize_session=False)
        )
        await session.execute(stmt_fix)
        await session.commit()

    stmt_settings = select(GlobalSettings).limit(1)
    global_settings = await session.scalar(stmt_settings)

    stmt = select(Account).where(Account.is_banned == False)
    result = await session.execute(stmt)
    accounts = result.scalars().all()

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

    # 🔒 اکانت‌های skipشده به دلیل نبود پراکسی معتبر (برای لاگ + گزارش ادمین)
    skipped_no_proxy: List[Account] = []
    skipped_direct_budget: List[Account] = []

    for account in accounts:
        if not account.session_string:
            continue

        proxy_dict = None
        if use_proxy_for_sending:
            # 🛡 رفع باگ: بررسی سلامت پروکسی قبل از بوت شدن اکانت
            if account.proxy_string:
                health = await session.scalar(select(Proxy.health_state).where(Proxy.proxy_string == account.proxy_string))
                if health == "DEAD":
                    logger.warning(f"Worker {account.id} is bound to a DEAD proxy. Trying to claim a new one...")
                    new_proxy = await claim_proxy_for_account(session, account.id)
                    account.proxy_string = new_proxy
                    await session.commit()

            proxy_dict = parse_proxy_string(account.proxy_string) if account.proxy_string else None
            if not proxy_dict and not direct_ip_fallback_enabled(account.id):
                logger.error(
                    f"IP Leak Guard: Skipping account {account.id} - no valid proxy_string!"
                )
                skipped_no_proxy.append(account)
                continue
        else:
            if not await direct_budget_ok(session):
                skipped_direct_budget.append(account)
                continue
            proxy_dict = None  # تضمین صریح ارسال بدون پروکسی
            logger.debug(f"Direct connection enabled (proxy bypassed) for account {account.id}.")
 
        client = await build_worker_client(account, session, proxy_dict, prefetched_apis=all_apis)
        if client is None:
            # خطای ساخت کلاینت نباید بقیه‌ی اکانت‌ها را متوقف کند
            continue

        worker_pool[account.id] = client
        
        if global_settings and global_settings.terminate_sessions:
            async def delayed_terminate(c: Client):
                for _ in range(30):
                    if c.is_connected:
                        break
                    await asyncio.sleep(2)
                if c.is_connected:
                    await terminate_other_sessions(c)

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
            # نمایش دقیق حداکثر ۲۰ ردیف و شمارش الباقی (F15)
            detail_lines = [
                f"▫️ آیدی {acc.id} (<code>{acc.phone_number if acc.phone_number else '؟'}</code>)"
                for acc in skipped_no_proxy[:20]
            ]
            if len(skipped_no_proxy) > 20:
                detail_lines.append(f"▫️ ... و {len(skipped_no_proxy) - 20} مورد دیگر")
            
            # اصلاح پیام ادمین: شفاف‌سازی حالت انتظار برای پروکسی جدید (باگ ۱)
            alert_text = (
                f"⚠️ <b>هشدار گارد IP (نشت آی‌پی)</b>\n"
                f"تعداد {len(skipped_no_proxy)} اکانت به دلیل نداشتن پروکسی معتبر متصل نشدند:\n\n"
                + "\n".join(detail_lines) +
                "\n\n💡 این اکانت‌ها در صف انتظار پروکسی (WAITING_PROXY) قرار گرفتند. به محض افزودن یا آزاد شدن پروکسی سالم، حلقه‌ی اتصال مجدد (هر ۵ دقیقه) آن‌ها را به استخر برمی‌گرداند."
            )
            await notify_admins(bot, alert_text)
            
    if skipped_direct_budget:
        skipped_budget_ids = ", ".join(str(acc.id) for acc in skipped_direct_budget)
        logger.warning(f"Direct Budget Guard: {len(skipped_direct_budget)} account(s) SKIPPED (MAX_DIRECT_ACCOUNTS) → [{skipped_budget_ids}]")
        if bot is not None:
            detail_lines = [
                f"▫️ آیدی {acc.id} (<code>{acc.phone_number if acc.phone_number else '؟'}</code>)"
                for acc in skipped_direct_budget[:30]
            ]
            if len(skipped_direct_budget) > 30:
                detail_lines.append("▫️ ... و موارد دیگر")
            
            alert_text = (
                f"⚠️ <b>سقف اکانت‌های مستقیم تکمیل شد!</b>\n"
                f"تعداد {len(skipped_direct_budget)} اکانت به دلیل رسیدن به سقف دایرکت (MAX_DIRECT_ACCOUNTS) متصل نشدند:\n"
                + "\n".join(detail_lines)
            )
            await notify_admins(bot, alert_text)



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
    
    use_proxy_for_sending = await get_use_proxy_for_sending(session)
    for account_id, client in list(worker_pool.items()):
        # 🔒 گارد دفاعی دوم (Defense-in-Depth): اگر به هر دلیلی کلاینتِ بدون پراکسی
        # داخل استخر باشد و فال‌بک صریح فعال نباشد، هرگز استارت نمی‌شود.
        if (
            use_proxy_for_sending
            and getattr(client, "proxy", None) is None 
            and not direct_ip_fallback_enabled(account_id)
        ):
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


# بازنویسی کامل: start_worker_with_rotation — workers/session_manager.py
async def start_worker_with_rotation(
    account_id: int,
    client: Client,
    session: AsyncSession,
    global_settings: Optional[GlobalSettings],
    bot: Optional[Bot] = None,
) -> bool:
    if await is_account_quarantined(account_id):
        logger.info(f"Worker {account_id} is quarantined (repeated AuthKeyUnregistered); skipping start attempt.")
        worker_pool.pop(account_id, None)
        return False

    MAX_RETRIES = 3
    auth_key_failures = 0
    
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            # 🟢 گارد پیشگیرانه: تست مسدودی آی‌پی سرور قبل از استارت بدون پروکسی
            if getattr(client, "proxy", None) is None:
                is_blocked = False
                try:
                    reader, writer = await asyncio.wait_for(
                        asyncio.open_connection("149.154.167.50", 443), timeout=3.0
                    )
                    writer.write(b'\xef')
                    await asyncio.wait_for(writer.drain(), timeout=2.0)
                    try:
                        data = await asyncio.wait_for(reader.read(1), timeout=1.5)
                        if not data:
                            is_blocked = True
                    except asyncio.TimeoutError:
                        pass
                    finally:
                        writer.close()
                        try: await writer.wait_closed()
                        except Exception: pass
                except Exception:
                    is_blocked = True

                if is_blocked:
                    logger.critical(f"Worker {account_id}: Server IP is BLOCKED (DPI/Ban). Aborting direct start.")
                    if bot is not None and not _no_proxy_alert_throttled(0):
                        try:
                            await notify_admins(bot, "🚨 <b>هشدار مسدودی آی‌پی سرور</b>\n\nپروکسی‌های سالم تمام شدند و آی‌پی سرور شما قابلیت اتصال مستقیم ندارد (مسدود یا در ایران است).\n<i>اکانت‌ها به وضعیت انتظار منتقل شدند.</i>")
                        except Exception: pass
                    
                    stmt_wait = (
                        update(Account)
                        .where(Account.id == account_id)
                        .values(proxy_status="WAITING_PROXY", proxy_queue_joined_at=datetime.now(timezone.utc))
                        .execution_options(synchronize_session=False)
                    )
                    await session.execute(stmt_wait)
                    await session.commit()
                    worker_pool.pop(account_id, None)
                    return False

            try:
                await asyncio.wait_for(client.start(), timeout=45)
            except asyncio.TimeoutError:
                raise Exception("Timeout connecting to Telegram during start().")
            except sqlite3.DatabaseError as db_err:
                session_file = SESSIONS_DIR / f"worker_acc_{account_id}.session"
                if session_file.exists() and not getattr(client, "in_memory", False):
                    logger.critical(f"Worker {account_id} session file is corrupt. Deleting and rebuilding via memory bootstrap. Error: {db_err}")
                    try:
                        session_file.unlink()
                    except Exception as unlink_err:
                        logger.error(f"Failed to delete corrupt session file for {account_id}: {unlink_err}")
                    
                    stmt_acc = select(Account).where(Account.id == account_id)
                    account_obj = await session.scalar(stmt_acc)
                    if account_obj:
                        new_client = await build_worker_client(account_obj, session, getattr(client, "proxy", None))
                        if new_client:
                            client = new_client
                            await client.start()
                        else:
                            raise db_err
                    else:
                        raise db_err
                else:
                    raise
            
            logger.info(f"Worker {account_id} connected successfully.")
            
            await warm_worker_cache(client, extra_peer_ids=config.FORCE_JOIN_CHANNEL_LIST or None)
            if getattr(client, "in_memory", False):
                await _persist_memory_session(client, account_id)

            worker_pool[account_id] = client
            
            task_profile = asyncio.create_task(
                randomize_profile(client, account_id, settings=global_settings)
            )
            _background_tasks.add(task_profile)
            task_profile.add_done_callback(_background_tasks.discard)

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
            # باگ ۲: ابتدا چک می‌کنیم که آیا پروکسی در دسترس است؟
            new_proxy_str = await claim_proxy_for_account(session, account_id)
            if not new_proxy_str:
                logger.warning(
                    f"Worker {account_id}: AuthKeyUnregistered but NO healthy proxy available for rotation. "
                    f"Moving to WAITING_PROXY instead of quarantining."
                )
                stmt_wait = (
                    update(Account)
                    .where(Account.id == account_id)
                    .values(proxy_status="WAITING_PROXY", proxy_queue_joined_at=datetime.now(timezone.utc))
                    .execution_options(synchronize_session=False)
                )
                await session.execute(stmt_wait)
                await session.commit()
                worker_pool.pop(account_id, None)
                return False # خروج تمیز، شمارنده AuthKeyUnregistered افزایش نیافت

            # اگر پروکسی بود، شمارنده خطای سشن اعمال می‌شود
            auth_key_failures += 1
            if auth_key_failures >= MAX_RETRIES:
                logger.error(f"Worker {account_id}: AuthKeyUnregistered persisted after {auth_key_failures} attempts - quarantining account.")
                worker_pool.pop(account_id, None)
                await mark_account_quarantined(account_id)
                if bot is not None:
                    try:
                        await notify_admins(
                            bot,
                            "🟠 <b>قرنطینه اکانت (AuthKeyUnregistered)</b>\n\n"
                            f"اکانت <code>{account_id}</code> موقتاً قرنطینه شد و سشن آن در DB محفوظ است."
                        )
                    except Exception: pass
                return False

            logger.warning(
                f"Worker {account_id}: AuthKeyUnregistered - {e}; "
                f"auth retry {auth_key_failures}/{MAX_RETRIES - 1} with fresh client + rotated proxy."
            )
            
            new_proxy_dict = parse_proxy_string(new_proxy_str)
            if not new_proxy_dict:
                try:
                    await mark_proxy_failed(session, new_proxy_str)
                    await session.commit()
                except Exception: pass
                await asyncio.sleep(2)
                continue
                
            try:
                await session.commit()
            except Exception:
                await session.rollback()
                
            if client.is_connected:
                try:
                    await client.stop()
                except Exception: pass
                
            stmt_acc = select(Account).where(Account.id == account_id)
            account_obj = await session.scalar(stmt_acc)
            if not account_obj or not account_obj.session_string:
                worker_pool.pop(account_id, None)
                return False
                
            new_client = await build_worker_client(account_obj, session, new_proxy_dict)
            if new_client is None:
                worker_pool.pop(account_id, None)
                return False
                
            worker_pool[account_id] = new_client
            client = new_client

        except AuthKeyDuplicated as e:
            logger.error(f"Worker {account_id}: AuthKeyDuplicated - session used elsewhere. Quarantining.")
            worker_pool.pop(account_id, None)
            await mark_account_quarantined(account_id)
            return False

        except (UserDeactivated, UserDeactivatedBan, Unauthorized) as e:
            logger.error(f"Worker {account_id} session revoked or banned: {e}")
            worker_pool.pop(account_id, None)
            try:
                banned_proxy_str = await session.scalar(
                    select(Account.proxy_string).where(Account.id == account_id)
                )
                stmt = update(Account).where(Account.id == account_id).values(is_banned=True, session_string=None)
                await session.execute(stmt)
                if banned_proxy_str:
                    await release_proxy_slot(session, banned_proxy_str)
                await session.commit()
            except Exception:
                await session.rollback()
            return False
            
        except Exception as e:
            logger.warning(f"Worker {account_id} connection failed (Attempt {attempt}/{MAX_RETRIES}): {e}")
            try:
                acc_stmt = select(Account).where(Account.id == account_id)
                failed_acc = await session.scalar(acc_stmt)
                if failed_acc and failed_acc.proxy_string:
                    await mark_proxy_failed(session, failed_acc.proxy_string)
                    await session.commit()
            except Exception as db_err:
                logger.error(f"Error reading original proxy string for account {account_id}: {db_err}")
            
            new_proxy_str = await claim_proxy_for_account(session, account_id)
            if new_proxy_str:
                new_proxy_dict = parse_proxy_string(new_proxy_str)
                if not new_proxy_dict:
                    try:
                        await mark_proxy_failed(session, new_proxy_str)
                        await session.commit()
                    except Exception: pass
                    await asyncio.sleep(2)
                    continue
                
                try:
                    await session.commit()
                except Exception:
                    await session.rollback()
                    
                if client.is_connected:
                    try:
                        await client.stop()
                    except Exception: pass
                        
                stmt_acc = select(Account).where(Account.id == account_id)
                account_obj = await session.scalar(stmt_acc)
                
                if not account_obj or not account_obj.session_string:
                    worker_pool.pop(account_id, None)
                    return False
                    
                new_client = await build_worker_client(account_obj, session, new_proxy_dict)
                if new_client is None:
                    worker_pool.pop(account_id, None)
                    return False
                
                worker_pool[account_id] = new_client
                client = new_client
            else:
                # 🟢 بررسی فال‌بک دایرکت آی‌پی در زمان قطعی کامل پروکسی‌ها با گارد تشخیص ایران/مسدودی
                from workers.session_manager import direct_ip_fallback_enabled, direct_budget_ok, _no_proxy_alert_throttled
                fallback_used = False
                
                if direct_ip_fallback_enabled(account_id) and await direct_budget_ok(session):
                    logger.warning(f"Worker {account_id}: No proxy available, checking direct IP fallback...")
                    
                    telegram_reachable = False
                    try:
                        # 🟢 تست هوشمند موقعیت سرور و دسترسی به تلگرام (MTProto + GeoIP)
                        import aiohttp
                        async with aiohttp.ClientSession(trust_env=False) as http_session:
                            # ۱. بررسی اینکه آیا سرور در ایران است؟
                            async with http_session.get("http://ip-api.com/json/", timeout=3.0) as geo_resp:
                                geo_data = await geo_resp.json()
                                if geo_data.get("countryCode") == "IR":
                                    logger.warning(f"Server is in Iran (IP: {geo_data.get('query')}). Direct connection is blocked by DPI.")
                                    telegram_reachable = False
                                else:
                                    # ۲. تست واقعی MTProto برای دیتاسنتر ۴ تلگرام (تشخیص آی‌پی بن شده)
                                    reader, writer = await asyncio.wait_for(
                                        asyncio.open_connection("149.154.167.50", 443), timeout=3.0
                                    )
                                    writer.write(b'\xef')
                                    await writer.drain()
                                    try:
                                        data = await asyncio.wait_for(reader.read(1), timeout=1.5)
                                        if not data:
                                            logger.warning("MTProto socket dropped instantly by server (IP Banned or DPI).")
                                            telegram_reachable = False
                                        else:
                                            telegram_reachable = True
                                    except asyncio.TimeoutError:
                                        telegram_reachable = True
                                    finally:
                                        writer.close()
                                        try:
                                            await writer.wait_closed()
                                        except Exception:
                                            pass
                    except Exception as e:
                        logger.debug(f"Direct connection check failed: {e}")
                        telegram_reachable = False
                        
                    if telegram_reachable:
                        logger.info(f"Direct IP is accessible. Switching worker {account_id} to direct connection.")
                        try:
                            stmt = update(Account).where(Account.id == account_id).values(proxy_string=None, proxy_status="NO_PROXY").execution_options(synchronize_session=False)
                            await session.execute(stmt)
                            await session.commit()
                            fallback_used = True
                        except Exception:
                            await session.rollback()
                            
                        if fallback_used:
                            if client.is_connected:
                                try: await client.stop()
                                except Exception: pass
                                
                            stmt_acc = select(Account).where(Account.id == account_id)
                            account_obj = await session.scalar(stmt_acc)
                            new_client = await build_worker_client(account_obj, session, None)
                            if new_client:
                                worker_pool[account_id] = new_client
                                client = new_client
                                continue # تلاش مجدد با کلاینت بدون پروکسی در همین حلقه
                    else:
                        logger.warning(f"Worker {account_id}: Direct IP fallback failed (Server IP is blocked or in Iran).")
                        if bot is not None and not _no_proxy_alert_throttled(0):
                            try:
                                await notify_admins(bot, "🚨 <b>هشدار مسدودی آی‌پی سرور</b>\n\nپروکسی‌های سالم تمام شدند، اما آی‌پی سرور شما قابلیت اتصال مستقیم به تلگرام را ندارد (احتمالاً ایران است یا مسدود شده).\n<i>تلاش برای فال‌بک لغو شد و اکانت‌ها متوقف شدند.</i>")
                            except Exception: pass

                if not fallback_used:
                    # اگر فال‌بک مجاز نبود یا آی‌پی مسدود بود، اکانت به صف انتظار می‌رود
                    logger.warning(f"Worker {account_id}: No healthy proxy available. Moving to WAITING_PROXY.")
                    stmt_wait = (
                        update(Account)
                        .where(Account.id == account_id)
                        .values(proxy_status="WAITING_PROXY", proxy_queue_joined_at=datetime.now(timezone.utc))
                        .execution_options(synchronize_session=False)
                    )
                    await session.execute(stmt_wait)
                    await session.commit()
                    worker_pool.pop(account_id, None)
                    return False

    worker_pool.pop(account_id, None)
    if bot is not None and not _disconnect_alert_throttled(account_id):
        try:
            await notify_admins(bot, f"⚠️ <b>قطعی ورکر</b>\nارتباط ورکر <code>{account_id}</code> قطع شد.")
        except Exception:
            pass

    return False
async def stop_all_workers() -> None:
    """Cleanly disconnects all active Pyrogram clients."""
    logger.info("Stopping all workers...")
    for account_id, client in list(worker_pool.items()):
        try:
            if getattr(client, "in_memory", False):
                await _persist_memory_session(client, account_id)
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

    stmt_settings = select(GlobalSettings).limit(1)
    global_settings = await session.scalar(stmt_settings)
    
    use_proxy_for_sending = await get_use_proxy_for_sending(session)

    if use_proxy_for_sending:
        proxy_dict = parse_proxy_string(account.proxy_string) if account.proxy_string else None
        if not proxy_dict:
            # 1. بررسی شرط مجاز بودن فال‌بک و خالی بودن بودجه دایرکت
            if direct_ip_fallback_enabled(account.id) and await direct_budget_ok(session):
                logger.warning(
                    f"Worker {account.id} has invalid/no proxy but fallback is allowed. "
                    "Switching to direct connection."
                )
                # 2. ریست وضعیت پراکسی اکانت در دیتابیس و قرار دادن پراکسی کلاینت روی None
                try:
                    stmt = (
                        update(Account)
                        .where(Account.id == account.id)
                        .values(proxy_string=None, proxy_status="NO_PROXY")
                        .execution_options(synchronize_session=False)
                    )
                    await session.execute(stmt)
                    await session.commit()
                except Exception as e:
                    logger.error(f"Failed to reset proxy state for worker {account.id}: {e}")
                    await session.rollback()
                    return False
                    
                proxy_dict = None
            else:
                # 3. عدم برقراری شروط فال‌بک
                logger.critical(
                    f"CRITICAL: Cannot start Worker {account.id} - No valid proxy found! "
                    "Direct IP fallback is disabled or budget is full. Aborting dynamically started worker to prevent IP leak."
                )
                return False
    else:
        proxy_dict = None
        if not await direct_budget_ok(session):
            logger.warning(f"Cannot start Worker {account.id} dynamically - MAX_DIRECT_ACCOUNTS reached.")
            return False

    # ساخت کلاینت از کارخانه‌ی واحد (همان منطق قبلی: API اختصاصی، Spoofing، CRM و «سین»)
    client = await build_worker_client(account, session, proxy_dict)
    if client is None:
        logger.error(f"Failed to build client for dynamically started Worker {account.id}.")
        return False

    try:
        try:
            await asyncio.wait_for(client.start(), timeout=45)
        except asyncio.TimeoutError:
            raise Exception("Timeout connecting to Telegram. Proxy or network might be dead.")
        except sqlite3.DatabaseError as db_err:
            session_file = SESSIONS_DIR / f"worker_acc_{account.id}.session"
            if session_file.exists() and not getattr(client, "in_memory", False):
                logger.critical(f"Worker {account.id} session file is corrupt. Deleting and rebuilding via memory bootstrap. Error: {db_err}")
                try:
                    session_file.unlink()
                except Exception as unlink_err:
                    logger.error(f"Failed to delete corrupt session file for {account.id}: {unlink_err}")
                new_client = await build_worker_client(account, session, proxy_dict)
                if new_client:
                    client = new_client
                    await client.start()
                else:
                    raise db_err
            else:
                raise

        await warm_worker_cache(client, extra_peer_ids=config.FORCE_JOIN_CHANNEL_LIST or None)
        if getattr(client, "in_memory", False):
            await _persist_memory_session(client, account.id)

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
        
        # 🎭 مدیریت پیشرفته پروفایل‌ها: آبجکت تنظیمات به randomize_profile پاس
        # می‌شود؛ سوئیچ‌های مستقل نام/بیو داخل خود تابع اعمال می‌شوند.
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

# ==========================================
# DEEP CLEANUP: پاکسازی کامل اکانت حذف‌شده
# ==========================================
async def remove_account_from_system(account_id: int) -> None:
    """
    حذف کامل اکانت از استخر ورکرها (RAM) و پاک کردن فایل فیزیکی سشن (Ghost Session).
    باید دقیقاً پس از حذف اکانت از دیتابیس فراخوانی شود.
    """
    logger.info(f"Deep cleaning account {account_id} from system...")
    
    # ۱. توقف کلاینت و اخراج از حافظه رم (worker_pool)
    client = worker_pool.pop(account_id, None)
    if client:
        try:
            if getattr(client, "is_connected", False):
                await client.stop()
            logger.info(f"Worker {account_id} stopped and removed from memory pool.")
        except Exception as e:
            logger.warning(f"Error stopping worker {account_id} during cleanup: {e}")
    else:
        logger.debug(f"Worker {account_id} was not active in the pool.")

    # ۲. پاک کردن فایل‌های فیزیکی روح از روی هارد سرور
    session_file = SESSIONS_DIR / f"worker_acc_{account_id}.session"
    journal_file = SESSIONS_DIR / f"worker_acc_{account_id}.session-journal"
    wal_file = SESSIONS_DIR / f"worker_acc_{account_id}.session-wal"
    shm_file = SESSIONS_DIR / f"worker_acc_{account_id}.session-shm"
    
    for f_path in [session_file, journal_file, wal_file, shm_file]:
        if f_path.exists():
            try:
                f_path.unlink()
                logger.info(f"Deleted ghost session file: {f_path.name}")
            except Exception as e:
                logger.error(f"Failed to delete session file {f_path.name}: {e}")
            