import asyncio
import html
import logging
import os
import random
import time
from datetime import datetime, timedelta, timezone
from typing import List, Optional
from zoneinfo import ZoneInfo
import redis.asyncio as aioredis
from pyrogram import Client
from pyrogram.errors import (
    AuthKeyUnregistered,
    ChannelInvalid,
    ChatForwardsRestricted,
    FloodWait,
    PeerFlood,
    PeerIdInvalid,
    Unauthorized,
    UserDeactivated,
    UserIsBlocked,
    UserIsBot,
    UserRestricted,
    UsernameInvalid,
    UsernameNotOccupied,
)
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from config import config
from database.engine import async_session
from database.models import (
    Account,
    AccountStatus,
    Admin,
    GlobalSettings,
    Order,
    OrderLog,
    OrderStatus,
    WorkerEvent,
)
from utils.advanced_anti_ban import check_spambot_status
from utils.anti_ban import parse_spintax, apply_adaptive_flood_wait
from utils.seen_watcher import (
    arm_seen_event,
    dismiss_seen_event,
    wait_for_seen,
)

try:
    import python_socks
except ImportError:
    python_socks = None

logger = logging.getLogger(__name__)

LOG_STATUS_SUCCESS = "success"
LOG_STATUS_ERROR = "error"
LOG_STATUS_FLOOD = "flood"
LOG_STATUS_RESTRICTED = "restricted"


# ==========================================
# 🎭 بخش ج: جهش متن هر ۱۰ ارسال (Anti-Fingerprint)
# ==========================================

# کاراکترهای نامرئی جهش متن
TEXT_MUTATION_INVISIBLE_CHARS: list = ["\u200b", "\u200c", "\u200d", "\u2060"]
# ایموجی‌های جهش متن (با فاصله‌ی ابتدایی)
TEXT_MUTATION_EMOJIS: list = [" 🔥", " ✅", " 👀", " 💫", " ⭐"]

# 🛡 فاز ۷ (R6): جهش متن با کاراکتر نامرئی/ایموجی چسبانده‌شده، خودش یک ویژگی آماری
# متمایز است (متن ~۹۰٪ مشابه + تفاوت کاراکتر نامرئی = سیگنال برای فیلترهای ضداسپم
# MTProto). «تنوع واقعی متن» از مسیرهای درست تأمین می‌شود:
#   • spintax روی هر ارسال (پیام ۱، بنر/پیام ۲ و پیام ۳) — آدمین می‌تواند متن را با
#     بلوک‌های تودرتوی بیشتر/گزینه‌های مترادف عمیق‌تر کند؛
#   • چرخش بنر در حالت کپی (random.choice روی پیام‌های کانال مبدا — موجود).
# جهش کاراکتر نامرئی فقط با فلگ صریح کانفیگ روشن می‌شود (پیش‌فرض خاموش):
#   TEXT_MUTATION_INVISIBLE_CHARS_ENABLED = True   # ← در config.py
TEXT_MUTATION_INVISIBLE_CHARS_ENABLED: bool = bool(
    getattr(config, "TEXT_MUTATION_INVISIBLE_CHARS_ENABLED", False)
)

# 🛡 فاز ۷ (BUG-19): شمارنده‌ی جایگزینِ درون‌حافظه‌ای حذف شد — با Redis واگرا می‌شد
# (پس از بازگشت Redis شماره‌ها می‌پرید/تکرار می‌شد). fallback جدید _next_send_index
# غیرواگراست.

# کلاینت Redis تنبل در سطح ماژول — عمداً جدا از کلاینت FSMِ main.py ساخته می‌شود
# (کلاینت FSM به sender پاس نمی‌شود؛ ساخت مستقیم اینجا ساده‌تر و ایزوله‌تر است)
_redis_client: Optional[aioredis.Redis] = None


def _get_redis() -> aioredis.Redis:
    """
    ساخت lazy کلاینت Redis برای شمارنده‌ی ارسال هر اکانت.
    دفعه‌ی اول ساخته می‌شود و در طول عمر پروسه زنده می‌ماند.
    """
    global _redis_client
    if _redis_client is None:
        redis_url = getattr(config, "REDIS_URL", None)
        if redis_url:
            # 🛡 فاز ۲: socket_timeout — دیسپچر حالا به این کلاینت وابسته است؛
            # hang شدن TCP نباید حلقه‌ی dispatch را فریز کند (خطا → fallback)
            _redis_client = aioredis.from_url(redis_url, decode_responses=True, socket_timeout=2)
        else:
            # fallback سازگار با main.py (متغیرهای محیطی) — اگر REDIS_URL در کانفیگ نبود
            _redis_client = aioredis.Redis(
                host=os.getenv("REDIS_HOST", "127.0.0.1"),
                port=int(os.getenv("REDIS_PORT", 6379)),
                password=os.getenv("REDIS_PASS") or None,
                decode_responses=True,
                socket_timeout=2,
            )
    return _redis_client


async def close_sender_redis() -> None:
    """بستن امن کلاینت Redis شمارنده (در shutdown اصلی صدا زده می‌شود)"""
    global _redis_client
    if _redis_client is not None:
        try:
            await _redis_client.aclose()
        except Exception:
            pass
        _redis_client = None


# ==========================================
# 🛡 فاز ۹ (BUG-27): فیلتر CRM — علامت‌گذاری «تارگت‌های ارسالِ اخیر»
# utils/crm_catcher.py فقط پیام‌های این تارگت‌ها را به ادمین‌ها فوروارد می‌کند؛
# پیام غریبه‌ها/اسپمرها دیگر بودجه‌ی Bot API (~30 msg/s) و حواس ادمین‌ها را
# مصرف نمی‌کنند. قرارداد کلید Redis (مشترک با crm_catcher):
#   crm_recent_target:{user_id} = "1"   با TTL = CRM_RELEVANT_WINDOW_HOURS
# ==========================================
_crm_mark_tasks: set = set()  # مرجع GC-safe برای تسک‌های fire-and-forget


async def mark_crm_recent_target(user_id) -> None:
    """
    ثبت «این peer اخیراً پیام کمپین دریافت کرده» — best-effort؛ خطا جریان
    ارسال را نمی‌شکند (فیلتر CRM به fail-open تنزل می‌کند و سقف نرخ همچنان فعال است).
    """
    try:
        if user_id is None:
            return
        await _get_redis().set(
            f"crm_recent_target:{int(user_id)}",
            "1",
            ex=max(1, int(getattr(config, "CRM_RELEVANT_WINDOW_HOURS", 72))) * 3600,
        )
    except Exception as e:
        logger.debug(
            f"CRM recent-target mark failed for {user_id} "
            f"({e.__class__.__name__}); CRM filter degrades to fail-open."
        )


def _spawn_crm_mark(user_id) -> None:
    """
    اجرای fire-and-forget علامت‌گذاری CRM با نگهداری مرجع تسک (GC-safe).
    عمداً بدون await → قید BUG-18 («هیچ await جدیدی بین پایان ارسال مرحله ۱ و
    wait_for_seen») نقض نمی‌شود؛ در همان لحظه رویداد «سین» مسلح شده است.
    """
    task = asyncio.create_task(mark_crm_recent_target(user_id))
    _crm_mark_tasks.add(task)
    task.add_done_callback(_crm_mark_tasks.discard)


async def _next_send_index(account_db_id: int) -> int:
    """
    شماره‌ی جاریِ ارسالِ یک اکانت را با INCR اتمیک (PIPELINE MULTI/EXEC) روی کلید
    send_count:{account_db_id} از Redis می‌گیرد.
    🛡 فاز ۷ (BUG-19):
      • TTL معنادار: کلید هر روز تا نیمه‌شب تهران (هم‌مرز با daily_send) منقضی
        می‌شود — شمارنده «دوره» دارد و کلید جاودانه نمی‌ماند؛
      • fallback واگرای حافظه‌ای حذف شد: در قطع لحظه‌ای Redis مقدار ۱ برگردانده
        می‌شود (۱ هرگز مضرب ۱۰ نیست → جهشی اعمال نمی‌شود) — محافظه‌کارانه و
        هرگز با شمارنده‌ی Redis واگرا نمی‌شود.
    """
    key = f"send_count:{account_db_id}"
    try:
        pipe = _get_redis().pipeline()  # MULTI/EXEC — INCR و EXPIRE اتمیک
        pipe.incr(key)
        pipe.expire(key, _seconds_until_tehran_midnight())
        value, _ = await pipe.execute()
        return int(value)
    except Exception as e:
        logger.warning(
            f"Redis INCR failed for {key} ({e.__class__.__name__}: {e}); "
            f"skipping text mutation for this message (non-divergent fallback)."
        )
        return 1

# ==========================================
# 🛡 فاز ۲ — موتور ضد-بن: سقف روزانه + cooldown دوره‌ای (BUG-03)
# کلیدهای Redis:
#   daily_send:{account_id}:{yyyymmdd} → INCR اتمیک؛ TTL تا نیمه‌شب (Asia/Tehran)
#   chunk_cooldown:{account_id}        → SET؛ TTL = cooldown_hours × ۳۶۰۰
# fallback درون‌حافظه‌ای مطابق الگوی send_count برای لحظات قطع Redis.
# ==========================================

try:
    TEHRAN_TZ = ZoneInfo("Asia/Tehran")
except Exception:

    TEHRAN_TZ = timezone(timedelta(hours=3, minutes=30))

_local_daily_counts: dict = {}      # fallback شمارنده‌ی روزانه
_local_cooldown_until: dict = {}    # آینه‌ی درون‌حافظه‌ای cooldown (همیشه نوشته می‌شود)


def _daily_key(account_db_id: int) -> str:
    return f"daily_send:{account_db_id}:{datetime.now(TEHRAN_TZ).strftime('%Y%m%d')}"


def _seconds_until_tehran_midnight() -> int:
    now = datetime.now(TEHRAN_TZ)
    tomorrow = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return max(60, int((tomorrow - now).total_seconds()))


def _cooldown_key(account_db_id: int) -> str:
    return f"chunk_cooldown:{account_db_id}"


async def get_daily_sent_count(account_db_id: int) -> int:
    """تعداد تلاش‌های ارسالِ ثبت‌شده برای این اکانت در روز جاری (به وقت تهران)."""
    key = _daily_key(account_db_id)
    try:
        val = await _get_redis().get(key)
        return int(val) if val else 0
    except Exception as e:
        logger.warning(f"Redis GET failed for {key} ({e.__class__.__name__}: {e}); daily counter using in-memory fallback.")
        return _local_daily_counts.get(key, 0)


async def incr_daily_sent_count(account_db_id: int) -> int:
    """افزایش اتمیک شمارنده‌ی روزانه (INCR) + تمدید TTL تا نیمه‌شب تهران."""
    key = _daily_key(account_db_id)
    try:
        pipe = _get_redis().pipeline()  # MULTI/EXEC — INCR و EXPIRE اتمیک
        pipe.incr(key)
        pipe.expire(key, _seconds_until_tehran_midnight())
        value, _ = await pipe.execute()
        return int(value)
    except Exception as e:
        logger.warning(f"Redis INCR failed for {key} ({e.__class__.__name__}: {e}); daily counter using in-memory fallback.")
        _local_daily_counts[key] = _local_daily_counts.get(key, 0) + 1
        today_suffix = ":" + key.rsplit(":", 1)[-1]
        for stale_key in [k for k in _local_daily_counts if not k.endswith(today_suffix)]:
            _local_daily_counts.pop(stale_key, None)
        return _local_daily_counts[key]


async def daily_cap_reached(account_db_id: int, daily_limit: int) -> bool:
    """
    چک سقف روزانه — هم در دیسپچر (قبل از اختصاص chunk) و هم داخل
    execute_bulk_send (قبل از هر ارسال) صدا زده می‌شود.
    مقدار ۰ یا منفی → همیشه True (بلاک کامل؛ مسیر امن).
    """
    return (await get_daily_sent_count(account_db_id)) >= daily_limit


async def mark_chunk_cooldown(account_db_id: int, cooldown_hours) -> None:
    """
    🛡 BUG-03/ج: ثبت «پایان chunk» این اکانت — معادل ذخیره‌ی زمان پایان + مدت cooldown،
    با TTL. تا انقضای TTL، دیسپچر chunk جدیدی به این اکانت نمی‌دهد.
    cooldown_hours <= 0 → خاموشی صریح cooldown توسط ادمین → علامت‌گذاری نمی‌شود.
    """
    if cooldown_hours is None or cooldown_hours <= 0:
        return
    ttl_seconds = int(cooldown_hours * 3600)
    key = _cooldown_key(account_db_id)
    now_utc = datetime.now(timezone.utc)
    for acc_id in [a for a, until in _local_cooldown_until.items() if until <= now_utc]:
        _local_cooldown_until.pop(acc_id, None)
    _local_cooldown_until[account_db_id] = now_utc + timedelta(seconds=ttl_seconds)

    try:
        await _get_redis().set(key, "1", ex=ttl_seconds)
    except Exception as e:
        logger.warning(f"Redis SET failed for {key} ({e.__class__.__name__}: {e}); cooldown kept in-memory only.")


async def is_in_cooldown(account_db_id: int) -> bool:
    """آیا اکانت هنوز در cooldown دوره‌ای بین chunkهاست؟ (Redis + آینه‌ی حافظه)"""
    key = _cooldown_key(account_db_id)
    try:
        if await _get_redis().exists(key):
            return True
    except Exception as e:
        logger.warning(f"Redis EXISTS failed for {key} ({e.__class__.__name__}: {e}); falling back to in-memory cooldown.")
    until = _local_cooldown_until.get(account_db_id)
    if until is not None:
        if until > datetime.now(timezone.utc):
            return True
        del _local_cooldown_until[account_db_id]
    return False

# ==========================================
# 🛡 فاز ۴ (R8): کاهش بار سراسری پس از FloodWait
# مشکل: پاسخ سیستم به FloodWait «جابه‌جایی بار» بود نه «کاهش بار» (تارگت‌ها به
# اکانت‌های دیگر یا همان اکانت بعد از جریمه برمی‌گشتند) ← در بلندمدت موج بن جمعی.
# فیکس: بعد از هر FloodWait یک پنجره‌ی «کاهش بار سراسری» کوتاه باز می‌شود:
#   کلید Redis: global_slowdown_until (مقدار = epoch پایان؛ TTL = مدت پنجره)
# تا پایان پنجره: تابع تاخیر ارسال (این فایل) ضریب ×۲–۳ اعمال می‌کند و دیسپچر
# (task_queue) chunk کوچک‌تر می‌دهد. دیسپچِ اکانتِ جریمه‌شده خودش از طریق
# flood_wait_until + busy-state + cooldown فاز ۲ متوقف می‌ماند.
# fallback درون-حافظه‌ای مطابق الگوی chunk_cooldown (معماری تک-پروسه).
# ==========================================
GLOBAL_SLOWDOWN_KEY = "global_slowdown_until"
GLOBAL_SLOWDOWN_MIN_SECONDS = 2 * 60    # حداقل پنجره: ۲ دقیقه
GLOBAL_SLOWDOWN_MAX_SECONDS = 10 * 60   # حداکثر پنجره: ۱۰ دقیقه

_local_global_slowdown_until: float = 0.0  # آینه‌ی درون‌حافظه‌ای


async def mark_global_slowdown(wait_seconds) -> None:
    """
    🛡 R8: باز کردن/تمدید پنجره‌ی «کاهش بار سراسری» پس از FloodWait.
    مدت پنجره بر اساس شدت جریمه (wait_seconds)، محدود بین ۲ تا ۱۰ دقیقه.
    """
    global _local_global_slowdown_until
    ttl = max(
        GLOBAL_SLOWDOWN_MIN_SECONDS,
        min(int(wait_seconds or 0), GLOBAL_SLOWDOWN_MAX_SECONDS),
    )
    until_epoch = datetime.now(timezone.utc).timestamp() + ttl
    _local_global_slowdown_until = until_epoch
    try:
        await _get_redis().set(GLOBAL_SLOWDOWN_KEY, str(int(until_epoch)), ex=ttl)
    except Exception as e:
        logger.warning(
            f"Redis SET failed for {GLOBAL_SLOWDOWN_KEY} "
            f"({e.__class__.__name__}: {e}); global slowdown kept in-memory only."
        )


async def is_global_slowdown() -> bool:
    """🛡 R8: آیا پنجره‌ی کاهش بار سراسری فعال است؟ (Redis + آینه‌ی حافظه)"""
    try:
        val = await _get_redis().get(GLOBAL_SLOWDOWN_KEY)
        if val and float(val) > datetime.now(timezone.utc).timestamp():
            return True
    except Exception as e:
        logger.warning(
            f"Redis GET failed for {GLOBAL_SLOWDOWN_KEY} "
            f"({e.__class__.__name__}: {e}); using in-memory fallback."
        )
    return _local_global_slowdown_until > datetime.now(timezone.utc).timestamp()


async def _humanized_send_delay() -> float:
    """Env-tunable humanized delay; global slowdown now multiplies by
    GLOBAL_SLOWDOWN_FACTOR_MIN..MAX (default 1.5-2.0 instead of 2-3)."""
    delay = random.uniform(config.SEND_DELAY_MIN, config.SEND_DELAY_MAX)
    if await is_global_slowdown():
        delay *= random.uniform(config.GLOBAL_SLOWDOWN_FACTOR_MIN, config.GLOBAL_SLOWDOWN_FACTOR_MAX)
    return delay

# ==========================================
# 🛑 فاز ۳ (BUG-02): Kill Switch واقعی
# کلید Redis: kill_order:{order_id} — هندلر لغوی ادمین باید آن را با TTL بنویسد.
# فایل هندلر لغو attach نشده است؛ در این فاز فقط «سمت خواندن» پیاده شده و
# چک status با session تازه به‌عنوان مسیر fallback/اصلی باقی مانده است.
# ==========================================

def _kill_flag_key(order_id: int) -> str:
    return f"kill_order:{order_id}"


async def is_order_killed(order_id: int) -> bool:
    """
    🛑 BUG-02: بررسی توقف سفارش با دیتای «تازه».
    مشکل قبلی: چک روی sessionِ طولانیِ ورکر انجام می‌شد؛ اولین read تراکنشِ
    ضمنی را باز می‌کرد و با ایزولیشن REPEATABLE READ همه‌ی readهای بعدی همان
    snapshot لحظه‌ی اول را می‌دیدند ← status=error ادمین تا پایان chunk دیده
    نمی‌شد و Kill Switch عملاً خراب بود.
    فیکس:
      ۱) فلگ Redis (نوشته‌شده توسط هندلر لغو با TTL) — مسیر سریع بدون تراکنش DB؛
      ۲) fallback: خواندن status در یک session کوتاهِ تازه (snapshot تازه).
    خطای Redis → ادامه به چک DB؛ خطای DB → فرض «کشته نشده» + لاگ هشدار
    (اگر DB واقعاً پایین باشد، commit افزایشی BUG-09 در مرز block بعدی chunk
    را قطع می‌کند و پنجره‌ی مبهمی محدود می‌ماند).
    """
    try:
        if await _get_redis().exists(_kill_flag_key(order_id)):
            return True
    except Exception as e:
        logger.warning(
            f"Redis EXISTS failed for {_kill_flag_key(order_id)} "
            f"({e.__class__.__name__}: {e}); falling back to DB status check."
        )
    try:
        async with async_session() as fresh:
            status = await fresh.scalar(select(Order.status).where(Order.id == order_id))
            return status == OrderStatus.error
    except Exception as e:
        logger.warning(
            f"Kill Switch DB check failed for Order #{order_id} "
            f"({e.__class__.__name__}: {e}); assuming NOT killed."
        )
        return False
    

def effective_daily_limit(account_created_at: Optional[datetime]) -> int:
    """
    🛡 فاز ۲ / مکمل (BUG-03/ب): سقف روزانه‌ی «مؤثر» یک اکانت بر اساس سن آن.
    - created_at=NULL یا سن < NEW_ACCOUNT_DAYS روز → سقف سخت‌گیرانه‌ی اکانت تازه
      (با min هرگز از سقف پایه بیشتر نمی‌شود).
    - در غیر این صورت → سقف پایه DAILY_SEND_LIMIT_PER_ACCOUNT.
    datetimeهای naive (خروجی معمول MySQL DATETIME) UTC فرض می‌شوند — هم‌راستا با
    قرارداد flood_wait_until که با datetime.now(timezone.utc) مقایسه می‌شود.
    """
    base_limit = config.DAILY_SEND_LIMIT_PER_ACCOUNT
    fresh_limit = min(base_limit, config.NEW_ACCOUNT_DAILY_SEND_LIMIT)
    if account_created_at is None:
        return fresh_limit
    created = account_created_at
    if created.tzinfo is None:
        created = created.replace(tzinfo=timezone.utc)
    age_days = (datetime.now(timezone.utc) - created).total_seconds() / 86400.0
    if age_days < config.NEW_ACCOUNT_DAYS:
        return fresh_limit
    return base_limit

def mutate_text(text: str, send_index: int) -> str:
    """
    🎭 جهش متن (🛡 فاز ۷ / R6 — پیش‌فرض خاموش): فقط وقتی
    TEXT_MUTATION_INVISIBLE_CHARS_ENABLED در کانفیگ روشن باشد فعال است.
    اگر send_index > 0 و مضرب ۱۰ باشد، یک کاراکتر نامرئی (ZWNJ و
    هم‌خانواده‌هایش) یا یک ایموجی به انتهای متن می‌چسباند. بقیه‌ی ارسال‌ها
    دست‌نخورده برمی‌گردند. تنوع واقعی متن بر عهده‌ی spintax (پیام ۱/۲/۳) و
    چرخش بنر (حالت کپی) است.
    """
    if not TEXT_MUTATION_INVISIBLE_CHARS_ENABLED:
        return text
    if send_index > 0 and send_index % 10 == 0:
        mutation = random.choice(TEXT_MUTATION_INVISIBLE_CHARS + TEXT_MUTATION_EMOJIS)
        mutated = text + mutation
        # دیباگ: با repr می‌توان کاراکتر نامرئی/ایموجی چسبانده‌شده را در لاگ تأیید کرد
        logger.debug(f"mutate_text: send_index={send_index}, suffix={mutation!r}, result={mutated!r}")
        return mutated
    return text


async def _send_via_fallback(
    client: Client,
    target: str,
    order: Order,
    chosen_msg_id: int,
    from_chat,
) -> Optional[object]:
    """
    🟢 فاز ۵: fallback برای کانال‌های دارای has_protected_content.
    
    پیام مبدا را با get_messages می‌گیرد و محتوایش را به‌صورت مستقیم به تارگت
    می‌فرستد (بدون header فوروارد). اگر پیام مدیا داشته باشد، آن را download
    کرده و دوباره upload می‌کند.
    """
    try:
        messages = await client.get_messages(chat_id=from_chat, message_ids=chosen_msg_id)
        if not messages:
            return None
        src_msg = messages[0] if isinstance(messages, list) else messages
    except Exception as e:
        logger.warning(
            f"Fallback get_messages failed for Order #{order.id} "
            f"(msg_id={chosen_msg_id}): {e.__class__.__name__}"
        )
        return None
    
    real_target = int(target) if target.lstrip("-").isdigit() else target
    markup = None
    if order.button_text and order.button_url:
        markup = InlineKeyboardMarkup(
            [[InlineKeyboardButton(text=str(order.button_text), url=str(order.button_url))]]
        )
    
    try:
        # اگر پیام مدیا دارد
        media = getattr(src_msg, "media", None)
        caption = getattr(src_msg, "caption", None) or getattr(src_msg, "text", None) or ""
        
        if media is not None:
            # download و re-upload
            try:
                media_path = await client.download_media(src_msg, in_memory=True)
                if media_path is None:
                    # download ناموفق → fallback به متن
                    text = getattr(src_msg, "text", "") or getattr(src_msg, "caption", "") or ""
                    return await client.send_message(
                        chat_id=real_target,
                        text=text or "(محتوای پیام قابل‌دسترسی نبود)",
                        reply_markup=markup,
                        disable_web_page_preview=True,
                    )
                
                # تشخیص نوع مدیا
                from pyrogram.enums import MessageMediaType
                media_type = getattr(src_msg, "media", None)
                
                if media_type == MessageMediaType.PHOTO:
                    return await client.send_photo(
                        chat_id=real_target,
                        photo=media_path,
                        caption=caption,
                        reply_markup=markup,
                    )
                elif media_type == MessageMediaType.VIDEO:
                    return await client.send_video(
                        chat_id=real_target,
                        video=media_path,
                        caption=caption,
                        reply_markup=markup,
                    )
                else:
                    # سایر مدیاها (سند، صوت، ...) ← send_document
                    return await client.send_document(
                        chat_id=real_target,
                        document=media_path,
                        caption=caption,
                        reply_markup=markup,
                    )
            except Exception as media_err:
                logger.warning(
                    f"Fallback media re-upload failed for Order #{order.id}: {media_err}"
                )
                # fallback نهایی: فقط متن
                text = getattr(src_msg, "text", "") or getattr(src_msg, "caption", "") or ""
                if text:
                    return await client.send_message(
                        chat_id=real_target,
                        text=text,
                        reply_markup=markup,
                        disable_web_page_preview=True,
                    )
                return None
        else:
            # پیام متنی ساده
            text = getattr(src_msg, "text", "") or ""
            if not text:
                return None
            return await client.send_message(
                chat_id=real_target,
                text=text,
                reply_markup=markup,
                disable_web_page_preview=True,
            )
    except Exception as e:
        logger.error(
            f"Fallback send failed for Order #{order.id} target={target}: {e.__class__.__name__}: {e}"
        )
        return None

async def _order_needs_fallback(order_id: int) -> bool:
    """بررسی اینکه آیا این سفارش قبلاً به fallback سوییچ کرده یا نه."""
    try:
        redis = _get_redis()
        return bool(await redis.get(f"fwd_fallback:{order_id}"))
    except Exception:
        return False

async def _mark_order_fallback(order_id: int) -> None:
    """ثبت اینکه این سفارش به fallback سوییچ کرده (برای ارسال‌های بعدی)."""
    try:
        redis = _get_redis()
        await redis.set(f"fwd_fallback:{order_id}", "1", ex=86400)  # 24 ساعت
    except Exception as e:
        logger.debug(f"Could not mark order {order_id} for fallback: {e}")


async def _ensure_source_channel_access(
    client: Client,
    order: Order,
) -> tuple[bool, Optional[int], str]:
    """
    🟢 فاز ۳: قبل از شروع ارسال، بررسی می‌کند که آیا ورکر به کانال مبدا دسترسی دارد.
    
    برمی‌گرداند:
      (accessible, resolved_chat_id, status_message)
    
    حالت‌ها:
      - accessible=True  → ورکر عضو است یا کانال پابلیک است؛ آماده‌ی ارسال.
      - accessible=False → ورکر دسترسی ندارد؛ chunk باید به‌طور کامل به unsent برگردد.
    
    نکته: این تابع سعی نمی‌کند به کانال مبدا join بزند (join کردن کانال مبدا
    معمولاً مطلوب admin نیست و می‌تواند باعث ban اکانت شود). فقط resolve و
    access-check می‌کند.
    """
    if not order.source_channel_id and not getattr(order, "source_message_ids", None):
        # سفارش فوروارد نیست — چیزی برای بررسی نیست
        return True, None, "no_source_channel"
    
    # تجزیه‌ی source_message_ids برای گرفتن source_username (قطب سوم)
    source_username = None
    if order.source_message_ids:
        parts = order.source_message_ids.split("|")
        if len(parts) > 2 and parts[2]:
            source_username = parts[2]  # مثلاً "@channelname"
    
    # اولویت ۱: اگر یوزرنیم داریم (کانال پابلیک)، resolve می‌کنیم
    if source_username:
        try:
            chat = await asyncio.wait_for(client.get_chat(source_username), timeout=15)
            return True, chat.id, "resolved_via_username"
        except FloodWait as e:
            # اگر FloodWait خوردیم، به‌معنای درست بودن کانال است ولی محدودیت داریم
            return False, None, f"limit:flood_wait:{int(e.value)}"
        except Exception as e:
            logger.warning(
                f"Preflight source-check via username '{source_username}' failed "
                f"for worker {client.name}: {e.__class__.__name__}"
            )
            # به مسیر chat_id ادامه می‌دهیم
    
    # اولویت ۲: استفاده از source_channel_id عددی
    if order.source_channel_id:
        try:
            # get_chat_member با "me" سریع‌تر از get_chat است و فقط عضویت را چک می‌کند
            from pyrogram.enums import ChatMemberStatus
            member = await asyncio.wait_for(
                client.get_chat_member(order.source_channel_id, "me"),
                timeout=15,
            )
            if member.status in (
                ChatMemberStatus.MEMBER,
                ChatMemberStatus.ADMINISTRATOR,
                ChatMemberStatus.OWNER,
            ):
                return True, order.source_channel_id, "member"
            return False, None, "not_member"
        except Exception as e:
            err_name = e.__class__.__name__
            logger.warning(
                f"Preflight source-check via chat_id {order.source_channel_id} failed "
                f"for worker {client.name}: {err_name}"
            )
            if err_name in ("ChannelPrivate", "ChannelInvalid", "PeerIdInvalid"):
                return False, None, "no_access"
            if err_name == "UserNotParticipant":
                return False, None, "not_member"
            return False, None, f"error:{err_name}"
    
    # هیچ منبعی برای resolve نداریم
    return False, None, "no_source_identifier"


async def _notify_admins_on_source_failure(
    session: AsyncSession, order_id: int, error_name: str
) -> None:
    """
    🚨 اطلاع‌رسانی به ادمین‌ها هنگامی که کل یک chunk به خطای کانال مبدا
    (ChannelInvalid / ChatForwardsRestricted) شکست خورده و سفارش error شده است.
    بهترین تلاش (best-effort): هر خطا فقط لاگ می‌شود و هرگز جریان ورکر را نمی‌شکند.
    ارسال از طریق Bot API (با توکن بات Aiogram) انجام می‌شود چون sender به
    instance زنده‌ی بات دسترسی ندارد.
    """
    notification_text = (
        f"🚨 <b>سفارش #{order_id} متوقف شد — خطای کانال مبدا</b>\n\n"
        f"خطا: <code>{html.escape(error_name)}</code>\n\n"
        "کانال مبدا حذف/بن شده، ورکرها از آن حذف شده‌اند یا «حفاظت از محتوا» "
        "(Restrict Saving) آن فعال است؛ به همین دلیل کپی پیام‌ها ممکن نیست و "
        "وضعیت سفارش به error تغییر یافت.\n"
        "لطفاً سفارش را با کانال مبدا‌ی سالم دوباره ثبت کنید."
    )

    chat_ids: List[int] = []

    # ۱) ادمین‌های فرعی ثبت‌شده در دیتابیس
    try:
        admin_ids = (await session.scalars(select(Admin.telegram_id))).all()
        chat_ids.extend(int(tid) for tid in admin_ids)
    except Exception as e:
        logger.warning(f"Source-failure notification: could not load admins from DB: {e}")

    # ۲) ادمین اصلی از کانفیگ (نام‌های رایج — در صورت نبود، نادیده گرفته می‌شود)
    for attr in ("ADMIN_IDS", "ADMIN_ID", "MAIN_ADMIN_ID", "OWNER_ID"):
        value = getattr(config, attr, None)
        if not value:
            continue
        try:
            if isinstance(value, (list, tuple, set)):
                chat_ids.extend(int(v) for v in value)
            else:
                chat_ids.append(int(value))
        except (TypeError, ValueError):
            pass
        break

    # حذف تکراری‌ها با حفظ ترتیب
    chat_ids = list(dict.fromkeys(chat_ids))

    if not chat_ids:
        logger.warning(f"Source-failure notification: no admin chat id found for Order #{order_id}.")
        return

    # 🔐 فاز ۹ (BUG-13/SEC-7): توکن بات از منبع واحد — config.BOT_TOKEN (env: BOT_TOKEN).
    # قبلاً getattr روی فیلدهایی می‌زد که در کلاس Config وجود نداشتند ← bot_token=None
    # ← پیام «سفارش متوقف شد — خطای کانال مبدا» هرگز به ادمین ارسال نمی‌شد.
    bot_token = config.BOT_TOKEN
    if not bot_token:
        logger.warning(f"Source-failure notification: BOT_TOKEN not configured (env BOT_TOKEN) for Order #{order_id}.")
        return

    try:
        import aiohttp  # lazy — وابستگی‌ی موجود در کنار aiogram
    except ImportError:
        logger.warning("Source-failure notification: aiohttp not available.")
        return

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as http:
            for chat_id in chat_ids:
                try:
                    async with http.post(url, json={
                        "chat_id": chat_id,
                        "text": notification_text,
                        "parse_mode": "HTML",
                    }) as resp:
                        if resp.status != 200:
                            body = (await resp.text())[:200]
                            logger.warning(
                                f"Source-failure notification to {chat_id}: HTTP {resp.status} {body}"
                            )
                except Exception as e:
                    logger.warning(f"Source-failure notification to {chat_id} failed: {e}")
    except Exception as e:
        logger.warning(f"Source-failure notification (HTTP session) failed for Order #{order_id}: {e}")


MAX_CONSECUTIVE_ERRORS = int(getattr(config, "MAX_CONSECUTIVE_ERRORS", 3))
COOLDOWN_MINUTES_ON_ERROR = int(getattr(config, "COOLDOWN_MINUTES_ON_ERROR", 30))
HOURLY_SEND_LIMIT_PER_ACCOUNT = int(getattr(config, "HOURLY_SEND_LIMIT_PER_ACCOUNT", 15))

def _hourly_key(account_db_id: int) -> str:
    return f"hourly_send:{account_db_id}:{datetime.now(TEHRAN_TZ).strftime('%Y%m%d%H')}"

async def get_hourly_sent_count(account_db_id: int) -> int:
    try:
        val = await _get_redis().get(_hourly_key(account_db_id))
        return int(val) if val else 0
    except Exception: return 0

async def incr_hourly_sent_count(account_db_id: int) -> int:
    key = _hourly_key(account_db_id)
    try:
        pipe = _get_redis().pipeline()
        pipe.incr(key)
        pipe.expire(key, 3600)  # انقضا بعد از یک ساعت
        value, _ = await pipe.execute()
        return int(value)
    except Exception: return 1

async def change_account_status(
    session: AsyncSession, account_id: int, new_status: AccountStatus, 
    reason: str, error_details: str = None, return_time: datetime = None
) -> None:
    """تغییر اتمیک وضعیت اکانت و ثبت لاگ در WorkerEvent"""
    account = await session.get(Account, account_id)
    if not account or account.status == new_status: return
    
    old_status = account.status.value if hasattr(account.status, 'value') else str(account.status)
    account.status = new_status
    account.status_reason = reason
    account.expected_return_time = return_time
    if new_status == AccountStatus.active:
        account.consecutive_errors = 0
        
    event = WorkerEvent(
        account_id=account_id,
        old_status=old_status,
        new_status=new_status.value if hasattr(new_status, 'value') else str(new_status),
        reason=reason,
        error_details=error_details
    )
    session.add(event)
    await session.commit()


async def execute_bulk_send(
    client: Client,
    account_db_id: int,
    order: Order,
    targets: List[str],
    session: AsyncSession,
    progress_reporter=None,
) -> tuple[List[str], Optional[str]]:
    """
    Isolated bulk-send execution for one chunk of targets.
    
    Phase 1/2 Observability Update:
    - Added structured JSON logging for every send attempt (StructuredLog).
    - Status values in OrderLog strictly mapped to: success, error, flood, restricted, partial.
    - Added duration (ms) and exact stage number of failure to both app logs and DB.
    """
    import json
    import time
    from datetime import datetime, timezone
    
    logger.info(
        f"Order #{order.id}: chunk started for worker user_{account_db_id}/ "
        f"({len(targets)} targets)."
    )

    async def _report(**fields) -> None:
        if progress_reporter is None:
            return
        try:
            await progress_reporter.update(**fields)
        except Exception as report_err:
            logger.debug(f"Progress report suppressed: {report_err}")

    account_tag = f"user_{account_db_id}/"

    # ==========================================
    # 🛑 فاز جدید: پیش‌پرواز سلامت اکانت (Preflight Check) + فاز ۱۰ (Account Status)
    # ==========================================
    now_utc = datetime.now(timezone.utc)
    try:
        from database.models import AccountStatus
        account_row = await session.get(Account, account_db_id)
        if account_row:
            if account_row.status != AccountStatus.active:
                await _report(status=f"اکانت فعال نیست (وضعیت: {account_row.status.value}) — بازگشت تارگت‌ها…", account=account_tag)
                return targets

            if account_row.is_banned:
                await _report(status="اکانت مسدود است — بازگشت تارگت‌ها به صف…", account=account_tag)
                return targets
                
            flooded = account_row.flood_wait_until and account_row.flood_wait_until.replace(tzinfo=timezone.utc) > now_utc
            restricted = account_row.restricted_until and account_row.restricted_until.replace(tzinfo=timezone.utc) > now_utc
            
            if flooded or restricted:
                await _report(status="اکانت دارای محدودیت زمانی است — بازگشت تارگت‌ها به صف…", account=account_tag)
                return targets

            spambot_enabled = getattr(config, "PREFLIGHT_SPAMBOT_CHECK_ENABLED", True)
            cache_hours = getattr(config, "PREFLIGHT_SPAMBOT_CACHE_HOURS", 4)
            
            last_check = account_row.spambot_checked_at
            if last_check and last_check.tzinfo is None:
                last_check = last_check.replace(tzinfo=timezone.utc)
                
            if spambot_enabled and (not last_check or (now_utc - last_check).total_seconds() > cache_hours * 3600):
                await _report(status="🔍 بررسی پیشگیرانه وضعیت Shadow-ban با @SpamBot...", account=account_tag)
                
                from utils.advanced_anti_ban import check_spambot_status
                status = await check_spambot_status(client, account_db_id)
                
                if status:
                    account_row.spambot_checked_at = now_utc
                    account_row.spambot_report = status["text"]
                    
                    if status["restricted"] and status["until"]:
                        account_row.restricted_until = status["until"]
                        await session.commit()
                        await _report(status="⚠️ اکانت Shadow-ban (محدود) است — بازگشت تارگت‌ها...", account=account_tag)
                        return targets
                    elif status["restricted"]:
                        account_row.is_banned = True
                        await session.commit()
                        from workers.sender import change_account_status
                        await change_account_status(session, account_db_id, AccountStatus.blocked, "SpamBot Restriction", status["text"])
                        await _report(status="⛔️ اکانت بن دائم است — بازگشت تارگت‌ها...", account=account_tag)
                        return targets
                        
                    await session.commit()
    except Exception as e:
        logger.warning(f"Preflight check failed for user_{account_db_id}/: {e}")
    # ==========================================

    # 🟢 فاز ۳: اگر سفارش از نوع فوروارد از کانال است، قبل از شروع ارسال
    # بررسی می‌کنیم که ورکر به کانال مبدا دسترسی دارد. اگر نه، کل chunk
    # را به‌عنوان unsent برمی‌گردانیم تا دیسپچر ورکر بعدی را امتحان کند.
    if order.source_channel_id or getattr(order, "source_message_ids", None):
        try:
            accessible, src_chat_id, src_status = await _ensure_source_channel_access(client, order)
        except Exception as pre_err:
            logger.warning(
                f"Preflight source-access crashed for Order #{order.id} "
                f"worker user_{account_db_id}/: {pre_err}"
            )
            accessible, src_chat_id, src_status = False, None, "preflight_crashed"
        
        if not accessible:
            await _report(
                status=(
                    f"⛔️ این ورکر به کانال مبدا دسترسی ندارد ({src_status}). "
                    f"تارگت‌ها به صف برمی‌گردند تا با ورکر بعدی ارسال شوند…"
                ),
                account=account_tag,
            )
            return targets
        
        if src_chat_id and src_chat_id != order.source_channel_id:
            try:
                order.source_channel_id = src_chat_id
            except Exception:
                pass

    unsent: List[str] = []
    sent_in_chunk = 0
    chunk_started_ts = time.monotonic()
    stage_label = "در حال ارسال پیام اول"

    chunk_limit = 80
    try:
        settings_row = await session.scalar(select(GlobalSettings).limit(1))
        if settings_row is not None and settings_row.send_limit_per_run:
            chunk_limit = int(settings_row.send_limit_per_run)
    except Exception as e:
        logger.warning(f"Could not load send_limit_per_run ({e}); using default {chunk_limit}.")

    daily_limit = int(config.DAILY_SEND_LIMIT_PER_ACCOUNT)
    try:
        if account_row is not None and account_row.created_at is not None:
            created = account_row.created_at
            if created.tzinfo is None:
                created = created.replace(tzinfo=timezone.utc)
            age_days = (datetime.now(timezone.utc) - created).total_seconds() / 86400.0
            if age_days < float(config.NEW_ACCOUNT_DAYS):
                daily_limit = int(config.NEW_ACCOUNT_DAILY_SEND_LIMIT)
    except Exception as e:
        logger.warning(f"Could not resolve daily cap for {account_tag}: {e}")

    hourly_limit = int(getattr(config, "HOURLY_SEND_LIMIT_PER_ACCOUNT", 15))
    from workers.sender import get_hourly_sent_count, incr_hourly_sent_count
    from workers.sender import get_daily_sent_count, incr_daily_sent_count, _humanized_send_delay, is_order_killed

    async def _daily_sent() -> int:
        return await get_daily_sent_count(account_db_id)

    async def _bump_daily() -> None:
        await incr_daily_sent_count(account_db_id)

    async def _pacing_delay() -> float:
        return await _humanized_send_delay()

    def _reply_markup(button_text, button_url):
        if button_text and button_url:
            return InlineKeyboardMarkup([[InlineKeyboardButton(text=str(button_text), url=str(button_url))]])
        return None

    async def _send_stage_message(target: str, text: Optional[str], media_path: Optional[str], media_type: Optional[str], button_text: Optional[str], button_url: Optional[str]):
        real_target = int(target) if target.lstrip("-").isdigit() else target
        markup = _reply_markup(button_text, button_url)
        if media_path and os.path.exists(media_path):
            if str(media_type or "").lower() == "video":
                return await client.send_video(chat_id=real_target, video=str(media_path), caption=text or "", reply_markup=markup)
            return await client.send_photo(chat_id=real_target, photo=str(media_path), caption=text or "", reply_markup=markup)
        return await client.send_message(chat_id=real_target, text=text or "", reply_markup=markup, disable_web_page_preview=True)

    stop_reason: Optional[str] = None

    for position, target in enumerate(targets):
        if not target or target.strip() in ["@None", "None", "@"]:
            continue
            
        target_start_ts = time.monotonic()
        current_stage = 0
        
        def _log_attempt(status_str: str, err_type: str = None, extra_msg: str = "") -> str:
            duration_ms = int((time.monotonic() - target_start_ts) * 1000)
            struct_log = {
                "event": "send_attempt",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "account_id": account_db_id,
                "target": target,
                "order_id": order.id,
                "stage": current_stage,
                "result": status_str,
                "error_type": err_type,
                "duration_ms": duration_ms
            }
            if status_str == "success":
                logger.info(f"StructuredLog: {json.dumps(struct_log)}")
            else:
                logger.warning(f"StructuredLog: {json.dumps(struct_log)}")
                
            base_msg = f"Dur: {duration_ms}ms | Stage: {current_stage}"
            if err_type:
                base_msg += f" | Err: {err_type}"
            if extra_msg:
                base_msg += f" | {extra_msg}"
            return base_msg

        if stop_reason is None:
            if await is_order_killed(order.id):
                stop_reason = "killed"
                await _report(status="🛑 توقف اضطراری (Kill Switch) دریافت شد — خروج ورکر...", account=account_tag)
    
        if stop_reason is None:
            if sent_in_chunk >= chunk_limit:
                stop_reason = "chunk_limit"
                await _report(status=f"سقف ظرفیت این دورِ ارسال ({chunk_limit} پیام) پر شد — ادامه با اکانت بعدی…", account=account_tag)
            elif await _daily_sent() >= daily_limit:
                stop_reason = "daily_cap"
                await _report(status=f"سقف روزانه اکانت ({daily_limit} پیام) پر شد — ارسال متوقف شد…", account=account_tag)
            elif await get_hourly_sent_count(account_db_id) >= hourly_limit:
                stop_reason = "hourly_cap"
                await _report(status=f"سقف ساعتی اکانت ({hourly_limit} پیام) پر شد — ادامه با اکانت بعدی…", account=account_tag)
            
        if stop_reason is not None:
            unsent.extend(targets[position:])
            break

        stage_label = "در حال ارسال پیام اول"
        peer_id = None
        stage_1_delivered = False 
        
        try:
            real_target = int(target) if target.lstrip("-").isdigit() else target
            
            if order.smart_flow:
                try:
                    peer = await client.resolve_peer(real_target)
                    from pyrogram.raw.types import InputPeerUser, InputPeerChat, InputPeerChannel
                    if isinstance(peer, InputPeerUser): peer_id = peer.user_id
                    elif isinstance(peer, InputPeerChat): peer_id = peer.chat_id
                    elif isinstance(peer, InputPeerChannel): peer_id = peer.channel_id
                    if peer_id:
                        arm_seen_event(client, peer_id)
                except Exception as e:
                    logger.debug(f"SmartFlow: Could not pre-resolve peer {target}: {e}")

            # ---- stage 1 ----
            current_stage = 1
            msg1 = None
            if order.source_channel_id and order.source_message_ids:
                parts = order.source_message_ids.split("|")
                src_msg_ids = [int(m.strip()) for m in parts[0].split(",") if m.strip().isdigit()]
                forward_style = parts[1] if len(parts) > 1 else "copy"
                source_username = parts[2] if len(parts) > 2 else None
                from_chat = source_username if source_username else order.source_channel_id
                
                if src_msg_ids:
                    chosen_msg_id = random.choice(src_msg_ids)
                    
                    try:
                        needs_fallback = await _order_needs_fallback(order.id)
                        
                        if not needs_fallback:
                            try:
                                if forward_style == "copy":
                                    msg1 = await client.copy_message(
                                        chat_id=real_target,
                                        from_chat_id=from_chat,
                                        message_id=chosen_msg_id,
                                        reply_markup=_reply_markup(order.button_text, order.button_url) if order.button_text else None,
                                    )
                                else:
                                    msg1 = await client.forward_messages(
                                        chat_id=real_target,
                                        from_chat_id=from_chat,
                                        message_ids=chosen_msg_id,
                                    )
                            except ChatForwardsRestricted as fwd_restricted:
                                logger.warning(
                                    f"Order #{order.id}: ChatForwardsRestricted on source channel — "
                                    f"switching to direct-send fallback for this order."
                                )
                                await _mark_order_fallback(order.id)
                                msg1 = await _send_via_fallback(
                                    client, target, order, chosen_msg_id, from_chat
                                )
                                if msg1 is None:
                                    raise fwd_restricted
                            except ChannelInvalid as src_err:
                                raise
                        else:
                            msg1 = await _send_via_fallback(
                                client, target, order, chosen_msg_id, from_chat
                            )
                            if msg1 is None:
                                raise ChatForwardsRestricted()

                    except ChannelInvalid as src_err:
                        err_type = src_err.__class__.__name__
                        log_msg = _log_attempt("error", err_type, "Source Channel Error")

                        session.add(OrderLog(
                            order_id=order.id, account_id=account_db_id, target=target,
                            status="error", error_message=log_msg
                        ))
                        await session.commit()

                        fail_count = 1
                        try:
                            redis = _get_redis()
                            fail_key = f"source_fail:{order.id}"
                            fail_count = await redis.incr(fail_key)
                            if fail_count == 1:
                                await redis.expire(fail_key, 3600)
                        except Exception as rc_err:
                            logger.warning(f"Could not increment source_fail counter for Order #{order.id}: {rc_err}")

                        max_source_fails = int(getattr(config, "SOURCE_FAIL_MAX_WORKERS", 3))
                        if fail_count >= max_source_fails:
                            await _notify_admins_on_source_failure(session, order.id, err_type)
                            from sqlalchemy import update
                            from database.models import OrderStatus
                            await session.execute(
                                update(Order).where(Order.id == order.id).values(status=OrderStatus.error)
                            )
                            await session.commit()
                            try:
                                await _get_redis().delete(f"source_fail:{order.id}")
                            except Exception:
                                pass
                            stop_reason = "source_failed"
                            unsent.extend(targets[position:])
                        else:
                            await _report(
                                status=(
                                    f"⚠️ این ورکر به کانال مبدا دسترسی نداشت ({err_type}). "
                                    f"تلاش {fail_count}/{max_source_fails} — ادامه با ورکر بعدی…"
                                ),
                                account=account_tag,
                            )
                            unsent.extend(targets[position:])
                            stop_reason = "source_failed_retryable"

                        dismiss_seen_event(client, peer_id)
                        break

                    except ChatForwardsRestricted as src_err:
                        err_type = "ChatForwardsRestricted (fallback exhausted)"
                        log_msg = _log_attempt("error", err_type, "Source Channel Error")

                        session.add(OrderLog(
                            order_id=order.id, account_id=account_db_id, target=target,
                            status="error", error_message=log_msg
                        ))
                        await session.commit()

                        fail_count = 1
                        try:
                            redis = _get_redis()
                            fail_key = f"source_fail:{order.id}"
                            fail_count = await redis.incr(fail_key)
                            if fail_count == 1:
                                await redis.expire(fail_key, 3600)
                        except Exception as rc_err:
                            logger.warning(f"Could not increment source_fail counter for Order #{order.id}: {rc_err}")

                        max_source_fails = int(getattr(config, "SOURCE_FAIL_MAX_WORKERS", 3))
                        if fail_count >= max_source_fails:
                            await _notify_admins_on_source_failure(session, order.id, err_type)
                            from sqlalchemy import update
                            from database.models import OrderStatus
                            await session.execute(
                                update(Order).where(Order.id == order.id).values(status=OrderStatus.error)
                            )
                            await session.commit()
                            try:
                                await _get_redis().delete(f"source_fail:{order.id}")
                            except Exception:
                                pass
                            stop_reason = "source_failed"
                            unsent.extend(targets[position:])
                        else:
                            await _report(
                                status=(
                                    f"⚠️ این ورکر به کانال مبدا دسترسی نداشت ({err_type}). "
                                    f"تلاش {fail_count}/{max_source_fails} — ادامه با ورکر بعدی…"
                                ),
                                account=account_tag,
                            )
                            unsent.extend(targets[position:])
                            stop_reason = "source_failed_retryable"

                        dismiss_seen_event(client, peer_id)
                        break
            else:
                msg1 = await _send_stage_message(target, order.message_text, order.media_path, order.media_type, order.button_text, order.button_url)

            if msg1:
                stage_1_delivered = True

            if not peer_id and msg1 and getattr(msg1, "chat", None):
                peer_id = msg1.chat.id
                if order.smart_flow:
                    arm_seen_event(client, peer_id)

            # ---- stage 2 ----
            if order.message_2_text or order.media_2_path:
                current_stage = 2
                if order.smart_flow and peer_id:
                    await _report(status="در انتظار دیده‌شدن پیام اول توسط تارگت", account=account_tag)
                    timeout_val = random.uniform(config.SMARTFLOW_SEEN_TIMEOUT_MIN, config.SMARTFLOW_SEEN_TIMEOUT_MAX)
                    seen = await wait_for_seen(client, peer_id, timeout=timeout_val)
                    if seen:
                        await asyncio.sleep(random.uniform(config.SMARTFLOW_STAGE_DELAY_MIN, config.SMARTFLOW_STAGE_DELAY_MAX))
                    if order.message_3_text or order.media_3_path:
                        arm_seen_event(client, peer_id)
                else:
                    await asyncio.sleep(await _pacing_delay())
                    
                stage_label = "در حال ارسال پیام دوم"
                await _send_stage_message(target, order.message_2_text, order.media_2_path, order.media_2_type, None, None)

            # ---- stage 3 ----
            if order.message_3_text or order.media_3_path:
                current_stage = 3
                if order.smart_flow and peer_id:
                    await _report(status="در انتظار دیده‌شدن پیام دوم توسط تارگت", account=account_tag)
                    timeout_val = random.uniform(config.SMARTFLOW_SEEN_TIMEOUT_MIN, config.SMARTFLOW_SEEN_TIMEOUT_MAX)
                    seen = await wait_for_seen(client, peer_id, timeout=timeout_val)
                    if seen:
                        await asyncio.sleep(random.uniform(config.SMARTFLOW_STAGE_DELAY_MIN, config.SMARTFLOW_STAGE_DELAY_MAX))
                else:
                    await asyncio.sleep(await _pacing_delay())
                    
                stage_label = "در حال ارسال پیام سوم"
                await _send_stage_message(target, order.message_3_text, order.media_3_path, order.media_3_type, None, None)

            # ---- Success Logging ----
            log_msg = _log_attempt("success")
            session.add(OrderLog(
                order_id=order.id, account_id=account_db_id, target=target,
                status="success", error_message=log_msg
            ))
            
            if account_row:
                account_row.consecutive_errors = 0
                
            await session.commit()
            await incr_hourly_sent_count(account_db_id)
            
            sent_in_chunk += 1
            await _bump_daily()

            if progress_reporter is not None and sent_in_chunk % 20 == 0:
                elapsed_min = max((time.monotonic() - chunk_started_ts) / 60.0, 1e-9)
                speed = sent_in_chunk / elapsed_min
                remaining = max(len(targets) - position - 1, 0)
                eta_seconds = int(remaining / speed * 60) if speed > 0 else None
                await _report(done=float(sent_in_chunk), total=float(len(targets)), speed=round(speed, 1), eta=eta_seconds, status=stage_label, account=account_tag)

        except FloodWait as e:
            wait_seconds = int(getattr(e, "value", 0) or 0)
            log_status = "partial" if stage_1_delivered else "flood"
            log_msg = _log_attempt(log_status, "FloodWait", f"Wait: {wait_seconds}s")
            
            session.add(OrderLog(
                order_id=order.id, account_id=account_db_id, target=target, 
                status=log_status, error_message=log_msg
            ))
            
            if account_row:
                account_row.consecutive_errors += 1
                max_errors = int(getattr(config, "MAX_CONSECUTIVE_ERRORS", 3))
                if account_row.consecutive_errors >= max_errors:
                    cooldown_mins = int(getattr(config, "COOLDOWN_MINUTES_ON_ERROR", 30))
                    ret_time = datetime.now(timezone.utc) + timedelta(minutes=cooldown_mins)
                    from workers.sender import change_account_status
                    from database.models import AccountStatus
                    await change_account_status(session, account_db_id, AccountStatus.cooldown, "Too many consecutive errors", log_msg, ret_time)
                    stop_reason = "consecutive_errors"
            
            await session.commit()
            await _report(status=f"⏳ قفل FloodWait — {wait_seconds} ثانیه صبر…", account=account_tag)
            
            from utils.limit_handler import register_account_limit
            await register_account_limit(session, account_db_id, client, "flood_wait", wait_seconds)
            
            if not stop_reason:
                stop_reason = "flood_wait"
            
            if stage_1_delivered:
                unsent.extend(targets[position+1:])
                sent_in_chunk += 1
                await _bump_daily()
                await incr_hourly_sent_count(account_db_id)
            else:
                unsent.extend(targets[position+1:])
                
            dismiss_seen_event(client, peer_id)
            break

        except PeerFlood as e:
            log_status = "partial" if stage_1_delivered else "restricted"
            log_msg = _log_attempt(log_status, "PeerFlood", "Spam Limit")
            
            session.add(OrderLog(
                order_id=order.id, account_id=account_db_id, target=target, 
                status=log_status, error_message=log_msg
            ))
            
            from workers.sender import change_account_status
            from database.models import AccountStatus
            await change_account_status(session, account_db_id, AccountStatus.blocked, "Spam Limit (PeerFlood)", log_msg)
            
            await session.commit()
            await _report(status="🚫 محدودیت اسپم (PeerFlood) — توقف ارسال و مسدود شدن اکانت…", account=account_tag)
            
            from utils.limit_handler import register_account_limit
            await register_account_limit(session, account_db_id, client, "peer_flood")
            
            stop_reason = "blocked"
            if stage_1_delivered:
                unsent.extend(targets[position+1:])
                sent_in_chunk += 1
                await _bump_daily()
                await incr_hourly_sent_count(account_db_id)
            else:
                unsent.extend(targets[position+1:])
                
            dismiss_seen_event(client, peer_id)
            break

        except (UserDeactivated, AuthKeyUnregistered, Unauthorized) as e:
            err_type = e.__class__.__name__
            log_status = "partial" if stage_1_delivered else "error"
            log_msg = _log_attempt(log_status, err_type, str(e)[:100])
            
            session.add(OrderLog(
                order_id=order.id, account_id=account_db_id, target=target, 
                status=log_status, error_message=log_msg
            ))
            
            from workers.sender import change_account_status
            from database.models import AccountStatus
            await change_account_status(session, account_db_id, AccountStatus.blocked, "Account Banned or Unregistered", str(e)[:100])
            
            await session.commit()
            await _report(status="⛔️ اکانت بن یا از دسترس خارج شد!", account=account_tag)
            
            from utils.limit_handler import register_account_limit
            await register_account_limit(session, account_db_id, client, "banned", is_banned=True)
            
            stop_reason = "blocked"
            if stage_1_delivered:
                unsent.extend(targets[position+1:])
                sent_in_chunk += 1
                await _bump_daily()
                await incr_hourly_sent_count(account_db_id)
            else:
                unsent.extend(targets[position+1:])
                
            dismiss_seen_event(client, peer_id)
            break

        except (UserIsBlocked, PeerIdInvalid, UsernameInvalid, UsernameNotOccupied, UserIsBot, ValueError, KeyError) as e:
            err_type = e.__class__.__name__
            log_status = "partial" if stage_1_delivered else "error"
            log_msg = _log_attempt(log_status, err_type, f"تارگت نامعتبر/حذف‌شده: {str(e)[:100]}")
            
            session.add(OrderLog(
                order_id=order.id, account_id=account_db_id, target=target, 
                status=log_status, error_message=log_msg
            ))
            await session.commit()
            dismiss_seen_event(client, peer_id)
            continue
 
        except (
            ConnectionError, 
            TimeoutError, 
            OSError, 
            getattr(python_socks, 'ProxyError', ConnectionError),
            getattr(python_socks, 'ProxyTimeoutError', TimeoutError),
            getattr(python_socks, 'ProxyConnectionError', OSError)
        ) as e:
            err_type = e.__class__.__name__
            log_status = "partial" if stage_1_delivered else "error"
            log_msg = _log_attempt(log_status, err_type, f"قطعی ارتباط (پروکسی/شبکه): {str(e)[:100]}")
            
            session.add(OrderLog(
                order_id=order.id, account_id=account_db_id, target=target, 
                status=log_status, error_message=log_msg
            ))
            
            if getattr(client, "proxy", None):
                if account_row and account_row.proxy_string:
                    from utils.health_checker import report_proxy_result
                    asyncio.create_task(report_proxy_result(account_row.proxy_string, is_success=False))
            
            await session.commit()
            await _report(status=f"⚠️ اتصال پروکسی قطع شد. توقف ارسال و انتقال موقت تارگت‌ها به وورکر بعدی...", account=account_tag)
            
            stop_reason = "proxy_connection_error"
            if stage_1_delivered:
                unsent.extend(targets[position+1:])
                sent_in_chunk += 1
                await _bump_daily()
                await incr_hourly_sent_count(account_db_id)
            else:
                unsent.extend(targets[position:])
                
            dismiss_seen_event(client, peer_id)
            break

        except Exception as e:
            err_type = e.__class__.__name__
            
            target_errors = [
                "InputUserDeactivated", "ChatWriteForbidden", "UserNotMutualContact", 
                "UserPrivacyRestricted", "YouBlockedUser", "ChannelPrivate", "ChatAdminRequired",
                "NotAcceptable", "BadRequest"
            ]
            error_str = str(e).lower()
            is_target_error = (
                err_type in target_errors or 
                "not occupied" in error_str or 
                "invalid" in error_str or 
                "not found" in error_str or 
                "deactivated" in error_str or
                "not acceptable" in error_str
            )
            
            if is_target_error:
                log_status = "partial" if stage_1_delivered else "error"
                log_msg = _log_attempt(log_status, err_type, f"تارگت نامعتبر/غیرقابل ارسال: {str(e)[:100]}")
                
                session.add(OrderLog(
                    order_id=order.id, account_id=account_db_id, target=target,
                    status=log_status, error_message=log_msg,
                ))
                await session.commit()
                dismiss_seen_event(client, peer_id)
                continue

            log_status = "partial" if stage_1_delivered else "error"
            log_msg = _log_attempt(log_status, err_type, str(e)[:100])
            
            session.add(OrderLog(
                order_id=order.id, account_id=account_db_id, target=target,
                status=log_status, error_message=log_msg,
            ))
            
            if account_row:
                account_row.consecutive_errors += 1
                max_errors = int(getattr(config, "MAX_CONSECUTIVE_ERRORS", 3))
                if account_row.consecutive_errors >= max_errors:
                    cooldown_mins = int(getattr(config, "COOLDOWN_MINUTES_ON_ERROR", 30))
                    ret_time = datetime.now(timezone.utc) + timedelta(minutes=cooldown_mins)
                    from workers.sender import change_account_status
                    from database.models import AccountStatus
                    await change_account_status(session, account_db_id, AccountStatus.cooldown, "Too many consecutive errors (Unknown)", log_msg, ret_time)
                    stop_reason = "consecutive_errors"
                    
            try:
                await session.commit()
            except Exception as log_err:
                await session.rollback()
                logger.warning(f"Order #{order.id}: error log commit failed: {log_err}")
            
            if not stop_reason:
                stop_reason = "error"
            
            if stage_1_delivered:
                sent_in_chunk += 1
                await _bump_daily()
                await incr_hourly_sent_count(account_db_id)
            else:
                unsent.append(target)
                
            dismiss_seen_event(client, peer_id)
            break

        if stop_reason is None and position < len(targets) - 1:
            await asyncio.sleep(await _pacing_delay())

    if sent_in_chunk > 0:
        try:
            await _get_redis().delete(f"source_fail:{order.id}")
        except Exception:
            pass

    if sent_in_chunk > 0 and order.source_channel_id:
        try:
            from workers.task_queue import _mark_source_membership
            await _mark_source_membership(account_db_id, order.source_channel_id)
        except Exception as mark_err:
            logger.debug(f"Could not mark source membership: {mark_err}")

    logger.info(
        f"Order #{order.id}: chunk finished for worker user_{account_db_id}/ "
        f"(sent={sent_in_chunk}, unsent={len(unsent)}, stop={stop_reason})."
    )
    return unsent, stop_reason