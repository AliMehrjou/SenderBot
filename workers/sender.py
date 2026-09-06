import asyncio
import html
import logging
import os
import random
from datetime import datetime, timedelta, timezone
from typing import List, Optional
from zoneinfo import ZoneInfo

import redis.asyncio as aioredis
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession
from pyrogram import Client
from pyrogram.errors import (
    FloodWait, 
    UserIsBlocked, 
    PeerIdInvalid, 
    UsernameInvalid,
    UsernameNotOccupied,
    UserIsBot,
    UserRestricted,
    # 📋 ارسال با کپی از کانال مبدا: خطاهای مخصوص کانال مبدا
    ChannelInvalid,
    ChatForwardsRestricted,
)

from config import config
from database.engine import async_session
from database.models import OrderLog, Order, OrderStatus, Admin, Account
from utils.anti_ban import parse_spintax, apply_adaptive_flood_wait
from utils.advanced_anti_ban import appeal_to_spambot
from utils.seen_watcher import (  # 🧠 جریان هوشمند: رصد «سین» تارگت
    arm_seen_event,      # 🛡 فاز ۷ (BUG-18): مسلح‌کردن رویداد قبل از send
    dismiss_seen_event,  # 🛡 فاز ۷ (BUG-18): جمع‌کردن رویداد در مسیرهای شکست مرحله ۱
    wait_for_seen,
)


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
    """
    تاخیر انسانی بین ارسال‌ها؛ 🛡 R8: در پنجره‌ی کاهش بار سراسری، ضریب تصادفی
    ×۲–۳ روی تاخیر اعمال می‌شود (کاهش نرخ ارسال کل سیستم، نه فقط یک اکانت).
    """
    delay = random.uniform(5, 12)
    if await is_global_slowdown():
        delay *= random.uniform(2.0, 3.0)
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

# مسیر فایل: workers/sender.py
async def execute_bulk_send(
    client: Client, 
    account_db_id: int, 
    order: Order, 
    targets: List[str], 
    session: AsyncSession
) -> List[str]:

    logger.info(f"Worker user_{account_db_id}/ starting chunk for Order #{order.id}.")
    
    success_count = 0
    unsent_targets = []
    
    # 📋 حالت کپی/فوروارد از کانال مبدا
    copy_source_ids: List[int] = []
    forward_style = "copy"
    
    if order.source_channel_id and order.source_message_ids:
        parts = order.source_message_ids.split("|")
        id_parts = parts[0]
        if len(parts) > 1:
            forward_style = parts[1]
            
        copy_source_ids = [
            int(part.strip())
            for part in id_parts.split(",")
            if part.strip().isdigit()
        ]
        
    copy_mode = bool(copy_source_ids and forward_style == "copy")
    forward_mode = bool(copy_source_ids and forward_style == "forward")
    is_source_mode = copy_mode or forward_mode  # هر نوع ارسالی از کانال مبدا
    try:
        account_created_at = await session.scalar(
            select(Account.created_at).where(Account.id == account_db_id)
        )
        daily_limit = effective_daily_limit(account_created_at)
    except Exception as e:
        logger.warning(
            f"Could not load created_at for worker user_{account_db_id}/ "
            f"({e.__class__.__name__}: {e}); using strict (new-account) daily limit."
        )
        daily_limit = effective_daily_limit(None)

    # 📋 شمارنده‌های خطای کانال مبدا
    copy_attempts = 0
    source_error_count = 0
    last_source_error: Optional[str] = None

    raw_message = ""
    safe_message = ""
    safe_message_2 = ""
    smart_flow_active = False
    send_message_2 = False

    if not copy_mode:
        raw_message = order.message_text or ""
        
        if order.button_text and order.button_url:
            raw_message += f"\n\n🔗 <a href='{order.button_url}'>{order.button_text}</a>"

        safe_message = raw_message.replace("{first_name}", "[[FIRST_NAME]]").replace("{username}", "[[USERNAME]]")

        # 🧠 بررسی وجود پیام دوم (چه بنر باشد چه پیام عادی)
        send_message_2 = bool(order.message_2_text) or bool(order.media_2_path and order.media_2_type)
        raw_message_2 = order.message_2_text or ""
        safe_message_2 = raw_message_2.replace("{first_name}", "[[FIRST_NAME]]").replace("{username}", "[[USERNAME]]")
        
        # جریان هوشمند فقط زمانی فعال می‌شود که هم فلگ روشن باشد و هم پیام دومی وجود داشته باشد
        smart_flow_active = bool(order.smart_flow and send_message_2)

        if smart_flow_active:
            logger.info(f"Worker user_{account_db_id}/ SmartFlow ENABLED for Order #{order.id} (icebreaker → seen-wait → banner).")
        elif send_message_2:
            logger.info(f"Worker user_{account_db_id}/ Normal Flow: message 2 ENABLED for Order #{order.id}.")

        raw_message_3 = order.message_3_text or ""
        safe_message_3 = raw_message_3.replace("{first_name}", "[[FIRST_NAME]]").replace("{username}", "[[USERNAME]]")
        send_message_3 = bool(order.message_3_text) or bool(order.media_3_path and order.media_3_type)
        if send_message_3:
            logger.info(
                f"Worker user_{account_db_id}/ Order #{order.id}: message 3 enabled "
                f"(text={bool(order.message_3_text)}, media={bool(order.media_3_path)})."
            )

        needs_user_data = (
            "[[FIRST_NAME]]" in safe_message
            or "[[USERNAME]]" in safe_message
            or (
                send_message_2
                and ("[[FIRST_NAME]]" in safe_message_2 or "[[USERNAME]]" in safe_message_2)
            )
            or (
                send_message_3
                and ("[[FIRST_NAME]]" in safe_message_3 or "[[USERNAME]]" in safe_message_3)
            )
        )
        user_data_cache: dict = {}
    else:
        logger.info(
            f"Worker user_{account_db_id}/ COPY MODE for Order #{order.id}: "
            f"{len(copy_source_ids)} source message(s) from channel {order.source_channel_id}."
        )

    for i, target in enumerate(targets):
        if await daily_cap_reached(account_db_id, daily_limit):
            logger.warning(
                f"Worker user_{account_db_id}/ reached DAILY_SEND_LIMIT_PER_ACCOUNT={daily_limit}; "
                f"aborting chunk for Order #{order.id} ({len(targets) - i} target(s) re-queued)."
            )
            unsent_targets.extend(targets[i:])
            break

        target = target.strip()
        if not target:
            continue

        if i % 10 == 0:
            try:
                await session.commit()
            except Exception as db_err:
                await session.rollback()
                logger.error(
                    f"Worker user_{account_db_id}/ incremental commit failed for Order #{order.id} "
                    f"({db_err.__class__.__name__}: {db_err}); aborting chunk to bound the ambiguity window."
                )
                unsent_targets.extend(targets[i:])
                break

            if await is_order_killed(order.id):
                logger.warning(f"Kill Switch activated! Worker user_{account_db_id}/ aborting chunk.")
                unsent_targets.extend(targets[i:])
                break 

        if is_source_mode:
            msg_id = random.choice(copy_source_ids)
            log_entry = OrderLog(order_id=order.id, account_id=account_db_id, target=target)
            copy_attempts += 1

            await incr_daily_sent_count(account_db_id)

            try:
                if copy_mode:
                    copied_msg = await client.copy_message(
                        chat_id=target,
                        from_chat_id=order.source_channel_id,
                        message_id=msg_id,
                    )
                else:
                    copied_msg = await client.forward_messages(
                        chat_id=target,
                        from_chat_id=order.source_channel_id,
                        message_ids=msg_id,
                    )
                    
                if getattr(copied_msg, "chat", None) is not None:
                    _spawn_crm_mark(copied_msg.chat.id)

            except FloodWait as e:
                wait_seconds = e.value
                logger.warning(f"Worker user_{account_db_id}/ triggered FloodWait ({wait_seconds}s) in copy mode.")

                log_entry.status = LOG_STATUS_FLOOD
                log_entry.error_message = f"FloodWait: {wait_seconds}s"
                session.add(log_entry)

                await apply_adaptive_flood_wait(session=session, account_id=account_db_id, wait_seconds=wait_seconds)
                await mark_global_slowdown(wait_seconds)
                unsent_targets.extend(targets[i:])
                break

            except UserRestricted as e:
                logger.warning(f"Worker user_{account_db_id}/ is RESTRICTED (Spam limit). Triggering SpamBot appeal.")
                log_entry.status = LOG_STATUS_RESTRICTED
                log_entry.error_message = "UserRestricted"
                session.add(log_entry)
                asyncio.create_task(appeal_to_spambot(client, account_db_id))
                unsent_targets.extend(targets[i:])
                break

            except (UserIsBlocked, PeerIdInvalid, UsernameInvalid, UsernameNotOccupied, UserIsBot) as e:
                logger.info(f"Worker user_{account_db_id}/ skipped {target} (copy): {e.__class__.__name__}")
                log_entry.status = LOG_STATUS_ERROR
                log_entry.error_message = e.__class__.__name__
                session.add(log_entry)
                continue

            except (ChannelInvalid, ChatForwardsRestricted) as e:
                error_name = e.__class__.__name__
                logger.error(f"Worker user_{account_db_id}/ copy source error on {target}: {error_name}")
                log_entry.status = LOG_STATUS_ERROR
                log_entry.error_message = f"SourceChannel: {error_name}"
                session.add(log_entry)
                source_error_count += 1
                last_source_error = error_name
                continue

            except (ConnectionError, TimeoutError, asyncio.TimeoutError) as e:
                logger.warning(
                    f"Worker user_{account_db_id}/ transient connection error on {target} "
                    f"({e.__class__.__name__}: {e}); aborting chunk for Order #{order.id}, "
                    f"{len(targets) - i} target(s) re-queued."
                )
                log_entry.status = LOG_STATUS_ERROR
                log_entry.error_message = f"TransientConnection: {e.__class__.__name__}: {e}"
                session.add(log_entry)
                unsent_targets.extend(targets[i:])
                break

            except Exception as e:
                logger.error(f"Worker user_{account_db_id}/ unexpected error on {target} (copy): {e}")
                log_entry.status = LOG_STATUS_ERROR
                log_entry.error_message = str(e)
                session.add(log_entry)
                continue

            log_entry.status = LOG_STATUS_SUCCESS
            session.add(log_entry)
            success_count += 1

            await asyncio.sleep(await _humanized_send_delay())
            continue

        spintaxed_text = parse_spintax(safe_message)
        final_text = spintaxed_text
        
        smart_flow_peer_id = None
        target_first_name = "دوست عزیز"
        target_username = str(target)

        if needs_user_data:
            user_info = user_data_cache.get(target)
            if user_info is None:
                try:
                    user_info = await client.get_users(target)
                    user_data_cache[target] = user_info
                except Exception:
                    user_info = None
                    final_text = spintaxed_text.replace("[[FIRST_NAME]]", "دوست عزیز").replace("[[USERNAME]]", str(target))
            if user_info is not None:
                smart_flow_peer_id = user_info.id
                target_first_name = user_info.first_name or "دوست عزیز"
                target_username = f"@{user_info.username}" if user_info.username else str(target)
                final_text = spintaxed_text.replace("[[FIRST_NAME]]", target_first_name).replace("[[USERNAME]]", target_username)

        if smart_flow_active and smart_flow_peer_id is None and target.isdigit():
            smart_flow_peer_id = int(target)
            
        if TEXT_MUTATION_INVISIBLE_CHARS_ENABLED:
            send_index = await _next_send_index(account_db_id)
            final_text = mutate_text(final_text, send_index)

        log_entry = OrderLog(order_id=order.id, account_id=account_db_id, target=target)
        await incr_daily_sent_count(account_db_id)

        if smart_flow_active and smart_flow_peer_id is not None:
            arm_seen_event(client, smart_flow_peer_id)

        # ─── مرحله ۱: ارسال پیام اول (یخ‌شکن) ───
        sent_message = None
        try:
            if order.media_path and order.media_type:
                if order.media_type == "photo":
                    sent_message = await client.send_photo(chat_id=target, photo=order.media_path, caption=final_text)
                elif order.media_type == "video":
                    sent_message = await client.send_video(chat_id=target, video=order.media_path, caption=final_text)
                elif order.media_type == "document":
                    sent_message = await client.send_document(chat_id=target, document=order.media_path, caption=final_text)
            else:
                sent_message = await client.send_message(chat_id=target, text=final_text)

            if (smart_flow_active and smart_flow_peer_id is None
                    and sent_message is not None
                    and getattr(sent_message, "chat", None) is not None):
                smart_flow_peer_id = sent_message.chat.id
                arm_seen_event(client, smart_flow_peer_id)

        except FloodWait as e:
            wait_seconds = e.value
            logger.warning(f"Worker user_{account_db_id}/ triggered FloodWait ({wait_seconds}s).")
            log_entry.status = LOG_STATUS_FLOOD
            log_entry.error_message = f"FloodWait: {wait_seconds}s"
            session.add(log_entry)
            dismiss_seen_event(client, smart_flow_peer_id)
            await apply_adaptive_flood_wait(session=session, account_id=account_db_id, wait_seconds=wait_seconds)
            await mark_global_slowdown(wait_seconds)
            unsent_targets.extend(targets[i:])
            break 
            
        except UserRestricted as e:
            logger.warning(f"Worker user_{account_db_id}/ is RESTRICTED (Spam limit). Triggering SpamBot appeal.")
            log_entry.status = LOG_STATUS_RESTRICTED
            log_entry.error_message = "UserRestricted"
            session.add(log_entry)
            dismiss_seen_event(client, smart_flow_peer_id)
            asyncio.create_task(appeal_to_spambot(client, account_db_id))
            unsent_targets.extend(targets[i:])
            break 
            
        except (UserIsBlocked, PeerIdInvalid, UsernameInvalid, UsernameNotOccupied, UserIsBot) as e:
            logger.info(f"Worker user_{account_db_id}/ skipped {target}: {e.__class__.__name__}")
            log_entry.status = LOG_STATUS_ERROR
            log_entry.error_message = e.__class__.__name__
            session.add(log_entry)
            dismiss_seen_event(client, smart_flow_peer_id)
            continue

        except (ConnectionError, TimeoutError, asyncio.TimeoutError) as e:
            logger.warning(
                f"Worker user_{account_db_id}/ transient connection error on {target} "
                f"({e.__class__.__name__}: {e}); aborting chunk for Order #{order.id}, "
                f"{len(targets) - i} target(s) re-queued."
            )
            log_entry.status = LOG_STATUS_ERROR
            log_entry.error_message = f"TransientConnection: {e.__class__.__name__}: {e}"
            session.add(log_entry)
            dismiss_seen_event(client, smart_flow_peer_id)
            unsent_targets.extend(targets[i:])
            break
            
        except Exception as e:
            logger.error(f"Worker user_{account_db_id}/ unexpected error on {target}: {e}")
            log_entry.status = LOG_STATUS_ERROR
            log_entry.error_message = str(e)
            session.add(log_entry)
            dismiss_seen_event(client, smart_flow_peer_id)
            continue

        if sent_message is not None and getattr(sent_message, "chat", None) is not None:
            _spawn_crm_mark(sent_message.chat.id)

        # ─── مرحله ۲: ارسال پیام دوم / بنر ───
        if smart_flow_active:
            # === منطق جریان هوشمند ===
            if smart_flow_peer_id is not None:
                seen = await wait_for_seen(client, smart_flow_peer_id, timeout=random.uniform(120, 420))
            else:
                seen = False
                logger.warning(f"Worker user_{account_db_id}/ SmartFlow: peer of '{target}' unresolved; seen-wait skipped.")

            if await is_order_killed(order.id):
                logger.warning(f"Kill Switch activated during SmartFlow wait! Worker user_{account_db_id}/ aborting chunk.")
                log_entry.status = LOG_STATUS_SUCCESS
                session.add(log_entry)
                success_count += 1
                unsent_targets.extend(targets[i + 1:])
                break

            if await daily_cap_reached(account_db_id, daily_limit):
                logger.warning(
                    f"Worker user_{account_db_id}/ reached DAILY_SEND_LIMIT_PER_ACCOUNT={daily_limit} "
                    f"before banner; aborting chunk for Order #{order.id}."
                )
                log_entry.status = LOG_STATUS_SUCCESS 
                session.add(log_entry)
                success_count += 1
                unsent_targets.extend(targets[i + 1:])
                break

            await asyncio.sleep(random.uniform(30, 180))

            banner_text = parse_spintax(safe_message_2)
            if "[[FIRST_NAME]]" in banner_text or "[[USERNAME]]" in banner_text:
                banner_text = banner_text.replace("[[FIRST_NAME]]", target_first_name).replace("[[USERNAME]]", target_username)

            await incr_daily_sent_count(account_db_id)

            try:
                if order.media_2_path and order.media_2_type:
                    if order.media_2_type == "photo":
                        await client.send_photo(chat_id=target, photo=order.media_2_path, caption=banner_text)
                    elif order.media_2_type == "video":
                        await client.send_video(chat_id=target, video=order.media_2_path, caption=banner_text)
                    elif order.media_2_type == "document":
                        await client.send_document(chat_id=target, document=order.media_2_path, caption=banner_text)
                else:
                    await client.send_message(chat_id=target, text=banner_text)
            except FloodWait as e:
                wait_seconds = e.value
                logger.warning(f"Worker user_{account_db_id}/ SmartFlow banner FloodWait ({wait_seconds}s).")
                log_entry.status = LOG_STATUS_SUCCESS
                log_entry.error_message = f"SmartFlow Banner FloodWait: {wait_seconds}s"
                session.add(log_entry)
                success_count += 1
                await apply_adaptive_flood_wait(session=session, account_id=account_db_id, wait_seconds=wait_seconds)
                await mark_global_slowdown(wait_seconds)
                unsent_targets.extend(targets[i + 1:])
                break
            except (ConnectionError, TimeoutError, asyncio.TimeoutError) as e:
                logger.warning(
                    f"Worker user_{account_db_id}/ SmartFlow banner transient error on {target} "
                    f"({e.__class__.__name__}: {e}); aborting chunk for Order #{order.id}, "
                    f"{len(targets) - i - 1} target(s) re-queued."
                )
                log_entry.status = LOG_STATUS_SUCCESS
                log_entry.error_message = f"SmartFlow Banner Transient: {e.__class__.__name__}: {e}"
                session.add(log_entry)
                success_count += 1
                unsent_targets.extend(targets[i + 1:])
                break
            except Exception as e:
                logger.error(f"Worker user_{account_db_id}/ SmartFlow banner error on {target}: {e}")
                log_entry.status = LOG_STATUS_SUCCESS
                log_entry.error_message = f"SmartFlow Banner: {e.__class__.__name__}: {e}"
                session.add(log_entry)
                success_count += 1
                continue

            logger.info(f"Worker user_{account_db_id}/ SmartFlow '{target}': seen={seen}; banner delivered.")

        elif send_message_2:
            # === منطق جریان ارسال عادی برای پیام دوم ===
            await asyncio.sleep(await _humanized_send_delay())

            if await daily_cap_reached(account_db_id, daily_limit):
                logger.warning(
                    f"Worker user_{account_db_id}/ reached DAILY_SEND_LIMIT_PER_ACCOUNT={daily_limit} "
                    f"before message 2; aborting chunk for Order #{order.id}."
                )
                log_entry.status = LOG_STATUS_SUCCESS 
                session.add(log_entry)
                success_count += 1
                unsent_targets.extend(targets[i + 1:])
                break

            banner_text = parse_spintax(safe_message_2)
            if "[[FIRST_NAME]]" in banner_text or "[[USERNAME]]" in banner_text:
                banner_text = banner_text.replace("[[FIRST_NAME]]", target_first_name).replace("[[USERNAME]]", target_username)

            await incr_daily_sent_count(account_db_id)

            try:
                if order.media_2_path and order.media_2_type:
                    if order.media_2_type == "photo":
                        await client.send_photo(chat_id=target, photo=order.media_2_path, caption=banner_text)
                    elif order.media_2_type == "video":
                        await client.send_video(chat_id=target, video=order.media_2_path, caption=banner_text)
                    elif order.media_2_type == "document":
                        await client.send_document(chat_id=target, document=order.media_2_path, caption=banner_text)
                else:
                    await client.send_message(chat_id=target, text=banner_text)
            except FloodWait as e:
                wait_seconds = e.value
                logger.warning(f"Worker user_{account_db_id}/ message-2 FloodWait ({wait_seconds}s).")
                log_entry.status = LOG_STATUS_SUCCESS
                log_entry.error_message = f"Message2 FloodWait: {wait_seconds}s"
                session.add(log_entry)
                success_count += 1
                await apply_adaptive_flood_wait(session=session, account_id=account_db_id, wait_seconds=wait_seconds)
                await mark_global_slowdown(wait_seconds)
                unsent_targets.extend(targets[i + 1:])
                break
            except (ConnectionError, TimeoutError, asyncio.TimeoutError) as e:
                logger.warning(
                    f"Worker user_{account_db_id}/ message-2 transient error on {target} "
                    f"({e.__class__.__name__}: {e}); aborting chunk for Order #{order.id}, "
                    f"{len(targets) - i - 1} target(s) re-queued."
                )
                log_entry.status = LOG_STATUS_SUCCESS
                log_entry.error_message = f"Message2 Transient: {e.__class__.__name__}: {e}"
                session.add(log_entry)
                success_count += 1
                unsent_targets.extend(targets[i + 1:])
                break
            except Exception as e:
                logger.error(f"Worker user_{account_db_id}/ message-2 error on {target}: {e}")
                log_entry.status = LOG_STATUS_SUCCESS
                log_entry.error_message = f"Message2: {e.__class__.__name__}: {e}"
                session.add(log_entry)
                success_count += 1
                continue
            
            logger.info(f"Worker user_{account_db_id}/ Normal Flow: message 2 delivered to '{target}'.")

        # ─── مرحله ۳: ارسال پیام سوم ───
        if send_message_3:
            await asyncio.sleep(await _humanized_send_delay())

            if await daily_cap_reached(account_db_id, daily_limit):
                logger.warning(
                    f"Worker user_{account_db_id}/ reached DAILY_SEND_LIMIT_PER_ACCOUNT={daily_limit} "
                    f"before message 3; aborting chunk for Order #{order.id}."
                )
                log_entry.status = LOG_STATUS_SUCCESS
                session.add(log_entry)
                success_count += 1
                unsent_targets.extend(targets[i + 1:])
                break

            message_3_text = parse_spintax(safe_message_3)
            if "[[FIRST_NAME]]" in message_3_text or "[[USERNAME]]" in message_3_text:
                message_3_text = message_3_text.replace("[[FIRST_NAME]]", target_first_name).replace("[[USERNAME]]", target_username)

            await incr_daily_sent_count(account_db_id)

            try:
                if order.media_3_path and order.media_3_type:
                    if order.media_3_type == "photo":
                        await client.send_photo(chat_id=target, photo=order.media_3_path, caption=message_3_text)
                    elif order.media_3_type == "video":
                        await client.send_video(chat_id=target, video=order.media_3_path, caption=message_3_text)
                    elif order.media_3_type == "document":
                        await client.send_document(chat_id=target, document=order.media_3_path, caption=message_3_text)
                else:
                    await client.send_message(chat_id=target, text=message_3_text)
            except FloodWait as e:
                wait_seconds = e.value
                logger.warning(f"Worker user_{account_db_id}/ message-3 FloodWait ({wait_seconds}s).")
                log_entry.status = LOG_STATUS_SUCCESS
                log_entry.error_message = f"Message3 FloodWait: {wait_seconds}s"
                session.add(log_entry)
                success_count += 1
                await apply_adaptive_flood_wait(session=session, account_id=account_db_id, wait_seconds=wait_seconds)
                await mark_global_slowdown(wait_seconds)
                unsent_targets.extend(targets[i + 1:])
                break
            except (ConnectionError, TimeoutError, asyncio.TimeoutError) as e:
                logger.warning(
                    f"Worker user_{account_db_id}/ message-3 transient error on {target} "
                    f"({e.__class__.__name__}: {e}); aborting chunk for Order #{order.id}, "
                    f"{len(targets) - i - 1} target(s) re-queued."
                )
                log_entry.status = LOG_STATUS_SUCCESS
                log_entry.error_message = f"Message3 Transient: {e.__class__.__name__}: {e}"
                session.add(log_entry)
                success_count += 1
                unsent_targets.extend(targets[i + 1:])
                break
            except Exception as e:
                logger.error(f"Worker user_{account_db_id}/ message-3 error on {target}: {e}")
                log_entry.status = LOG_STATUS_SUCCESS
                log_entry.error_message = f"Message3: {e.__class__.__name__}: {e}"
                session.add(log_entry)
                success_count += 1
                continue

            logger.info(f"Worker user_{account_db_id}/ message 3 delivered to '{target}'.")

        log_entry.status = LOG_STATUS_SUCCESS
        session.add(log_entry)
        success_count += 1
        
        await asyncio.sleep(await _humanized_send_delay())

    copy_source_broken = (
        copy_mode
        and copy_attempts > 0
        and source_error_count == copy_attempts
        and success_count == 0
    )
    if copy_source_broken:
        logger.error(
            f"Order #{order.id}: ALL {copy_attempts} copy attempts of worker "
            f"user_{account_db_id}/ failed with source-channel error "
            f"({last_source_error}); marking order as error."
        )
        try:
            await session.execute(
                update(Order).where(Order.id == order.id).values(status=OrderStatus.error)
            )
            session.add(OrderLog(
                order_id=order.id,
                account_id=account_db_id,
                target="source_channel",
                status=LOG_STATUS_ERROR,
                error_message=(
                    f"SourceChannel: کل chunk با خطای {last_source_error} شکست خورد — "
                    "کانال مبدا حذف/بن شده یا کپی از آن محدود است."
                ),
            ))
        except Exception as e:
            await session.rollback()
            logger.error(f"Failed to mark Order #{order.id} as error (source channel): {e}")

    try:
        await session.commit()
    except Exception as db_err:
        await session.rollback()
        logger.error(f"Database commit failed for worker user_{account_db_id}/ chunk: {db_err}")

    if copy_source_broken:
        await _notify_admins_on_source_failure(session, order.id, last_source_error or "ChannelInvalid")

    logger.info(f"Worker user_{account_db_id}/ finished chunk for Order #{order.id}. Sent: {success_count}/{len(targets)}.")
    
    return unsent_targets