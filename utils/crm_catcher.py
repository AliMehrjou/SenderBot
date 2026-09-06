import logging
import time
from typing import Optional, Set, Tuple

import aiohttp
import redis.asyncio as aioredis
from pyrogram import Client
from pyrogram.types import Message
from sqlalchemy import select
import html
from config import config
from database.engine import async_session
from database.models import Admin

logger = logging.getLogger(__name__)
# ==========================================
# فیکس فاز ۱: سشن شبکه گلوبال (جلوگیری از نشت پورت)
# ==========================================
_http_session = None

async def get_http_session() -> aiohttp.ClientSession:
    """
    تولید و نگهداری یک سشن پایدار HTTP برای بهینه‌سازی منابع سرور
    """
    global _http_session
    if _http_session is None or _http_session.closed:
        _http_session = aiohttp.ClientSession()
    return _http_session


# ==========================================
# 🧹 فاز ۹ (BUG-27): کلاینت Redis فیلتر تارگت + سقف نرخ per admin
# (همان الگوی sender.py — lazy و ایزوله؛ در shutdown با close_crm_redis بسته می‌شود)
# ==========================================
_redis_client: Optional[aioredis.Redis] = None


def _get_redis() -> aioredis.Redis:
    """ساخت lazy کلاینت Redis (در اولین استفاده ساخته می‌شود)."""
    global _redis_client
    if _redis_client is None:
        _redis_client = aioredis.from_url(
            config.REDIS_URL, decode_responses=True, socket_timeout=2
        )
    return _redis_client


# --- File: crm_catcher.py ---

async def close_crm_redis() -> None:
    """بستن امن کلاینت Redis CRM و سشن HTTP (در shutdown اصلی صدا زده می‌شود)."""
    global _redis_client
    if _redis_client is not None:
        try:
            await _redis_client.aclose()
        except Exception:
            pass
        _redis_client = None

    # --- FIX M9: Close the module-level aiohttp session on shutdown ---
    global _http_session
    if _http_session is not None and not _http_session.closed:
        try:
            await _http_session.close()
        except Exception:
            pass
        _http_session = None
    

_local_rate_counts: dict = {} 


async def _is_recent_campaign_target(user_id: int) -> bool:
    """
    آیا این فرستنده تارگتِ ارسالِ اخیرِ کمپین است؟ (علامت‌گذاری sender پس از هر ارسال)
    fail-open: خطای Redis → اجازه‌ی عبور + لاگ؛ سقف نرخ همچنان فعال می‌ماند.
    """
    try:
        return bool(await _get_redis().exists(f"crm_recent_target:{int(user_id)}"))
    except Exception as e:
        logger.warning(
            f"CRM filter Redis check failed ({e.__class__.__name__}); "
            f"allowing notification (fail-open); rate limiter still active."
        )
        return True


async def _notify_rate_allows(admin_id: int) -> bool:
    """
    سقف نرخ per admin — پنجره‌ی ثابت (حداکثر CRM_NOTIFY_LIMIT_PER_ADMIN پیام در
    هر CRM_NOTIFY_WINDOW_SECONDS ثانیه). پنجره با SET NX (اتمیک + TTL تضمینی)
    باز می‌شود تا کلید «جاودانه» ممکن نباشد؛ INCR در بقیه‌ی پنجره.
    fallback درون‌حافظه‌ای در قطع Redis (تا سیل پیام همچنان مهار شود).
    """
    limit = max(1, int(getattr(config, "CRM_NOTIFY_LIMIT_PER_ADMIN", 20)))
    window = max(1, int(getattr(config, "CRM_NOTIFY_WINDOW_SECONDS", 60)))
    key = f"crm_notify_rate:{admin_id}"
    try:
        r = _get_redis()
        if await r.set(key, 1, ex=window, nx=True):
            return True  # اولین پیامِ پنجره‌ی جدید
        count = int(await r.incr(key))
        return count <= limit
    except Exception as e:
        logger.warning(
            f"CRM rate-limit Redis failed ({e.__class__.__name__}); using in-memory fallback."
        )
        now = time.monotonic()
        for k in [k for k, (c, ts) in _local_rate_counts.items() if now - ts >= window]:
            _local_rate_counts.pop(k, None)
        count, started = _local_rate_counts.get(admin_id, (0, now))
        if count == 0:
            _local_rate_counts[admin_id] = (1, now)
            return True
        _local_rate_counts[admin_id] = (count + 1, started)
        return (count + 1) <= limit


async def _bump_suppressed(admin_id: int) -> None:
    """شمارش پیام‌های حذف‌شده توسط سقف نرخ (برای digest پیام بعدی) — best-effort."""
    try:
        key = f"crm_notify_suppressed:{admin_id}"
        pipe = _get_redis().pipeline()
        pipe.incr(key)
        pipe.expire(key, 6 * 3600)  # بیشینه‌ی عمر شمارنده‌ی digest
        await pipe.execute()
    except Exception:
        pass


async def _take_suppressed(admin_id: int) -> int:
    """خواندن و صفرکردن شمارنده‌ی پیام‌های suppressشده — best-effort."""
    try:
        val = await _get_redis().getset(f"crm_notify_suppressed:{admin_id}", "0")
        return int(val) if val else 0
    except Exception:
        return 0


# 🛡 فاز ۹ (BUG-27/BUG-29): کش TTL لیست ادمین‌ها — SELECT فقط یک‌بار در هر بازه
_admins_cache: Tuple[Set[int], float] = (set(), 0.0)


async def _get_target_admins() -> Set[int]:
    """
    لیست ادمین‌ها (اصلی + فرعی) با کش TTL (ADMIN_ROLE_CACHE_TTL، محدوده ۶۰–۳۰۰s).
    در قطعی DB: کشِ کهنه تمدید می‌شود (بهتر از هیچ) تا SELECT پیاپی زده نشود.
    """
    global _admins_cache
    ids, fetched_at = _admins_cache
    now = time.monotonic()
    ttl = max(60, min(int(getattr(config, "ADMIN_ROLE_CACHE_TTL", 120)), 300))
    if ids and (now - fetched_at) < ttl:
        return ids

    fresh: Set[int] = set()
    if config.ADMIN_ID and config.ADMIN_ID != 0:
        fresh.add(config.ADMIN_ID)
    try:
        async with async_session() as db_session:
            sub_admins = (await db_session.scalars(select(Admin.telegram_id))).all()
            fresh.update(int(admin_id) for admin_id in sub_admins)
    except Exception as e:
        logger.error(f"CRM Catcher failed to fetch sub-admins from DB: {e}")
        if ids:
            _admins_cache = (ids, now)  # تمدید کشِ کهنه در قطعی DB
            return ids
        return fresh
    _admins_cache = (fresh, now)
    return fresh


# ==========================================
# هندلر دریافت پیام‌های تارگت‌ها (CRM Engine)
# ==========================================
async def incoming_message_handler(client: Client, message: Message) -> None:
    # 🔐 فاز ۹ (SEC-7): توکن بات از منبع واحد config (env: BOT_TOKEN)
    bot_token = config.BOT_TOKEN
    if not bot_token:
        logger.error("BOT_TOKEN is not configured in config/env. CRM Catcher cannot notify admins.")
        return

    # نادیده گرفتن پیام‌های خود اکانت، ربات‌ها یا پیام‌های سرویس
    if not message.from_user or message.from_user.is_self or message.from_user.is_bot:
        return

    # ==========================================
    # 🛡 فاز ۹ (BUG-27): فیلتر ارتباط — فقط تارگت‌های ارسالِ اخیرِ کمپین
    # ==========================================
    if not await _is_recent_campaign_target(message.from_user.id):
        logger.debug(
            f"CRM: message from {message.from_user.id} is not a recent campaign "
            f"target; skipped (BUG-27 filter)."
        )
        return

    # ==========================================
    # فیکس فاز ۳ + 🛡 فاز ۹ (BUG-27): واکشی لیست ادمین‌ها با کش TTL
    # ==========================================
    target_admins = await _get_target_admins()
    if not target_admins:
        return

    try:
        # نام و یوزرنیم با html.escape ایمن شدند
        sender_name_raw = message.from_user.first_name or "کاربر"
        sender_name = html.escape(sender_name_raw)
        
        sender_username_raw = f"(@{message.from_user.username})" if message.from_user.username else ""
        sender_username = html.escape(sender_username_raw)
        
        worker_id = client.name.replace("worker_acc_", "")

        # ==========================================
        # فیکس فاز ۴: تشخیص هوشمند نوع مدیا
        # ==========================================
        media_type_str = ""
        if message.photo:
            media_type_str = "🖼 [عکس]"
        elif message.video:
            media_type_str = "🎥 [ویدیو]"
        elif message.document:
            media_type_str = "📁 [فایل/سند]"
        elif message.voice:
            media_type_str = "🎤 [ویس]"
        elif message.audio:
            media_type_str = "🎵 [موزیک]"
        elif message.sticker:
            media_type_str = "🧩 [استیکر]"
        elif message.animation:
            media_type_str = "🎞 [گیف]"
        
        raw_text = message.text or message.caption or ""
        
        if raw_text and media_type_str:
            msg_text = f"{media_type_str}\n{raw_text}"
        elif raw_text:
            msg_text = raw_text
        elif media_type_str:
            msg_text = f"<i>{media_type_str} - برای مشاهده به اکانت ورکر مراجعه کنید.</i>"
        else:
            msg_text = "<i>[محتوای نامشخص یا پشتیبانی نشده]</i>"

        # ==========================================
        # فیکس فاز ۲: کنترل طول پیام (محدودیت ۴۰۹۶ کاراکتری تلگرام)
        # ترکیب با فاز ۱: امن‌ترین راه‌حل: کوتاه‌سازی متن پیش از escape انجام می‌شود تا تگ‌های escape شده در میانه راه قطع نشوند.
        # ==========================================
        MAX_TEXT_LENGTH = 3500
        if len(msg_text) > MAX_TEXT_LENGTH:
            msg_text = html.escape(msg_text[:MAX_TEXT_LENGTH]) + "\n\n... <i>[✂️ متن به دلیل طولانی بودن برش داده شد. برای مطالعه کامل مستقیماً به اکانت مراجعه کنید]</i>"
        else:
            # اگر پیام شامل متن خام کاربر است، اسکیپ می‌کنیم تا تگ‌های HTML سیستمی آسیب نبینند
            if raw_text:
                msg_text = html.escape(msg_text)

        info_text = (
            "📩 <b>پیام جدید از تارگت (سیستم CRM)</b>\n\n"
            f"👤 <b>فرستنده:</b> {sender_name} {sender_username}\n"
            f"🆔 <b>آیدی فرستنده:</b> <code>{message.from_user.id}</code>\n"
            f"🤖 <b>دریافت شده در ورکر:</b> <code>{worker_id}</code>\n\n"
            f"💬 <b>متن پیام:</b>\n{msg_text}"
        )
        
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        http_session = await get_http_session()
        
        # ==========================================
        # فیکس فاز ۵: تزریق دکمه شیشه‌ای برای پاسخ‌دهی
        # ==========================================
        reply_markup = {
            "inline_keyboard": [
                [
                    {
                        "text": "✉️ پاسخ به این کاربر",
                        "callback_data": f"crm_reply_{worker_id}_{message.from_user.id}"
                    }
                ]
            ]
        }
        
        sent_count = 0
        for admin_tg_id in target_admins:
            if not await _notify_rate_allows(admin_tg_id):
                await _bump_suppressed(admin_tg_id)
                logger.warning(
                    f"CRM: rate limit reached for admin {admin_tg_id}; "
                    f"message from {message.from_user.id} suppressed."
                )
                continue
            admin_text = info_text
            suppressed = await _take_suppressed(admin_tg_id)
            if suppressed > 0:
                admin_text += (
                    f"\n\n⚠️ <i>— {suppressed} پیام دیگر در این بازه به دلیل "
                    f"محدودیت نرخ ارسال نمایش داده نشد —</i>"
                )
            payload = {
                "chat_id": admin_tg_id,
                "text": admin_text,
                "parse_mode": "HTML",
                "reply_markup": reply_markup
            }
            try:
                async with http_session.post(url, json=payload) as response:
                    if response.status == 200:
                        sent_count += 1
                    else:
                        logger.error(f"CRM API Error for admin {admin_tg_id}: HTTP {response.status}")
            except Exception as req_err:
                logger.error(f"Failed to send CRM message to admin {admin_tg_id}: {req_err}")

        if sent_count:
            logger.info(f"CRM: notified {sent_count} admin(s) about message from {message.from_user.id}")
             
    except Exception as e:
        logger.error(f"CRM Catcher failed to process incoming message: {e}")