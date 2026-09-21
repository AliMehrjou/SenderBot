# -*- coding: utf-8 -*-
"""
🔒 Force-Join Middleware — Phase 6 (T3 + T4)
============================================
bot/middlewares/force_join.py

Gate every update behind channel-membership verification.

Phase 6 (T3) — Redis caching (fixes F3: getChatMember hammering):
    * Negative cache — `forcejoin:miss:{user_id}` (TTL 600s): a user who
      failed the check is rejected from cache without re-querying Telegram
      on every update.
    * Positive cache — `forcejoin:hit:{user_id}` (TTL 60s): a recently
      verified member skips the check entirely.
    * The «✅ عضو شدم» callback ALWAYS performs a real re-check; on success
      the miss-cache entry is deleted and the hit-cache is set.

Phase 6 (T4) — credentials: everything (channels, admin bypass) comes from
config — no inline tokens / api hashes / hard-coded URLs in this module.

All user-facing texts are Persian; comments are English.
"""

import logging
from typing import Any, Awaitable, Callable, Dict, Optional

from aiogram import BaseMiddleware, Bot
from aiogram.exceptions import TelegramBadRequest
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
    TelegramObject,
    User,
)

from config import config
from utils.telegram_helpers import safe_callback_answer
from workers.sender import _get_redis  # shared async Redis client (BUG-04 registry client)
from bot.middlewares.admin_auth import is_sub_admin_cached

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------- #
# Cache tuning (Phase 6 / T3 — fixed values per spec)
# ---------------------------------------------------------------- #
FORCEJOIN_MISS_TTL = 600  # seconds — failing users are cached for 10 minutes
FORCEJOIN_HIT_TTL = 60    # seconds — verified members skip re-checks briefly

# Telegram membership statuses that count as "joined"
_MEMBER_STATUSES = {"creator", "administrator", "member", "restricted"}

# Callback data of the «I joined» button produced by this middleware
CHECK_JOIN_CALLBACK = "forcejoin_check_joined"

JOIN_REQUIRED_TEXT = (
    "👋 <b>کاربر گرامی، سلام!</b>\n\n"
    "🔒 <b>جهت استفاده از امکانات ربات، لطفاً ابتدا در کانال‌های زیر عضو شوید:</b>\n\n"
    "{channels}\n\n"
    "👇 <i>پس از عضویت، جهت ادامه‌ی کار روی دکمه زیر کلیک کنید.</i>"
)


def _miss_key(user_id: int) -> str:
    return f"forcejoin:miss:{user_id}"


def _hit_key(user_id: int) -> str:
    return f"forcejoin:hit:{user_id}"


# ---------------------------------------------------------------- #
# Redis helpers — every call degrades gracefully: if Redis is down we
# fall back to per-update checks (the pre-T3 behavior).
# ---------------------------------------------------------------- #
import time

_memory_cache = {}

async def _cache_exists(key: str) -> bool:
    if key in _memory_cache:
        if time.time() < _memory_cache[key]:
            return True
        else:
            del _memory_cache[key]
    return False


async def _cache_set(key: str, ttl: int) -> None:
    _memory_cache[key] = time.time() + ttl


async def clear_force_join_miss(user_id: int) -> None:
    """
    Delete the negative-cache entry for a user. Called on the
    successful-join callback path; safe to call from any external
    handler that verifies membership on its own.
    """
    key = _miss_key(user_id)
    if key in _memory_cache:
        del _memory_cache[key]


# ---------------------------------------------------------------- #
# Channel helpers (T4: identifiers/URLs derived from config only)
# ---------------------------------------------------------------- #
def _channel_url(channel: str) -> str:
    ch = channel.strip()
    if ch.startswith(("http://", "https://")):
        return ch
    return f"https://t.me/{ch.lstrip('@')}"


def _chat_identifier(channel: str) -> Optional[str]:
    """
    Config value -> getChatMember chat_id.
    @username / plain username -> @username; public t.me link -> @username;
    invite links (+hash / joinchat/) can't be queried -> None (skipped).
    """
    ch = channel.strip()
    if ch.startswith("@"):
        return ch
    if "t.me/" in ch:
        username = ch.split("t.me/", 1)[1].strip("/ ")
        if username.startswith("+") or username.startswith("joinchat/"):
            return None
        return f"@{username}" if username else None
    return f"@{ch}" if ch else None


def build_force_join_keyboard(channels: list) -> InlineKeyboardMarkup:
    # ساخت دکمه‌های مجزا برای هر کانال (با شماره‌گذاری شیک)
    rows = [
        [InlineKeyboardButton(text=f"📢 عضویت در کانال {i+1}", url=_channel_url(ch))]
        for i, ch in enumerate(channels)
    ]
    
    # اضافه کردن دکمه عریض در آخرین ردیف (چون تنها المان آرایه است، تمام‌عرض می‌شود)
    rows.append([InlineKeyboardButton(text="✅ عضو شدم / بررسی مجدد", callback_data=CHECK_JOIN_CALLBACK)])
    
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _join_required_text(channels: list) -> str:
    lines = "\n".join(f"▫️ {_channel_url(ch)}" for ch in channels)
    return JOIN_REQUIRED_TEXT.format(channels=lines)


# ---------------------------------------------------------------- #
# Middleware
# ---------------------------------------------------------------- #
class ForceJoinMiddleware(BaseMiddleware):
    """
    Class name unchanged (dispatcher registration untouched). Check order:

        1. no user context / admin / empty channel list -> pass
        2. «✅ عضو شدم» callback -> REAL re-check (caches bypassed)
        3. positive cache hit  -> pass
        4. negative cache hit  -> reject immediately (no Telegram call)
        5. real membership check -> cache miss or hit, then reject or pass
    """

    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any],
    ) -> Any:
        user: Optional[User] = data.get("event_from_user")
        bot: Optional[Bot] = data.get("bot")

        if user is None or bot is None:
            return await handler(event, data)

        if user.id == config.ADMIN_ID or await is_sub_admin_cached(user.id, data.get("session")):
            return await handler(event, data)

        channels = config.FORCE_JOIN_CHANNEL_LIST
        if not channels:
            return await handler(event, data)

        # (2) The join-verification button bypasses every cache: the user
        # clicked it precisely to invalidate a previous failure.
        if isinstance(event, CallbackQuery) and event.data == CHECK_JOIN_CALLBACK:
            return await self._handle_check_joined(event, bot, user, channels)

        # (3) positive cache — recently verified member
        if await _cache_exists(_hit_key(user.id)):
            return await handler(event, data)

        # (4) negative cache — reject without touching Telegram
        if await _cache_exists(_miss_key(user.id)):
            await self._reject(event, user.id, channels)
            return None

        # (5) real check
        if await self._all_joined(bot, user.id, channels):
            await _cache_set(_hit_key(user.id), FORCEJOIN_HIT_TTL)
            return await handler(event, data)

        await _cache_set(_miss_key(user.id), FORCEJOIN_MISS_TTL)
        await self._reject(event, user.id, channels)
        return None

    # ------------------------------------------------------------------ #
    async def _all_joined(self, bot: Bot, user_id: int, channels: list) -> bool:
        for channel in channels:
            if not await self._is_member(bot, user_id, channel):
                return False
        return True

    async def _is_member(self, bot: Bot, user_id: int, channel: str) -> bool:
        chat_id = _chat_identifier(channel)
        if chat_id is None:
            # Invite-link channels cannot be queried via getChatMember —
            # unverifiable, so we must not lock the user out.
            logger.debug(f"Force-join: {channel!r} is an invite link; check skipped.")
            return True
        try:
            member = await bot.get_chat_member(chat_id=chat_id, user_id=user_id)
            return member.status in _MEMBER_STATUSES
        except Exception as e:
            error_text = str(e).lower()
            # در صورتی که ربات ادمین نباشد خطای مربوطه را هندل کرده و دسترسی کاربر را باز می‌گذاریم
            if "chatadminrequired" in error_text or ("not a member" in error_text and "bot" in error_text):
                logger.error(f"Force-join Admin Error: Bot is not admin in {channel}. Bypassing check to avoid lockout. Detail: {e}")
                return True
            
            if isinstance(e, TelegramBadRequest):
                # "user not found" / not a member
                return False
                
            logger.warning(
                f"Force-join: membership check failed (user {user_id}, {channel}): {e}"
            )
            return True

    async def _handle_check_joined(
        self, event: CallbackQuery, bot: Bot, user: User, channels: list
    ) -> Any:
        # Always a REAL re-check — the whole point of this button.
        if await self._all_joined(bot, user.id, channels):
            # Success: delete the miss key (T3 requirement) + set hit cache
            try:
                await clear_force_join_miss(user.id)
                await _cache_set(_hit_key(user.id), FORCEJOIN_HIT_TTL)
            except Exception as e:
                logger.warning(
                    f"Force-join: cache update after join failed (user {user.id}): {e}"
                )
            await safe_callback_answer(
                event,
                "✅ عضویت شما تأیید شد. لطفاً درخواست خود را دوباره ارسال کنید.",
            )
            
            # +++ اضافه شدن حذف پیام برای فیدبک بصری به کاربر +++
            try:
                await event.message.delete()
            except Exception:
                pass
            # +++++++++++++++++++++++++++++++++++++++++++++++++++
            
            return None

        # Still not a member — refresh the negative-cache window
        await _cache_set(_miss_key(user.id), FORCEJOIN_MISS_TTL)
        await safe_callback_answer(
            event,
            "❌ هنوز عضویت شما تأیید نشده است. ابتدا در کانال عضو شوید.",
            show_alert=True,
        )
        return None

    async def _reject(self, event: TelegramObject, user_id: int, channels: list) -> None:
        """ارسال اخطار عضویت اجباری برای کاربرانی که عضو نیستند"""
        text = _join_required_text(channels)
        markup = build_force_join_keyboard(channels)

        if isinstance(event, Message):
            await event.answer(text, reply_markup=markup, disable_web_page_preview=True)
        elif isinstance(event, CallbackQuery):
            await safe_callback_answer(event, "❌ برای استفاده از ربات، عضویت در کانال‌ها الزامی است.", show_alert=True)
            if getattr(event, "message", None):
                try:
                    await event.message.answer(text, reply_markup=markup, disable_web_page_preview=True)
                except Exception:
                    pass