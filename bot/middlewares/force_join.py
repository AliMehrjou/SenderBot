import logging
import time
from contextlib import suppress
from typing import Callable, Dict, Any, Awaitable

from config import config

from aiogram import BaseMiddleware, Bot
from aiogram.types import TelegramObject, Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton

logger = logging.getLogger(__name__)

# 🔴 فاز ۱۱ (BUG-17a): کانال‌های اجباری به سطح ماژول منتقل شدند تا هندلر
# verify_join هم برای «بررسی واقعی عضویت» از همان منبع واحد استفاده کند.
# 📌 فاز ۱۲: منبع این لیست به config منتقل می‌شود — فقط همین یک خط تغییر می‌کند.

REQUIRED_CHANNELS = config.FORCE_JOIN_CHANNEL_LIST or []

class ForceJoinMiddleware(BaseMiddleware):
    """
    میدلور جوین اجباری
    (آپدیت فاز ۵: جلوگیری از تخریب استیت FSM کاربر در صورت عدم عضویت)
    """
    def __init__(self) -> None:
        self.required_channels = REQUIRED_CHANNELS
        self._cache: Dict[int, float] = {}
        self.cache_ttl = 300  # ۵ دقیقه
        self.max_cache_size = 10_000  # 🔴 فاز ۱۱ (BUG-17c): سقف اندازهٔ کش
        # 🔴 فاز ۱۱ (BUG-17b): پرچم «اطلاع یک‌باره» به ازای هر کانال
        self._admin_notified_channels: set = set()
        super().__init__()

    def _cleanup_cache(self) -> None:
        """
        🔴 فاز ۱۱ (BUG-17c): جلوگیری از رشد بی‌حد _cache —
        ۱) حذف ورودی‌های منقضی‌شده؛ ۲) سقف اندازه با حذف قدیمی‌ترین‌ها.
        (فراخوانی پیش از هر درج؛ چون رشد کش فقط از مسیر درج است، همین کافی است.)
        """
        now = time.time()
        # ۱) حذف منقضی‌ها
        for uid in [uid for uid, exp in self._cache.items() if exp <= now]:
            del self._cache[uid]
        # ۲) سقف اندازه: قدیمی‌ترین‌ها (کوچک‌ترین زمان انقضا) حذف می‌شوند
        if len(self._cache) >= self.max_cache_size:
            overflow = len(self._cache) - self.max_cache_size + 1  # +۱ برای ورودی جدید
            for uid, _ in sorted(self._cache.items(), key=lambda kv: kv[1])[:overflow]:
                del self._cache[uid]

    async def _notify_admin_once(self, bot: Bot, channel: str, fallback_user_id: int) -> None:
        """
        🔴 فاز ۱۱ (BUG-17b): اطلاع «یک‌باره» از خرابی بررسی عضویت (به ازای هر کانال).
        هدف اطلاع فعلاً کاربر جاری است (کاربران این پنل، ادمین هستند).
        📌 معلق: منبع canonical ادمین‌ها (config/DB) در فاز ۱۲ جایگزین می‌شود.
        """
        if channel in self._admin_notified_channels:
            return
        self._admin_notified_channels.add(channel)
        text = (
            "⚠️ <b>هشدار سیستم (Force-Join)</b>\n\n"
            f"ربات نتوانست عضویت را در <code>{channel}</code> بررسی کند.\n"
            "محتمل‌ترین علت: ربات ادمین این کانال <b>نیست</b>.\n"
            "لطفاً ربات را به‌عنوان ادمین به کانال اضافه کنید تا بررسی عضویت مجدداً فعال شود."
        )
        with suppress(Exception):
            await bot.send_message(chat_id=fallback_user_id, text=text)

    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any]
    ) -> Any:
        
        user = data.get("event_from_user")
        bot = data.get("bot")
        
        if not user or not bot:
            return await handler(event, data)

        current_time = time.time()
        if user.id in self._cache and current_time < self._cache[user.id]:
            return await handler(event, data)

        not_joined_channels = []

        # در حدود خط ۵۵
        if not self.required_channels or user.id == config.ADMIN_ID:
            return await handler(event, data)
        
        for channel in self.required_channels:
            try:
                chat_member = await bot.get_chat_member(chat_id=channel, user_id=user.id)
                if chat_member.status in ["left", "kicked", "banned"]:
                    not_joined_channels.append(channel)
            except Exception as e:
                # 🔴 فاز ۱۱ (BUG-17b): خطا دیگر «بی‌صدا رد» نمی‌شود؛
                # کانالِ بررسی‌نشده مثل عضو-نشده تلقی می‌شود (fail-closed) تا
                # با حذف ادمینیِ ربات، force-join عملاً خاموش نماند.
                logger.warning(f"ForceJoin: membership check failed for {channel}: {e}")
                not_joined_channels.append(channel)
                await self._notify_admin_once(bot, channel, user.id)

        if not_joined_channels:
            # 🔴 حذف دستورات state.clear() و cleanup_client() 
            # تا کاربر در صورت لفت دادن، فرآیند ثبت سفارش خود را از دست ندهد.
            
            keyboard = []
            for channel in not_joined_channels:
                url = f"https://t.me/{channel.replace('@', '')}"
                keyboard.append([InlineKeyboardButton(text=f"عضویت در {channel}", url=url)])
            
            keyboard.append([InlineKeyboardButton(text="✅ عضو شدم", callback_data="menu_verify_join/")])
            reply_markup = InlineKeyboardMarkup(inline_keyboard=keyboard)
            
            text = (
                "🛑 <b>دسترسی محدود شد</b>\n\n"
                "برای ادامه کار و بدون از دست رفتن مراحل فعلی‌تان، لطفاً ابتدا در کانال‌های زیر عضو شوید:"
            )
            
            if isinstance(event, Message):
                await event.answer(text, reply_markup=reply_markup)
            elif isinstance(event, CallbackQuery):
                await event.message.answer(text, reply_markup=reply_markup)
                await event.answer()
                
            return
            
        self._cleanup_cache()
        self._cache[user.id] = current_time + self.cache_ttl
        return await handler(event, data)