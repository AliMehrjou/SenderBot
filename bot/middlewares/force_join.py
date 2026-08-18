import logging
import time
from typing import Callable, Dict, Any, Awaitable

from aiogram import BaseMiddleware
from aiogram.types import TelegramObject, Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton

logger = logging.getLogger(__name__)

class ForceJoinMiddleware(BaseMiddleware):
    """
    میدلور جوین اجباری
    (آپدیت فاز ۵: جلوگیری از تخریب استیت FSM کاربر در صورت عدم عضویت)
    """
    def __init__(self) -> None:
        self.required_channels = ["@linkdoonifun", "@robotsfunlink"]
        self._cache: Dict[int, float] = {}
        self.cache_ttl = 300  # ۵ دقیقه
        super().__init__()

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

        for channel in self.required_channels:
            try:
                chat_member = await bot.get_chat_member(chat_id=channel, user_id=user.id)
                if chat_member.status in ["left", "kicked", "banned"]:
                    not_joined_channels.append(channel)
            except Exception as e:
                logger.warning(f"Could not check membership for {channel}: {e}")

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
            
        self._cache[user.id] = current_time + self.cache_ttl
        return await handler(event, data)