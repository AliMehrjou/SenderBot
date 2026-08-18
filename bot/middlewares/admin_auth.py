from typing import Callable, Dict, Any, Awaitable
from aiogram import BaseMiddleware
from aiogram.types import TelegramObject, Message, CallbackQuery
from config import config

class AdminMiddleware(BaseMiddleware):
    """
    این میدلور تضمین می‌کند که فقط ادمین اصلی (تعریف شده در فایل کانفیگ) 
    بتواند با کنترل‌پنل کار کند و اکانت اضافه کند.
    """
    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any]
    ) -> Any:
        
        user = data.get("event_from_user")
        
        # اگر کاربری وجود نداشت یا آیدی او با ADMIN_ID در .env برابر نبود
        if not user or user.id != config.ADMIN_ID:
            if isinstance(event, Message):
                await event.answer("⛔️ <b>دسترسی غیرمجاز.</b> شما ادمین این سیستم نیستید.")
            elif isinstance(event, CallbackQuery):
                await event.answer("⛔️ دسترسی غیرمجاز.", show_alert=True)
            
            # دراپ کردن آپدیت (توقف پردازش و عدم اجازه ورود به هندلرها)
            return
            
        # در صورت موفقیت، آپدیت به مرحله بعد می‌رود
        return await handler(event, data)