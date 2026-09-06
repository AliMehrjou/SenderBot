import logging
from typing import Callable, Dict, Any, Awaitable
from aiogram import BaseMiddleware
from aiogram.types import TelegramObject
from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncSession

logger = logging.getLogger(__name__)

class DatabaseMiddleware(BaseMiddleware):
    """
    میدلور دیتابیس (فاز ۳ اصلاح‌شده):
    این لایه فقط وظیفه تزریق سشن به هندلرها و مدیریت چرخه حیات (Lifecycle) اتصال را دارد.
    برای جلوگیری از تداخل (Race Condition و InvalidRequestError)، تراکنش‌ها (commit/rollback) 
    منحصراً درون خود هندلرها مدیریت می‌شوند.
    """
    def __init__(self, session_maker: async_sessionmaker[AsyncSession]) -> None:
        self.session_maker = session_maker

    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any]
    ) -> Any:
        
        # بلاک async with به صورت خودکار سشن را در پایان کار می‌بندد (session.close)
        async with self.session_maker() as session:
            data["session"] = session
            
            try:
                # اجرای هندلر
                result = await handler(event, data)
                return result
                
            except Exception as e:
                # لاگ کردن خطاهای پیش‌بینی‌نشده‌ای که در هندلرها کنترل نشده‌اند
                logger.error(f"Unhandled exception in handler layer: {e}")
                raise