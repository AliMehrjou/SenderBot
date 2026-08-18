import logging
from typing import Callable, Dict, Any, Awaitable
from aiogram import BaseMiddleware
from aiogram.types import TelegramObject
from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncSession

logger = logging.getLogger(__name__)

class DatabaseMiddleware(BaseMiddleware):
    """
    میدلور دیتابیس برای تزریق سشن به هندلرها.
    (آپدیت فاز ۴: افزودن مدیریت مرکزی تراکنش‌ها برای جلوگیری از نشت داده)
    """
    def __init__(self, session_maker: async_sessionmaker[AsyncSession]) -> None:
        self.session_maker = session_maker

    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any]
    ) -> Any:
        
        async with self.session_maker() as session:
           
            data["session"] = session
            
            try:
               
                result = await handler(event, data)
                

                await session.commit()
                return result
                
            except Exception as e:

                await session.rollback()
                logger.error(f"Database transaction rolled back due to error in handler: {e}")
                raise