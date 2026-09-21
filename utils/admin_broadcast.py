# File: utils/admin_broadcast.py
import json
import logging
from typing import List, Union, Dict, Optional

from aiogram import Bot
from aiogram.types import InlineKeyboardMarkup
from sqlalchemy import select

from database.engine import async_session
from database.models import Admin
from config import config
from workers.sender import _get_redis

logger = logging.getLogger(__name__)

async def _get_all_admin_ids() -> List[int]:
    """واکشی آیدی تمام ادمین‌ها از کش Redis یا دیتابیس در صورت نبود در کش"""
    redis = _get_redis()
    cache_key = "cached_admin_ids"
    
    try:
        cached = await redis.get(cache_key)
        if cached:
            return json.loads(cached)
    except Exception as e:
        logger.warning(f"Failed to read admin IDs from Redis: {e}")

    admin_ids = set()
    
    # اضافه کردن ادمین اصلی
    if getattr(config, "ADMIN_ID", None) and int(config.ADMIN_ID) != 0:
        admin_ids.add(int(config.ADMIN_ID))

    # واکشی از دیتابیس
    try:
        async with async_session() as db_session:
            result = await db_session.execute(select(Admin.telegram_id))
            for row in result.scalars().all():
                admin_ids.add(int(row))
    except Exception as e:
        logger.error(f"Failed to fetch sub-admins from DB: {e}")

    ids_list = list(admin_ids)
    
    # کش کردن به مدت ۶۰ ثانیه
    try:
        await redis.set(cache_key, json.dumps(ids_list), ex=60)
    except Exception as e:
        logger.warning(f"Failed to cache admin IDs in Redis: {e}")

    return ids_list


async def invalidate_admin_cache() -> None:
    """بی‌اعتبارسازی کش ادمین‌ها در ردیس پس از افزودن/حذف ادمین"""
    redis = _get_redis()
    cache_key = "cached_admin_ids"
    try:
        await redis.delete(cache_key)
    except Exception as e:
        logger.warning(f"Failed to invalidate admin cache in Redis: {e}")


async def broadcast_to_admins(
    bot: Bot, 
    text: str, 
    exclude: Union[int, List[int], None] = None
) -> Dict[str, Union[int, List[int]]]:
    """ارسال یک پیام متنی به همه‌ی ادمین‌ها."""
    return await broadcast_to_admins_with_keyboard(
        bot=bot, text=text, keyboard=None, exclude=exclude
    )


async def broadcast_to_admins_with_keyboard(
    bot: Bot, 
    text: str, 
    keyboard: Optional[InlineKeyboardMarkup] = None, 
    exclude: Union[int, List[int], None] = None
) -> Dict[str, Union[int, List[int]]]:
    """ارسال یک پیام متنی همراه با دکمه شیشه‌ای (InlineKeyboard) به همه‌ی ادمین‌ها."""
    if exclude is None:
        exclude_list = []
    elif isinstance(exclude, int):
        exclude_list = [exclude]
    else:
        exclude_list = exclude

    admin_ids = await _get_all_admin_ids()
    target_ids = [aid for aid in admin_ids if aid not in exclude_list]
    
    result: Dict[str, Union[int, List[int]]] = {"sent": 0, "failed": 0, "failed_ids": []}
    
    for admin_id in target_ids:
        try:
            await bot.send_message(
                chat_id=admin_id,
                text=text,
                reply_markup=keyboard,
                parse_mode="HTML",
                disable_web_page_preview=True
            )
            result["sent"] += 1
        except Exception as e:
            logger.error(f"Failed to send broadcast to admin {admin_id}: {e}")
            result["failed"] += 1
            if isinstance(result["failed_ids"], list):
                result["failed_ids"].append(admin_id)
                
    return result