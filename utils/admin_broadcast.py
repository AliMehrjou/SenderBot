import logging
from typing import List, Union, Dict, Optional

from aiogram import Bot
from aiogram.types import InlineKeyboardMarkup
from sqlalchemy import select

from database.engine import async_session
from database.models import Admin
from config import config

logger = logging.getLogger(__name__)

async def _get_all_admin_ids() -> List[int]:
    """واکشی آیدی ادمین اصلی از کانفیگ و ساب‌ادمین‌ها از دیتابیس"""
    admin_ids = []
    if getattr(config, "ADMIN_ID", None):
        admin_ids.append(int(config.ADMIN_ID))
        
    try:
        async with async_session() as session:
            sub_admins = (await session.scalars(select(Admin.telegram_id))).all()
            admin_ids.extend(int(aid) for aid in sub_admins)
    except Exception as e:
        logger.error(f"Failed to fetch admins: {e}")
        
    return list(set(admin_ids))

async def invalidate_admin_cache() -> None:
    pass

async def broadcast_to_admins(
    bot: Bot, 
    text: str, 
    exclude: Union[int, List[int], None] = None
) -> Dict[str, Union[int, List[int]]]:
    """ارسال پیام متنی به ادمین‌ها"""
    admin_ids = await _get_all_admin_ids()
    if exclude:
        exclude_list = [exclude] if isinstance(exclude, int) else exclude
        admin_ids = [aid for aid in admin_ids if aid not in exclude_list]

    sent, failed = 0, 0
    failed_ids = []
    for admin_id in admin_ids:
        try:
            await bot.send_message(chat_id=admin_id, text=text)
            sent += 1
        except Exception:
            failed += 1
            failed_ids.append(admin_id)
            
    return {"sent": sent, "failed": failed, "failed_ids": failed_ids}

async def broadcast_to_admins_with_keyboard(
    bot: Bot, 
    text: str, 
    keyboard: Optional[InlineKeyboardMarkup] = None, 
    exclude: Union[int, List[int], None] = None
) -> Dict[str, Union[int, List[int]]]:
    """ارسال پیام دکمه‌دار (مثل پنل تایید سفارش) به ادمین‌ها"""
    admin_ids = await _get_all_admin_ids()
    if exclude:
        exclude_list = [exclude] if isinstance(exclude, int) else exclude
        admin_ids = [aid for aid in admin_ids if aid not in exclude_list]

    sent, failed = 0, 0
    failed_ids = []
    for admin_id in admin_ids:
        try:
            await bot.send_message(chat_id=admin_id, text=text, reply_markup=keyboard)
            sent += 1
        except Exception:
            failed += 1
            failed_ids.append(admin_id)
            
    return {"sent": sent, "failed": failed, "failed_ids": failed_ids}