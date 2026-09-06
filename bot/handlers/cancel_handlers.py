# bot/handlers/cancel_handlers.py

import logging
from contextlib import suppress

from aiogram import Router, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from sqlalchemy.ext.asyncio import AsyncSession
from bot.keyboards.main_menu import get_main_menu_keyboard, get_main_menu_button
from utils.fsm_cleanup import cleanup_fsm_temp_files
from bot.handlers.login_handlers import release_login_reservations
logger = logging.getLogger(__name__)

router = Router(name="cancel_handlers_router")


# ==========================================
# 🟣 فاز ۱ — لغو فلوی جاری از طریق دکمه inline
# ==========================================
@router.callback_query(F.data == "cancel_current_flow/")
async def cancel_current_flow(callback: types.CallbackQuery, state: FSMContext, session: AsyncSession) -> None:
    had_active_state = await state.get_state() is not None

    if had_active_state:
        await cleanup_fsm_temp_files(state)
        await state.clear()
        
        # آزادسازی رزروهای لاگین رها شده
        await release_login_reservations(callback.from_user.id, session)
        
        toast = "عملیات لغو شد."
        result_text = "❌ عملیات لغو شد.\nشما به منوی اصلی بازگشتید."
    else:
        toast = "هیچ عملیاتی فعال نبود."
        result_text = "🏛 شما به منوی اصلی بازگشتید."

    await callback.answer(toast)

    try:
        await callback.message.edit_text(result_text, reply_markup=get_main_menu_keyboard())
    except Exception:
        with suppress(Exception):
            await callback.message.answer(result_text, reply_markup=get_main_menu_keyboard())
    
# ==========================================
# 🟣 فاز ۱ — لغو فلوی جاری از طریق کامند /cancel
# ==========================================
@router.message(Command("cancel"))
async def cancel_command(message: types.Message, state: FSMContext, session: AsyncSession) -> None:
    if await state.get_state() is None:
        return await message.answer(
            "هیچ عملیاتی برای لغو کردن وجود ندارد.",
            reply_markup=get_main_menu_button()
        )

    await cleanup_fsm_temp_files(state)
    await state.clear()
    
    # آزادسازی رزروهای لاگین رها شده
    await release_login_reservations(message.from_user.id, session)

    await message.answer(
        "❌ عملیات لغو شد.\nشما به منوی اصلی بازگشتید.",
        reply_markup=get_main_menu_keyboard()
    )