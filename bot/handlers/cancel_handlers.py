# bot/handlers/cancel_handlers.py

import logging
from contextlib import suppress
from aiogram import Bot
from bot.keyboards.main_menu import get_main_menu_keyboard, get_main_menu_reply_keyboard
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
            "هیچ عملیاتی برای لغو کردن وجود ندارد."

        )

    await cleanup_fsm_temp_files(state)
    await state.clear()
    
    # آزادسازی رزروهای لاگین رها شده
    await release_login_reservations(message.from_user.id, session)

    await message.answer(
        "❌ عملیات لغو شد.\nشما به منوی اصلی بازگشتید.",
        reply_markup=get_main_menu_keyboard()
    )
# ==========================================
# 🔄 فاز ۱ (زیر فاز ۱) — سیستم ریستارت و پین هوشمند منو
# ==========================================
async def execute_smart_restart(
    event: types.Message | types.CallbackQuery,
    state: FSMContext,
    session: AsyncSession,
    bot: Bot
) -> None:
    """منطق یکپارچه برای ریستارت، پاکسازی استیت‌ها و پین هوشمند منو"""
    # ۱. پاکسازی کامل استیت‌ها و تسک‌های موقت
    await cleanup_fsm_temp_files(state)
    await state.clear()
    
    user_id = event.from_user.id
    await release_login_reservations(user_id, session)
    
    # ۲. آنپین کردن پیام‌های قبلی ربات برای جلوگیری از اسپم هدر
    try:
        await bot.unpin_all_chat_messages(chat_id=user_id)
    except Exception as e:
        logger.warning(f"Failed to unpin messages for user {user_id}: {e}")

    if isinstance(event, types.CallbackQuery):
        await event.answer("ربات با موفقیت ریستارت شد.", show_alert=False)
        try:
            # پاک کردن پیام منوی قبلی جهت ارسال فرش و جدید
            await event.message.delete()
        except Exception:
            pass
            
    # ۳. ارسال کیبورد پایین (Reply Keyboard) با یک پیام گذرا
    await bot.send_message(
        chat_id=user_id,
        text="🔄 سیستم در حال راه‌اندازی مجدد...",
        reply_markup=get_main_menu_reply_keyboard()
    )
    
    # ۴. ارسال منوی شیشه‌ای و پین کردن آن به صورت سایلنت
    main_menu_msg = await bot.send_message(
        chat_id=user_id,
        text="🎛 <b>پنل کنترل اصلی</b>\n\nلطفاً یک گزینه را انتخاب کنید:",
        reply_markup=get_main_menu_keyboard()
    )
    
    try:
        await main_menu_msg.pin(disable_notification=True)
    except Exception as e:
        logger.warning(f"Failed to pin main menu for user {user_id}: {e}")


@router.callback_query(F.data == "menu_restart_bot/")
async def restart_callback_handler(callback: types.CallbackQuery, state: FSMContext, session: AsyncSession, bot: Bot):
    await execute_smart_restart(callback, state, session, bot)


@router.message(F.text == "🔄 ریستارت ربات")
@router.message(Command("start", "reset", "restart"))
async def restart_message_handler(message: types.Message, state: FSMContext, session: AsyncSession, bot: Bot):
    await execute_smart_restart(message, state, session, bot)