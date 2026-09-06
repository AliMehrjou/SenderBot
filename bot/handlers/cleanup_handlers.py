import asyncio
import logging
import random

from aiogram import Router, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.utils.keyboard import InlineKeyboardBuilder
from pyrogram.enums import ChatType
from sqlalchemy import update

from database.engine import async_session
from database.models import OrderJoin
from workers.session_manager import worker_pool
from utils.safe_edit import safe_edit_or_answer

logger = logging.getLogger(__name__)
router = Router(name="cleanup_handlers_router")

class CleanupStates(StatesGroup):
    waiting_for_leave_domain = State()
    waiting_for_leave_confirm = State()
    
    waiting_for_delete_domain = State()
    waiting_for_delete_confirm = State()

# ==========================================
# فیچر ۱: خروج دسته‌جمعی از گروه‌ها / کانال‌ها
# ==========================================
@router.message(Command("LeaveGroups"))
async def cmd_leave_groups(message: types.Message, state: FSMContext):
    builder = InlineKeyboardBuilder()
    builder.button(text="👥 فقط گروه‌ها/سوپرگروه‌ها", callback_data="leave_domain_groups")
    builder.button(text="🌐 همه (شامل کانال‌ها)", callback_data="leave_domain_all")
    builder.button(text="❌ لغو", callback_data="cancel_cleanup")
    builder.adjust(1)
    
    await message.answer("📌 <b>مرحله اول:</b> محدوده خروج اکانت‌ها را مشخص کنید.", reply_markup=builder.as_markup())
    await state.set_state(CleanupStates.waiting_for_leave_domain)

@router.callback_query(CleanupStates.waiting_for_leave_domain, F.data.startswith("leave_domain_"))
async def confirm_leave_groups(callback: types.CallbackQuery, state: FSMContext):
    domain = "all" if callback.data == "leave_domain_all" else "groups"
    await state.update_data(leave_domain=domain)
    
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ تأیید و شروع خروج", callback_data="confirm_leave_start")
    builder.button(text="❌ لغو", callback_data="cancel_cleanup")
    
    text = "همه اکانت‌ها از همه گروه‌ها خارج شوند؟" if domain == "groups" else "همه اکانت‌ها از همه گروه‌ها و کانال‌ها خارج شوند؟"
    
    await safe_edit_or_answer(callback.message, f"⚠️ <b>تأیید دومرحله‌ای:</b>\n\n{text}", reply_markup=builder.as_markup())
    await state.set_state(CleanupStates.waiting_for_leave_confirm)

@router.callback_query(CleanupStates.waiting_for_leave_confirm, F.data == "confirm_leave_start")
async def execute_leave_groups(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    domain = data.get("leave_domain", "groups")
    await state.clear()
    
    status_msg = await safe_edit_or_answer(callback.message, "⏳ در حال اجرای عملیات خروج، لطفاً صبور باشید...")
    
    report = []
    for acc_id, client in list(worker_pool.items()):
        if not client.is_connected:
            report.append(f"• اکانت <code>{acc_id}</code>: آفلاین ❌")
            continue
            
        success, errors = 0, 0
        try:
            async for dialog in client.get_dialogs():
                is_group = dialog.chat.type in [ChatType.GROUP, ChatType.SUPERGROUP]
                is_channel = dialog.chat.type == ChatType.CHANNEL
                
                if is_group or (domain == "all" and is_channel):
                    try:
                        await client.leave_chat(dialog.chat.id)
                        success += 1
                        
                        # همگام‌سازی با جدول OrderJoin جهت جلوگیری از اختلال در Sweep
                        async with async_session() as db:
                            stmt = update(OrderJoin).where(
                                OrderJoin.account_id == acc_id,
                                OrderJoin.chat_id == dialog.chat.id
                            ).values(leave_done=True)
                            await db.execute(stmt)
                            await db.commit()
                            
                        # تأخیر انسانی بین خروج‌ها (بر اساس anti-ban)
                        await asyncio.sleep(random.uniform(2.0, 5.0))
                    except Exception as e:
                        errors += 1
                        logger.debug(f"Account {acc_id} failed to leave {dialog.chat.id}: {e}")
        except Exception as e:
            logger.error(f"Error fetching dialogs for {acc_id}: {e}")
            errors += 1
            
        report.append(f"• اکانت <code>{acc_id}</code>: {success} موفق ✅ | {errors} خطا ❌")
        
    final_text = "📊 <b>گزارش نهایی خروج از چت‌ها:</b>\n\n" + "\n".join(report)
    await status_msg.edit_text(final_text)

# ==========================================
# فیچر ۲: پاکسازی تاریخچه چت‌ها (DeleteChats)
# ==========================================
@router.message(Command("DeleteChats"))
async def cmd_delete_chats(message: types.Message, state: FSMContext):
    builder = InlineKeyboardBuilder()
    builder.button(text="👤 فقط چت‌های شخصی (DM)", callback_data="delete_domain_dm")
    builder.button(text="🌐 همه چت‌ها", callback_data="delete_domain_all")
    builder.button(text="❌ لغو", callback_data="cancel_cleanup")
    builder.adjust(1)
    
    await message.answer("📌 <b>مرحله اول:</b> محدوده حذف چت‌ها را مشخص کنید.", reply_markup=builder.as_markup())
    await state.set_state(CleanupStates.waiting_for_delete_domain)

@router.callback_query(CleanupStates.waiting_for_delete_domain, F.data.startswith("delete_domain_"))
async def confirm_delete_chats(callback: types.CallbackQuery, state: FSMContext):
    domain = "dm" if callback.data == "delete_domain_dm" else "all"
    await state.update_data(delete_domain=domain)
    
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ تأیید و شروع حذف", callback_data="confirm_delete_start")
    builder.button(text="❌ لغو", callback_data="cancel_cleanup")
    
    text = "حذف تاریخچه چت‌های شخصی (DM) تمام اکانت‌ها؟" if domain == "dm" else "حذف تاریخچه **تمام چت‌های** اکانت‌ها (DM، گروه‌ها و...)؟"
    
    await safe_edit_or_answer(callback.message, f"⚠️ <b>تأیید دومرحله‌ای:</b>\n\n{text}", reply_markup=builder.as_markup())
    await state.set_state(CleanupStates.waiting_for_delete_confirm)

@router.callback_query(CleanupStates.waiting_for_delete_confirm, F.data == "confirm_delete_start")
async def execute_delete_chats(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    domain = data.get("delete_domain", "dm")
    await state.clear()
    
    status_msg = await safe_edit_or_answer(callback.message, "⏳ در حال اجرای عملیات حذف تاریخچه چت‌ها...")
    
    report = []
    for acc_id, client in list(worker_pool.items()):
        if not client.is_connected:
            report.append(f"• اکانت <code>{acc_id}</code>: آفلاین ❌")
            continue
            
        success, errors = 0, 0
        try:
            async for dialog in client.get_dialogs():
                if domain == "dm" and dialog.chat.type != ChatType.PRIVATE:
                    continue
                    
                try:
                    # استفاده از متد Pyrofork برای پاکسازی کامل تاریخچه
                    await client.delete_history(dialog.chat.id)
                    success += 1
                    
                    # تأخیر Rate Limit انسانی
                    await asyncio.sleep(random.uniform(1.5, 4.0))
                except Exception as e:
                    errors += 1
                    logger.debug(f"Account {acc_id} failed to delete history for {dialog.chat.id}: {e}")
        except Exception as e:
            logger.error(f"Error fetching dialogs for {acc_id}: {e}")
            errors += 1
            
        report.append(f"• اکانت <code>{acc_id}</code>: {success} موفق ✅ | {errors} خطا ❌")
        
    final_text = "📊 <b>گزارش نهایی حذف چت‌ها:</b>\n\n" + "\n".join(report)
    await status_msg.edit_text(final_text)

# ==========================================
# هندلر عمومی لغو عملیات (Cancel)
# ==========================================
@router.callback_query(F.data == "cancel_cleanup")
async def cancel_cleanup_process(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await safe_edit_or_answer(callback.message, "❌ عملیات توسط ادمین لغو شد.")