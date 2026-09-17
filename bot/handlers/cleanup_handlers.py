import asyncio
import logging
import random
import time

from aiogram import Router, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.utils.keyboard import InlineKeyboardBuilder
from pyrogram.enums import ChatType
from sqlalchemy import update

from database.engine import async_session
from database.models import OrderJoin
from pyrogram.errors import FloodWait
from utils.limit_handler import register_account_limit
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
@router.callback_query(F.data == "cleanup_leave_groups")
async def cmd_leave_groups(event: types.Message | types.CallbackQuery, state: FSMContext):
    is_callback = isinstance(event, types.CallbackQuery)
    if is_callback:
        await event.answer()
        message = event.message
    else:
        message = event

    builder = InlineKeyboardBuilder()
    builder.button(text="👥 فقط گروه‌ها/سوپرگروه‌ها", callback_data="leave_domain_groups")
    builder.button(text="🌐 همه (شامل کانال‌ها)", callback_data="leave_domain_all")
    builder.button(text="❌ لغو", callback_data="cancel_cleanup")
    builder.adjust(1)
    
    await safe_edit_or_answer(message, "📌 <b>مرحله اول:</b> محدوده خروج اکانت‌ها را مشخص کنید.", reply_markup=builder.as_markup())
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

# REWRITTEN
@router.callback_query(CleanupStates.waiting_for_leave_confirm, F.data == "confirm_leave_start")
async def execute_leave_groups(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    domain = data.get("leave_domain", "groups")
    await state.clear()
    
    await safe_edit_or_answer(callback.message, "⏳ در حال اجرای عملیات خروج، لطفاً صبور باشید...")
    
    last_edit_time = time.time()
    report = []
    pool_items = list(worker_pool.items())
    total_accounts = len(pool_items)
    
    for i, (acc_id, client) in enumerate(pool_items, 1):
        now = time.time()
        if now - last_edit_time >= 3:
            await safe_edit_or_answer(callback.message, f"⏳ در حال اجرای عملیات خروج...\nپردازش اکانت {i} از {total_accounts}...")
            last_edit_time = now
            
        if not client.is_connected:
            report.append(f"• اکانت <code>{acc_id}</code>: آفلاین ❌")
            continue
            
        success, errors = 0, 0
        error_reasons = {}
        outer_error = None
        try:
            async for dialog in client.get_dialogs():
                is_group = dialog.chat.type in [ChatType.GROUP, ChatType.SUPERGROUP]
                is_channel = dialog.chat.type == ChatType.CHANNEL
                
                if is_group or (domain == "all" and is_channel):
                    try:
                        await client.leave_chat(dialog.chat.id)
                        success += 1
                        
                        async with async_session() as db:
                            stmt = update(OrderJoin).where(
                                OrderJoin.account_id == acc_id,
                                OrderJoin.chat_id == dialog.chat.id
                            ).values(leave_done=True)
                            await db.execute(stmt)
                            await db.commit()
                            
                        await asyncio.sleep(random.uniform(2.0, 5.0))
                    except Exception as e:
                        errors += 1
                        reason = type(e).__name__
                        error_reasons[reason] = error_reasons.get(reason, 0) + 1
                        logger.debug(f"Account {acc_id} failed to leave {dialog.chat.id}: {e}")
        except Exception as e:
            logger.error(f"Error fetching dialogs for {acc_id}: {e}")
            outer_error = str(e)
            errors += 1
            
        report_line = f"• اکانت <code>{acc_id}</code>: {success} موفق ✅ | {errors} خطا ❌"
        if outer_error:
            report_line += f"\n  └ <i>خطای کلی: {outer_error}</i>"
        elif error_reasons:
            top_errors = sorted(error_reasons.items(), key=lambda x: x[1], reverse=True)[:3]
            reasons_str = ", ".join(f"{k}: {v}" for k, v in top_errors)
            report_line += f"\n  └ <i>دلایل: {reasons_str}</i>"
            
        report.append(report_line)
        
    final_text = "📊 <b>گزارش نهایی خروج از چت‌ها:</b>\n\n" + "\n".join(report)
    await safe_edit_or_answer(callback.message, final_text)


# ==========================================
# فیچر ۲: پاکسازی تاریخچه چت‌ها (DeleteChats)
# ==========================================
# REWRITTEN
@router.message(Command("DeleteChats"))
@router.callback_query(F.data == "cleanup_delete_chats")
async def cmd_delete_chats(event: types.Message | types.CallbackQuery, state: FSMContext):
    is_callback = isinstance(event, types.CallbackQuery)
    if is_callback:
        await event.answer()
        message = event.message
    else:
        message = event

    builder = InlineKeyboardBuilder()
    builder.button(text="👤 فقط چت‌های شخصی (DM)", callback_data="delete_domain_dm")
    builder.button(text="🌐 همه چت‌ها", callback_data="delete_domain_all")
    builder.button(text="❌ لغو", callback_data="cancel_cleanup")
    builder.adjust(1)
    
    await safe_edit_or_answer(message, "📌 <b>مرحله اول:</b> محدوده حذف چت‌ها را مشخص کنید.", reply_markup=builder.as_markup())
    await state.set_state(CleanupStates.waiting_for_delete_domain)

@router.callback_query(CleanupStates.waiting_for_delete_domain, F.data.startswith("delete_domain_"))
async def confirm_delete_chats(callback: types.CallbackQuery, state: FSMContext):
    domain = "dm" if callback.data == "delete_domain_dm" else "all"
    await state.update_data(delete_domain=domain)
    
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ تأیید و شروع حذف", callback_data="confirm_delete_start")
    builder.button(text="❌ لغو", callback_data="cancel_cleanup")
    
    text = "حذف تاریخچه چت‌های شخصی (DM) تمام اکانت‌ها؟\n(دقت کنید: پیام‌های PV دوطرفه برای مخاطب هم پاک می‌شوند)" if domain == "dm" else "حذف تاریخچه **تمام چت‌های** اکانت‌ها (DM، گروه‌ها و...)؟\n(دقت کنید: پیام‌های PV دوطرفه برای مخاطب هم پاک می‌شوند)"
    
    await safe_edit_or_answer(callback.message, f"⚠️ <b>تأیید دومرحله‌ای:</b>\n\n{text}", reply_markup=builder.as_markup())
    await state.set_state(CleanupStates.waiting_for_delete_confirm)

# REWRITTEN
@router.callback_query(CleanupStates.waiting_for_delete_confirm, F.data == "confirm_delete_start")
async def execute_delete_chats(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    domain = data.get("delete_domain", "dm")
    await state.clear()
    
    await safe_edit_or_answer(callback.message, "⏳ در حال اجرای عملیات حذف تاریخچه چت‌ها...")
    
    last_edit_time = time.time()
    report = []
    pool_items = list(worker_pool.items())
    total_accounts = len(pool_items)
    
    total_flood_temp = 0  # شمارنده کل برای گزارش نهایی
    
    for i, (acc_id, client) in enumerate(pool_items, 1):
        now = time.time()
        if now - last_edit_time >= 3:
            await safe_edit_or_answer(callback.message, f"⏳ در حال اجرای عملیات حذف تاریخچه...\nپردازش اکانت {i} از {total_accounts}...")
            last_edit_time = now
            
        if not client.is_connected:
            report.append(f"• اکانت <code>{acc_id}</code>: آفلاین ❌")
            continue
            
        success, flood_temp, permanent = 0, 0, 0
        error_reasons = {}
        outer_error = None
        
        try:
            async for dialog in client.get_dialogs():
                if domain == "dm" and dialog.chat.type != ChatType.PRIVATE:
                    continue
                    
                try:
                    # 🟢 استفاده از متدهای Raw API تلگرام برای حذف اصولی و دوطرفه
                    from pyrogram.raw import functions
                    
                    peer = await client.resolve_peer(dialog.chat.id)
                    
                    if dialog.chat.type in [ChatType.SUPERGROUP, ChatType.CHANNEL]:
                        await client.invoke(
                            functions.channels.DeleteHistory(
                                channel=peer,
                                max_id=0,
                                for_everyone=True
                            )
                        )
                    else:
                        await client.invoke(
                            functions.messages.DeleteHistory(
                                peer=peer,
                                max_id=0,
                                revoke=True
                            )
                        )
                        
                    success += 1
                    
                    await asyncio.sleep(random.uniform(1.5, 4.0))
                    
                except FloodWait as e:
                    wait_time = e.value
                    flood_temp += 1
                    
                    # ثبت محدودیت در دیتابیس با سشن مستقل
                    async with async_session() as session:
                        await register_account_limit(session, acc_id, client, "flood_wait", wait_time)
                        
                    if wait_time > 900:
                        logger.warning(f"Account {acc_id} hit large FloodWait ({wait_time}s). Skipping.")
                        outer_error = f"FloodWait > 15m ({wait_time}s)"
                        break  # پرش کامل از این اکانت
                    else:
                        jitter = random.uniform(1.0, 5.0)
                        logger.info(f"Account {acc_id} sleeping for {wait_time + jitter}s due to FloodWait.")
                        await asyncio.sleep(wait_time + jitter)
                        # ادامه پردازش دیالوگ بعدی در همین اکانت
                        
                except Exception as e:
                    permanent += 1
                    reason = type(e).__name__
                    error_reasons[reason] = error_reasons.get(reason, 0) + 1
                    logger.debug(f"Account {acc_id} failed to delete history for {dialog.chat.id}: {e}")
                    
        except Exception as e:
            logger.error(f"Error fetching dialogs for {acc_id}: {e}")
            outer_error = str(e)
            permanent += 1
            
        total_flood_temp += flood_temp
        
        report_line = f"• اکانت <code>{acc_id}</code>: {success} موفق ✅ | {flood_temp} موقت ⏳ | {permanent} دائم ❌"
        if outer_error:
            report_line += f"\n  └ <i>توقف/خطای کلی: {outer_error}</i>"
        elif error_reasons:
            top_errors = sorted(error_reasons.items(), key=lambda x: x[1], reverse=True)[:3]
            reasons_str = ", ".join(f"{k}: {v}" for k, v in top_errors)
            report_line += f"\n  └ <i>دلایل دائم: {reasons_str}</i>"
            
        report.append(report_line)
        
        # مکث کوتاه ۵ تا ۱۰ ثانیه‌ای بین اکانت‌ها برای جلوگیری از ترافیک شدید
        if i < total_accounts:
            await asyncio.sleep(random.uniform(5.0, 10.0))
            
    final_text = "📊 <b>گزارش نهایی حذف چت‌ها:</b>\n\n" + "\n".join(report)
    
    if total_flood_temp > 0:
        final_text += f"\n\n⚠️ {total_flood_temp} چت به‌خاطر محدودیت موقت باقی مانده؛ فرایند قابل تکرار است."
        
    await safe_edit_or_answer(callback.message, final_text)


# ==========================================
# هندلر عمومی لغو عملیات (Cancel)
# ==========================================
@router.callback_query(F.data == "cancel_cleanup")
async def cancel_cleanup_process(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await safe_edit_or_answer(callback.message, "❌ عملیات توسط ادمین لغو شد.")