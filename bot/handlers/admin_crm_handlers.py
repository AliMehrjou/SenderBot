from aiogram import Router, types, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.utils.keyboard import InlineKeyboardBuilder
from workers.session_manager import worker_pool
import logging

logger = logging.getLogger(__name__)
router = Router(name="crm_reply_router")

# تعریف State (وضعیت) برای انتظار دریافت پیام از ادمین
class CRMReplyStates(StatesGroup):
    waiting_for_reply = State()

# ۱. هندلر کلیک ادمین روی دکمه شیشه‌ای «✉️ پاسخ به این کاربر»
@router.callback_query(F.data.startswith("crm_reply_"))
async def crm_reply_button_handler(callback: types.CallbackQuery, state: FSMContext):
    try:
        parts = callback.data.split("_")
        worker_id = int(parts[2])
        target_id = int(parts[3])
    except (IndexError, ValueError):
        return await callback.answer("⚠️ داده‌های این دکمه نامعتبر است.", show_alert=True)
    
    # ذخیره آیدی‌ها در حافظه موقت FSM
    await state.update_data(worker_id=worker_id, target_id=target_id)
    await state.set_state(CRMReplyStates.waiting_for_reply)
    
    builder = InlineKeyboardBuilder()
    builder.button(text="❌ انصراف", callback_data="cancel_crm_reply")
    
    await callback.message.reply(
        "✍️ <b>لطفاً پاسخ خود را بفرستید:</b>\n\n"
        f"این پیام مستقیماً از طریق اکانت ورکری که پیام را دریافت کرده (آیدی ورکر: <code>{worker_id}</code>) برای کاربر ارسال می‌شود.",
        reply_markup=builder.as_markup()
    )
    await callback.answer()

# ۲. هندلر دکمه انصراف
@router.callback_query(F.data == "cancel_crm_reply")
async def cancel_crm_reply(callback: types.CallbackQuery, state: FSMContext):
    current_state = await state.get_state()
    if current_state == CRMReplyStates.waiting_for_reply.state:
        await state.clear()
        await callback.message.edit_text("🚫 عملیات پاسخ‌دهی لغو شد.")
    await callback.answer()

# ۳. هندلر دریافت متن پاسخ از ادمین و ارسال آن با Pyrogram
@router.message(CRMReplyStates.waiting_for_reply, F.text)
async def process_crm_reply_message(message: types.Message, state: FSMContext):
    data = await state.get_data()
    worker_id = data.get("worker_id")
    target_id = data.get("target_id")
    
    # خواندن ورکر مربوطه از استخر فعال
    worker_client = worker_pool.get(worker_id)
    
    if not worker_client or not worker_client.is_connected:
        await message.reply("❌ <b>ارسال ناموفق:</b> اکانت ورکر در حال حاضر متصل یا فعال نیست.")
        await state.clear()
        return
        
    wait_msg = await message.reply("⏳ در حال ارسال پیام توسط ورکر...")
    
    try:
        # ارسال پیام به تارگت از طریق اکانت ورکر
        await worker_client.send_message(
            chat_id=target_id,
            text=message.html_text
        )
        await wait_msg.edit_text("✅ <b>پاسخ شما با موفقیت ارسال شد!</b>")
    except Exception as e:
        logger.error(f"Failed to send CRM reply to {target_id} via worker {worker_id}: {e}")
        await wait_msg.edit_text(f"❌ <b>خطا در ارسال پیام:</b>\n<code>{e}</code>")
        
    # خروج از حالت انتظار
    await state.clear()