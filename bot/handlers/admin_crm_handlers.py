import io
import time
import logging
import os
from aiogram import Router, types, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.utils.keyboard import InlineKeyboardBuilder
from workers.session_manager import worker_pool

logger = logging.getLogger(__name__)
router = Router(name="crm_reply_router")

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
    
    await state.update_data(
        worker_id=worker_id, 
        target_id=target_id,
        expires_at=time.time() + 600
    )
    await state.set_state(CRMReplyStates.waiting_for_reply)
    
    builder = InlineKeyboardBuilder()
    builder.button(text="❌ انصراف", callback_data="cancel_crm_reply")
    
    text = (
        f"✍️ ارسال پاسخ به تارگت: <code>{target_id}</code>\n"
        f"🤖 از طریق اکانت ورکر: <code>{worker_id}</code>\n\n"
        "لطفاً پاسخ خود را ارسال کنید:\n"
        "- متن (فورمت ضخیم/ایتالیک/کد حفظ می‌شود)\n"
        "- عکس، ویدیو، فایل، ویس، گیف و <b>استیکر ثابت</b>\n"
        "<i>(⚠️ ارسال استیکرهای متحرک پشتیبانی نمی‌شود)</i>\n\n"
        "⌛️ نشست پس از ۱۰ دقیقه خودکار منقضی می‌شود.\n\n"
        "برای انصراف، /cancel را ارسال کنید یا روی دکمه انصراف کلیک کنید."
    )
    
    await callback.message.reply(text, reply_markup=builder.as_markup())
    await callback.answer()


# ۲. هندلر دکمه انصراف
@router.callback_query(F.data == "cancel_crm_reply")
async def cancel_crm_reply(callback: types.CallbackQuery, state: FSMContext):
    current_state = await state.get_state()
    if current_state == CRMReplyStates.waiting_for_reply.state:
        await state.clear()
        await callback.message.edit_text("🚫 عملیات پاسخ‌دهی لغو شد.")
    await callback.answer()


# ۳. هندلر دریافت متن و مدیا از ادمین و ارسال آن با Pyrogram
@router.message(CRMReplyStates.waiting_for_reply)
async def process_crm_reply_message(message: types.Message, state: FSMContext):
    # الف) بررسی انصراف از طریق دستور متنی
    if message.text and message.text.strip().lower() == "/cancel":
        await state.clear()
        await message.reply("🚫 عملیات پاسخ‌دهی لغو شد.")
        return

    data = await state.get_data()
    
    # ب) بررسی محدودیت زمانی ۱۰ دقیقه‌ای
    expires_at = data.get("expires_at", 0)
    if time.time() > expires_at:
        await state.clear()
        return await message.reply("⏳ <b>خطا:</b> زمان ۱۰ دقیقه‌ای شما برای پاسخ به این نشست به پایان رسیده است.")

    worker_id = data.get("worker_id")
    target_id = data.get("target_id")
    
    worker_client = worker_pool.get(worker_id)
    
    if not worker_client or not worker_client.is_connected:
        await message.reply("❌ <b>ارسال ناموفق:</b> اکانت ورکر در حال حاضر متصل یا فعال نیست.")
        await state.clear()
        return
        
    wait_msg = await message.reply("⏳ در حال پردازش و ارسال پیام توسط ورکر...")
    
    try:
        # ارسال متن ساده
        if message.text:
            await worker_client.send_message(
                chat_id=target_id,
                text=message.html_text
            )
        
        # پردازش و ارسال انواع مدیا 
        elif message.photo or message.video or message.document or message.sticker or message.voice or message.animation:
            
            # === مسدودسازی سریع استیکرهای متحرک قبل از دانلود فایل ===
            if message.sticker and (message.sticker.is_animated or message.sticker.is_video):
                await wait_msg.edit_text("❌ <b>محدودیت تلگرام:</b> اکانت‌های ورکر امکان آپلود استیکر متحرک یا ویدیویی را ندارند. لطفاً استیکر ثابت (.webp)، عکس یا گیف ارسال کنید.")
                # نشست را نمی‌بندیم تا ادمین بتواند فایل جایگزین بفرستد
                return
            
            file_id = None
            ext = ""
            
            # پیدا کردن شناسه فایل و پسوند دقیق برای تشخیص درست Pyrogram
            if message.photo:
                file_id = message.photo[-1].file_id
                ext = ".jpg"
            elif message.video:
                file_id = message.video.file_id
                ext = ".mp4"
            elif message.document:
                file_id = message.document.file_id
                ext = f"_{message.document.file_name}" if message.document.file_name else ".dat"
            elif message.sticker:
                file_id = message.sticker.file_id
                ext = ".webp" # استیکر متحرک را بالا مسدود کردیم، پس قطعا webp است
            elif message.voice:
                file_id = message.voice.file_id
                ext = ".ogg"
            elif message.animation:
                file_id = message.animation.file_id
                ext = ".mp4"
            
            # دانلود مدیا در یک فایل موقت روی هارد با پسوند صحیح
            file_path = f"temp_media_{worker_id}_{target_id}_{file_id[:10]}{ext}"
            await message.bot.download(file_id, destination=file_path)
            
            caption = message.html_text if message.caption else ""
            
            # ارسال فایل از طریق Pyrogram ورکر
            try:
                if message.sticker:
                    await worker_client.send_sticker(chat_id=target_id, sticker=file_path)
                elif message.photo:
                    await worker_client.send_photo(chat_id=target_id, photo=file_path, caption=caption)
                elif message.video:
                    await worker_client.send_video(chat_id=target_id, video=file_path, caption=caption)
                elif message.animation:
                    await worker_client.send_animation(chat_id=target_id, animation=file_path, caption=caption)
                elif message.voice:
                    await worker_client.send_voice(chat_id=target_id, voice=file_path, caption=caption)
                elif message.document:
                    await worker_client.send_document(chat_id=target_id, document=file_path, caption=caption)
            finally:
                # حذف فایل موقت بلافاصله بعد از ارسال
                if os.path.exists(file_path):
                    os.remove(file_path)
        else:
            await wait_msg.edit_text("❌ فرمت این پیام پشتیبانی نمی‌شود (فقط متن، عکس، ویدیو، استیکر ثابت، گیف، ویس و فایل مجاز است).")
            # نشست را نمی‌بندیم تا ادمین بتواند فایل جایگزین بفرستد
            return

        await wait_msg.edit_text("✅ <b>پاسخ شما با موفقیت ارسال شد!</b>")
        await state.clear()
        
    except Exception as e:
        logger.error(f"Failed to send CRM reply to {target_id} via worker {worker_id}: {e}")
        await wait_msg.edit_text(f"❌ <b>خطا در ارسال پیام:</b>\n<code>{e}</code>")
        await state.clear()