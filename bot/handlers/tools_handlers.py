import os
import re
import datetime
import logging
from aiogram import Router, types, F
from aiogram.fsm.context import FSMContext
from aiogram.types import FSInputFile
from aiogram.utils.keyboard import InlineKeyboardBuilder
import aiofiles
from bot.states.tools_fsm import ToolsStates

logger = logging.getLogger(__name__)

router = Router(name="tools_handlers_router")

# اطمینان از وجود پوشه برای خروجی فایل‌ها
os.makedirs("exports", exist_ok=True)

# دکمه تست در منوی اصلی (می‌توانید بعداً به main_menu.py منتقل کنید)
@router.callback_query(F.data == "menu_txt_generator/")
async def ask_for_raw_ids(callback: types.CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    await state.set_state(ToolsStates.waiting_for_raw_ids)
    
    builder = InlineKeyboardBuilder()
    builder.button(text="❌ انصراف", callback_data="menu_home/")
    
    await callback.message.edit_text(
        "🛠 <b>ابزار ساخت فایل TXT آیدی‌ها</b>\n\n"
        "لطفاً لیست آیدی‌های خود را به صورت متنی ارسال کنید.\n"
        "<i>ربات فرمت‌های مختلف (مثل @user1@user2 یا با فاصله و اینتر) را به صورت خودکار تشخیص داده و جدا می‌کند.</i>",
        reply_markup=builder.as_markup()
    )

@router.message(ToolsStates.waiting_for_raw_ids, F.text)
async def process_raw_ids(message: types.Message, state: FSMContext) -> None:
    raw_text = message.text
    
    # استخراج تمام کلماتی که با @ شروع می‌شوند با استفاده از Regex
    extracted_usernames = re.findall(r'@([a-zA-Z0-9_]+)', raw_text)
    
    # فیلتر کردن آیدی‌های تکراری و مرتب‌سازی
    unique_usernames = sorted(list(set(extracted_usernames)))
    
    if not unique_usernames:
        return await message.answer(
            "⚠️ هیچ آیدی معتبری (با پیشوند @) در متن شما پیدا نشد. لطفاً دوباره ارسال کنید."
        )

    # نام‌گذاری فایل با فرمت زمانی
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = f"exports/generated_list_{timestamp}.txt"
    
    try:
        # استفاده از aiofiles برای جلوگیری از مسدود شدن Event Loop
        async with aiofiles.open(file_path, 'w', encoding='utf-8') as f:
            for username in unique_usernames:
                await f.write(f"@{username}\n")
                
        # ارسال فایل به کاربر
        document = FSInputFile(file_path)
        await message.answer_document(
            document=document,
            caption=(
                f"✅ <b>فایل شما با موفقیت ساخته شد!</b>\n\n"
                f"👥 تعداد آیدی‌های استخراج شده: <b>{len(unique_usernames)}</b>\n"
                f"🧹 <i>آیدی‌های تکراری حذف و لیست کاملاً پاکسازی شده است.</i>"
            )
        )
    except Exception as e:
        logger.error(f"Failed to generate txt file: {e}")
        await message.answer("❌ خطایی در ساخت فایل رخ داد.")
    finally:
        # پاک کردن فایل از روی سرور برای جلوگیری از پر شدن هارد
        if os.path.exists(file_path):
            os.remove(file_path)
            
    await state.clear()