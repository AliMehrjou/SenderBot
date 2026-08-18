import asyncio
import os
import random
import logging
from datetime import datetime

from aiogram import Router, types, F, Bot
from aiogram.fsm.context import FSMContext
from aiogram.types import FSInputFile
from aiogram.utils.keyboard import InlineKeyboardBuilder

import aiofiles
import uuid

from bot.states.extractor_fsm import ExtractorStates
from workers.session_manager import worker_pool
from utils.smart_extractor import join_and_wait_for_approval
from utils.golden_extractor import extract_golden_list
from config import config

logger = logging.getLogger(__name__)

router = Router(name="extractor_handlers_router")

# اطمینان از وجود پوشه خروجی
os.makedirs("exports", exist_ok=True)

# ==========================================
# BACKGROUND TASK: اجرای سناریوی استخراج در پس‌زمینه
# ==========================================
async def run_extraction_task(bot: Bot, link: str, admin_tg_id: int):
    """
    این تسک به صورت Asynchronous در بک‌گراند اجرا می‌شود تا در صورت
    نیاز به انتظار ۲۴ ساعته برای تایید ریکوست، ربات اصلی قفل نشود.
    """
    # ۱. انتخاب رندوم یک اکانت فعال از استخر
    active_workers = [client for client in worker_pool.values() if client.is_connected]
    
    if not active_workers:
        return await bot.send_message(
            chat_id=admin_tg_id, 
            text="❌ <b>خطا:</b> هیچ اکانت فعالی در سیستم برای استخراج یافت نشد."
        )
        
    client = random.choice(active_workers)
    logger.info(f"Randomly selected Worker {client.name} for extraction on {link}")

    # ۲. تلاش برای ورود و انتظار برای تایید (ماژول بخش دوم)
    chat_id = await join_and_wait_for_approval(client, link, admin_tg_id)
    
    if not chat_id:
        # در صورت عدم تایید یا خطا، تابع متوقف می‌شود (پیام‌ها در خود ماژول ارسال شده‌اند)
        return

    # ۳. استخراج گلدن لیست (ماژول بخش سوم)
    try:
        await bot.send_message(chat_id=admin_tg_id, text="🔍 ورود موفقیت‌آمیز بود. در حال اسکن و فیلترینگ پیشرفته اعضا...")
        
        golden_list = await extract_golden_list(client, chat_id)
        
        if not golden_list:
            return await bot.send_message(
                chat_id=admin_tg_id,
                text="⚠️ استخراج تمام شد اما هیچ عضو فعال و معتبری (دارای یوزرنیم و غیر ربات/ادمین) یافت نشد."
            )
            
        # ۴. ساخت فایل خروجی txt با نام یکتا برای جلوگیری از تداخل
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        unique_id = uuid.uuid4().hex[:6]
        file_path = f"exports/extracted_{chat_id}_{timestamp}_{unique_id}.txt"
        
        # استفاده از aiofiles برای جلوگیری از فریز شدن (مسدود شدن Event Loop) ربات
        async with aiofiles.open(file_path, 'w', encoding='utf-8') as f:
            for username in golden_list:
                await f.write(f"{username}\n")
                
        # ۵. ارسال پیام دقیقاً طبق متن درخواستی
        await bot.send_message(
            chat_id=admin_tg_id,
            text=f"لیست آیدی استخراج شده از لینک:\n{link} 👇👇👇👇"
        )
        
        document = FSInputFile(file_path)
        await bot.send_document(
            chat_id=admin_tg_id,
            document=document,
            caption=f"✅ تعداد <b>{len(golden_list)}</b> تارگت طلایی (فعال و واقعی) استخراج شد."
        )
        
    except Exception as e:
        logger.error(f"Error in extraction task: {e}")
        await bot.send_message(chat_id=admin_tg_id, text="❌ خطای سیستمی در حین تولید فایل رخ داد.")
    
    finally:
        # ۶. پاکسازی ایمن فایل از هارد سرور (Garbage Collection)
        if 'file_path' in locals() and os.path.exists(file_path):
            try:
                os.remove(file_path)
            except Exception as e:
                logger.warning(f"Could not delete {file_path}: {e}")


# ==========================================
# UI HANDLERS: تعامل با ادمین در ربات
# ==========================================
@router.callback_query(F.data == "menu_extract_users/")
async def ask_for_extraction_link(callback: types.CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    await state.set_state(ExtractorStates.waiting_for_link)
    
    builder = InlineKeyboardBuilder()
    builder.button(text="❌ انصراف", callback_data="menu_home/")
    
    await callback.message.edit_text(
        "🟢 <b>استخراج کاربران فعال (Golden List)</b>\n\n"
        "لطفاً لینک گروه یا کانال مورد نظر خود را ارسال کنید:\n"
        "<i>(پشتیبانی از لینک‌های عمومی و خصوصی Request to Join)</i>",
        reply_markup=builder.as_markup()
    )

@router.message(ExtractorStates.waiting_for_link, F.text)
async def process_extraction_link(message: types.Message, state: FSMContext, bot: Bot) -> None:
    link = message.text.strip()
    
    if "t.me/" not in link and not link.startswith("@"):
        return await message.answer("⚠️ لطفاً یک لینک معتبر تلگرامی ارسال کنید.")
        
    await state.clear()
    
    # ارسال پیام اولیه به ادمین
    await message.answer(
        "⏳ <b>عملیات آغاز شد...</b>\n\n"
        "سیستم در حال اختصاص یک اکانت تصادفی و بررسی لینک است. "
        "در صورت خصوصی بودن لینک، ربات وارد حالت انتظار می‌شود. به محض اتمام، نتیجه برای شما ارسال خواهد شد."
    )
    
    admin_tg_id = message.from_user.id
    
    # اجرای تسک در پس‌زمینه بدون مسدود کردن ربات
    asyncio.create_task(run_extraction_task(bot, link, admin_tg_id))