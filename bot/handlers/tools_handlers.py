import os
import re
import datetime
import logging
import html

from aiogram import Router, types, F
from aiogram.fsm.context import FSMContext
from aiogram.types import FSInputFile
from aiogram.utils.keyboard import InlineKeyboardBuilder
import aiofiles
from pyrogram.errors import PeerIdInvalid

from bot.states.tools_fsm import ToolsStates

# 🟣 فاز ۵ (رفع بن‌بست FSM): افزودن import های زیر
from bot.keyboards.cancel import get_cancel_keyboard, with_cancel_hint
from utils.fsm_cleanup import cleanup_fsm_temp_files
from utils.safe_edit import safe_edit_or_answer

# وارد کردن استخر ورکرها جهت دسترسی به کلاینت‌های لاگین شده 
from workers.session_manager import worker_pool

logger = logging.getLogger(__name__)
router = Router(name="tools_handlers_router")

os.makedirs("exports", exist_ok=True)


# ==========================================
# 🟣 فاز ۵: کیبورد پایان کار ابزار TXT
# ==========================================
def get_tools_finish_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="🛠 ساخت فایل دیگر", callback_data="menu_txt_generator/")
    builder.button(text="🏛 منوی اصلی", callback_data="menu_home/")
    builder.adjust(2)
    return builder.as_markup()


@router.callback_query(F.data == "menu_txt_generator/")
async def ask_for_raw_ids(callback: types.CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    await cleanup_fsm_temp_files(state)
    await state.clear()
    
    await state.set_state(ToolsStates.waiting_for_raw_ids)
    
    await safe_edit_or_answer(
        callback.message,
        with_cancel_hint(
            "🛠 <b>ابزار ساخت فایل TXT آیدی‌ها</b>\n\n"
            "لطفاً لیست آیدی‌های خود را ارسال کنید.\n"
            "<i>پشتیبانی از فرمت‌های: @username و t.me/username و telegram.me/username</i>"
        ),
        reply_markup=get_cancel_keyboard()
    )

@router.message(ToolsStates.waiting_for_raw_ids, F.text)
async def process_raw_ids(message: types.Message, state: FSMContext) -> None:
    raw_text = message.text
    
    extracted_usernames = re.findall(r'(?:@|t\.me/|telegram\.me/)([a-zA-Z0-9_]+)', raw_text)
    unique_usernames = sorted(list(set(extracted_usernames)))
    
    if not unique_usernames:
        return await message.answer(
            with_cancel_hint(
                "⚠️ هیچ آیدی معتبری در متن شما پیدا نشد.\n"
                "لطفاً دوباره ارسال کنید یا از دکمه‌های زیر استفاده کنید."
            ),
            reply_markup=get_cancel_keyboard()
        )

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = f"exports/generated_list_{timestamp}.txt"
    
    try:
        async with aiofiles.open(file_path, 'w', encoding='utf-8') as f:
            for username in unique_usernames:
                await f.write(f"@{username}\n")
                
        document = FSInputFile(file_path)
        await message.answer_document(
            document=document,
            caption=(
                f"✅ <b>فایل شما با موفقیت ساخته شد!</b>\n\n"
                f"👥 تعداد تارگت‌های استخراج شده: <b>{len(unique_usernames)}</b>\n"
                f"🧹 <i>آیدی‌های تکراری حذف شده‌اند.</i>"
            ),
            reply_markup=get_tools_finish_keyboard()
        )
    except Exception as e:
        logger.error(f"Failed to generate txt file: {e}")
        await message.answer(
            "❌ خطایی در ساخت فایل رخ داد. لطفاً دوباره تلاش کنید.",
            reply_markup=get_tools_finish_keyboard()
        )
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)
            
    await state.clear()


# ==========================================
# فیچر جدید ۱ — دریافت اطلاعات کاربر تلگرام (/d_<user_id>)
# ==========================================
@router.message(F.text.regexp(r"^/d_\d+$"))
async def user_details_handler(message: types.Message) -> None:
    match = re.match(r"^/d_(\d+)$", message.text)
    if not match:
        return
        
    target_user_id = int(match.group(1))

    # جستجو در worker_pool برای یافتن اولین کلاینت متصل[cite: 2]
    active_client = next((client for client in worker_pool.values() if client.is_connected), None)
    
    if not active_client:
        await message.answer("⚠️ هیچ اکانت متصلی (ورکر) برای استعلام اطلاعات یافت نشد. لطفاً ابتدا یک اکانت لاگین کنید.")
        return

    loading_msg = await message.answer("🔄 در حال دریافت اطلاعات از سرور تلگرام...")

    try:
        user = await active_client.get_users(target_user_id)
        
        name = html.escape(user.first_name or "")
        if user.last_name:
            name += f" {html.escape(user.last_name)}"
            
        username = f"@{user.username}" if user.username else "ندارد"
        premium = "✅" if user.is_premium else "❌"
        is_bot = "✅" if user.is_bot else "❌"
        restricted = "✅" if user.is_restricted else "❌"
        
        details = (
            f"👤 <b>جزئیات کاربر تلگرام</b>\n\n"
            f"▪️ <b>آیدی:</b> <code>{user.id}</code>\n"
            f"▪️ <b>نام:</b> {name}\n"
            f"▪️ <b>نام کاربری:</b> {username}\n"
            f"▪️ <b>پریمیوم:</b> {premium}\n"
            f"▪️ <b>ربات:</b> {is_bot}\n"
            f"▪️ <b>محدود شده (Restricted):</b> {restricted}\n"
        )
        
        if user.is_restricted and user.restriction_reason:
            reasons = ", ".join(f"{r.platform}: {r.reason}" for r in user.restriction_reason)
            details += f"▪️ <b>دلیل محدودیت:</b> {html.escape(reasons)}\n"
            
        if hasattr(user, "dc_id") and user.dc_id:
            details += f"▪️ <b>دیتاسنتر:</b> {user.dc_id}\n"

        await loading_msg.edit_text(details)

    except PeerIdInvalid:
        await loading_msg.edit_text(
            "❌ خطای USER_ID_INVALID:\n"
            "کاربر با این آیدی یافت نشد یا هیچ‌کدام از ورکرها قبلاً با این کاربر تعاملی نداشته‌اند (در کش وجود ندارد)."
        )
    except Exception as e:
        logger.error(f"Error fetching user details for {target_user_id}: {e}")
        await loading_msg.edit_text(f"❌ خطا در دریافت اطلاعات:\n<code>{html.escape(str(e))}</code>")


# ==========================================
# فیچر جدید ۲ — پنل مدیریت کش کلاینت‌ها (/cache)
# ==========================================
@router.message(F.text == "/cache")
async def cache_management_panel(message: types.Message) -> None:
    if not worker_pool:
        return await message.answer("⚠️ استخر ورکرها کاملاً خالی است.")

    connected_count = sum(1 for c in worker_pool.values() if c.is_connected)
    
    text = (
        "🗄 <b>پنل مدیریت کش کلاینت‌ها (Entity Cache)</b>\n\n"
        f"تعداد کل ورکرها: <b>{len(worker_pool)}</b>\n"
        f"وضعیت فعال (متصل): <b>{connected_count}</b>\n\n"
        "<i>توضیح: هر کلاینت (pyrofork) برای ارسال پیام نیاز به access_hash تارگت‌ها دارد "
        "که در کش داخلی نگهداری می‌شود. پاکسازی کش باعث ری‌استارت کلاینت و دریافت مجدد داده‌ها می‌شود.</i>"
    )

    builder = InlineKeyboardBuilder()
    
    for account_id, client in worker_pool.items():
        status = "🟢" if client.is_connected else "🔴"
        
        builder.button(
            text=f"{status} اکانت {account_id}", 
            callback_data="ignore_action"
        )
        builder.button(
            text="♻️ پاکسازی کش",
            callback_data=f"cache_clear_{account_id}"
        )
        
    builder.adjust(2) 
    builder.row(types.InlineKeyboardButton(text="♻️ پاکسازی کش همه اکانت‌ها", callback_data="cache_clear_all"))
    
    await message.answer(text, reply_markup=builder.as_markup())


async def _restart_client_for_cache(client) -> bool:
    """
    بررسی دسترسی به Entity Cache کتابخانه pyrofork:
    pyrofork از SQLite برای نگهداری کش peers استفاده می‌کند. 
    از آنجایی که کلاینت‌های ما با پارامتر in_memory=True در session_manager ساخته شده‌اند[cite: 2]،
    دسترسی مستقیم و کوئری زدن به دیتابیس درون-حافظه‌ای مستعد خطاست و دسترسی API مشخصی برای تخلیه دستی تعبیه نشده است.
    قطع (stop) و وصل مجدد (start) کلاینت به‌صورت خودکار دیتابیس in-memory قبلی را
    منهدم کرده و کش کاملاً تمیزی برای آن سشن ایجاد می‌کند. این روش ایده‌آل و استاندارد است.
    """
    try:
        if client.is_connected:
            await client.stop()
        await client.start()
        return True
    except Exception as e:
        logger.error(f"Cache clear restart failed: {e}")
        return False


@router.callback_query(F.data.startswith("cache_clear_"))
async def handle_cache_clear_callback(callback: types.CallbackQuery) -> None:
    action = callback.data.replace("cache_clear_", "")
    
    if action == "all":
        await callback.answer("🔄 در حال ری‌استارت همه ورکرها... لطفاً صبر کنید.", show_alert=True)
        success_count = 0
        for client in list(worker_pool.values()):
            if await _restart_client_for_cache(client):
                success_count += 1
                
        await callback.message.answer(f"✅ کش <b>{success_count}</b> اکانت با ری‌استارت موفقیت‌آمیز پاکسازی شد.")
        return

    try:
        account_id = int(action)
    except ValueError:
        return await callback.answer("❌ دیتای نامعتبر.", show_alert=True)

    client = worker_pool.get(account_id)
    if not client:
        return await callback.answer("❌ کلاینت این اکانت یافت نشد یا حذف شده است.", show_alert=True)

    await callback.answer("🔄 در حال ری‌استارت کلاینت...")
    
    if await _restart_client_for_cache(client):
        await callback.message.answer(f"✅ کلاینت اکانت <code>{account_id}</code> ری‌استارت شد و کش آن پاکسازی گردید.")
    else:
        await callback.message.answer(f"❌ خطا در پاکسازی کش و اتصال مجدد اکانت <code>{account_id}</code>.")


@router.callback_query(F.data == "ignore_action")
async def ignore_action_callback(callback: types.CallbackQuery) -> None:
    await callback.answer()