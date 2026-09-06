import logging

from aiogram import Router, types, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.filters import Command
from bot.keyboards.main_menu import get_main_menu_keyboard, get_main_menu_button
from workers.session_manager import worker_pool
import html
from bot.keyboards.cancel import with_cancel_hint
from bot.middlewares.force_join import REQUIRED_CHANNELS  # 🔴 فاز ۱۱ (BUG-17a)
from utils.fsm_cleanup import cleanup_fsm_temp_files
from utils.safe_edit import safe_edit_or_answer

logger = logging.getLogger(__name__)

router = Router(name="general_handlers_router")

# ==========================================
# VERIFY JOIN HANDLER
# ==========================================
@router.callback_query(F.data == "menu_verify_join/")
async def verify_join_callback(callback: types.CallbackQuery) -> None:
    # 🔴 فاز ۱۱ (BUG-17a): بررسی «واقعی» عضویت قبل از صدور پیام موفقیت.
    # قبلاً بدون هیچ get_chat_member ای تایید صادر می‌شد (UX گمراه‌کننده).
    bot = callback.bot
    not_joined_channels = []

    for channel in REQUIRED_CHANNELS:
        try:
            chat_member = await bot.get_chat_member(chat_id=channel, user_id=callback.from_user.id)
            if chat_member.status in ["left", "kicked", "banned"]:
                not_joined_channels.append(channel)
        except Exception as e:
            # بررسی ناموفق → مثل عضو-نشده تلقی می‌شود (fail-closed،
            # هماهنگ با رفتار جدید میدلور force_join در BUG-17b)
            logger.warning(f"Verify-join: membership check failed for {channel}: {e}")
            not_joined_channels.append(channel)

    if not_joined_channels:
        channels_list = "\n".join(f"• {ch}" for ch in not_joined_channels)
        return await callback.answer(
            "❌ عضویت شما هنوز تایید نشده است!\n\n"
            f"لطفاً ابتدا در کانال(های) زیر عضو شوید:\n{channels_list}",
            show_alert=True
        )

    await callback.answer("✅ عضویت شما تایید شد!", show_alert=True)
    
    await callback.message.edit_text(
        "🎛 <b>پنل کنترل اصلی</b>\n\nلطفاً یک گزینه را انتخاب کنید:",
        reply_markup=get_main_menu_keyboard()
    )

# ==========================================
# HELP MENU HANDLER
# ==========================================

HELP_TEXT = (
    "❓ <b>راهنمای استفاده از ربات</b>\n\n"
    "<b>۱. تنظیمات اولیه:</b>\n"
    "از منوی «⚙️ تنظیمات»، دسته‌بندی‌ها و لیست پراکسی‌های خود را اضافه کنید.\n\n"
    "<b>۲. افزودن API:</b>\n"
    "از منوی «📥 افزودن Api»، API ID و API Hash تلگرام را اضافه کنید.\n\n"
    "<b>۳. افزودن اکانت:</b>\n"
    "از منوی «📱 افزودن اکانت»، اکانت‌های تلگرام خود را لاگین کنید.\n\n"
    "<b>۴. ثبت سفارش:</b>\n"
    "از منوی «🛍 ثبت سفارش»، لینک گروه یا لیست آیدی‌ها را ارسال کنید.\n\n"
    "<b>۵. استخراج اعضا:</b>\n"
    "از منوی «🌐 آنالیز»، لینک گروه را ارسال کنید تا اعضا استخراج شوند.\n\n"
    "<b>۶. مدیریت ادمین‌ها:</b>\n"
    "از منوی «👨‍💻 افزودن ادمین»، ادمین‌های جدید اضافه یا حذف کنید.\n\n"
    "<b>📋 دستورات مفید:</b>\n"
    "• /cancel - لغو عملیات جاری\n"
    "• /reset - ریست سیستم\n"
    "• /gtg_&lt;id&gt; - مشاهده داشبورد سفارش\n"
    "• /sessions_&lt;id&gt; - مدیریت نشست‌های اکانت\n"
    "• /d_&lt;id&gt; - مشاهده جزئیات کاربر\n"
    "• /cache - مدیریت و ویرایش کش کانال\n"
    "• /import - افزودن اکانت از طریق سشن\n"
    "• /LeaveGroups - خروج دسته‌جمعی اکانت‌ها از گروه‌ها\n"
    "• /DeleteChats - حذف تاریخچه چت‌های اکانت‌ها\n\n"
    "<i>💡 سیستم به صورت خودکار دارای Anti-Ban، تاخیرهای انسانی و Proxy Rotation می‌باشد.</i>\n\n"
    "درصورت داشتن مشکل با پشتیبانی ما در ارتباط باشید:\n"
    ".➖➖➖➖➖➖"
)

@router.callback_query(F.data == "menu_help/")
async def show_help_menu(callback: types.CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    
    await cleanup_fsm_temp_files(state)
    await state.clear()
    
    builder = InlineKeyboardBuilder()
    builder.button(text="🏛 منوی اصلی", callback_data="menu_home/")
    
    await safe_edit_or_answer(callback.message, HELP_TEXT, reply_markup=builder.as_markup())

@router.message(Command("help"))
async def show_help_command(message: types.Message, state: FSMContext) -> None:
    await cleanup_fsm_temp_files(state)
    await state.clear()
    
    builder = InlineKeyboardBuilder()
    builder.button(text="🏛 منوی اصلی", callback_data="menu_home/")
    
    await message.answer(HELP_TEXT, reply_markup=builder.as_markup())




@router.callback_query(F.data.in_([
    "menu_coming_soon_example/" 
]))
async def handle_coming_soon_menus(callback: types.CallbackQuery) -> None:
    await callback.answer(
        "⏳ این بخش در حال توسعه است و به زودی اضافه خواهد شد!", 
        show_alert=True
    )

# ==========================================
# هندلر خنثی برای نشانگر صفحه‌بندی (فاز ۴)
# ==========================================
@router.callback_query(F.data == "pagination_info_ignore/")
async def ignore_pagination_info(callback: types.CallbackQuery) -> None:
    await callback.answer()

# ==========================================
# --- استیت‌های مربوط به CRM ---
# ==========================================
class CRMStates(StatesGroup):
    waiting_for_reply = State()

def get_crm_cancel_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="❌ انصراف", callback_data="cancel_crm_reply/")
    return builder.as_markup()

# ==========================================
# هندلر کلیک روی دکمه پاسخ
# ==========================================
@router.callback_query(F.data.startswith("crm_reply_"))
async def crm_reply_callback(callback: types.CallbackQuery, state: FSMContext) -> None:
    parts = callback.data.split("_")
    
    if len(parts) != 4:
        return await callback.answer("⚠️ دیتای نامعتبر.", show_alert=True)

    worker_id = parts[2]
    target_id = parts[3]

    await state.update_data(crm_worker_id=worker_id, crm_target_id=target_id)
    await state.set_state(CRMStates.waiting_for_reply)

    await callback.message.reply(
        with_cancel_hint(
            f"✍️ <b>ارسال پاسخ به تارگت:</b> <code>{target_id}</code>\n"
            f"🤖 <b>از طریق اکانت ورکر:</b> <code>{worker_id}</code>\n\n"
            "لطفاً متن پاسخ خود را ارسال کنید:"
        ),
        reply_markup=get_crm_cancel_keyboard()
    )
    await callback.answer()

# ==========================================
# هندلر لغو پاسخ‌گویی
# ==========================================
@router.callback_query(F.data == "cancel_crm_reply/")
async def cancel_crm_reply(callback: types.CallbackQuery, state: FSMContext) -> None:
    if await state.get_state() is None:
        await callback.answer("هیچ عملیاتی فعال نبود.")
        return await safe_edit_or_answer(
            callback.message,
            "🏛 شما به منوی اصلی بازگشتید.",
            reply_markup=get_main_menu_keyboard()
        )
    
    await state.clear()
    await callback.answer("عملیات لغو شد.")
    
    await safe_edit_or_answer(
        callback.message,
        "❌ عملیات پاسخ‌گویی لغو شد.",
        reply_markup=get_main_menu_keyboard()
    )

# ==========================================
# هندلر دریافت متن و ارسال با Pyrogram
# ==========================================
@router.message(CRMStates.waiting_for_reply)
async def send_crm_reply(message: types.Message, state: FSMContext) -> None:
    data = await state.get_data()
    worker_id_str = data.get("crm_worker_id")
    target_id = data.get("crm_target_id")

    if not worker_id_str or not target_id:
        await state.clear()
        return await message.answer(
            "⚠️ اطلاعات نشست از دست رفته است. لطفاً دوباره روی دکمه پاسخ کلیک کنید.",
            reply_markup=get_main_menu_button()
        )

    reply_text = message.text or message.caption or ""
    if not reply_text:
        return await message.answer(
            with_cancel_hint("⚠️ لطفاً فقط متن ارسال کنید."),
            reply_markup=get_crm_cancel_keyboard()
        )

    try:
        worker_id_int = int(worker_id_str)
    except ValueError:
        await state.clear()
        return await message.answer(
            "⚠️ خطای سیستمی: آیدی ورکر نامعتبر است.",
            reply_markup=get_main_menu_button()
        )

    client = worker_pool.get(worker_id_int)
    
    if not client or not client.is_connected:
        await state.clear()
        return await message.answer(
            f"⚠️ <b>ارسال ناموفق:</b>\n"
            f"اکانت ورکر <code>{worker_id_str}</code> در حال حاضر آفلاین است یا اتصال آن با تلگرام قطع شده است.",
            reply_markup=get_main_menu_button()
        )

    try:
        await client.send_message(chat_id=int(target_id), text=reply_text)
        await message.answer(
            f"✅ <b>پیام شما با موفقیت ارسال شد!</b>\n👤 <b>مقصد:</b> <code>{target_id}</code>",
            reply_markup=get_main_menu_button()
        )
    except Exception as e:
        await message.answer(
            f"❌ <b>خطا در ارسال پیام:</b>\n<code>{html.escape(str(e))}</code>",
            reply_markup=get_main_menu_button()
        )
    finally:
        await state.clear()