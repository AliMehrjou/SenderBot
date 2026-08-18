import logging
from aiogram import Router, types, F
from aiogram.utils.keyboard import InlineKeyboardBuilder

from bot.keyboards.main_menu import get_main_menu_keyboard

logger = logging.getLogger(__name__)

router = Router(name="general_handlers_router")

# ==========================================
# VERIFY JOIN HANDLER
# ==========================================
@router.callback_query(F.data == "menu_verify_join/")
async def verify_join_callback(callback: types.CallbackQuery) -> None:
    """
    میدلور ForceJoin قبل از این هندلر اجرا می‌شود.
    اگر کاربر به اینجا رسیده، یعنی واقعاً عضو کانال‌ها شده است.
    """
    await callback.answer("✅ عضویت شما تایید شد!", show_alert=True)
    
    await callback.message.edit_text(
        "🎛 <b>Master Control Panel</b>\n\nعضویت شما تایید شد. لطفاً یک گزینه را انتخاب کنید:",
        reply_markup=get_main_menu_keyboard()
    )


# ==========================================
# HELP MENU HANDLER
# ==========================================
@router.callback_query(F.data == "menu_help/")
async def show_help_menu(callback: types.CallbackQuery) -> None:
    await callback.answer()
    
    help_text = (
        "❓ <b>راهنمای استفاده از موتور سندر</b>\n\n"
        "<b>۱. تنظیمات اولیه:</b> ابتدا از منوی «⚙️ تنظیمات»، دسته‌بندی‌ها و لیست پراکسی‌های خود را اضافه کنید.\n"
        "<b>۲. افزودن اکانت:</b> از منوی اصلی «➕ اضافه کردن اکانت» را انتخاب کرده و لاگین کنید. اکانت به صورت خودکار از استخر سیستم یک پراکسی دریافت می‌کند.\n"
        "<b>۳. ثبت سفارش:</b> روی «🚀 اضافه کردن اردر» کلیک کنید. تارگت‌ها (لینک گروه یا لیست آیدی) را بفرستید تا در صف (Queue) قرار بگیرد.\n"
        "<b>۴. آمار و مانیتورینگ:</b> از طریق دکمه‌های «📊 آنالیز» و «📈 آمار» می‌توانید وضعیت شبکه‌ی پراکسی‌ها، ارورها و پیشرفت ارسال‌ها را لحظه به لحظه رصد کنید.\n\n"
        "<i>💡 سیستم به صورت خودکار دارای مکانیزم Anti-Ban، تاخیرهای انسانی و Proxy Rotation می‌باشد.</i>"
    )
    
    builder = InlineKeyboardBuilder()
    builder.button(text="🔙 بازگشت به منو", callback_data="menu_home/")
    
    await callback.message.edit_text(
        help_text,
        reply_markup=builder.as_markup()
    )


# ==========================================
# FALLBACK FOR UNIMPLEMENTED MENUS
# ==========================================
# الان این بخش فقط برای دکمه‌هایی که در آینده ممکنه اضافه بشن و هنوز لاجیک ندارن کار می‌کنه
@router.callback_query(F.data.in_([
    "menu_coming_soon_example/" 
]))
async def handle_coming_soon_menus(callback: types.CallbackQuery) -> None:
    await callback.answer(
        "⏳ این بخش در حال توسعه است و به زودی اضافه خواهد شد!", 
        show_alert=True
    )