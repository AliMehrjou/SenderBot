from aiogram.types import InlineKeyboardMarkup, KeyboardButton, ReplyKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram import types
# چیدمان منوی اصلی — منبع واحد برای نسخه‌ی شیشه‌ای (inline) و دکمه‌ای (reply)
MAIN_MENU_LAYOUT = [
    ["🛍 ثبت سفارش 🛍", "💾 لیست سفارشات 💾"],
    ["📱 افزودن اکانت 📱", "📲 لیست اکانت‌ها 📲"],
    ["📥 افزودن Api 📥", "📤 لیست Api 📤"],
    ["📄 افزودن دسته‌بندی 📄", "🗂 لیست دسته‌بندی‌ها 🗂"],
    ["🌐 آنالیز 🌐", "📊 آمار 📊"],
    ["👨‍💻 افزودن ادمین 👨‍💻", "⚙️ تنظیمات ⚙️"],
    ["📚 راهنما 📚"],
    ["🧰 ابزارها", "🎨 بنرها", "🖼 پکیج پروفایل 🖼"],
]

_MAIN_MENU_CALLBACKS = [
    "menu_create_order/", "menu_active_orders/",
    "menu_add_account/", "menu_list_accounts/",
    "menu_add_api/", "menu_list_api/",
    "settings_add_cat/", "menu_list_categories/",
    "menu_analysis/", "menu_stats/",
    "menu_add_admin/", "menu_settings/",
    "menu_help/",
    "menu_txt_generator/", "menu_banners/", "photo_pkg_panel/"
]


def get_main_menu_keyboard() -> types.InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    flat_texts = [btn for row in MAIN_MENU_LAYOUT for btn in row]
    
    for text, cb in zip(flat_texts, _MAIN_MENU_CALLBACKS):
        builder.button(text=text, callback_data=cb)
        
    # تنظیم ردیف‌ها مطابق با LAYOUT
    builder.adjust(2, 2, 2, 2, 2, 2, 1, 3)
    return builder.as_markup()

def get_main_menu_reply_keyboard() -> ReplyKeyboardMarkup:
    """⌨️ نسخه‌ی دکمه‌ای منوی اصلی — کیبورد پایین چت"""
    keyboard = [[KeyboardButton(text=t) for t in row] for row in MAIN_MENU_LAYOUT]
    return ReplyKeyboardMarkup(
        keyboard=keyboard,
        resize_keyboard=True,
        is_persistent=True,
        input_field_placeholder="🏢 Master Control Panel…",
    )

get_main_menu_button = get_main_menu_keyboard