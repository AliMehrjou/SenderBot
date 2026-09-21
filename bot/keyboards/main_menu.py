from aiogram.types import InlineKeyboardMarkup, KeyboardButton, ReplyKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram import types

# ساختار یکپارچه: شامل متن، کال‌بک و استایل رنگی دکمه
MAIN_MENU_LAYOUT = [
    # ردیف ۱ (۲ تایی): عملیات اصلی
    [{"text": "🛍 ثبت سفارش", "cb": "menu_create_order/", "style": "success"},
     {"text": "📋 لیست سفارشات", "cb": "menu_active_orders/", "style": "primary"}],
    
    # ردیف ۲ (۱ تایی): تمام‌عرض
    [{"text": "🌐 آنالیز", "cb": "menu_analysis/", "style": "primary"}],
    
    # ردیف ۳ (۲ تایی): مدیریت اکانت‌ها
    [{"text": "📱 افزودن اکانت", "cb": "menu_add_account/", "style": "success"},
     {"text": "📲 لیست اکانت‌ها", "cb": "menu_list_accounts/", "style": "primary"}],
    
    # ردیف ۴ (۲ تایی): مدیریت APIها
    [{"text": "📥 افزودن API", "cb": "menu_add_api/", "style": "success"},
     {"text": "📤 لیست API", "cb": "menu_list_api/", "style": "primary"}],
    
    # ردیف ۵ (۲ تایی)
    [{"text": "📊 آمار", "cb": "menu_stats/", "style": "primary"},
     {"text": "🧰 ابزارها", "cb": "menu_txt_generator/", "style": "primary"}],
    
    # ردیف ۶ (۳ تایی): پاکسازی را با استایل قرمز (danger) مشخص کردیم
    [{"text": "🖼 پروفایل‌ها", "cb": "photo_pkg_panel/", "style": "primary"},
     {"text": "🎨 بنرها", "cb": "menu_banners/", "style": "primary"},
     {"text": "🧹 پاکسازی", "cb": "menu_cleanup_tools/", "style": "danger"}],
    
    # ردیف ۷ (۲ تایی)
    [{"text": "⚙️ تنظیمات", "cb": "menu_settings/", "style": "primary"},
     {"text": "📂 دسته‌بندی‌ها", "cb": "menu_list_categories/", "style": "primary"}],
    
    # ردیف ۸ (۲ تایی)
    [{"text": "👨‍💻 ادمین‌ها", "cb": "menu_add_admin/", "style": "primary"},
     {"text": "📚 راهنما", "cb": "menu_help/", "style": "primary"}],
    
    # ردیف ۹ (۱ تایی): ریستارت را با استایل قرمز (danger) مشخص کردیم
    [{"text": "🔄 ریستارت ربات", "cb": "menu_restart_bot/", "style": "danger"}]
]

def get_main_menu_keyboard() -> types.InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    
    # خواندن هوشمند مقادیر و اعمال استایل روی دکمه شیشه‌ای
    for row in MAIN_MENU_LAYOUT:
        for btn in row:
            builder.button(
                text=btn["text"], 
                callback_data=btn["cb"], 
                style=btn.get("style", "primary") # دیفالت: آبی
            )
            
    # چیدمان خودکار بر اساس طول ردیف‌ها در لیست
    builder.adjust(*[len(row) for row in MAIN_MENU_LAYOUT])
    return builder.as_markup()

def get_main_menu_reply_keyboard() -> ReplyKeyboardMarkup:
    """⌨️ نسخه‌ی دکمه‌ای منوی اصلی — کیبورد پایین چت با پشتیبانی از رنگ‌ها"""
    keyboard = [
        [
            KeyboardButton(
                text=btn["text"], 
                style=btn.get("style", "primary")
            ) for btn in row
        ] for row in MAIN_MENU_LAYOUT
    ]
    
    return ReplyKeyboardMarkup(
        keyboard=keyboard,
        resize_keyboard=True,
        is_persistent=True,
        input_field_placeholder="🏢 Master Control Panel...",
    )

# ==========================================
# Phase 5 — live progress reporting (settings toggle helpers)
# ==========================================
PROGRESS_NOTIFY_TOGGLE_CB = "toggle_progress_notify/"

def get_progress_notify_button_text(enabled: bool) -> str:
    if enabled:
        return "✅ 📊 گزارش پیشرفت لحظه‌ای: روشن"
    return "❌ 📊 گزارش پیشرفت لحظه‌ای: خاموش"

get_main_menu_button = get_main_menu_keyboard