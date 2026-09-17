from aiogram.types import InlineKeyboardMarkup, KeyboardButton, ReplyKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram import types

MAIN_MENU_LAYOUT = [
    # ردیف ۱ (۲ تایی): عملیات اصلی و روزمره در بالاترین سطح
    ["🛍 ثبت سفارش", "📋 لیست سفارشات"],
    
    # ردیف ۲ (۱ تایی): بخش مهم و پرکاربرد (تمام‌عرض برای کلیک راحت‌تر)
    ["🌐 آنالیز"],
    
    # ردیف ۳ (۲ تایی): مدیریت اکانت‌ها (افزودن و لیست در کنار هم)
    ["📱 افزودن اکانت", "📲 لیست اکانت‌ها"],
    
    # ردیف ۴ (۲ تایی): مدیریت APIها (افزودن و لیست در کنار هم)
    ["📥 افزودن API", "📤 لیست API"],
    
    # ردیف ۵ (۲ تایی): آمار و سایر ابزارهای جانبی
    ["📊 آمار", "🧰 ابزارها"],
    
    # ردیف ۶ (۳ تایی): مدیریت فایل‌ها و دیتابیس
    ["🖼 پروفایل‌ها", "🎨 بنرها", "🧹 پاکسازی"],
    
    # ردیف ۷ (۲ تایی): تنظیمات و ساختار
    ["⚙️ تنظیمات", "📂 دسته‌بندی‌ها"],
    
    # ردیف ۸ (۲ تایی): مدیریت سیستم
    ["👨‍💻 ادمین‌ها", "📚 راهنما"],
    
    # ردیف ۹ (۱ تایی): کنترل ربات (انتقال به پایین برای جلوگیری از کلیک اشتباهی)
    ["🔄 ریستارت ربات"]
]

# کال‌بک‌ها دقیقاً متناظر با دکمه‌های آپدیت شده در لیست بالا تنظیم شدند
_MAIN_MENU_CALLBACKS = [
    # ردیف ۱
    "menu_create_order/", "menu_active_orders/",
    
    # ردیف ۲
    "menu_analysis/",
    
    # ردیف ۳
    "menu_add_account/", "menu_list_accounts/",
    
    # ردیف ۴
    "menu_add_api/", "menu_list_api/",
    
    # ردیف ۵
    "menu_stats/", "menu_txt_generator/",
    
    # ردیف ۶
    "photo_pkg_panel/", "menu_banners/", "menu_cleanup_tools/",
    
    # ردیف ۷
    "menu_settings/", "menu_list_categories/",
    
    # ردیف ۸
    "menu_add_admin/", "menu_help/",
    
    # ردیف ۹
    "menu_restart_bot/"
]

def get_main_menu_keyboard() -> types.InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    flat_texts = [btn for row in MAIN_MENU_LAYOUT for btn in row]
    
    for text, cb in zip(flat_texts, _MAIN_MENU_CALLBACKS):
        builder.button(text=text, callback_data=cb)
        
    # چیدمان کاملاً هوشمند: طول هر ردیف را مستقیماً از آرایه MAIN_MENU_LAYOUT می‌خواند
    builder.adjust(*[len(row) for row in MAIN_MENU_LAYOUT])
    return builder.as_markup()

def get_main_menu_reply_keyboard() -> ReplyKeyboardMarkup:
    """⌨️ نسخه‌ی دکمه‌ای منوی اصلی — کیبورد پایین چت"""
    keyboard = [[KeyboardButton(text=t) for t in row] for row in MAIN_MENU_LAYOUT]
    return ReplyKeyboardMarkup(
        keyboard=keyboard,
        resize_keyboard=True,
        is_persistent=True,
        input_field_placeholder="🏢 Master Control Panel...",
    )

# ==========================================
# Phase 5 — live progress reporting (settings toggle helpers)
# ==========================================
# Callback is handled in bot/handlers/settings_handlers.py::toggle_progress_notify
PROGRESS_NOTIFY_TOGGLE_CB = "toggle_progress_notify/"


def get_progress_notify_button_text(enabled: bool) -> str:
    """Persian label for the live-progress toggle entry in the settings menu."""
    if enabled:
        return "✅ 📊 گزارش پیشرفت لحظه‌ای: روشن"
    return "❌ 📊 گزارش پیشرفت لحظه‌ای: خاموش"


get_main_menu_button = get_main_menu_keyboard