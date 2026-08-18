from aiogram.types import InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder

def get_main_menu_keyboard() -> InlineKeyboardMarkup:
    """
    Constructs the main menu inline keyboard.
    Strictly adheres to the `prefix_xxx/` callback_data formatting.
    """
    builder = InlineKeyboardBuilder()

    # ردیف اول
    builder.button(text="📊 آنالیز", callback_data="menu_analysis/")
    builder.button(text="⚙️ تنظیمات", callback_data="menu_settings/")
    
    # ردیف دوم
    builder.button(text="❓ راهنما", callback_data="menu_help/")
    builder.button(text="📈 آمار", callback_data="menu_stats/")
    
    # ردیف سوم
    builder.button(text="➕ اضافه کردن اکانت", callback_data="menu_add_account/")
    builder.button(text="🚀 ثبت سفارش جدید", callback_data="menu_create_order/")
    
    # ردیف چهارم: جایگزینی استخراج با ابزار تولید لیست (استخراج حالا از داخل ثبت سفارش انجام می‌شود)
    builder.button(text="🛠 ابزار ساخت لیست TXT", callback_data="menu_txt_generator/")
    builder.button(text="🛑 مدیریت و توقف سفارشات", callback_data="menu_active_orders/")

    # چیدمان منظم دکمه‌ها: ۴ ردیف و در هر ردیف ۲ دکمه
    builder.adjust(2, 2, 2, 2)

    return builder.as_markup()