# bot/keyboards/cancel.py
"""
🟣 فاز ۱ (رفع بن‌بست FSM): ماژول مشترک کیبورد و راهنمای انصراف

الگوی استاندارد پروژه: هر پیامی که کاربر را وارد یک FSM state می‌کند باید
۱) این کیبورد را داشته باشد و ۲) راهنمای /cancel را در متن خود نشان دهد.
به این ترتیب کاربر هرگز در یک state گیر نمی‌کند.
"""
from aiogram.types import InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder
# راهنمای استاندارد انصراف — باید به انتهای «همه» پیام‌های FSM prompt اضافه شود
CANCEL_HINT = "<i>برای انصراف، /cancel را ارسال کنید یا روی دکمه انصراف کلیک کنید.</i>"


def get_cancel_keyboard() -> InlineKeyboardMarkup:
    """
    کیبورد استاندارد انصراف برای همه فلوهای FSM:

    - «❌ انصراف»    → cancel_current_flow/ (هندلر عمومی: پاک کردن state + بازگشت به منو)
    - «🏛 منوی اصلی» → menu_home/
    """
    builder = InlineKeyboardBuilder()
    builder.button(text="❌ انصراف", callback_data="cancel_current_flow/")
    builder.button(text="🏛 منوی اصلی", callback_data="menu_home/")
    builder.adjust(2)
    return builder.as_markup()


def with_cancel_hint(text: str) -> str:
    """
    تابع کمکی: افزودن راهنمای انصراف به انتهای متن پیام‌های FSM prompt.
    (اگر پیام از قبل راهنما را داشته باشد، دوباره اضافه نمی‌شود)
    """
    if CANCEL_HINT in text:
        return text
    return f"{text}\n\n{CANCEL_HINT}"