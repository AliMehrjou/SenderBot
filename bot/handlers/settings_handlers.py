import logging
from aiogram import Router, types, F
from aiogram.fsm.context import FSMContext
from aiogram.utils.keyboard import InlineKeyboardBuilder
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from database.models import GlobalSettings, Category, Proxy
from bot.states.settings_fsm import SettingsStates

logger = logging.getLogger(__name__)

router = Router(name="settings_handlers_router")

def get_cancel_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="❌ انصراف و بازگشت", callback_data="menu_settings/")
    return builder.as_markup()

# ==========================================
# MAIN SETTINGS MENU
# ==========================================
@router.callback_query(F.data == "menu_settings/")
async def show_settings_menu(callback: types.CallbackQuery, state: FSMContext, session: AsyncSession) -> None:
    await callback.answer()
    await state.clear()
    
    # دریافت تنظیمات فعلی از دیتابیس
    stmt = select(GlobalSettings).limit(1)
    result = await session.execute(stmt)
    settings = result.scalar_one_or_none()
    
    send_limit = settings.send_limit_per_run if settings else "N/A"
    penalty = settings.spam_penalty_days if settings else "N/A"

    builder = InlineKeyboardBuilder()
    builder.button(text="📁 افزودن دسته‌بندی جدید", callback_data="settings_add_cat/")
    builder.button(text="🌐 افزودن لیست پراکسی", callback_data="settings_add_proxy/")
    builder.button(text="⚙️ تغییر ظرفیت ارسال", callback_data="settings_edit_limit/")
    builder.button(text="🔙 بازگشت به منوی اصلی", callback_data="menu_home/")
    builder.adjust(1)

    text = (
        "⚙️ <b>پنل تنظیمات سیستم</b>\n\n"
        f"🔹 <b>ظرفیت ارسال هر ورکر در هر دوره:</b> <code>{send_limit} پیام</code>\n"
        f"🔹 <b>جریمه پیش‌فرض اسپم:</b> <code>{penalty} روز</code>\n\n"
        "<i>برای مدیریت سیستم، یکی از گزینه‌های زیر را انتخاب کنید:</i>"
    )
    
    await callback.message.edit_text(text, reply_markup=builder.as_markup())


# ==========================================
# ADD CATEGORY FLOW
# ==========================================
@router.callback_query(F.data == "settings_add_cat/")
async def ask_for_category(callback: types.CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    await state.set_state(SettingsStates.waiting_for_category)
    await callback.message.edit_text(
        "📁 <b>افزودن دسته‌بندی جدید</b>\n\n"
        "لطفاً نام دسته‌بندی جدید را ارسال کنید (مثلاً: <code>VIP_Users</code>):",
        reply_markup=get_cancel_keyboard()
    )

@router.message(SettingsStates.waiting_for_category, F.text)
async def process_new_category(message: types.Message, state: FSMContext, session: AsyncSession) -> None:
    cat_name = message.text.strip()
    
    new_category = Category(name=cat_name)
    try:
        session.add(new_category)
        await session.commit()
        await message.answer(f"✅ دسته‌بندی <b>{cat_name}</b> با موفقیت اضافه شد.")
    except Exception as e:
        await session.rollback()
        logger.error(f"Error adding category {cat_name}: {e}")
        await message.answer("❌ این نام قبلاً ثبت شده یا خطایی رخ داده است.")
        
    await state.clear()


# ==========================================
# ADD PROXIES FLOW
# ==========================================
@router.callback_query(F.data == "settings_add_proxy/")
async def ask_for_proxies(callback: types.CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    await state.set_state(SettingsStates.waiting_for_proxies)
    await callback.message.edit_text(
        "🌐 <b>افزودن پراکسی</b>\n\n"
        "لطفاً لیست پراکسی‌های خود را (هر کدام در یک خط) ارسال کنید.\n"
        "<i>فرمت قابل قبول: socks5://user:pass@ip:port یا socks5://ip:port</i>",
        reply_markup=get_cancel_keyboard()
    )

@router.message(SettingsStates.waiting_for_proxies, F.text)
async def process_new_proxies(message: types.Message, state: FSMContext, session: AsyncSession) -> None:
    raw_proxies = message.text.strip().split('\n')
    added_count = 0
    
    for line in raw_proxies:
        proxy_str = line.strip()
        if not proxy_str:
            continue
            
        new_proxy = Proxy(proxy_string=proxy_str, is_active=True, fail_count=0)
        session.add(new_proxy)
        try:
            await session.commit()
            added_count += 1
        except Exception:
            await session.rollback() # در صورت تکراری بودن رد می‌شود
            continue
            
    await state.clear()
    await message.answer(f"✅ تعداد <b>{added_count}</b> پراکسی جدید و یونیک به استخر شبکه اضافه شد.")


# ==========================================
# EDIT SEND LIMIT FLOW
# ==========================================
@router.callback_query(F.data == "settings_edit_limit/")
async def ask_for_send_limit(callback: types.CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    await state.set_state(SettingsStates.waiting_for_send_limit)
    await callback.message.edit_text(
        "⚙️ <b>تغییر ظرفیت ارسال</b>\n\n"
        "لطفاً یک عدد وارد کنید (تعداد پیامی که هر ورکر در یک دوره اجرای سفارش ارسال می‌کند، پیش‌فرض ۴۰):",
        reply_markup=get_cancel_keyboard()
    )

@router.message(SettingsStates.waiting_for_send_limit, F.text)
async def process_new_send_limit(message: types.Message, state: FSMContext, session: AsyncSession) -> None:
    if not message.text.isdigit():
        await message.answer("⚠️ لطفاً فقط یک عدد صحیح ارسال کنید.")
        return
        
    new_limit = int(message.text)
    
    try:
        stmt = update(GlobalSettings).where(GlobalSettings.id == 1).values(send_limit_per_run=new_limit)
        await session.execute(stmt)
        await session.commit()
        await message.answer(f"✅ محدودیت ارسال هر ورکر به <b>{new_limit}</b> تغییر یافت.")
    except Exception as e:
        await session.rollback()
        logger.error(f"Error updating settings: {e}")
        await message.answer("❌ خطای دیتابیس در ثبت تنظیمات.")
        
    await state.clear()