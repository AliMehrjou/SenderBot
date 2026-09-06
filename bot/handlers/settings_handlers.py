import html
import logging
import re
from typing import Optional

from aiogram import Router, types, F
from aiogram.fsm.context import FSMContext
from aiogram.utils.keyboard import InlineKeyboardBuilder
from sqlalchemy import select, update, delete, or_, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from workers.session_manager import worker_pool, parse_proxy_string
from config import config
from database.models import GlobalSettings, Category, Account, Proxy
from bot.states.settings_fsm import SettingsStates
from bot.states.confirm_fsm import ConfirmStates
from bot.keyboards.main_menu import get_main_menu_button

# 🟣 فاز ۴ (رفع بن‌بست FSM): helper های قبلی
from bot.keyboards.cancel import with_cancel_hint
from utils.fsm_cleanup import cleanup_fsm_temp_files
from utils.safe_edit import safe_edit_or_answer
from workers.session_manager import worker_pool, parse_proxy_string
# 🛡 فاز ۲ (مدیریت خطا): helper های فاز ۱ + اصلاحیه answer_callback_error
from utils.error_messages import report_db_error
from utils.telegram_helpers import (
    answer_callback_error,
    safe_callback_answer,
    safe_edit_message,
    send_loading_message,
)

# 📄 فاز ۲ (صفحه‌بندی استاندارد): زیرساخت مشترک paginate (فاز ۱)
from utils.pagination import (
    PAGINATION_SIZE,
    add_list_footer,
    add_pagination_nav_row,
    calculate_total_pages,
    clamp_page,
    get_page_offset,
    parse_page_from_callback,
)

logger = logging.getLogger(__name__)

router = Router(name="settings_handlers_router")


# ==========================================
# 🟣 فاز ۴: کیبوردهای استاندارد فلوهای تنظیمات
# ==========================================
def get_settings_cancel_keyboard():
    """
    کیبورد انصراف برای پیام‌های FSM prompt فلوهای تنظیمات.
    دکمه انصراف به منوی تنظیمات (menu_settings/) برمی‌گردد.
    """
    builder = InlineKeyboardBuilder()
    builder.button(text="❌ انصراف", callback_data="menu_settings/")
    builder.button(text="🏛 منوی اصلی", callback_data="menu_home/")
    builder.adjust(2)
    return builder.as_markup()


def get_settings_return_keyboard():
    """
    کیبورد بازگشت برای پیام‌های پایانی (موفقیت/خطای دیتابیس) که
    state در آن‌ها به پایان رسیده است.
    """
    builder = InlineKeyboardBuilder()
    builder.button(text="⚙️ بازگشت به تنظیمات", callback_data="menu_settings/")
    builder.button(text="🏛 منوی اصلی", callback_data="menu_home/")
    builder.adjust(2)
    return builder.as_markup()

# ==========================================
# MAIN SETTINGS MENU
# ==========================================
@router.callback_query(F.data == "menu_settings/")
async def show_settings_menu(callback: types.CallbackQuery, state: FSMContext, session: AsyncSession) -> None:
    # 🛡 فاز ۲: safe_callback_answer به جای callback.answer خام.
    await safe_callback_answer(callback)
    
    await cleanup_fsm_temp_files(state)
    # ---------------------------------------------------------------------------------
    await state.clear()

    # 🛡 فاز ۲: خطای دیتابیس هنگام «خواندن» تنظیمات دیگر باعث کرش بی‌صدای
    # هندلر نمی‌شود (مثلاً قطع دیتابیس → پیام واضح به کاربر)
    try:
        stmt = select(GlobalSettings).limit(1)
        result = await session.execute(stmt)
        settings = result.scalar_one_or_none()
    except Exception as e:
        await session.rollback()
        return await callback.message.answer(
            report_db_error("تنظیمات", e),
            reply_markup=get_settings_return_keyboard()
        )

    send_limit = settings.send_limit_per_run if settings else "N/A"
    penalty = settings.spam_penalty_days if settings else "N/A"
    max_acc_api = settings.max_accounts_per_api if settings else "N/A"
    cooldown = settings.cooldown_hours if settings else "N/A"

    daily_cap = config.DAILY_SEND_LIMIT_PER_ACCOUNT
    new_acc_days = config.NEW_ACCOUNT_DAYS
    new_acc_cap = config.NEW_ACCOUNT_DAILY_SEND_LIMIT

    builder = InlineKeyboardBuilder()

    # دکمه‌های عددی
    builder.button(text="تغییر محدودیت API", callback_data="settings_edit_max_api/")
    builder.button(text="تغییر زمان استراحت", callback_data="settings_edit_cooldown/")
    builder.button(text="تغییر جریمه اسپم", callback_data="settings_edit_penalty/")
    builder.button(text="تغییر ظرفیت ارسال", callback_data="settings_edit_limit/")

    # --- دکمه‌های Toggle (خاموش/روشن) ---
    if settings:
        btn_2fa = "✅ 🔐 تنظیم پسورد دوم (2FA)" if settings.auto_set_2fa else "❌ 🔐 تنظیم پسورد دوم (2FA)"
        btn_term = "✅ 🚪 خروج از سایر نشست‌ها" if settings.terminate_sessions else "❌ 🚪 خروج از سایر نشست‌ها"
        # 🎭 مدیریت پیشرفته پروفایل‌ها: سوئیچ واحد قبلی («تنظیم نام، بایو، پروفایل»)
        # به سه سوئیچ مستقل تفکیک شد؛ دکمه «تنظیم یوزرنیم» کامل حذف شد.
        btn_name = "✅ 📛 تنظیم نام" if settings.auto_set_name else "❌ 📛 تنظیم نام"
        btn_bio = "✅ 📝 تنظیم بیو" if settings.auto_set_bio else "❌ 📝 تنظیم بیو"
        btn_photo = "✅ 🖼 تنظیم عکس پروفایل" if settings.auto_set_photo else "❌ 🖼 تنظیم عکس پروفایل"
        btn_access = "✅ 🌐 دسترسی عمومی به کد سفارش" if settings.public_order_access else "❌ 🌐 دسترسی عمومی به کد سفارش"

        builder.button(text=btn_2fa, callback_data="toggle_auto_set_2fa/")
        builder.button(text=btn_term, callback_data="toggle_terminate_sessions/")
        builder.button(text=btn_name, callback_data="toggle_auto_set_name/")
        builder.button(text=btn_bio, callback_data="toggle_auto_set_bio/")
        builder.button(text=btn_photo, callback_data="toggle_auto_set_photo/")
        builder.button(text=btn_access, callback_data="toggle_public_order_access/")
    # ------------------------------------

    # دکمه‌های مدیریت
    builder.button(text="📁 افزودن دسته‌بندی جدید", callback_data="settings_add_cat/")
    builder.button(text="🌐 افزودن لیست پراکسی", callback_data="settings_add_proxy/")
    builder.button(text="🔄 دریافت خودکار پراکسی", callback_data="settings_auto_fetch_proxy/")
    builder.button(text="🗑 پاکسازی پراکسی‌های مرده", callback_data="settings_flush_proxies/")

    builder.button(text="🏛 منوی اصلی", callback_data="menu_home/")

    # چیدمان: ۲ ردیف عددی + ۳ ردیف سوئیچ (جفتی) + ردیف مدیریت + پاکسازی + منوی اصلی
    builder.adjust(2, 2, 2, 2, 2, 2, 1, 1)

    text = (
        "⚙️ <b>پنل تنظیمات پیشرفته سیستم</b>\n\n"
        f"🔹 <b>حداکثر اکانت روی هر API:</b> <code>{max_acc_api}</code> عدد\n"
        f"🔹 <b>ظرفیت ارسال هر ورکر:</b> <code>{send_limit}</code> پیام\n"
        f"🔹 <b>زمان استراحت هر اکانت:</b> <code>{cooldown}</code> ساعت\n"
        f"🔹 <b>سقف روزانه هر اکانت (ضد-بن):</b> <code>{daily_cap}</code> پیام <i>(از .env)</i>\n"
        f"🔹 <b>اکانت تازه (&lt;{new_acc_days} روز):</b> <code>{new_acc_cap}</code> پیام/روز <i>(از .env)</i>\n"
        f"🔹 <b>جریمه پیش‌فرض اسپم:</b> <code>{penalty}</code> روز\n\n"
        "<i>برای تغییر وضعیت هر قابلیت، روی دکمه مربوطه کلیک کنید:</i>"
    )

    await safe_edit_or_answer(callback.message, text, reply_markup=builder.as_markup())


# ==========================================
# TOGGLE HANDLER (تغییر وضعیت دکمه‌های On/Off)
# ==========================================
# 🎭 مدیریت پیشرفته پروفایل‌ها: هر سه سوئیچ جدید (auto_set_name / auto_set_bio /
# auto_set_photo) دقیقاً با همان الگوی toggleهای موجود کار می‌کنند — یعنی همین
# هندلر عمومیِ پایین، callback هر سه را (از طریق نام فیلد + hasattr) پوشش می‌دهد
# و هر کدام فقط ستون متناظر خودش را در GlobalSettings فعال/غیرفعال می‌کند.
TOGGLE_FIELD_LABELS = {
    "auto_set_2fa": "تنظیم پسورد دوم (2FA)",
    "terminate_sessions": "خروج از سایر نشست‌ها",
    "auto_set_name": "تنظیم نام",
    "auto_set_bio": "تنظیم بیو",
    "auto_set_photo": "تنظیم عکس پروفایل",
    "public_order_access": "دسترسی عمومی به کد سفارش",
}

@router.callback_query(F.data.startswith("toggle_") & F.data.endswith("/"))
async def toggle_boolean_settings(callback: types.CallbackQuery, state: FSMContext, session: AsyncSession) -> None:
    field_name = callback.data.replace("toggle_", "").replace("/", "")

    # 🛡 فاز ۲: خواندن تنظیمات بدون حفاظ بود
    try:
        stmt = select(GlobalSettings).limit(1)
        result = await session.execute(stmt)
        settings = result.scalar_one_or_none()
    except Exception as e:
        await session.rollback()
        return await answer_callback_error(
            callback, report_db_error("تنظیمات", e), get_main_menu_button()
        )

    if not settings:
        return await callback.answer("⚠️ تنظیمات یافت نشد.", show_alert=True)

    # 🛡 گارد امنیتی: فقط فیلدهای تعریف‌شده در TOGGLE_FIELD_LABELS و موجود روی مدل
    # قابل toggle هستند تا کال‌بک جعلی (مثل toggle_id/) نتواند ستون‌های غیربولی
    # GlobalSettings را دستکاری کند.
    if field_name not in TOGGLE_FIELD_LABELS or not hasattr(settings, field_name):
        return await callback.answer("❌ فیلد نامعتبر است.", show_alert=True)

    current_value = getattr(settings, field_name)
    new_value = not current_value

    # 🛡 فاز ۲: commit بدون error handling بود (مشکل ۱) — خطای قفل/اتصال
    # دیتابیس باعث کرش بی‌صدای هندلر می‌شد و toggle ظاهراً «کاری نکرد»
    try:
        setattr(settings, field_name, new_value)
        await session.commit()
    except Exception as e:
        await session.rollback()
        return await answer_callback_error(
            callback, report_db_error("تنظیمات", e), get_main_menu_button()
        )

    status_text = "فعال" if new_value else "غیرفعال"
    field_label = TOGGLE_FIELD_LABELS[field_name]
    await safe_callback_answer(callback, f"✅ وضعیت «{field_label}» به {status_text} تغییر یافت.")

    # پاس دادن state واقعی (show_settings_menu حالا با safe_callback_answer
    # شروع می‌شود، پس answer دوباره‌ای در کار نیست)
    await show_settings_menu(callback, state, session)


# ==========================================
# ADD CATEGORY FLOW
# ==========================================
@router.callback_query(F.data == "settings_add_cat/")
async def add_category_start(callback: types.CallbackQuery, state: FSMContext) -> None:
    await callback.answer()

    await cleanup_fsm_temp_files(state)
    await state.clear()

    await state.set_state(SettingsStates.waiting_for_category)

    await safe_edit_or_answer(
        callback.message,
        with_cancel_hint(
            "📁 <b>افزودن دسته‌بندی جدید</b>\n\n"
            "لطفاً نام دسته‌بندی را وارد کنید:"
        ),
        reply_markup=get_settings_cancel_keyboard()
    )

@router.message(SettingsStates.waiting_for_category, F.text)
async def process_add_category(message: types.Message, state: FSMContext, session: AsyncSession) -> None:
    cat_name = message.text.strip()

    if not cat_name:
        return await message.answer(
            with_cancel_hint("⚠️ نام دسته‌بندی نمی‌تواند خالی باشد."),
            reply_markup=get_settings_cancel_keyboard()
        )

    if len(cat_name) > 50:
        return await message.answer(
            with_cancel_hint("⚠️ نام دسته‌بندی حداکثر ۵۰ کاراکتر است."),
            reply_markup=get_settings_cancel_keyboard()
        )

    if cat_name.lower() == "default":
        return await message.answer(
            with_cancel_hint("⚠️ این نام برای سیستم رزرو شده است. نام دیگری انتخاب کنید."),
            reply_markup=get_settings_cancel_keyboard()
        )

    # 🛡 فاز ۲: کل بخش دیتابیس (بررسی تکراری + درج) در یک try — خطای خواندن هم

    # 🛡 فاز ۲: کل بخش دیتابیس (بررسی تکراری + درج) در یک try — خطای خواندن هم
    # پوشش داده می‌شود؛ پیام خطا بر اساس «نوع» خطا انتخاب می‌شود (مشکل ۱):
    #   تکراری (race condition روی IntegrityError) → «قبلاً ثبت شده است»
    #   قطع اتصال → «چند لحظه بعد تلاش کنید»
    try:
        stmt = select(Category).where(Category.name == cat_name)
        existing_cat = await session.scalar(stmt)

        if existing_cat:
            # کاربر همچنان در state است؛ کیبورد انصراف + راهنمای /cancel
            return await message.answer(
                with_cancel_hint("⚠️ این دسته‌بندی از قبل وجود دارد! یک نام دیگر وارد کنید."),
                reply_markup=get_settings_cancel_keyboard()
            )

        session.add(Category(name=cat_name))
        await session.commit()
    except Exception as e:
        await session.rollback()
        # 🟣 رفتار فاز ۴ حفظ شد: state پاک نمی‌شود تا کاربر همان‌جا دوباره تلاش کند
        return await message.answer(
            with_cancel_hint(report_db_error("دسته‌بندی", e)),
            reply_markup=get_settings_cancel_keyboard()
        )

    await state.clear()
    await message.answer(
        "✅ با موفقیت اضافه شد",
        reply_markup=get_settings_return_keyboard()
    )


# ==========================================
# ADD PROXIES FLOW
# ==========================================
async def _fetch_existing_proxy_strings(session: AsyncSession, candidates: list[str]) -> set[str]:
    """
    🛡 فاز ۲: واکشی پراکسی‌های موجود در دیتابیس از میان لیست کاندیدها.

    کوئری به صورت chunk اجرا می‌شود تا در دیتابیس‌هایی که محدودیت تعداد پارامتر
    دارند (مثل SQLite با سقف ~۹۹۹ پارامتر)، لیست‌های بزرگ باعث خطای
    "too many SQL variables" نشوند.
    """
    existing: set[str] = set()
    CHUNK_SIZE = 500
    for i in range(0, len(candidates), CHUNK_SIZE):
        chunk = candidates[i:i + CHUNK_SIZE]
        stmt = select(Proxy.proxy_string).where(Proxy.proxy_string.in_(chunk))
        existing.update(await session.scalars(stmt))
    return existing


@router.callback_query(F.data == "settings_add_proxy/")
async def ask_for_proxies(callback: types.CallbackQuery, state: FSMContext) -> None:
    await callback.answer()

    await cleanup_fsm_temp_files(state)
    await state.clear()

    await state.set_state(SettingsStates.waiting_for_proxies)
    await safe_edit_or_answer(
        callback.message,
        with_cancel_hint(
            "🌐 <b>افزودن پراکسی</b>\n\n"
            "لطفاً لیست پراکسی‌های خود را (هر کدام در یک خط) ارسال کنید.\n"
            "<i>فرمت قابل قبول: socks5://user:pass@ip:port یا socks5://ip:port</i>"
        ),
        reply_markup=get_settings_cancel_keyboard()
    )

@router.message(SettingsStates.waiting_for_proxies, F.text)
async def process_new_proxies(message: types.Message, state: FSMContext, session: AsyncSession) -> None:
    raw_proxies = message.text.strip().split('\n')

    # ── ۱. اعتبارسنجی فرمت + حذف تکراری‌های داخل خود پیام ──
    seen_in_message: set[str] = set()
    valid_proxies: list[str] = []
    invalid_count = 0
    duplicate_in_message_count = 0

    for line in raw_proxies:
        proxy_str = line.strip()
        if not proxy_str:
            continue
        if not parse_proxy_string(proxy_str):
            invalid_count += 1
            continue
        if proxy_str in seen_in_message:
            duplicate_in_message_count += 1
            continue
        seen_in_message.add(proxy_str)
        valid_proxies.append(proxy_str)

    # ── ۲. هیچ پراکسی معتبری نیست → کاربر در state می‌ماند و لیست اصلاح‌شده
    #        را دوباره می‌فرستد (به جای پایان فلو با پیام «۰ پراکسی اضافه شد»)
    if not valid_proxies:
        parts = ["⚠️ <b>هیچ پراکسی معتبری در لیست ارسالی یافت نشد.</b>"]
        if invalid_count > 0:
            parts.append(f"🔸 تعداد <b>{invalid_count}</b> خط به دلیل فرمت نامعتبر رد شد (فقط socks4/socks5 مجاز است).")
        if duplicate_in_message_count > 0:
            parts.append(f"🔸 تعداد <b>{duplicate_in_message_count}</b> خط تکراری بود.")
        parts.append("لطفاً لیست اصلاح‌شده را دوباره ارسال کنید.")
        return await message.answer(
            with_cancel_hint("\n".join(parts)),
            reply_markup=get_settings_cancel_keyboard()
        )

    # ── ۳. درج در دیتابیس — 🛡 فاز ۲: یک commit اتمیک برای کل لیست.
    #        نسخه قبلی خط‌به‌خط commit می‌کرد: خطای وسط لیست = ثبت نصفه‌کاره،
    #        و پراکسی‌های تکراری بی‌صدا drop می‌شدند (بدون اطلاع به کاربر)
    try:
        existing_set = await _fetch_existing_proxy_strings(session, valid_proxies)
        new_proxies = [p for p in valid_proxies if p not in existing_set]
        duplicate_in_db_count = len(valid_proxies) - len(new_proxies)

        if new_proxies:
            session.add_all(
                [Proxy(proxy_string=p, is_active=True, fail_count=0) for p in new_proxies]
            )
            await session.commit()
    except Exception as e:
        await session.rollback()
        await state.clear()
        return await message.answer(
            report_db_error("پراکسی‌ها", e),
            reply_markup=get_settings_return_keyboard()
        )

    await state.clear()

    added_count = len(new_proxies)
    response_parts = [f"✅ تعداد <b>{added_count}</b> پراکسی جدید و یونیک به استخر شبکه اضافه شد."]
    if duplicate_in_db_count > 0:
        response_parts.append(f"♻️ تعداد <b>{duplicate_in_db_count}</b> پراکسی از قبل در استخر موجود بود و دوباره ثبت نشد.")
    if duplicate_in_message_count > 0:
        response_parts.append(f"♻️ تعداد <b>{duplicate_in_message_count}</b> خط تکراری در خود لیست نادیده گرفته شد.")
    if invalid_count > 0:
        response_parts.append(f"⚠️ تعداد <b>{invalid_count}</b> پراکسی به دلیل فرمت نامعتبر (فقط socks4/socks5 مجاز است) رد شدند.")

    builder = InlineKeyboardBuilder()
    builder.button(text="🌐 افزودن پراکسی دیگر", callback_data="settings_add_proxy/")
    builder.button(text="⚙️ بازگشت به تنظیمات", callback_data="menu_settings/")
    builder.button(text="🏛 منوی اصلی", callback_data="menu_home/")
    builder.adjust(1, 2)
    await message.answer("\n".join(response_parts), reply_markup=builder.as_markup())


# ==========================================
# EDIT SEND LIMIT FLOW
# ==========================================
@router.callback_query(F.data == "settings_edit_limit/")
async def ask_for_send_limit(callback: types.CallbackQuery, state: FSMContext) -> None:
    await callback.answer()

    await cleanup_fsm_temp_files(state)
    await state.clear()

    await state.set_state(SettingsStates.waiting_for_send_limit)
    await safe_edit_or_answer(
        callback.message,
        with_cancel_hint(
            "⚙️ <b>تغییر ظرفیت ارسال</b>\n\n"
            "لطفاً یک عدد وارد کنید (تعداد پیامی که هر ورکر در یک دوره اجرای سفارش ارسال می‌کند، پیش‌فرض ۴۰):"
        ),
        reply_markup=get_settings_cancel_keyboard()
    )

@router.message(SettingsStates.waiting_for_send_limit, F.text)
async def process_new_send_limit(message: types.Message, state: FSMContext, session: AsyncSession) -> None:
    if not message.text.isdecimal():
        return await message.answer(
            with_cancel_hint("⚠️ لطفاً فقط یک عدد صحیح ارسال کنید."),
            reply_markup=get_settings_cancel_keyboard()
        )

    new_limit = int(message.text)

    if not (1 <= new_limit <= 500):
        return await message.answer(
            with_cancel_hint("⚠️ مقدار وارد شده باید بین ۱ تا ۵۰۰ باشد."),
            reply_markup=get_settings_cancel_keyboard()
        )

    try:
        settings = (await session.scalars(select(GlobalSettings).limit(1))).first()
        if settings is None:
            await state.clear()
            return await message.answer("❌ ردیف تنظیمات در دیتابیس یافت نشد. لطفاً با پشتیبانی تماس بگیرید.", reply_markup=get_settings_return_keyboard())
        
        settings.send_limit_per_run = new_limit
        await session.commit()
    except Exception as e:
        await session.rollback()
        await state.clear()
        return await message.answer(
            report_db_error("تنظیمات", e),
            reply_markup=get_settings_return_keyboard()
        )

    await state.clear()
    await message.answer(
        f"✅ محدودیت ارسال هر ورکر به <b>{new_limit}</b> تغییر یافت.",
        reply_markup=get_settings_return_keyboard()
    )


@router.message(SettingsStates.waiting_for_max_accounts_api, F.text)
async def process_max_accounts_api(message: types.Message, state: FSMContext, session: AsyncSession) -> None:
    if not message.text.isdecimal():
        return await message.answer(
            with_cancel_hint("⚠️ لطفاً فقط یک عدد صحیح ارسال کنید."),
            reply_markup=get_settings_cancel_keyboard()
        )

    new_value = int(message.text)

    if not (1 <= new_value <= 100):
        return await message.answer(
            with_cancel_hint("⚠️ مقدار وارد شده باید بین ۱ تا ۱۰۰ باشد."),
            reply_markup=get_settings_cancel_keyboard()
        )

    try:
        settings = (await session.scalars(select(GlobalSettings).limit(1))).first()
        if settings is None:
            await state.clear()
            return await message.answer("❌ ردیف تنظیمات در دیتابیس یافت نشد. لطفاً با پشتیبانی تماس بگیرید.", reply_markup=get_settings_return_keyboard())
        
        settings.max_accounts_per_api = new_value
        await session.commit()
    except Exception as e:
        await session.rollback()
        await state.clear()
        return await message.answer(
            report_db_error("تنظیمات", e),
            reply_markup=get_settings_return_keyboard()
        )

    await state.clear()
    await message.answer(
        f"✅ ظرفیت ثبت‌نام روی هر API به <b>{new_value}</b> تغییر یافت.",
        reply_markup=get_settings_return_keyboard()
    )


@router.message(SettingsStates.waiting_for_cooldown_hours, F.text)
async def process_cooldown_hours(message: types.Message, state: FSMContext, session: AsyncSession) -> None:
    if not message.text.isdecimal():
        return await message.answer(
            with_cancel_hint("⚠️ لطفاً فقط یک عدد صحیح ارسال کنید."),
            reply_markup=get_settings_cancel_keyboard()
        )

    new_value = int(message.text)

    if not (0 <= new_value <= 720):
        return await message.answer(
            with_cancel_hint("⚠️ مقدار وارد شده باید بین ۰ تا ۷۲۰ باشد."),
            reply_markup=get_settings_cancel_keyboard()
        )

    try:
        settings = (await session.scalars(select(GlobalSettings).limit(1))).first()
        if settings is None:
            await state.clear()
            return await message.answer("❌ ردیف تنظیمات در دیتابیس یافت نشد. لطفاً با پشتیبانی تماس بگیرید.", reply_markup=get_settings_return_keyboard())
        
        settings.cooldown_hours = new_value
        await session.commit()
    except Exception as e:
        await session.rollback()
        await state.clear()
        return await message.answer(
            report_db_error("تنظیمات", e),
            reply_markup=get_settings_return_keyboard()
        )

    await state.clear()
    success_text = f"✅ زمان استراحت دوره‌ای اکانت‌ها به <b>{new_value} ساعت</b> تغییر یافت."
    if new_value == 0:
        success_text += "\n⚠️ مقدار ۰ یعنی استراحت دوره‌ای بین chunkها خاموش می‌شود."
        
    await message.answer(
        success_text,
        reply_markup=get_settings_return_keyboard()
    )


@router.message(SettingsStates.waiting_for_spam_penalty, F.text)
async def process_spam_penalty(message: types.Message, state: FSMContext, session: AsyncSession) -> None:
    if not message.text.isdecimal():
        return await message.answer(
            with_cancel_hint("⚠️ لطفاً فقط یک عدد صحیح ارسال کنید."),
            reply_markup=get_settings_cancel_keyboard()
        )

    new_value = int(message.text)

    if not (0 <= new_value <= 30):
        return await message.answer(
            with_cancel_hint("⚠️ مقدار وارد شده باید بین ۰ تا ۳۰ باشد."),
            reply_markup=get_settings_cancel_keyboard()
        )

    try:
        settings = (await session.scalars(select(GlobalSettings).limit(1))).first()
        if settings is None:
            await state.clear()
            return await message.answer("❌ ردیف تنظیمات در دیتابیس یافت نشد. لطفاً با پشتیبانی تماس بگیرید.", reply_markup=get_settings_return_keyboard())
        
        settings.spam_penalty_days = new_value
        await session.commit()
    except Exception as e:
        await session.rollback()
        await state.clear()
        return await message.answer(
            report_db_error("تنظیمات", e),
            reply_markup=get_settings_return_keyboard()
        )

    await state.clear()
    success_text = f"✅ مدت زمان جریمه اسپم به <b>{new_value} روز</b> تغییر یافت."
    if new_value == 0:
        success_text += "\n⚠️ مقدار ۰ یعنی جریمهٔ FloodWait عملاً غیرفعال می‌شود."
        
    await message.answer(
        success_text,
        reply_markup=get_settings_return_keyboard()
    )


# ==========================================
# REPORTS & MANAGEMENT: CATEGORIES LIST
# (📄 فاز ۲: صفحه‌بندی استاندارد + دکمه‌های «✏️ ویرایش» و «🗑 حذف»)
# ==========================================
def _truncate_cat_label(name: str, max_len: int = 12) -> str:
    """
    📄 فاز ۲: کوتاه‌سازی نام دسته برای برچسب دکمه‌های دو ستونی
    (دکمه‌های «ویرایش» و «حذف» در یک ردیف قرار می‌گیرند؛ نام کامل
    در متن پیام بالای کیبورد نمایش داده می‌شود).
    """
    return name if len(name) <= max_len else name[: max_len - 1] + "…"


async def render_categories_list(
    callback: types.CallbackQuery,
    session: AsyncSession,
    state: Optional[FSMContext] = None,
    page: int = 1,
) -> None:
    """
    📄 فاز ۲: رندر لیست دسته‌بندی‌ها با صفحه‌بندی استاندارد.

    - ۱۰ دسته در هر صفحه (PAGINATION_SIZE) با کوئری‌های efficient:
      یک COUNT برای کل تعداد + یک OFFSET/LIMIT روی همان کوئری تجمعی
      (JOIN + GROUP BY) برای صفحهٔ فعلی — قبلاً همهٔ دسته‌ها یک‌جا
      واکشی می‌شدند
    - هر دسته دو دکمه دارد: «✏️ ویرایش» و «🗑 حذف» (در یک ردیف)
    - ردیف ناوبری صفحات + ➕ دستهٔ جدید + 🔄 بروزرسانی + 🏛 منوی اصلی

    نکته: این تابع callback را answer نمی‌کند؛ همهٔ فراخوانی‌کننده‌ها باید
    قبل از فراخوانش callback را پاسخ داده باشند (الگوی فاز ۱).
    """
    # 🛡 الگوی استاندارد خطای دیتابیس (تابع از چند هندلر صدا زده می‌شود)
    try:
        # ۱. شمارش کل دسته‌ها (برای محاسبهٔ تعداد صفحات)
        total_count = await session.scalar(select(func.count(Category.id))) or 0
        total_pages = calculate_total_pages(total_count)

        # اصلاح هوشمند شماره صفحه (مثلاً بعد از حذف آخرین دستهٔ صفحهٔ آخر)
        page = clamp_page(page, total_pages)
        offset = get_page_offset(page)

        # ۲. آیتم‌های صفحهٔ فعلی + تعداد اکانت‌های متصل (همان JOIN تجمعی قبلی)
        stmt = (
            select(Category, func.count(Account.id).label("acc_count"))
            .outerjoin(Account)
            .group_by(Category.id)
            .order_by(Category.id.asc())
            .offset(offset)
            .limit(PAGINATION_SIZE)
        )
        rows = (await session.execute(stmt)).all()
    except Exception as e:
        await session.rollback()
        return await answer_callback_error(
            callback, report_db_error("دسته‌بندی‌ها", e), get_main_menu_button()
        )

    # 📄 فاز ۲: ثبت صفحهٔ فعلی در FSM برای بازگشت درست بعد از حذف/ویرایش/انصراف
    if state is not None:
        await state.update_data(categories_list_page=page)

    builder = InlineKeyboardBuilder()

    # ── لیست خالی ──
    if total_count == 0:
        builder.button(text="➕ افزودن دسته‌بندی جدید", callback_data="settings_add_cat/")
        builder.button(text="⚙️ بازگشت به تنظیمات", callback_data="menu_settings/")
        builder.button(text="🏛 منوی اصلی", callback_data="menu_home/")
        builder.adjust(1)
        return await safe_edit_or_answer(
            callback.message,
            "📁 <b>لیست دسته‌بندی‌ها</b>\n\n"
            "⚠️ هیچ دسته‌بندی یافت نشد.\n\n"
            "برای شروع، یک دسته‌بندی جدید بسازید:",
            reply_markup=builder.as_markup()
        )

    text = (
        "📁 <b>لیست دسته‌بندی‌ها</b>\n"
        f"🔢 مجموع: <b>{total_count}</b> دسته‌بندی\n"
        f"📄 صفحهٔ <b>{page}</b> از <b>{total_pages}</b>\n\n"
    )

    if not rows:
        # نتیجهٔ شمارش با آیتم‌های صفحه هم‌خوان نیست (مثلاً حذف هم‌زمان توسط
        # نشست دیگر) — دکمهٔ 🔄 بروزرسانی در همان کیبورد موجود است
        text += "⚠️ موردی در این صفحه یافت نشد. لطفاً با دکمهٔ 🔄 بروزرسانی دوباره تلاش کنید.\n"
    else:
        for idx, row in enumerate(rows, start=offset + 1):
            cat = row.Category
            acc_count = row.acc_count

            # 🛡 escape نام دسته (ورودی کاربر — می‌تواند شامل < یا & باشد)
            text += f"{idx}. <b>{html.escape(cat.name)}</b> — 👥 {acc_count} اکانت\n"

            btn_label = _truncate_cat_label(cat.name)
            builder.button(text=f"✏️ ویرایش {btn_label}", callback_data=f"edit_cat_{cat.id}/")
            builder.button(text=f"🗑 حذف {btn_label}", callback_data=f"del_cat_{cat.id}/")

        # چیدمان جفتی: [✏️ ویرایش X] [🗑 حذف X] در هر ردیف
        builder.adjust(2)

        text += "\n👇 برای «ویرایش» یا «حذف» هر دسته‌بندی، روی دکمهٔ مربوطه کلیک کنید:"

    # ── ردیف ناوبری صفحات (فقط وقتی بیش از یک صفحه باشد) ──
    add_pagination_nav_row(builder, page, total_pages, callback_prefix="list_categories_")

    # ── افزودن سریع دستهٔ جدید ──
    builder.row(types.InlineKeyboardButton(text="➕ دسته‌بندی جدید", callback_data="settings_add_cat/"))

    # ── دکمه‌های پایانی: 🔄 بروزرسانی (رفرش همان صفحه) + 🏛 منوی اصلی ──
    add_list_footer(builder, refresh_callback=f"list_categories_page_{page}/")

    # 🛡 ویرایش امن
    await safe_edit_or_answer(callback.message, text, reply_markup=builder.as_markup())


@router.callback_query(F.data == "menu_list_categories/")
async def list_categories_handler(callback: types.CallbackQuery, state: FSMContext, session: AsyncSession) -> None:
    await safe_callback_answer(callback)

    await cleanup_fsm_temp_files(state)
    await state.clear()

    # 📄 فاز ۲: ورود به لیست همیشه از صفحهٔ ۱ (renderer صفحه را در FSM ذخیره می‌کند)
    await render_categories_list(callback, session, state=state, page=1)


@router.callback_query(F.data.startswith("list_categories_page_"))
async def list_categories_paginated_handler(callback: types.CallbackQuery, state: FSMContext, session: AsyncSession) -> None:
    """
    📄 فاز ۲: هندلر مشترک «صفحه بعد/قبل» و «🔄 بروزرسانی» — دکمهٔ بروزرسانی
    دقیقاً همان کال‌بک صفحهٔ فعلی را صدا می‌زند، پس یک هندلر هر دو را پوشش می‌دهد.
    """
    await safe_callback_answer(callback)

    page = parse_page_from_callback(callback.data)
    await render_categories_list(callback, session, state=state, page=page)


# ==========================================
# 📄 فاز ۲: EDIT CATEGORY FLOW (ویرایش نام دسته‌بندی)
# ==========================================
def get_cat_edit_cancel_keyboard():
    """
    📄 فاز ۲: کیبورد انصراف فلوی ویرایش دسته‌بندی.
    دکمهٔ انصراف به هندلر اختصاصی cancel_cat_edit/ متصل است که به همان
    صفحهٔ لیست برمی‌گردد (برخلاف کیبورد عمومی تنظیمات).
    """
    builder = InlineKeyboardBuilder()
    builder.button(text="❌ انصراف", callback_data="cancel_cat_edit/")
    builder.button(text="🏛 منوی اصلی", callback_data="menu_home/")
    builder.adjust(2)
    return builder.as_markup()


@router.callback_query(F.data.startswith("edit_cat_") & F.data.endswith("/"))
async def edit_category_start(callback: types.CallbackQuery, state: FSMContext, session: AsyncSession) -> None:
    cat_id_str = callback.data.replace("edit_cat_", "").replace("/", "")

    if not cat_id_str.isdigit():
        return await safe_callback_answer(callback, "⚠️ آیدی نامعتبر.", show_alert=True)

    cat_id = int(cat_id_str)

    # 📄 فاز ۲: صفحهٔ فعلی را «قبل از» پاک کردن state ذخیره می‌کنیم
    fsm_data = await state.get_data()
    current_page = fsm_data.get("categories_list_page", 1)

    # 🛡 الگوی استاندارد خطای دیتابیس
    try:
        category = await session.scalar(select(Category).where(Category.id == cat_id))
    except Exception as e:
        await session.rollback()
        return await answer_callback_error(
            callback, report_db_error("دسته‌بندی", e), get_main_menu_button()
        )

    if not category:
        await safe_callback_answer(callback, "⚠️ این دسته‌بندی وجود ندارد یا قبلاً حذف شده است.", show_alert=True)
        return await render_categories_list(callback, session, state=state, page=current_page)

    # 🛡 هم‌راستا با محافظت حذف: دستهٔ «default» نامش قابل تغییر نیست
    # (منطق سیستم وضعیت این دسته را با نامش چک می‌کند)
    if category.name.lower() == "default":
        return await safe_callback_answer(callback, "❌ دسته‌بندی پیش‌فرض قابل ویرایش نیست.", show_alert=True)

    await safe_callback_answer(callback)

    # 🟣 الگوی استاندارد ورود به فلوی جدید: پاکسازی state قبلی
    await cleanup_fsm_temp_files(state)
    await state.clear()

    await state.update_data(edit_cat_id=cat_id, categories_list_page=current_page)
    await state.set_state(SettingsStates.waiting_for_category_edit)

    # 🛡 ویرایش امن (fallback به answer تا دکمه‌های انصراف همیشه در دسترس بمانند)
    await safe_edit_message(
        callback.message,
        with_cancel_hint(
            "✏️ <b>ویرایش دسته‌بندی</b>\n\n"
            f"🆔 شناسه: <code>{cat_id}</code>\n"
            f"📁 نام فعلی: <b>{html.escape(category.name)}</b>\n\n"
            "لطفاً نام جدید دسته‌بندی را ارسال کنید:"
        ),
        reply_markup=get_cat_edit_cancel_keyboard()
    )


@router.message(SettingsStates.waiting_for_category_edit, F.text)
async def process_category_edit(message: types.Message, state: FSMContext, session: AsyncSession) -> None:
    new_name = message.text.strip()

    fsm_data = await state.get_data()
    cat_id = fsm_data.get("edit_cat_id")
    return_page = fsm_data.get("categories_list_page", 1)

    # گارد: state بدون شناسهٔ هدف (مثلاً بعد از ری‌استارت ربات)
    if cat_id is None:
        await state.clear()
        return await message.answer(
            "⚠️ اطلاعات ویرایش یافت نشد. لطفاً از لیست دسته‌بندی‌ها مجدداً اقدام کنید.",
            reply_markup=get_settings_return_keyboard()
        )

    # ── اعتبارسنجی‌ها (کاربر در state می‌ماند و همان‌جا اصلاح می‌کند) ──
    if not new_name:
        return await message.answer(
            with_cancel_hint("⚠️ نام دسته‌بندی نمی‌تواند خالی باشد. لطفاً یک نام معتبر ارسال کنید:"),
            reply_markup=get_cat_edit_cancel_keyboard()
        )

    if len(new_name) > 50:
        return await message.answer(
            with_cancel_hint("⚠️ نام دسته‌بندی نباید بیشتر از ۵۰ کاراکتر باشد. لطفاً نام کوتاه‌تری ارسال کنید:"),
            reply_markup=get_cat_edit_cancel_keyboard()
        )

    if new_name.lower() == "default":
        return await message.answer(
            with_cancel_hint("⚠️ نام «default» برای دسته‌بندی‌ها رزرو شده است. لطفاً نام دیگری انتخاب کنید:"),
            reply_markup=get_cat_edit_cancel_keyboard()
        )

    # 🛡 کل عملیات دیتابیس (بررسی تکراری + واکشی + آپدیت) داخل یک try
    try:
        # بررسی تکراری نبودن نام (به‌جز خود دستهٔ فعلی)
        duplicate_stmt = select(Category).where(
            Category.name == new_name,
            Category.id != cat_id
        )
        existing_cat = await session.scalar(duplicate_stmt)

        if existing_cat:
            return await message.answer(
                with_cancel_hint("⚠️ این نام از قبل برای دسته‌بندی دیگری ثبت شده است. لطفاً نام دیگری ارسال کنید:"),
                reply_markup=get_cat_edit_cancel_keyboard()
            )

        # واکشی مجدد (بررسی Race Condition — دسته ممکن است هم‌زمان حذف شده باشد)
        category = await session.scalar(select(Category).where(Category.id == cat_id))
        if not category:
            await state.clear()
            return await message.answer(
                "⚠️ این دسته‌بندی وجود ندارد یا قبلاً حذف شده است.",
                reply_markup=get_settings_return_keyboard()
            )

        old_name = category.name
        category.name = new_name
        await session.commit()
    except Exception as e:
        await session.rollback()
        # state حفظ می‌شود تا کاربر همان‌جا نام دیگری ارسال کند
        return await message.answer(
            with_cancel_hint(report_db_error("دسته‌بندی", e)),
            reply_markup=get_cat_edit_cancel_keyboard()
        )

    await state.clear()

    # 📄 فاز ۲: بازگشت به همان صفحه‌ای که کاربر در آن بوده
    builder = InlineKeyboardBuilder()
    builder.button(text="🔙 بازگشت به لیست دسته‌بندی‌ها", callback_data=f"list_categories_page_{return_page}/")
    builder.button(text="⚙️ بازگشت به تنظیمات", callback_data="menu_settings/")
    builder.button(text="🏛 منوی اصلی", callback_data="menu_home/")
    builder.adjust(1)

    await message.answer(
        "✅ <b>دسته‌بندی با موفقیت ویرایش شد.</b>\n\n"
        f"📁 نام قبلی: <s>{html.escape(old_name)}</s>\n"
        f"📁 نام جدید: <b>{html.escape(new_name)}</b>",
        reply_markup=builder.as_markup()
    )


@router.callback_query(F.data == "cancel_cat_edit/")
async def cancel_category_edit(callback: types.CallbackQuery, state: FSMContext, session: AsyncSession) -> None:
    fsm_data = await state.get_data()
    return_page = fsm_data.get("categories_list_page", 1)

    # فقط اگر واقعاً در فلوی ویرایش هستیم state پاک شود (دکمهٔ قدیمی/تکراری)
    if await state.get_state() == SettingsStates.waiting_for_category_edit:
        await state.clear()

    await safe_callback_answer(callback, "🚫 عملیات ویرایش دسته‌بندی لغو شد.")

    return await render_categories_list(callback, session, state=state, page=return_page)


# ==========================================
# 🟠 فاز ۳ — توابع کمکی مشترک (بدون تغییر)
# ==========================================
def build_cat_delete_confirmation_text(cat_name: str, cat_id: int, acc_count: int, order_count: int) -> str:
    safe_name = html.escape(cat_name)

    # 🛡 اصلاح فاز ۱۰: نمایش مرحله تایید دوم (اخطار) برای دسته‌بندی‌هایی که اکانت متصل دارند[cite: 1]
    if acc_count > 0:
        return (
            "⚠️ <b>تأیید حذف دسته‌بندی (دارای اکانت)</b>\n\n"
            f"آیا از حذف دسته‌بندی «<b>{safe_name}</b>» مطمئن هستید؟\n\n"
            f"🆔 شناسه: <code>{cat_id}</code>\n"
            f"📁 نام: <code>{safe_name}</code>\n"
            f"👥 اکانت‌های متصل: <b>{acc_count}</b>\n"
            f"🛍 سفارشات متصل: <b>{order_count}</b>\n\n"
            f"⚠️ <b>این دسته {acc_count} اکانت دارد؛ با حذف دسته، اکانت‌ها حفظ می‌شوند اما بدون دسته قرار می‌گیرند. ادامه می‌دهید؟</b>"
        )

    return (
        "⚠️ <b>تأیید حذف دسته‌بندی</b>\n\n"
        f"آیا از حذف دسته‌بندی «<b>{safe_name}</b>» مطمئن هستید؟\n\n"
        f"🆔 شناسه: <code>{cat_id}</code>\n"
        f"📁 نام: <code>{safe_name}</code>\n"
        f"👥 اکانت‌های متصل: <b>{acc_count}</b>\n"
        f"🛍 سفارشات متصل: <b>{order_count}</b>\n\n"
        "⚠️ <b>توجه: این عمل قابل بازگشت نیست.</b>"
    )


def build_cat_delete_confirmation_keyboard(cat_id: int) -> types.InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ بله، حذف کن", callback_data=f"confirm_delete_cat_{cat_id}/")
    builder.button(text="❌ انصراف", callback_data="cancel_confirm_delete_cat/")
    builder.adjust(2)
    return builder.as_markup()


# ==========================================
# 🟠 فاز ۳ — مرحله ۱ (مسیر دکمه inline): DELETE CATEGORY
# (📄 فاز ۲: ذخیره صفحهٔ فعلی برای بازگشت بعد از حذف/انصراف)
# ==========================================
@router.callback_query(F.data.startswith("del_cat_") & F.data.endswith("/"))
async def delete_category_handler(callback: types.CallbackQuery, state: FSMContext, session: AsyncSession) -> None:
    cat_id_str = callback.data.replace("del_cat_", "").replace("/", "")

    if not cat_id_str.isdigit():
        return await callback.answer("⚠️ آیدی نامعتبر.", show_alert=True)

    cat_id = int(cat_id_str)

    try:
        stmt = select(Category).options(
            selectinload(Category.accounts),
            selectinload(Category.orders)
        ).where(Category.id == cat_id)
        result = await session.execute(stmt)
        category = result.scalar_one_or_none()
    except Exception as e:
        await session.rollback()
        return await answer_callback_error(
            callback, report_db_error("دسته‌بندی", e), get_main_menu_button()
        )

    if not category:
        await callback.answer("⚠️ این دسته‌بندی وجود ندارد یا قبلاً حذف شده است.", show_alert=True)
        fsm_data = await state.get_data()
        return await render_categories_list(
            callback, session, state=state,
            page=fsm_data.get("categories_list_page", 1)
        )

    if category.name.lower() == "default":
        return await callback.answer("❌ دسته‌بندی پیش‌فرض قابل حذف نیست.", show_alert=True)

    # 🛡 اصلاح فاز ۱۰: فقط در صورت وجود سفارش، عملیات حذف را مسدود کن[cite: 1]
    if len(category.orders) > 0:
        return await callback.answer(
            "⛔️ امکان حذف وجود ندارد!\nسفارشی به این دسته‌بندی متصل است. ابتدا آن‌ها را پاک کنید.",
            show_alert=True
        )

    await callback.answer()

    fsm_data = await state.get_data()
    await cleanup_fsm_temp_files(state)
    await state.update_data(
        confirm_action="delete_cat",
        target_id=cat_id,
        return_page=fsm_data.get("categories_list_page", 1),
    )
    await state.set_state(ConfirmStates.waiting_for_confirmation)

    await safe_edit_message(
        callback.message,
        build_cat_delete_confirmation_text(category.name, cat_id, len(category.accounts), len(category.orders)),
        reply_markup=build_cat_delete_confirmation_keyboard(cat_id)
    )


# ==========================================
# 🟠 فاز ۳ — مرحله ۲ (اجرای واقعی)
# (📄 فاز ۲: بازگشت به همان صفحهٔ قبلی بعد از حذف)
# ==========================================
@router.callback_query(F.data.startswith("confirm_delete_cat_") & F.data.endswith("/"))
async def confirm_delete_category_handler(callback: types.CallbackQuery, state: FSMContext, session: AsyncSession) -> None:
    current_state = await state.get_state()
    fsm_data = await state.get_data()

    if current_state != ConfirmStates.waiting_for_confirmation or fsm_data.get("confirm_action") != "delete_cat":
        return await callback.answer(
            "⚠️ این درخواست تأیید منقضی شده است. لطفاً از ابتدا اقدام کنید.",
            show_alert=True
        )

    cat_id_str = callback.data.replace("confirm_delete_cat_", "").replace("/", "")

    if not cat_id_str.isdigit():
        return await callback.answer("⚠️ آیدی نامعتبر.", show_alert=True)

    cat_id = int(cat_id_str)

    if fsm_data.get("target_id") != cat_id:
        return await callback.answer(
            "⚠️ این درخواست تأیید با دسته‌بندی نمایش‌داده‌شده مطابقت ندارد. لطفاً از ابتدا اقدام کنید.",
            show_alert=True
        )

    return_page = fsm_data.get("return_page", 1)

    try:
        stmt = select(Category).options(
            selectinload(Category.accounts),
            selectinload(Category.orders)
        ).where(Category.id == cat_id)
        result = await session.execute(stmt)
        category = result.scalar_one_or_none()
    except Exception as e:
        await session.rollback()
        await state.clear()
        return await answer_callback_error(
            callback, report_db_error("دسته‌بندی", e), get_main_menu_button()
        )

    if not category:
        await state.clear()
        await callback.answer("⚠️ این دسته‌بندی وجود ندارد یا قبلاً حذف شده است.", show_alert=True)
        return await render_categories_list(callback, session, state=state, page=return_page)

    if category.name.lower() == "default":
        await state.clear()
        return await callback.answer("❌ دسته‌بندی پیش‌فرض قابل حذف نیست.", show_alert=True)

    # 🛡 اصلاح فاز ۱۰: چک نهایی در سطح دیتابیس (قبل از کامیت) برای جلوگیری از باگ اگر سفارشی وجود داشته باشد[cite: 1]
    if len(category.orders) > 0:
        await state.clear()
        return await callback.answer(
            "⛔️ امکان حذف وجود ندارد!\nسفارشی به این دسته‌بندی متصل شده است. ابتدا آن‌ها را پاک کنید.",
            show_alert=True
        )

    cat_name = category.name

    try:
        await session.delete(category)
        await session.commit()
    except Exception as e:
        await session.rollback()
        await state.clear()
        await safe_callback_answer(callback, report_db_error("دسته‌بندی", e), show_alert=True)
        return await render_categories_list(callback, session, state=state, page=return_page)

    await state.clear()
    await callback.answer(f"✅ دسته‌بندی {cat_name} با موفقیت حذف شد.", show_alert=False)

    return await render_categories_list(callback, session, state=state, page=return_page)


# ==========================================
# 🟠 فاز ۳ — انصراف از حذف دسته‌بندی
# (📄 فاز ۲: بازگشت به همان صفحهٔ قبلی)
# ==========================================
@router.callback_query(F.data == "cancel_confirm_delete_cat/")
async def cancel_delete_category_confirmation(callback: types.CallbackQuery, state: FSMContext, session: AsyncSession) -> None:
    fsm_data = await state.get_data()
    return_page = fsm_data.get("return_page", 1)

    if fsm_data.get("confirm_action") == "delete_cat":
        await state.clear()

    await callback.answer("🚫 عملیات حذف دسته‌بندی لغو شد.")

    return await render_categories_list(callback, session, state=state, page=return_page)


# ==========================================
# EDIT MAX ACCOUNTS PER API
# ==========================================
@router.callback_query(F.data == "settings_edit_max_api/")
async def ask_max_accounts_api(callback: types.CallbackQuery, state: FSMContext) -> None:
    await callback.answer()

    await cleanup_fsm_temp_files(state)
    await state.clear()

    await state.set_state(SettingsStates.waiting_for_max_accounts_api)
    await safe_edit_or_answer(
        callback.message,
        with_cancel_hint(
            "⚙️ <b>تنظیم ظرفیت API</b>\n\n"
            "حداکثر تعداد اکانتی که مجاز است روی یک API لاگین کند را وارد کنید (توصیه: ۱):"
        ),
        reply_markup=get_settings_cancel_keyboard()
    )

@router.message(SettingsStates.waiting_for_max_accounts_api, F.text)
async def process_max_accounts_api(message: types.Message, state: FSMContext, session: AsyncSession) -> None:
    if not message.text.isdecimal():
        return await message.answer(
            with_cancel_hint("⚠️ لطفاً فقط یک عدد صحیح ارسال کنید."),
            reply_markup=get_settings_cancel_keyboard()
        )

    new_value = int(message.text)

    if not (1 <= new_value <= 100):
        return await message.answer(
            with_cancel_hint("⚠️ مقدار وارد شده باید بین ۱ تا ۱۰۰ باشد."),
            reply_markup=get_settings_cancel_keyboard()
        )

    try:
        settings = (await session.scalars(select(GlobalSettings).limit(1))).first()
        if settings is None:
            await state.clear()
            return await message.answer("❌ ردیف تنظیمات در دیتابیس یافت نشد. لطفاً با پشتیبانی تماس بگیرید.", reply_markup=get_settings_return_keyboard())
        
        settings.max_accounts_per_api = new_value
        await session.commit()
    except Exception as e:
        await session.rollback()
        await state.clear()
        return await message.answer(
            report_db_error("تنظیمات", e),
            reply_markup=get_settings_return_keyboard()
        )

    await state.clear()
    await message.answer(
        f"✅ ظرفیت ثبت‌نام روی هر API به <b>{new_value}</b> تغییر یافت.",
        reply_markup=get_settings_return_keyboard()
    )


# ==========================================
# EDIT COOLDOWN HOURS
# ==========================================
@router.callback_query(F.data == "settings_edit_cooldown/")
async def ask_cooldown_hours(callback: types.CallbackQuery, state: FSMContext) -> None:
    await callback.answer()

    await cleanup_fsm_temp_files(state)
    await state.clear()

    await state.set_state(SettingsStates.waiting_for_cooldown_hours)
    await safe_edit_or_answer(
        callback.message,
        with_cancel_hint(
            "⚙️ <b>تنظیم زمان استراحت دوره‌ای</b>\n\n"
            "اکانت‌ها هر چند ساعت یک‌بار مجاز به استفاده مجدد باشند؟ (مثلاً ۲۶):"
        ),
        reply_markup=get_settings_cancel_keyboard()
    )

@router.message(SettingsStates.waiting_for_cooldown_hours, F.text)
async def process_cooldown_hours(message: types.Message, state: FSMContext, session: AsyncSession) -> None:
    if not message.text.isdecimal():
        return await message.answer(
            with_cancel_hint("⚠️ لطفاً فقط یک عدد صحیح ارسال کنید."),
            reply_markup=get_settings_cancel_keyboard()
        )

    new_value = int(message.text)

    if not (0 <= new_value <= 720):
        return await message.answer(
            with_cancel_hint("⚠️ مقدار وارد شده باید بین ۰ تا ۷۲۰ باشد."),
            reply_markup=get_settings_cancel_keyboard()
        )

    try:
        settings = (await session.scalars(select(GlobalSettings).limit(1))).first()
        if settings is None:
            await state.clear()
            return await message.answer("❌ ردیف تنظیمات در دیتابیس یافت نشد. لطفاً با پشتیبانی تماس بگیرید.", reply_markup=get_settings_return_keyboard())
        
        settings.cooldown_hours = new_value
        await session.commit()
    except Exception as e:
        await session.rollback()
        await state.clear()
        return await message.answer(
            report_db_error("تنظیمات", e),
            reply_markup=get_settings_return_keyboard()
        )

    await state.clear()
    success_text = f"✅ زمان استراحت دوره‌ای اکانت‌ها به <b>{new_value} ساعت</b> تغییر یافت."
    if new_value == 0:
        success_text += "\n⚠️ مقدار ۰ یعنی استراحت دوره‌ای بین chunkها خاموش می‌شود."
        
    await message.answer(
        success_text,
        reply_markup=get_settings_return_keyboard()
    )


# ==========================================
# EDIT SPAM PENALTY DAYS
# ==========================================
@router.callback_query(F.data == "settings_edit_penalty/")
async def ask_spam_penalty(callback: types.CallbackQuery, state: FSMContext) -> None:
    await callback.answer()

    await cleanup_fsm_temp_files(state)
    await state.clear()

    await state.set_state(SettingsStates.waiting_for_spam_penalty)
    await safe_edit_or_answer(
        callback.message,
        with_cancel_hint(
            "⚙️ <b>تنظیم جریمه اسپم</b>\n\n"
            "وقتی اکانتی ارور FloodWait دریافت می‌کند، چند روز از چرخه ارسال خارج شود؟ (توصیه: ۱):"
        ),
        reply_markup=get_settings_cancel_keyboard()
    )
@router.message(SettingsStates.waiting_for_spam_penalty, F.text)
async def process_spam_penalty(message: types.Message, state: FSMContext, session: AsyncSession) -> None:
    if not message.text.isdecimal():
        return await message.answer(
            with_cancel_hint("⚠️ لطفاً فقط یک عدد صحیح ارسال کنید."),
            reply_markup=get_settings_cancel_keyboard()
        )

    new_value = int(message.text)

    if not (0 <= new_value <= 30):
        return await message.answer(
            with_cancel_hint("⚠️ مقدار وارد شده باید بین ۰ تا ۳۰ باشد."),
            reply_markup=get_settings_cancel_keyboard()
        )

    try:
        settings = (await session.scalars(select(GlobalSettings).limit(1))).first()
        if settings is None:
            await state.clear()
            return await message.answer("❌ ردیف تنظیمات در دیتابیس یافت نشد. لطفاً با پشتیبانی تماس بگیرید.", reply_markup=get_settings_return_keyboard())
        
        settings.spam_penalty_days = new_value
        await session.commit()
    except Exception as e:
        await session.rollback()
        await state.clear()
        return await message.answer(
            report_db_error("تنظیمات", e),
            reply_markup=get_settings_return_keyboard()
        )

    await state.clear()
    success_text = f"✅ مدت زمان جریمه اسپم به <b>{new_value} روز</b> تغییر یافت."
    if new_value == 0:
        success_text += "\n⚠️ مقدار ۰ یعنی جریمهٔ FloodWait عملاً غیرفعال می‌شود."
        
    await message.answer(
        success_text,
        reply_markup=get_settings_return_keyboard()
    )

# ==========================================
# 🟠 فاز ۳ — مرحله ۱ (مسیر کامند متنی): DELETE CATEGORY COMMAND
# ==========================================
@router.message(F.text.regexp(r"^/DeleteCat_(\d+)$"))
async def delete_category_command(message: types.Message, state: FSMContext, session: AsyncSession) -> None:
    match = re.match(r"^/DeleteCat_(\d+)$", message.text)
    if not match:
        return

    cat_id = int(match.group(1))

    try:
        stmt = select(Category).options(
            selectinload(Category.accounts),
            selectinload(Category.orders)
        ).where(Category.id == cat_id)
        result = await session.execute(stmt)
        category = result.scalar_one_or_none()
    except Exception as e:
        await session.rollback()
        return await message.answer(
            report_db_error("دسته‌بندی", e),
            reply_markup=get_main_menu_button()
        )

    if not category:
        return await message.answer(
            "⚠️ این دسته‌بندی یافت نشد یا قبلاً حذف شده است.",
            reply_markup=get_main_menu_button()
        )

    if category.name.lower() == "default":
        return await message.answer(
            "❌ دسته‌بندی پیش‌فرض قابل حذف نیست.",
            reply_markup=get_main_menu_button()
        )

    # 🛡 اصلاح فاز ۱۰: عدم مسدودسازی عملیات در صورت وجود اکانت (فقط بررسی وابستگی به سفارشات)[cite: 1]
    if len(category.orders) > 0:
        return await message.answer(
            "⛔️ <b>امکان حذف وجود ندارد!</b>\n\n"
            f"در حال حاضر <b>{len(category.orders)} سفارش</b> "
            f"به این دسته‌بندی متصل هستند.\n"
            f"<i>لطفاً ابتدا سفارشات متصل را تعیین تکلیف کنید.</i>",
            reply_markup=get_main_menu_button()
        )

    await cleanup_fsm_temp_files(state)
    await state.update_data(confirm_action="delete_cat", target_id=cat_id, return_page=1)
    await state.set_state(ConfirmStates.waiting_for_confirmation)

    await message.answer(
        build_cat_delete_confirmation_text(category.name, cat_id, len(category.accounts), len(category.orders)),
        reply_markup=build_cat_delete_confirmation_keyboard(cat_id)
    )


# ==========================================
# BACKEND LOGIC: PROXY GARBAGE COLLECTOR
# ==========================================
async def flush_dead_proxies(session: AsyncSession) -> int:
    """
    منطق بک‌اند برای پاکسازی پراکسی‌های مرده از دیتابیس.
    پراکسی‌هایی که غیرفعال شده‌اند یا بیش از ۵ بار خطا داشته‌اند را حذف می‌کند.

    🛡 فاز ۲: در نسخه قبلی خطای دیتابیس را می‌بلعید و ۰ برمی‌گرداند — و هندلر
    هم ۰ را با پیام «سیستم بهینه است» ترجمه می‌کرد؛ یعنی قطعِ کامل دیتابیس
    به کاربر پیام «موفقیت» می‌داد! حالا بعد از rollback استثنا دوباره raise
    می‌شود تا هندلر پیام درست را نمایش دهد.

    Raises:
        Exception: خطای دیتابیس (بعد از rollback مجدداً raise می‌شود)
    """
    stmt = delete(Proxy).where(
        or_(
            Proxy.is_active == False,
            Proxy.fail_count >= 5
        )
    )

    try:
        result = await session.execute(stmt)
        await session.commit()
    except Exception:
        await session.rollback()
        raise

    deleted_count = result.rowcount
    logger.info(f"Proxy Garbage Collector: Flushed {deleted_count} dead proxies from database.")
    return deleted_count

# ==========================================
# FLUSH PROXIES HANDLER
# ==========================================
@router.callback_query(F.data == "settings_flush_proxies/")
async def flush_proxies_callback(callback: types.CallbackQuery, session: AsyncSession) -> None:
    await safe_callback_answer(callback)

    # 🛡 فاز ۲: به جای toastِ گذرا، پیام انتظار واقعی (قید پروژه: همهٔ عملیات‌های
    # طولانی loading message دارند) که در پایان به نتیجه ویرایش می‌شود
    wait_msg = await send_loading_message(callback.message, "⏳ در حال اسکن و پاکسازی پراکسی‌های مرده...")

    try:
        deleted_count = await flush_dead_proxies(session)
    except Exception as e:
        # rollback داخل flush_dead_proxies انجام شده است
        return await safe_edit_message(
            wait_msg,
            report_db_error("پراکسی‌ها", e),
            reply_markup=get_settings_return_keyboard()
        )

    if deleted_count > 0:
        await safe_edit_message(
            wait_msg,
            f"✅ <b>عملیات پاکسازی با موفقیت انجام شد!</b>\n\n"
            f"تعداد <b>{deleted_count}</b> پراکسی مرده (قطع شده یا دارای بیش از ۵ خطا) از دیتابیس حذف گردید تا حجم سیستم بهینه‌سازی شود.",
            reply_markup=get_settings_return_keyboard()
        )
    else:
        await safe_edit_message(
            wait_msg,
            "✅ <b>سیستم بهینه است.</b>\n\nهیچ پراکسی مرده‌ای در دیتابیس یافت نشد.",
            reply_markup=get_settings_return_keyboard()
        )

import aiohttp # حتماً این ایمپورت را به بالای فایل اضافه کنید

@router.callback_query(F.data == "settings_auto_fetch_proxy/")
async def auto_fetch_proxies_handler(callback: types.CallbackQuery, session: AsyncSession) -> None:
    await safe_callback_answer(callback)
    
    # نمایش پیام انتظار به کاربر
    wait_msg = await send_loading_message(callback.message, "⏳ در حال برقراری ارتباط با API و دریافت پراکسی‌ها...")

    try:
        # ۱. دریافت پراکسی‌ها از API به صورت غیرهمگام
        async with aiohttp.ClientSession() as http_session:
            async with http_session.get(
                "https://databay.com/api/v1/proxy-list",
                params={"ssl": "strict", "protocol": "socks5", "format": "json"},
                timeout=10
            ) as resp:
                if resp.status != 200:
                    raise Exception(f"API HTTP Error: {resp.status}")
                data = await resp.json()
                proxies_data = data.get("data", [])

        # ۲. تبدیل ریسپانس API به فرمت استاندارد ربات (socks5://ip:port)
        valid_proxies = []
        for p in proxies_data:
            proxy_str = f"{p['protocol']}://{p['ip']}:{p['port']}"
            valid_proxies.append(proxy_str)

        if not valid_proxies:
            return await safe_edit_message(
                wait_msg,
                "⚠️ هیچ پراکسی معتبری از سمت API دریافت نشد.",
                reply_markup=get_settings_return_keyboard()
            )

        # ۳. بررسی تکراری نبودن و ثبت در دیتابیس (مشابه منطق ثبت دستی)
        existing_set = await _fetch_existing_proxy_strings(session, valid_proxies)
        new_proxies = [p for p in valid_proxies if p not in existing_set]
        duplicate_count = len(valid_proxies) - len(new_proxies)

        if new_proxies:
            session.add_all(
                [Proxy(proxy_string=p, is_active=True, fail_count=0) for p in new_proxies]
            )
            await session.commit()

        # ۴. نمایش نتیجه نهایی
        added_count = len(new_proxies)
        text = (
            f"✅ <b>دریافت خودکار با موفقیت انجام شد!</b>\n\n"
            f"🌐 کل پراکسی‌های دریافتی از API: <b>{len(valid_proxies)}</b> عدد\n"
            f"➕ پراکسی‌های جدید ثبت شده: <b>{added_count}</b> عدد\n"
            f"♻️ تکراری (از قبل موجود): <b>{duplicate_count}</b> عدد\n"
        )
        await safe_edit_message(wait_msg, text, reply_markup=get_settings_return_keyboard())

    except asyncio.TimeoutError:
        await session.rollback()
        await safe_edit_message(
            wait_msg,
            "❌ خطا: ارتباط با API بیش از حد طول کشید (Timeout).",
            reply_markup=get_settings_return_keyboard()
        )
    except Exception as e:
        await session.rollback()
        await safe_edit_message(
            wait_msg,
            f"❌ خطای پیش‌بینی نشده در دریافت پراکسی:\n<code>{str(e)}</code>",
            reply_markup=get_settings_return_keyboard()
        )

