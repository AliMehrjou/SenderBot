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
from database.engine import async_session
from database.models import GlobalSettings, Category, Account, Proxy, Admin
from bot.states.settings_fsm import SettingsStates
from bot.states.confirm_fsm import ConfirmStates
from bot.keyboards.main_menu import (
    get_main_menu_button,
    get_progress_notify_button_text,
    PROGRESS_NOTIFY_TOGGLE_CB,
)

from utils.health_checker import check_all_proxies
from bot.keyboards.cancel import with_cancel_hint
from utils.fsm_cleanup import cleanup_fsm_temp_files
from utils.safe_edit import safe_edit_or_answer
import asyncio

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
    max_acc_api = settings.max_accounts_per_api if settings else "N/A"
    
    # 🛡 نمایش مقادیر موثر (Effective Values) برای راستی‌آزمایی
    raw_penalty = settings.spam_penalty_days if settings else 3
    penalty = f"{raw_penalty} (مؤثر: {max(raw_penalty, 1)})" if raw_penalty < 1 else str(raw_penalty)
    
    raw_cooldown = settings.cooldown_hours if settings else 1
    cooldown = f"{raw_cooldown} (مؤثر: {max(raw_cooldown, 1)})" if raw_cooldown < 1 else str(raw_cooldown)

    # Phase 5 — per-admin live progress reporting preference (default: on)
    progress_notify_on = True
    try:
        admin_row = await session.scalar(
            select(Admin).where(Admin.telegram_id == callback.from_user.id)
        )
        if admin_row is not None:
            progress_notify_on = bool(admin_row.progress_notify)
    except Exception as e:
        logger.warning(f"Settings menu: progress_notify lookup failed: {e}")

    daily_cap = config.DAILY_SEND_LIMIT_PER_ACCOUNT
    new_acc_days = config.NEW_ACCOUNT_DAYS
    new_acc_cap = config.NEW_ACCOUNT_DAILY_SEND_LIMIT

    builder = InlineKeyboardBuilder()

    # دکمه‌های عددی
    builder.button(text="تغییر محدودیت API", callback_data="settings_edit_max_api/")
    builder.button(text="تغییر زمان استراحت", callback_data="settings_edit_cooldown/")
    builder.button(text="تغییر جریمه اسپم", callback_data="settings_edit_penalty/")
    builder.button(text="تغییر ظرفیت ارسال", callback_data="settings_edit_limit/")

    # --- دکمه‌های سرعت و Toggle (خاموش/روشن) ---
    builder.button(text="⚡ حالت سرعت استخراج", callback_data="settings_speed_mode/")
    
    if settings:
        btn_2fa = "✅ 🔐 تنظیم پسورد دوم (2FA)" if settings.auto_set_2fa else "❌ 🔐 تنظیم پسورد دوم (2FA)"
        btn_term = "✅ 🚪 خروج از سایر نشست‌ها" if settings.terminate_sessions else "❌ 🚪 خروج از سایر نشست‌ها"
        # 🎭 مدیریت پیشرفته پروفایل‌ها: سوئیچ واحد قبلی («تنظیم نام، بایو، پروفایل»)
        # به سه سوئیچ مستقل تفکیک شد؛ دکمه «تنظیم یوزرنیم» کامل حذف شد.
        btn_name = "✅ 📛 تنظیم نام" if settings.auto_set_name else "❌ 📛 تنظیم نام"
        btn_bio = "✅ 📝 تنظیم بیو" if settings.auto_set_bio else "❌ 📝 تنظیم بیو"
        btn_photo = "✅ 🖼 تنظیم عکس پروفایل" if settings.auto_set_photo else "❌ 🖼 تنظیم عکس پروفایل"
        btn_access = "✅ 🌐 دسترسی عمومی به کد سفارش" if settings.public_order_access else "❌ 🌐 دسترسی عمومی به کد سفارش"
        
        # 🛡 رفع باگ: خواندن تنظیمات ضدبن هوشمند از Redis به دلیل عدم وجود ستون در جدول اصلی
        anti_ban_active = True
        try:
            from workers.sender import _get_redis
            ab_val = await _get_redis().get("settings:smart_anti_ban")
            anti_ban_active = (ab_val is None or ab_val == "1")
        except Exception: pass
        btn_antiban = "✅ 🛡 محافظت هوشمند ضدبن" if anti_ban_active else "❌ 🛡 محافظت هوشمند ضدبن"

        builder.button(text=btn_2fa, callback_data="toggle_auto_set_2fa/")
        builder.button(text=btn_term, callback_data="toggle_terminate_sessions/")
        builder.button(text=btn_name, callback_data="toggle_auto_set_name/")
        builder.button(text=btn_bio, callback_data="toggle_auto_set_bio/")
        builder.button(text=btn_photo, callback_data="toggle_auto_set_photo/")
        builder.button(text=btn_access, callback_data="toggle_public_order_access/")
        builder.button(text=btn_antiban, callback_data="toggle_smart_anti_ban/") # دکمه اضافه شد
        builder.button(text="🌐 مدیریت پروکسی‌ها", callback_data="menu_proxies/")
        builder.button(text="🩺 وضعیت سلامت اکانت‌ها", callback_data="menu_account_health_page_1/")
    # ------------------------------------

    # Phase 5 — live progress reporting toggle (per-admin, always shown)
    builder.button(
        text=get_progress_notify_button_text(progress_notify_on),
        callback_data=PROGRESS_NOTIFY_TOGGLE_CB,
    )

    # دکمه‌های مدیریت
    builder.button(text="📁 افزودن دسته‌بندی جدید", callback_data="settings_add_cat/")
    builder.button(text="🏛 منوی اصلی", callback_data="menu_home/")

    # چیدمان جدید با اضافه شدن دکمه مدیریت پروکسی
    builder.adjust(2, 2, 1, 2, 2, 2, 2, 1, 1, 2)

    speed_mode_fa = {"safe": "🔒 امن", "fast": "⚡ سریع", "turbo": "🚀 توربو"}.get(settings.extraction_speed_mode if settings else "safe", "🔒 امن")
    text = (
        "⚙️ <b>پنل تنظیمات پیشرفته سیستم</b>\n\n"
        f"🔹 <b>حالت سرعت استخراج:</b> {speed_mode_fa}\n"
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
# Phase 5 — live progress reporting toggle (per-admin, callback-only, no FSM)
# ==========================================
@router.callback_query(F.data == PROGRESS_NOTIFY_TOGGLE_CB)
async def toggle_progress_notify(
    callback: types.CallbackQuery,
    state: FSMContext,
    session: AsyncSession,
) -> None:
    """
    Flips the CURRENT admin's progress_notify flag immediately. The change
    applies to the NEXT task (the flag is read at reporter-creation time).
    """
    telegram_id = callback.from_user.id

    try:
        admin_row = await session.scalar(
            select(Admin).where(Admin.telegram_id == telegram_id)
        )
    except Exception as e:
        await session.rollback()
        return await answer_callback_error(
            callback, report_db_error("تنظیمات", e), get_main_menu_button()
        )

    current_value = bool(admin_row.progress_notify) if admin_row is not None else True
    new_value = not current_value

    try:
        if admin_row is None:
            # No admins-table row yet (typical for the primary ADMIN_ID which is
            # configured via env only). Create one so the preference persists;
            # only the primary admin may bootstrap a row this way — everyone
            # else must already exist as a sub-admin to be able to toggle.
            if int(config.ADMIN_ID or 0) != int(telegram_id):
                return await callback.answer(
                    "❌ این تنظیم فقط برای ادمین‌های ثبت‌شده قابل تغییر است.",
                    show_alert=True,
                )
            session.add(Admin(telegram_id=telegram_id, progress_notify=new_value))
        else:
            admin_row.progress_notify = new_value
        await session.commit()
    except Exception as e:
        await session.rollback()
        return await answer_callback_error(
            callback, report_db_error("تنظیمات", e), get_main_menu_button()
        )

    status_text = "روشن" if new_value else "خاموش"
    await safe_callback_answer(
        callback,
        f"✅ گزارش پیشرفت لحظه‌ای {status_text} شد — از وظیفهٔ بعدی اعمال می‌شود.",
    )

    # Re-render the settings menu so the button label updates immediately
    await show_settings_menu(callback, state, session)


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
    "smart_anti_ban": "محافظت هوشمند ضدبن",
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
    # 🛡 رفع باگ: مدیریت مجزای smart_anti_ban روی Redis
    if field_name == "smart_anti_ban":
        try:
            from workers.sender import _get_redis
            redis = _get_redis()
            ab_val = await redis.get("settings:smart_anti_ban")
            is_active = (ab_val is None or ab_val == "1")
            await redis.set("settings:smart_anti_ban", "0" if is_active else "1")
            
            status_text = "غیرفعال" if is_active else "فعال"
            await safe_callback_answer(callback, f"✅ وضعیت «محافظت هوشمند ضدبن» به {status_text} تغییر یافت.")
            return await show_settings_menu(callback, state, session)
        except Exception as e:
            return await answer_callback_error(callback, f"خطای ردیس: {e}", get_main_menu_button())

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
# PROXY MANAGEMENT MENU & FLOW
# ==========================================

@router.callback_query(F.data == "menu_proxies/")
async def show_proxies_menu(callback: types.CallbackQuery, state: FSMContext, session: AsyncSession) -> None:
    await safe_callback_answer(callback)
    await cleanup_fsm_temp_files(state)
    await state.clear()

    try:
        settings = await session.scalar(select(GlobalSettings).limit(1))
    except Exception as e:
        await session.rollback()
        return await answer_callback_error(callback, report_db_error("تنظیمات", e), get_main_menu_button())

    use_sender_proxy = bool(getattr(settings, "use_proxy_for_sending", False)) if settings else False

    builder = InlineKeyboardBuilder()
    
    # دکمه سوییچ وضعیت پروکسی ارسال
    sender_proxy_status = "🟢 فعال" if use_sender_proxy else "⚪️ غیرفعال (مستقیم)"
    builder.button(
    text=f"وضعیت پروکسی ارسال: {sender_proxy_status}", 
    callback_data="switch_sender_proxy/"
    )
    
    builder.button(text="➕ افزودن پروکسی", callback_data="settings_add_proxy_start/")
    builder.button(text="🗑 مدیریت و حذف پروکسی‌ها", callback_data="settings_manage_proxies/")
    builder.button(text="🔄 بررسی مجدد سلامت پروکسی‌ها", callback_data="settings_recheck_proxies/")
    builder.button(text="🧹 پاکسازی پراکسی‌های مرده", callback_data="settings_flush_proxies/")
    builder.button(text="⚙️ بازگشت به تنظیمات", callback_data="menu_settings/")
    builder.button(text="🏛 منوی اصلی", callback_data="menu_home/")

    builder.adjust(1, 1, 1, 1, 1, 2)

    text = (
        "🌐 <b>پنل مدیریت پروکسی‌ها</b>\n\n"
        "💡 <b>راهنمای تفکیک پروکسی‌ها:</b>\n"
        "🔸 <b>پروکسی لاگین:</b> مخصوص فرآیند حساس ثبت و ورود اکانت‌ها (جلوگیری از بلاک شدن شماره در لحظه دریافت کد).\n"
        "🔸 <b>پروکسی ارسال:</b> مخصوص ارسال پیام انبوه. شما می‌توانید انتخاب کنید ترافیک پیام‌ها مستقیماً (Direct) از سرور باشد یا از طریق پروکسی‌های این بخش عبور کند.\n\n"
        "👇 <i>از منوی زیر برای مدیریت شبکه‌ی اتصال استفاده کنید:</i>"
    )

    await safe_edit_or_answer(callback.message, text, reply_markup=builder.as_markup())




async def _background_proxy_check(wait_msg: types.Message, total: int):
    # باز کردن یک اتصال کاملاً جدید و مستقل از میدل‌ور
    async with async_session() as session:
        try:
            # اعمال تایم‌اوت ۵ دقیقه‌ای (۳۰۰ ثانیه) برای جلوگیری از گیر کردن تسک
            await asyncio.wait_for(check_all_proxies(bot=wait_msg.bot), timeout=300.0)

            # واکشی آمار بعد از اتمام بررسی بر اساس وضعیت جدید سلامت (Hysteresis)
            healthy = await session.scalar(select(func.count(Proxy.id)).where(Proxy.health_state == 'HEALTHY', Proxy.is_active == True)) or 0
            weak = await session.scalar(select(func.count(Proxy.id)).where(Proxy.health_state == 'WEAK', Proxy.is_active == True)) or 0
            dead = await session.scalar(select(func.count(Proxy.id)).where(Proxy.health_state == 'DEAD')) or 0
            inactive = await session.scalar(select(func.count(Proxy.id)).where(Proxy.is_active == False, Proxy.health_state != 'DEAD')) or 0

            result_text = (
                f"✅ <b>بررسی مجدد پایان یافت</b>\n\n"
                f"🟢 سالم (عالی): <b>{healthy}</b>\n"
                f"🟡 ضعیف (کُند): <b>{weak}</b>\n"
                f"🔴 مرده (DEAD): <b>{dead}</b>\n"
                f"⚪️ غیرفعال: <b>{inactive}</b>"
            )
            await safe_edit_message(wait_msg, result_text, reply_markup=get_settings_return_keyboard())

        except asyncio.TimeoutError:
            await safe_edit_message(
                wait_msg, 
                "❌ زمان بررسی بیش از حد طول کشید (Timeout). بررسی متوقف شد.", 
                reply_markup=get_settings_return_keyboard()
            )
        except Exception as e:
            # از آنجایی که سشن اختصاصی است، rollback مشکلی برای ربات ایجاد نمی‌کند
            await session.rollback()
            await safe_edit_message(
                wait_msg, 
                f"❌ خطای سیستمی:\n<code>{e}</code>", 
                reply_markup=get_settings_return_keyboard()
            )
@router.callback_query(F.data == "settings_recheck_proxies/")
async def recheck_proxies_handler(callback: types.CallbackQuery, state: FSMContext, session: AsyncSession) -> None:
    await safe_callback_answer(callback)

    total = await session.scalar(select(func.count(Proxy.id))) or 0
    if total == 0:
        return await callback.message.answer(
            "⚠️ هیچ پراکسی ثبت شده‌ای جهت بررسی وجود ندارد.",
            reply_markup=get_settings_return_keyboard()
        )

    wait_msg = await send_loading_message(callback.message, "⏳ عملیات در پس‌زمینه آغاز شد. لطفاً منتظر بمانید...")

    # تسک را در پس‌زمینه ایجاد می‌کنیم و هندلر را می‌بندیم
    asyncio.create_task(
        _background_proxy_check(wait_msg, total)
    )

@router.callback_query(F.data == "settings_add_proxy_start/")
async def add_proxy_ask_type(callback: types.CallbackQuery, state: FSMContext) -> None:
    await safe_callback_answer(callback)
    
    builder = InlineKeyboardBuilder()
    builder.button(text="🔐 پروکسی ورود (Login)", callback_data="add_proxy_type_login/")
    builder.button(text="📤 پروکسی ارسال (Sender)", callback_data="add_proxy_type_sender/")
    builder.button(text="🌐 برای هر دو (دوکاره)", callback_data="add_proxy_type_both/")
    builder.button(text="🔙 بازگشت", callback_data="menu_proxies/")
    builder.adjust(2, 1, 1)
    
    await safe_edit_or_answer(
        callback.message,
        "➕ <b>افزودن پروکسی جدید</b>\n\nلطفاً تعیین کنید این پروکسی برای کدام بخش استفاده می‌شود؟",
        reply_markup=builder.as_markup()
    )

@router.callback_query(F.data.in_({"add_proxy_type_login/", "add_proxy_type_sender/", "add_proxy_type_both/"}))
async def ask_for_proxies_input(callback: types.CallbackQuery, state: FSMContext) -> None:
    if "login" in callback.data:
        proxy_type = "login"
        type_fa = "ورود (Login)"
    elif "sender" in callback.data:
        proxy_type = "sender"
        type_fa = "ارسال پیام (Sender)"
    else:
        proxy_type = "both"
        type_fa = "هر دو (دوکاره)"
    
    await safe_callback_answer(callback)
    await cleanup_fsm_temp_files(state)
    await state.clear()

    await state.update_data(target_proxy_type=proxy_type)
    await state.set_state(SettingsStates.waiting_for_proxies)
    
    await safe_edit_or_answer(
        callback.message,
        with_cancel_hint(
            f"🌐 <b>افزودن پروکسی برای بخش: {type_fa}</b>\n\n"
            "لطفاً لیست پروکسی‌های خود را (هر کدام در یک خط) ارسال کنید.\n"
            "<i>فرمت قابل قبول: socks5://user:pass@ip:port یا socks5://ip:port</i>"
        ),
        reply_markup=get_settings_cancel_keyboard()
    )


async def _fetch_existing_proxy_strings(session: AsyncSession, candidates: list[str]) -> set[str]:
    existing: set[str] = set()
    CHUNK_SIZE = 500
    for i in range(0, len(candidates), CHUNK_SIZE):
        chunk = candidates[i:i + CHUNK_SIZE]
        stmt = select(Proxy.proxy_string).where(Proxy.proxy_string.in_(chunk))
        existing.update(await session.scalars(stmt))
    return existing

@router.message(SettingsStates.waiting_for_proxies, F.text)
async def process_new_proxies(message: types.Message, state: FSMContext, session: AsyncSession) -> None:
    data = await state.get_data()
    proxy_type = data.get("target_proxy_type", "login")
    
    if proxy_type == "login":
        type_fa = "لاگین"
    elif proxy_type == "sender":
        type_fa = "ارسال"
    else:
        type_fa = "دوکاره"

    raw_proxies = message.text.strip().split('\n')
    seen_in_message, valid_proxies = set(), []
    invalid_count, duplicate_in_message_count = 0, 0

    for line in raw_proxies:
        proxy_str = line.strip()
        if not proxy_str: continue
        if not parse_proxy_string(proxy_str):
            invalid_count += 1
            continue
        if proxy_str in seen_in_message:
            duplicate_in_message_count += 1
            continue
        seen_in_message.add(proxy_str)
        valid_proxies.append(proxy_str)

    if not valid_proxies:
        return await message.answer(
            with_cancel_hint("⚠️ هیچ پروکسی معتبری یافت نشد. لیست اصلاح‌شده را ارسال کنید."),
            reply_markup=get_settings_cancel_keyboard()
        )

    updated_to_both_count = 0

    try:
        # ۱. استخراج آبجکت‌های پراکسی تکراری از دیتابیس در دسته‌های ۵۰۰تایی (برای جلوگیری از طولانی شدن کوئری)
        existing_proxies = []
        CHUNK_SIZE = 500
        for i in range(0, len(valid_proxies), CHUNK_SIZE):
            chunk = valid_proxies[i:i + CHUNK_SIZE]
            stmt = select(Proxy).where(Proxy.proxy_string.in_(chunk))
            result = await session.scalars(stmt)
            existing_proxies.extend(result.all())

        # ۲. بررسی پراکسی‌های موجود و ارتقای نوع آن‌ها در صورت نیاز
        existing_set = set()
        for p in existing_proxies:
            existing_set.add(p.proxy_string)
            if p.usage_type != proxy_type and p.usage_type != "both":
                p.usage_type = "both"
                updated_to_both_count += 1

        # ۳. یافتن پراکسی‌های کاملاً جدید
        new_proxies = [p for p in valid_proxies if p not in existing_set]
        duplicate_in_db_count = len(valid_proxies) - len(new_proxies)

        # ۴. ثبت پراکسی‌های جدید در دیتابیس
        if new_proxies:
            session.add_all(
                [Proxy(proxy_string=p, is_active=True, fail_count=0, usage_type=proxy_type) for p in new_proxies]
            )
            
        await session.commit()
        
        # بیدار کردن صف انتظار در صورت افزودن پروکسی جدید
        from workers.session_manager import background_process_proxy_queue
        asyncio.create_task(background_process_proxy_queue())

    except Exception as e:
        await session.rollback()
        await state.clear()
        return await message.answer(report_db_error("پروکسی‌ها", e), reply_markup=get_settings_return_keyboard())

    await state.clear()
    
    added_count = len(new_proxies)
    response_parts = [f"✅ تعداد <b>{added_count}</b> پروکسی کاملاً جدید به استخر <b>{type_fa}</b> اضافه شد."]
    
    if updated_to_both_count > 0:
        response_parts.append(f"🔄 تعداد <b>{updated_to_both_count}</b> پروکسی موجود، به وضعیت «دوکاره» ارتقا یافتند.")
    
    unchanged_duplicates = duplicate_in_db_count - updated_to_both_count
    if unchanged_duplicates > 0:
        response_parts.append(f"♻️ تعداد <b>{unchanged_duplicates}</b> پروکسی تکراری نادیده گرفته شد.")
        
    if invalid_count > 0:
        response_parts.append(f"⚠️ تعداد <b>{invalid_count}</b> پروکسی نامعتبر رد شد.")

    builder = InlineKeyboardBuilder()
    builder.button(text="🌐 بازگشت به پنل پروکسی", callback_data="menu_proxies/")
    builder.adjust(1)
    await message.answer("\n".join(response_parts), reply_markup=builder.as_markup())



# ==========================================
# MANAGE & DELETE PROXIES (GLASS MENU)
# ==========================================

@router.callback_query(F.data == "settings_manage_proxies/")
async def manage_proxies_list(callback: types.CallbackQuery, session: AsyncSession) -> None:
    try:
        # واکشی ۵۰ پروکسی آخر (محدود شده برای جلوگیری از خطای تلگرام در کیبورد اینلاین)
        stmt = select(Proxy).order_by(Proxy.id.desc()).limit(50)
        proxies = (await session.scalars(stmt)).all()
    except Exception as e:
        await session.rollback()
        return await answer_callback_error(callback, report_db_error("پروکسی‌ها", e), get_main_menu_button())

    builder = InlineKeyboardBuilder()

    if not proxies:
        builder.button(text="🔙 بازگشت", callback_data="menu_proxies/")
        return await safe_edit_or_answer(
            callback.message,
            "⚠️ هیچ پروکسی ثبت شده‌ای در دیتابیس وجود ندارد.",
            reply_markup=builder.as_markup()
        )

    for p in proxies:
        status_emoji = "🟢" if p.is_active else "🔴"
        type_str = {"login": "لاگین", "sender": "ارسال", "both": "دوکاره"}.get(p.usage_type, "دوکاره")
        # استخراج آی‌پی و پورت برای نمایش کوتاه‌تر
        short_addr = p.proxy_string.split("@")[-1] if "@" in p.proxy_string else p.proxy_string.replace("socks5://", "")
        
        btn_text = f"{status_emoji} [{type_str}] {short_addr} ❌"
        builder.button(text=btn_text, callback_data=f"del_proxy_{p.id}/")

    builder.adjust(1) # هر پروکسی در یک سطر
    builder.row(types.InlineKeyboardButton(text="🔙 بازگشت به منوی پروکسی", callback_data="menu_proxies/"))

    text = "🗑 <b>لیست پروکسی‌ها</b>\n\nبرای حذف فوری، روی هر پروکسی کلیک کنید:"
    await safe_edit_or_answer(callback.message, text, reply_markup=builder.as_markup())


@router.callback_query(F.data.startswith("del_proxy_") & F.data.endswith("/"))
async def delete_proxy_inline_handler(callback: types.CallbackQuery, session: AsyncSession) -> None:
    proxy_id_str = callback.data.replace("del_proxy_", "").replace("/", "")
    if not proxy_id_str.isdigit():
        return await callback.answer("⚠️ شناسه نامعتبر.", show_alert=True)

    try:
        proxy = await session.scalar(select(Proxy).where(Proxy.id == int(proxy_id_str)))
        if proxy:
            # آزاد کردن اکانت‌های متصل قبل از حذف
            unbind_stmt = (
                update(Account)
                .where(Account.proxy_string == proxy.proxy_string)
                .values(
                    proxy_string=None,
                    proxy_status="WAITING_PROXY",
                    proxy_queue_joined_at=func.now()
                )
                .execution_options(synchronize_session=False)
            )
            await session.execute(unbind_stmt)
            
            await session.delete(proxy)
            await session.commit()
    except Exception as e:
        await session.rollback()
        return await answer_callback_error(callback, report_db_error("پروکسی‌ها", e), get_main_menu_button())

    await callback.answer("✅ پروکسی با موفقیت حذف شد.", show_alert=False)
    await manage_proxies_list(callback, session)


@router.callback_query(F.data == "switch_sender_proxy/")
async def toggle_sender_proxy_handler(callback: types.CallbackQuery, state: FSMContext, session: AsyncSession) -> None:
    try:
        settings = await session.scalar(select(GlobalSettings).limit(1))
        if not settings:
            return await callback.answer("⚠️ تنظیمات یافت نشد.", show_alert=True)
            
        current_val = bool(getattr(settings, "use_proxy_for_sending", False))
        settings.use_proxy_for_sending = not current_val
        await session.commit()
    except Exception as e:
        await session.rollback()
        return await answer_callback_error(callback, report_db_error("تنظیمات", e), get_main_menu_button())

    await safe_callback_answer(callback, "✅ این تنظیم از ری‌استارت بعدی روی همه ورکرها اعمال می‌شود", show_alert=True)
    await show_proxies_menu(callback, state, session)


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
    if not message.text.lstrip('-').isdecimal():
        return await message.answer(
            with_cancel_hint("⚠️ لطفاً فقط یک عدد صحیح ارسال کنید."),
            reply_markup=get_settings_cancel_keyboard()
        )

    new_value = int(message.text)

    # 🛡 اعتبارسنجی سمت سرور: جلوگیری قطعی از مقدار ۰ یا منفی
    if not (1 <= new_value <= 720):
        return await message.answer(
            with_cancel_hint("⚠️ مقدار وارد شده باید بین ۱ تا ۷۲۰ ساعت باشد (مقدار ۰ غیرمجاز است)."),
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
    await message.answer(
        f"✅ زمان استراحت دوره‌ای اکانت‌ها به <b>{new_value} ساعت</b> تغییر یافت.",
        reply_markup=get_settings_return_keyboard()
    )


@router.message(SettingsStates.waiting_for_spam_penalty, F.text)
async def process_spam_penalty(message: types.Message, state: FSMContext, session: AsyncSession) -> None:
    if not message.text.lstrip('-').isdecimal():
        return await message.answer(
            with_cancel_hint("⚠️ لطفاً فقط یک عدد صحیح ارسال کنید."),
            reply_markup=get_settings_cancel_keyboard()
        )

    new_value = int(message.text)

    # 🛡 اعتبارسنجی سمت سرور: حداقل ۱ روز اجباری است
    if not (1 <= new_value <= 30):
        return await message.answer(
            with_cancel_hint("⚠️ مقدار وارد شده باید حداقل ۱ و حداکثر ۳۰ روز باشد."),
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
    await message.answer(
        f"✅ مدت زمان جریمه اسپم به <b>{new_value} روز</b> تغییر یافت.",
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
    پراکسی‌هایی که به وضعیت DEAD درآمده‌اند را حذف می‌کند.
    اکانت‌های متصل به آن‌ها در همان تراکنش آزاد می‌شوند و وارد صف می‌گردند.
    """
    try:
        # ۱. واکشی پراکسی‌های مرده برای آزاد کردن اکانت‌ها
        dead_proxies_stmt = select(Proxy.proxy_string).where(Proxy.health_state == 'DEAD')
        dead_proxy_strings = (await session.scalars(dead_proxies_stmt)).all()
        
        if dead_proxy_strings:
            unbind_stmt = (
                update(Account)
                .where(Account.proxy_string.in_(dead_proxy_strings))
                .values(
                    proxy_string=None,
                    proxy_status="WAITING_PROXY",
                    proxy_queue_joined_at=func.now()
                )
                .execution_options(synchronize_session=False)
            )
            await session.execute(unbind_stmt)
        
        # ۲. حذف پراکسی‌های مرده
        stmt = delete(Proxy).where(Proxy.health_state == 'DEAD')
        result = await session.execute(stmt)
        await session.commit()
        
        deleted_count = result.rowcount
        logger.info(f"Proxy Garbage Collector: Flushed {deleted_count} DEAD proxies from database.")
        
        # بیدار کردن صف انتظار در صورت پاکسازی
        if deleted_count > 0:
            try:
                from workers.session_manager import background_process_proxy_queue
                import asyncio
                asyncio.create_task(background_process_proxy_queue())
            except ImportError:
                pass
                
        return deleted_count
    except Exception:
        await session.rollback()
        raise

@router.callback_query(F.data == "settings_flush_proxies/")
async def flush_proxies_callback(callback: types.CallbackQuery, session: AsyncSession) -> None:
    await safe_callback_answer(callback)
    wait_msg = await send_loading_message(callback.message, "⏳ در حال اسکن و پاکسازی پراکسی‌های مرده...")

    try:
        deleted_count = await flush_dead_proxies(session)
    except Exception as e:
        return await safe_edit_message(
            wait_msg,
            report_db_error("پراکسی‌ها", e),
            reply_markup=get_settings_return_keyboard()
        )

    if deleted_count > 0:
        await safe_edit_message(
            wait_msg,
            f"✅ <b>عملیات پاکسازی با موفقیت انجام شد!</b>\n\n"
            f"تعداد <b>{deleted_count}</b> پراکسی مرده (DEAD) از دیتابیس حذف گردید تا حجم سیستم بهینه‌سازی شود.",
            reply_markup=get_settings_return_keyboard()
        )
    else:
        await safe_edit_message(
            wait_msg,
            "✅ <b>سیستم بهینه است.</b>\n\nهیچ پراکسی مرده‌ای در دیتابیس یافت نشد.",
            reply_markup=get_settings_return_keyboard()
        )
# ==========================================
# EXTRACTION SPEED MODE MENU
# ==========================================
@router.callback_query(F.data == "settings_speed_mode/")
async def show_speed_mode_menu(callback: types.CallbackQuery, state: FSMContext, session: AsyncSession) -> None:
    await safe_callback_answer(callback)
    await state.clear()

    try:
        settings = (await session.scalars(select(GlobalSettings).limit(1))).first()
        current_mode = settings.extraction_speed_mode if settings else "safe"
        residency = getattr(settings, "worker_residency", "ephemeral") if settings else "ephemeral"
    except Exception as e:
        await session.rollback()
        return await answer_callback_error(callback, report_db_error("تنظیمات", e), get_main_menu_button())

    builder = InlineKeyboardBuilder()
    builder.button(text="🔒 امن (پیش‌فرض)" + (" ✅" if current_mode=="safe" else ""), callback_data="set_speed_mode_safe/")
    builder.button(text="⚡ سریع (~5x)" + (" ✅" if current_mode=="fast" else ""), callback_data="set_speed_mode_fast/")
    builder.button(text="🚀 توربو (Extreme)" + (" ✅" if current_mode=="turbo" else ""), callback_data="set_speed_mode_turbo/")
    
    residency_text = "✅ 🏘 عضویت ورکرها: ساکن" if residency == "resident" else "❌ 🏘 عضویت ورکرها: لحظه‌ای"
    
    # استفاده از کال‌بک کاملاً متمایز
    builder.button(text=residency_text, callback_data="set_residency_mode/")
    
    builder.button(text="⚙️ بازگشت به تنظیمات", callback_data="menu_settings/")
    builder.adjust(1, 1, 1, 1, 1)

    text = (
        "⚡ <b>تنظیمات پروفایل سرعت استخراج</b>\n\n"
        "سرعت رفتار ربات را برای جوین و اسکن اعضا انتخاب کنید:\n"
        "▫️ <b>امن:</b> وقفه طولانی، بدون ریسک\n"
        "▫️ <b>سریع:</b> کاهش تأخیرها، سرعت بالا در استخراج موازی\n"
        "▫️ <b>توربو:</b> حداقل تأخیر ممکن!\n\n"
        "🏘 <b>عضویت ورکرها (لحظه‌ای/ساکن):</b> در حالت ساکن، ورکرها از گروه‌های پرتکرار مشتری خارج نمی‌شوند تا در سفارشات بعدی بدون نیاز به تأیید مجدد ادمین، بلافاصله استخراج را آغاز کنند."
    )
    await safe_edit_or_answer(callback.message, text, reply_markup=builder.as_markup())


@router.callback_query(F.data == "set_residency_mode/")
async def toggle_worker_residency_handler(callback: types.CallbackQuery, state: FSMContext, session: AsyncSession) -> None:
    try:
        settings = (await session.scalars(select(GlobalSettings).limit(1))).first()
        if not settings:
            return await callback.answer("⚠️ تنظیمات یافت نشد.", show_alert=True)
            
        current_val = getattr(settings, "worker_residency", "ephemeral")
        new_val = "resident" if current_val == "ephemeral" else "ephemeral"
        
        settings.worker_residency = new_val
        await session.commit()
    except Exception as e:
        await session.rollback()
        return await answer_callback_error(callback, report_db_error("تنظیمات", e), get_main_menu_button())

    mode_fa = "ساکن" if new_val == "resident" else "لحظه‌ای"
    await safe_callback_answer(callback, f"✅ حالت عضویت به «{mode_fa}» تغییر یافت.")
    await show_speed_mode_menu(callback, state, session)


@router.callback_query(F.data == "switch_worker_residency/")
async def switch_worker_residency_handler(callback: types.CallbackQuery, state: FSMContext, session: AsyncSession) -> None:
    try:
        settings = (await session.scalars(select(GlobalSettings).limit(1))).first()
        if not settings:
            return await callback.answer("⚠️ تنظیمات یافت نشد.", show_alert=True)
            
        current_val = getattr(settings, "worker_residency", "ephemeral")
        new_val = "resident" if current_val == "ephemeral" else "ephemeral"
        
        settings.worker_residency = new_val
        await session.commit()
    except Exception as e:
        await session.rollback()
        return await answer_callback_error(callback, report_db_error("تنظیمات", e), get_main_menu_button())

    mode_fa = "ساکن" if new_val == "resident" else "لحظه‌ای"
    await safe_callback_answer(callback, f"✅ حالت عضویت به «{mode_fa}» تغییر یافت.")
    await show_speed_mode_menu(callback, state, session)


@router.callback_query(F.data == "toggle_worker_residency/")
async def toggle_worker_residency(callback: types.CallbackQuery, state: FSMContext, session: AsyncSession) -> None:
    try:
        settings = (await session.scalars(select(GlobalSettings).limit(1))).first()
        if not settings:
            return await callback.answer("⚠️ تنظیمات یافت نشد.", show_alert=True)
            
        new_val = "resident" if settings.worker_residency == "ephemeral" else "ephemeral"
        settings.worker_residency = new_val
        await session.commit()
    except Exception as e:
        await session.rollback()
        return await answer_callback_error(callback, report_db_error("تنظیمات", e), get_main_menu_button())

    mode_fa = "ساکن" if new_val == "resident" else "لحظه‌ای"
    await safe_callback_answer(callback, f"✅ حالت عضویت به «{mode_fa}» تغییر یافت.")
    await show_speed_mode_menu(callback, state, session)

@router.callback_query(F.data == "set_speed_mode_turbo/")
async def confirm_turbo_mode(callback: types.CallbackQuery, state: FSMContext) -> None:
    await safe_callback_answer(callback)
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ فعال‌سازی توربو", callback_data="confirm_turbo_enable/")
    builder.button(text="❌ انصراف", callback_data="settings_speed_mode/")
    builder.adjust(2)
    text = (
        "⚠️ <b>هشدار فعال‌سازی حالت توربو</b>\n\n"
        "حالت توربو فشار شدیدی به تلگرام می‌آورد. لطفاً موارد زیر را در نظر بگیرید:\n"
        "• میکرو-تأخیرها برای جوین و استخراج به حداقل ممکن می‌رسند.\n"
        "• مدار قطع‌کن خودکار (Circuit Breaker) فعال می‌شود.\n"
        "• با ۲ مسدودی (FloodWait بزرگ) در ۱۰ دقیقه، سیستم به حالت سریع برمی‌گردد.\n\n"
        "آیا با علم به ریسک‌های احتمالی ادامه می‌دهید؟"
    )
    await safe_edit_message(callback.message, text, reply_markup=builder.as_markup())


@router.callback_query(F.data.startswith("set_speed_mode_") | (F.data == "confirm_turbo_enable/"))
async def apply_speed_mode(callback: types.CallbackQuery, state: FSMContext, session: AsyncSession) -> None:
    mode = "turbo" if callback.data == "confirm_turbo_enable/" else callback.data.replace("set_speed_mode_", "").replace("/", "")
    if mode not in ["safe", "fast", "turbo"]:
        return await safe_callback_answer(callback, "❌ حالت نامعتبر است.", show_alert=True)

    try:
        settings = (await session.scalars(select(GlobalSettings).limit(1))).first()
        if not settings:
            return await callback.answer("⚠️ تنظیمات یافت نشد.", show_alert=True)
            
        settings.extraction_speed_mode = mode
        if mode == "turbo":
            settings.turbo_risk_acknowledged = True
            
        await session.commit()
        
        from workers.sender import _get_redis
        await _get_redis().delete("settings:speed_mode")
        
    except Exception as e:
        await session.rollback()
        return await answer_callback_error(callback, report_db_error("تنظیمات", e), get_main_menu_button())

    mode_fa = {"safe": "🔒 امن", "fast": "⚡ سریع", "turbo": "🚀 توربو"}[mode]
    await safe_callback_answer(callback, f"✅ پروفایل سرعت به «{mode_fa}» تغییر یافت.")
    await show_speed_mode_menu(callback, state, session)

# ==========================================
# 🩺 سیستم مانیتورینگ دیداری سلامت اکانت‌ها
# ==========================================
from datetime import datetime, timezone

@router.callback_query(F.data.startswith("menu_account_health_page_"))
async def account_health_dashboard(callback: types.CallbackQuery, session: AsyncSession):
    await safe_callback_answer(callback)
    page = parse_page_from_callback(callback.data)
    
    try:
        total_count = await session.scalar(select(func.count(Account.id))) or 0
        total_pages = calculate_total_pages(total_count)
        page = clamp_page(page, total_pages)
        offset = get_page_offset(page)

        accounts = (await session.scalars(
            select(Account).order_by(Account.id.asc()).offset(offset).limit(PAGINATION_SIZE)
        )).all()
    except Exception as e:
        return await answer_callback_error(callback, report_db_error("اکانت‌ها", e), get_main_menu_button())

    builder = InlineKeyboardBuilder()
    
    text = (
        "🩺 <b>مانیتورینگ سلامت اکانت‌ها</b>\n"
        f"تعداد کل: <b>{total_count}</b> اکانت | صفحه <b>{page}</b> از <b>{total_pages}</b>\n\n"
    )

    now = datetime.now(timezone.utc)

    for acc in accounts:
        # تشخیص وضعیت اکانت
        is_cooldown = acc.expected_return_time and acc.expected_return_time > now
        
        if getattr(acc, "session_invalid", False):
            emoji_status = "⚪️" 
            status_text = "نامعتبر/خروج"
        elif getattr(acc, "spam_restricted", False):
            emoji_status = "🔴"
            status_text = "محدود/اسپم"
        elif is_cooldown:
            emoji_status = "🟡"
            rem_time = acc.expected_return_time - now
            hours, remainder = divmod(int(rem_time.total_seconds()), 3600)
            mins, _ = divmod(remainder, 60)
            status_text = f"استراحت ({hours}h:{mins}m)"
        else:
            emoji_status = "🟢"
            status_text = "سالم و فعال"

        text += f"{emoji_status} <code>{acc.phone_number}</code> - {status_text}\n"
        builder.button(text=f"{emoji_status} {acc.phone_number}", callback_data=f"acc_health_{acc.id}/")

    builder.adjust(2)
    add_pagination_nav_row(builder, page, total_pages, callback_prefix="menu_account_health_")
    
    builder.row(types.InlineKeyboardButton(text="⚙️ بازگشت به تنظیمات", callback_data="menu_settings/"))
    
    await safe_edit_or_answer(callback.message, text, reply_markup=builder.as_markup())


@router.callback_query(F.data.startswith("acc_health_") & F.data.endswith("/"))
async def account_health_detail(callback: types.CallbackQuery, session: AsyncSession):
    await safe_callback_answer(callback)
    acc_id = int(callback.data.replace("acc_health_", "").replace("/", ""))
    
    acc = await session.get(Account, acc_id)
    if not acc:
        return await callback.message.answer("⚠️ اکانت یافت نشد.")

    now = datetime.now(timezone.utc)
    is_cooldown = acc.expected_return_time and acc.expected_return_time > now
    
    builder = InlineKeyboardBuilder()
    if is_cooldown:
        builder.button(text="🔄 لغو دستی استراحت", callback_data=f"reset_cooldown_{acc.id}/")
    else:
        builder.button(text="⏸ اعمال استراحت ۲۴ ساعته", callback_data=f"force_cooldown_{acc.id}/")
        
    builder.button(text="🔙 بازگشت به لیست", callback_data="menu_account_health_page_1/")
    builder.adjust(1)

    status_str = "🟢 فعال" if not getattr(acc, "spam_restricted", False) else "🔴 اسپم"
    text = (
        f"👤 <b>جزئیات سلامت اکانت</b>\n\n"
        f"📱 شماره: <code>{acc.phone_number}</code>\n"
        f"وضعیت فعلی: {status_str}\n"
        f"استراحت فعال: {'بله' if is_cooldown else 'خیر'}\n"
    )
    
    await safe_edit_message(callback.message, text, reply_markup=builder.as_markup())