import logging
import re
import time
import random
import asyncio
from typing import Dict, Optional, Tuple
from contextlib import suppress
import html
import os
import uuid
from sqlalchemy import text
from utils.advanced_anti_ban import terminate_other_sessions
from aiogram import Router, types, F
from aiogram.fsm.context import FSMContext
from aiogram.utils.keyboard import InlineKeyboardBuilder
from pyrogram import Client
from pyrogram.errors import (
    SessionPasswordNeeded,
    PhoneCodeInvalid,
    PhoneCodeExpired,
    PasswordHashInvalid
)
from utils.health_checker import report_proxy_result
from sqlalchemy import or_
from pyrogram.errors import AuthKeyUnregistered, SessionRevoked, UserDeactivated, UserDeactivatedBan, Unauthorized, AuthKeyDuplicated
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from workers.session_manager import (
    parse_proxy_string,
    start_single_worker,
    claim_proxy_for_account,
    release_proxy_slot,
    mark_proxy_failed,
)
from database.models import Account, Category, APIKey, GlobalSettings, Proxy
from database.engine import async_session
import secrets
from bot.states.login_fsm import LoginStates
from config import config
from utils.crypto import encrypt_session, mask_phone

# 🟣 فاز ۲ (رفع بن‌بست FSM): افزودن import های زیر
#   - with_cancel_hint       : راهنمای استاندارد /cancel برای انتهای همه پیام‌های FSM prompt
#   - get_main_menu_keyboard : از وسط فایل به ابتدای فایل منتقل شد (اصلاح ساختاری import)
#   - cleanup_fsm_temp_files : پاک‌ساز عمومی فایل‌های موقت فلوی قبلی هنگام ورود به فلوی جدید
#   - safe_edit_or_answer    : ویرایش امن پیام با fallback (جلوگیری از گیر کردن پس از خطای edit_text)
from bot.keyboards.cancel import with_cancel_hint
from bot.keyboards.main_menu import get_main_menu_keyboard, MAIN_MENU_LAYOUT
from utils.fsm_cleanup import cleanup_fsm_temp_files
from aiogram.filters import Command
from datetime import datetime, timedelta, timezone
from workers.session_manager import warmup_hours
import tempfile
_KNOWN_MENU_BUTTON_TEXTS = {t["text"] for row in MAIN_MENU_LAYOUT for t in row} | {"❌ انصراف"}
from utils.safe_edit import safe_edit_or_answer

# 🔴 اصلاح به سبک فاز ۶: ادغام import های تکراری
# (قبلاً «from sqlalchemy import func» دوبار و import های database/workers جدا بودند)

logger = logging.getLogger(__name__)

router = Router(name="login_fsm_router")

temp_clients: Dict[int, Client] = {}


# ==========================================
# 🔴 فاز ۱۱ (BUG-28): مهلت نشست لاگین + رزرو ظرفیت API
# ==========================================
# حداکثر عمر یک فلوی لاگین از لحظه ارسال کد تا وارد کردن پسورد 2FA.
# پس از این مهلت، نشست «منقضی» و پاکسازی می‌شود.
LOGIN_SESSION_TIMEOUT = 900  # ۱۵ دقیقه

# عمر رزرو ظرفیت؛ باید بزرگ‌تر از مجموع مهلت مراحل لاگین باشد
# (حداکثر عمر مجاز فلوی زنده = ۱۵ دقیقه مرحله کد + ۱۵ دقیقه مرحله 2FA = ۳۰ دقیقه)
# تا هیچ فلوی زنده‌ای رزروش را از دست ندهد؛ رزروِ فلوی رهاشده پس از این مهلت جمع می‌شود.
RESERVATION_TTL = 3600  # ۱ ساعت

# admin_id -> (db_api_key_id, زمان رزرو)
_active_login_reservations: Dict[int, Tuple[int, float]] = {}
_login_capacity_lock: Optional[asyncio.Lock] = None

async def release_login_reservations(admin_id: int, session: AsyncSession) -> None:
    """آزادسازی امن رزرو پراکسی و ظرفیت API ادمین — idempotent و fail-safe."""
    try:
        # ۱. آزادسازی رزرو API
        _release_admin_reservation(admin_id)

        # ۲. آزادسازی رزرو پراکسی در صورت وجود
        proxy_str = _active_proxy_reservations.pop(admin_id, None)
        if proxy_str:
            # کاهش امن ظرفیت رزرو شده در دیتابیس (جلوگیری از double-release)
            stmt = text("UPDATE proxies SET in_use = in_use - 1 WHERE proxy_string = :p AND in_use > 0")
            await session.execute(stmt, {"p": proxy_str})
            await session.commit()
    except Exception as e:
        logger.error(f"Error releasing login reservations for admin {admin_id}: {e}")
        with suppress(Exception):
            await session.rollback()

def _get_login_capacity_lock() -> asyncio.Lock:
    """ساخت تنبلِ قفل — در اولین استفاده داخل event loop فعال ساخته می‌شود
    (سازگار با نسخه‌های پایتون که قفل را هنگامِ ساخت به loop مقید می‌کنند)."""
    global _login_capacity_lock
    if _login_capacity_lock is None:
        _login_capacity_lock = asyncio.Lock()
    return _login_capacity_lock


def _prune_expired_reservations(now: Optional[float] = None) -> None:
    """حذف رزروهای رهاشده (جلوگیری از نشت ظرفیت/حافظه در فلوی نیمه‌کاره)."""
    now = time.time() if now is None else now
    for admin_id, (_, reserved_at) in list(_active_login_reservations.items()):
        if now - reserved_at > RESERVATION_TTL:
            _active_login_reservations.pop(admin_id, None)


def _reserve_api_slot(admin_id: int, api_db_key_id: int) -> None:
    """ثبت رزرو یک جایگاه ظرفیت برای API انتخاب‌شده توسط این ادمین."""
    _prune_expired_reservations()
    _active_login_reservations[admin_id] = (api_db_key_id, time.time())


def _release_admin_reservation(admin_id: int) -> None:
    """آزادسازی رزرو — باید در «تمام» مسیرهای خروج صدا زده شود:
    موفقیت (پس از commit)، خطا، انصراف، تایم‌اوت. (idempotent است)"""
    _active_login_reservations.pop(admin_id, None)


def _reserved_count_for_api(api_db_key_id: int) -> int:
    """تعداد رزروهای فعال روی یک API (لاگین‌هایی که هنوز commit نشده‌اند)."""
    _prune_expired_reservations()
    return sum(1 for (api_id, _) in _active_login_reservations.values() if api_id == api_db_key_id)


# ==========================================
# 🧲 فاز ۴ (رفع آنتی‌پترن لاگین): رزرو اسلات پراکسی
# ==========================================
# mirror الگوی _active_login_reservations: یک دیکشنری در سطح ماژول که
# admin_id را به proxy_string ای که در process_phone_number به‌صورت اتمیک
# claim شده (in_use++) نگه می‌دارد. این رزرو صرفاً for cleanup tracking است
# تا در مسیرهای شکست/انصراف بتوانیم release_proxy_slot را صدا بزنیم.
#
# چرا in-memory و نه در DB؟ چون مکانیزم مشابه برای رزرو API همین‌گونه است و
# نشتی احتمالی (مثلاً کرش سرور وسط فلوی لاگین) توسط reconcile_proxy_usage
# در استارتاپ بعدی از منبع حقیقت (accounts.proxy_string) بازسازی می‌شود.
_active_proxy_reservations: Dict[int, str] = {}


def _reserve_proxy_slot(admin_id: int, proxy_string: str) -> None:
    """ثبت proxy_stringای که claim شده تا در صورت شکست فلوی لاگین آزاد شود.
    اگر رزرو قبلی برای همین admin_id وجود داشته باشد (فلوی رهاشده‌ی قبلی)،
    مقدار جدید جایگزین می‌شود — نشتی DB در این حالت نادر به reconcile
    استارتاپی سپرده می‌شود (همانند _release_admin_reservation)."""
    _active_proxy_reservations[admin_id] = proxy_string


async def _release_admin_proxy_slot(admin_id: int) -> None:
    """آزادسازی اسلات پراکسیِ claimشده در جریان لاگینِ شکست‌خورده/لغوشده.
    باید در «تمام» مسیرهای خروج شکست/انصراف (نه موفقیت) صدا زده شود و
    idempotent است. از یک session تازه استفاده می‌کند تا تحت تأثیر rollbackِ
    تراکنشِ caller نباشد (مثلاً وقتی finalize_login_and_save بعد از rollback
    این را صدا می‌زند). در مسیر موفقیت finalize فقط باید از دیکشنری pop شود
    (اکانت با proxy_string commit شده و مالک اسلات است)."""
    proxy_str = _active_proxy_reservations.pop(admin_id, None)
    if not proxy_str:
        return
    try:
        async with async_session() as rel_session:
            await release_proxy_slot(rel_session, proxy_str)
            await rel_session.commit()
    except Exception as e:
        logger.error(
            f"Failed to release claimed proxy slot for admin {admin_id} "
            f"(proxy={proxy_str[:24]}...): {e}"
        )
        # ایمنی نهایی: reconcile_proxy_usage در استارتاپ بعدی، in_use را از
        # accounts.proxy_string بازسازی می‌کند و نشتی را به‌صورت خودترمیم پاک می‌کند.



# ==========================================
# 🟣 فاز ۲: کیبورد انصراف اختصاصی مراحل لاگین
# ==========================================
def get_login_cancel_keyboard():
    """
    کیبورد انصراف مراحل لاگین.

    برخلاف کیبورد استاندارد (cancel_current_flow/)، این کیبورد به هندلر
    اختصاصی cancel_login_flow/ متصل است که علاوه بر پاک کردن state،
    کلاینت موقت Pyrogram (temp_clients) را نیز به صورت امن می‌بندد.

    🟣 فاز ۲: دکمه «🏛 منوی اصلی» اضافه شد تا کاربر همیشه مسیر خروج داشته باشد.
    """
    builder = InlineKeyboardBuilder()
    builder.button(text="❌ انصراف", callback_data="cancel_login_flow/")
    builder.button(text="🏛 منوی اصلی", callback_data="menu_home/")
    builder.adjust(2)
    return builder.as_markup()


# ==========================================
# CONSTANTS: SPOOFING DATA
# ==========================================
DEVICE_MODELS = [
    "iPhone 13 Pro Max", "iPhone 14 Plus", "iPhone 15 Pro",
    "Samsung Galaxy S22 Ultra", "Samsung Galaxy S23",
    "Xiaomi 13 Pro", "OnePlus 11", "Google Pixel 7"
]
SYSTEM_VERSIONS = ["15.0", "16.0", "13.0", "12.0", "14.0", "17.0"]
APP_VERSIONS = ["9.6.5", "9.7.0", "9.5.2", "10.0.1", "10.1.3", "10.2.0"]


# ==========================================
# ENTRY POINT: WAITING FOR CATEGORY
# ==========================================
@router.callback_query(F.data == "menu_add_account/")
async def enter_add_account_flow(callback: types.CallbackQuery, state: FSMContext, session: AsyncSession) -> None:
    await callback.answer()

    # 🟣 فاز ۲ (اصلاح اضافی ۴): پاکسازی state و فایل‌های موقت فلوی قبلی در ابتدای تابع.
    # اگر کاربر وسط فلوی دیگری (مثل ثبت سفارش با فایل‌های آپلودی) بوده باشد،
    # فایل‌های موقتش با این کار orphan نمی‌مانند.
    await cleanup_fsm_temp_files(state)
    await state.clear()
    # 🟣 فاز ۲: پاکسازی کلاینت موقت کهنه (اگر فلوی لاگین قبلی نیمه‌کاره رها شده باشد)
    await cleanup_client(callback.from_user.id)
    # 🔴 فاز ۱۱ (BUG-28c): آزادسازی رزرو فلوی لاگین نیمه‌کاره قبلی
    _release_admin_reservation(callback.from_user.id)
    # 🧲 فاز ۴: آزادسازی اسلات پراکسیِ claimشده‌ی فلوی قبلی (در صورت وجود)
    await _release_admin_proxy_slot(callback.from_user.id)

    stmt = select(Category)
    result = await session.execute(stmt)
    categories = result.scalars().all()

    if not categories:
        # 🟣 فاز ۲: edit_text به جای answer (جلوگیری از duplicate UI)
        # + کیبورد ناوبری به تنظیمات و منوی اصلی (رفع بن‌بست)
        builder = InlineKeyboardBuilder()
        builder.button(text="⚙️ رفتن به تنظیمات", callback_data="menu_settings/")
        builder.button(text="🏛 منوی اصلی", callback_data="menu_home/")
        builder.adjust(2)
        return await safe_edit_or_answer(
            callback.message,
            "⚠️ هیچ دسته‌بندی یافت نشد. لطفاً ابتدا از بخش تنظیمات یک دسته‌بندی ایجاد کنید.",
            reply_markup=builder.as_markup()
        )

    builder = InlineKeyboardBuilder()
    for cat in categories:
        # استفاده از الگوی استاندارد همراه با اسلش انتهایی
        builder.button(text=cat.name, callback_data=f"logincat_{cat.id}/")
    builder.adjust(2)

    # 🟣 فاز ۲: دکمه انصراف و بازگشت به منو از همین مرحله اول
    # (رفع بن‌بست waiting_for_category — قبلاً فقط از waiting_for_phone به بعد دکمه انصراف وجود داشت)
    builder.row(
        types.InlineKeyboardButton(text="❌ انصراف", callback_data="cancel_login_flow/"),
        types.InlineKeyboardButton(text="🏛 منوی اصلی", callback_data="menu_home/"),
    )

    await state.set_state(LoginStates.waiting_for_category)

    # 🟣 فاز ۲: edit_text به جای answer (جلوگیری از duplicate UI — پیام قبلی منو جایگزین می‌شود)
    # + راهنمای استاندارد /cancel در متن prompt
    await safe_edit_or_answer(
        callback.message,
        with_cancel_hint(
            "📁 <b>انتخاب دسته‌بندی</b>\n\n"
            "لطفاً مشخص کنید این اکانت به کدام دسته‌بندی تعلق دارد:"
        ),
        reply_markup=builder.as_markup()
    )


# ==========================================
# STATE: PROCESS CATEGORY & ASK FOR PHONE
# ==========================================
@router.callback_query(LoginStates.waiting_for_login_method, F.data == "login_method_zip/")
async def method_zip_selected(callback: types.CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    await state.set_state(LoginStates.waiting_for_string_session)
    
    with suppress(Exception):
        await callback.message.delete()
        
    await callback.message.answer(
        with_cancel_hint(
            "📦 <b>افزودن گروهی اکانت‌ها (فایل ZIP)</b>\n\n"
            "لطفاً یک فایل <b><code>.zip</code></b> حاوی فایل‌های <code>.session</code> را به صورت Document ارسال کنید:"
        ),
        reply_markup=get_login_cancel_keyboard()
    )

@router.callback_query(LoginStates.waiting_for_category, F.data.startswith("logincat_") & F.data.endswith("/"))
async def process_category_selection(callback: types.CallbackQuery, state: FSMContext) -> None:
    raw_id = callback.data.replace("logincat_", "").replace("/", "")
    if not raw_id.isdigit():
        return await callback.answer("⚠️ خطای نامعتبر در انتخاب دسته‌بندی.", show_alert=True)

    await callback.answer()
    category_id = int(raw_id)
    await state.update_data(category_id=category_id)
    await state.set_state(LoginStates.waiting_for_login_method)
    
    # تغییرات کیبورد برای اضافه شدن گزینه ZIP
    builder = InlineKeyboardBuilder()
    builder.button(text="📱 با شماره تلفن", callback_data="login_method_phone/")
    builder.button(text="🔑 با سشن (تکی)", callback_data="login_method_session/")
    builder.button(text="📦 با فایل Zip (گروهی)", callback_data="login_method_zip/")
    builder.adjust(1, 2) # دکمه شماره در یک سطر، سشن و زیپ در سطر بعدی
    
    builder.row(
        types.InlineKeyboardButton(text="❌ انصراف", callback_data="cancel_login_flow/"),
        types.InlineKeyboardButton(text="🏛 منوی اصلی", callback_data="menu_home/"),
    )
    
    await safe_edit_or_answer(
        callback.message,
        with_cancel_hint(
            "⚙️ <b>انتخاب روش افزودن اکانت</b>\n\n"
            "لطفاً مشخص کنید قصد دارید اکانت‌ها را چگونه وارد کنید:"
        ),
        reply_markup=builder.as_markup()
    )


# ==========================================
# STATE: WAITING FOR PHONE
# ==========================================

@router.message(LoginStates.waiting_for_phone, F.text)
async def process_phone_number(message: types.Message, state: FSMContext, session: AsyncSession) -> None:
    phone_number = message.text.strip()
    admin_id = message.from_user.id

    # گارد جلوگیری از باگ الف (دکمه‌های متنی باقیمانده)
    if phone_number == "🏛 منوی اصلی":
        await cleanup_client(admin_id)
        await release_login_reservations(admin_id, session)
        await state.clear()
        return await message.answer("🏛 شما به منوی اصلی بازگشتید.", reply_markup=get_main_menu_keyboard())
    
    if phone_number == "❌ انصراف":
        await cleanup_client(admin_id)
        await release_login_reservations(admin_id, session)
        await state.clear()
        return await message.answer("🚫 <b>عملیات لاگین لغو شد.</b>\nنشست‌های موقت به صورت امن از حافظه پاک شدند.", reply_markup=get_main_menu_keyboard())
        
    if phone_number in _KNOWN_MENU_BUTTON_TEXTS:
        return await message.answer("⚠️ شما در میانه فلوی لاگین هستید. ابتدا «❌ انصراف» را بزنید یا فلوی فعلی را کامل کنید.")

    if not re.match(r"^\+\d{7,15}$", phone_number):
        return await message.answer(
            with_cancel_hint("⚠️ فرمت نامعتبر! لطفاً شماره موبایل را با فرمت بین‌المللی و همراه با علامت + ارسال کنید."),
            reply_markup=get_login_cancel_keyboard()
        )

    stmt_acc = select(Account).where(Account.phone_number == phone_number)
    result_acc = await session.execute(stmt_acc)
    if result_acc.scalar_one_or_none():
        return await message.answer(
            with_cancel_hint("⚠️ این شماره موبایل قبلاً در سیستم ثبت شده است. لطفاً شماره دیگری ارسال کنید."),
            reply_markup=get_login_cancel_keyboard()
        )

    # بازخورد فوری به کاربر
    ack_msg = await message.answer(
        f"✅ شماره <code>{phone_number}</code> دریافت شد.\n⏳ در حال اتصال به تلگرام و تخصیص پروکسی لاگین..."
    )

    settings_stmt = select(GlobalSettings).limit(1)
    settings = await session.scalar(settings_stmt)
    max_acc_per_api = settings.max_accounts_per_api if settings else 1

    async with _get_login_capacity_lock():
        candidates_stmt = (
            select(APIKey.id, func.count(Account.id))
            .outerjoin(Account, Account.api_id == APIKey.id)
            .where(APIKey.is_active == True)
            .group_by(APIKey.id)
        )
        result = await session.execute(candidates_stmt)
        candidates = result.all()

        api_obj = None
        for api_key_id, committed_count in candidates:
            if committed_count + _reserved_count_for_api(api_key_id) < max_acc_per_api:
                stmt_api = (
                    select(APIKey)
                    .where(APIKey.id == api_key_id)
                    .with_for_update(skip_locked=True)
                )
                api_obj = await session.scalar(stmt_api)
                if api_obj:
                    break

        if not api_obj:
            await state.clear()
            builder = InlineKeyboardBuilder()
            builder.button(text="⚙️ رفتن به تنظیمات", callback_data="menu_settings/")
            builder.button(text="🏛 منوی اصلی", callback_data="menu_home/")
            builder.adjust(2)
            return await safe_edit_or_answer(
                ack_msg,
                with_cancel_hint(
                    "⚠️ <b>ظرفیت تکمیل است!</b>\n"
                    "هیچ API آزادی که ظرفیت خالی داشته باشد یافت نشد. لطفاً یک API جدید اضافه کنید."
                ),
                reply_markup=builder.as_markup()
            )

        _reserve_api_slot(admin_id, api_obj.id)

    active_api_id = int(api_obj.api_id)
    active_api_hash = str(api_obj.api_hash)
    db_api_key_id = int(api_obj.id)

    # 🟢 دریافت پروکسی اختصاصی لاگین بدون اشغال ظرفیت (کوئری دو مرحله‌ای)
    stmt_total_login_proxies = select(func.count(Proxy.id)).where(Proxy.usage_type.in_(("login", "both")))
    total_login_proxies = await session.scalar(stmt_total_login_proxies) or 0

    proxy_string = None
    proxy_dict = None

    if total_login_proxies == 0:
        from workers.session_manager import login_proxy_dict
        env_proxy_dict = login_proxy_dict()
        
        if env_proxy_dict:
            proxy_string = getattr(config, "LOGIN_PROXY_URL", "")
            proxy_dict = env_proxy_dict
        else:
            await state.clear()
            await release_login_reservations(admin_id, session)
            builder = InlineKeyboardBuilder()
            builder.button(text="⚙️ رفتن به تنظیمات", callback_data="menu_settings/")
            builder.button(text="🏛 منوی اصلی", callback_data="menu_home/")
            builder.adjust(2)
            return await safe_edit_or_answer(
                ack_msg,
                "شما هنوز هیچ پروکسی لاگینی در سیستم ثبت نکرده‌اید. لطفاً ابتدا از بخش تنظیمات یک پروکسی لاگین ثبت کنید یا متغیر LOGIN_PROXY_URL را مقداردهی نمایید.",
                reply_markup=builder.as_markup()
            )
    else:
        stmt_login_proxy = (
            select(Proxy)
            .where(Proxy.usage_type.in_(("login", "both")), Proxy.is_active == True, Proxy.health_state != "DEAD")
            .order_by(func.random())
            .limit(1)
        )
        login_proxy_obj = await session.scalar(stmt_login_proxy)
        
        if not login_proxy_obj:
            await state.clear()
            await release_login_reservations(admin_id, session)
            builder = InlineKeyboardBuilder()
            builder.button(text="⚙️ رفتن به تنظیمات", callback_data="menu_settings/")
            builder.button(text="🏛 منوی اصلی", callback_data="menu_home/")
            builder.adjust(2)
            return await safe_edit_or_answer(
                ack_msg,
                "شما پروکسی لاگین در سیستم دارید، اما در حال حاضر همگی از دسترس خارج (DEAD) شده‌اند. لطفاً وضعیت سرور پروکسی خود را بررسی کنید.",
                reply_markup=builder.as_markup()
            )
            
        proxy_string = login_proxy_obj.proxy_string
        proxy_dict = parse_proxy_string(proxy_string)
    
    if not proxy_dict:
        logger.error(f"Login proxy for admin {admin_id} is malformed.")
        await state.clear()
        await release_login_reservations(admin_id, session)
        builder = InlineKeyboardBuilder()
        builder.button(text="⚙️ رفتن به تنظیمات", callback_data="menu_settings/")
        builder.button(text="🏛 منوی اصلی", callback_data="menu_home/")
        builder.adjust(2)
        return await safe_edit_or_answer(
            ack_msg,
            "🚨 <b>اخطار امنیتی:</b>\n\n"
            "پراکسی لاگین یافت شده معتبر نیست (parse نشد). عملیات متوقف شد.",
            reply_markup=builder.as_markup()
        )

    device_model = random.choice(DEVICE_MODELS)
    system_version = random.choice(SYSTEM_VERSIONS)
    app_version = random.choice(APP_VERSIONS)

    for attempt in range(2):
        client = Client(
            name=f"temp_{admin_id}",
            api_id=active_api_id,
            api_hash=active_api_hash,
            proxy=proxy_dict,
            in_memory=True,
            device_model=str(device_model),
            system_version=str(system_version),
            app_version=str(app_version),
            lang_code="en"
        )

        temp_clients[admin_id] = client

        try:
            await asyncio.wait_for(client.connect(), timeout=45)
            sent_code = await asyncio.wait_for(client.send_code(phone_number), timeout=45)
            
            if await state.get_state() is None:
                return

            # رفع باگ struct.error
            if await client.storage.user_id() is None:
                await client.storage.user_id(0)

            temp_session = await client.export_session_string()

            # ذخیره پروکسی لاگین فقط به عنوان سابقه موقت در FSM
            await state.update_data(
                phone_number=phone_number,
                phone_code_hash=sent_code.phone_code_hash,
                temp_session=temp_session,
                api_id=active_api_id,
                api_hash=active_api_hash,
                db_api_key_id=db_api_key_id,
                device_model=device_model,
                system_version=system_version,
                app_version=app_version,
                proxy_string=proxy_string,
                login_flow_started_at=time.time()
            )

            await state.set_state(LoginStates.waiting_for_code)
            await safe_edit_or_answer(
                ack_msg,
                with_cancel_hint(f"✅ کد تایید به <code>{phone_number}</code> ارسال شد.\nلطفاً کد ۵ رقمی را وارد کنید:"),
                reply_markup=get_login_cancel_keyboard()
            )
            break

        except Exception as e:
            logger.error(f"Failed to send code for {mask_phone(phone_number)} (Attempt {attempt+1}): {e}")
            await cleanup_client(admin_id)

            # ارسال سیگنال خرابی برای پروکسی بلافاصله پس از بروز خطا
            if proxy_string:
                asyncio.create_task(report_proxy_result(proxy_string, is_success=False))

            if attempt == 0:
                # تلاش با یک پروکسی لاگین جایگزین در صورت بروز خطا
                # 🩹 FIX: فیلتر health_state != 'DEAD' (همان انتخاب اولیه) — قبلاً
                # is_healthy==True بود و پروکسی‌های WEAK سینک‌شده از ربات‌ساز را هم
                # حذف می‌کرد ← پیام کاذب «پروکسی جایگزین سالمی یافت نشد» با وجود
                # پروکسی‌های قابل‌استفاده.
                stmt_retry = (
                    select(Proxy)
                    .where(Proxy.usage_type.in_(("login", "both")), Proxy.is_active == True, Proxy.health_state != "DEAD", Proxy.proxy_string != proxy_string)
                    .order_by(func.random())
                    .limit(1)
                )
                # اعمال همان شروط انتخاب اولیه (usage_type/is_active/غیرDEAD) برای دریافت پراکسی جایگزین در مسیر خطا.
                retry_proxy = await session.scalar(stmt_retry)
                if not retry_proxy:
                    e = Exception("پروکسی لاگین جایگزین سالمی برای تلاش مجدد یافت نشد. لطفاً در دیتابیس پراکسی جدید اضافه کنید یا متغیر LOGIN_PROXY_URL را ست کنید.")
                else:
                    proxy_string = retry_proxy.proxy_string
                    proxy_dict = parse_proxy_string(proxy_string)
                    if not proxy_dict:
                        e = Exception("پروکسی لاگین جایگزین نامعتبر است.")
                    else:
                        continue

            await state.clear()
            await release_login_reservations(admin_id, session)
            
            if isinstance(e, asyncio.TimeoutError):
                error_text = "⏱ مهلت اتصال به تلگرام (۴۵ ثانیه) به پایان رسید. به دلیل کندی شبکه ارتباط قطع شد. لطفاً دوباره تلاش کنید."
            else:
                error_text = f"❌ <b>خطا:</b> امکان ارسال کد وجود ندارد.\n\n<code>{html.escape(str(e))}</code>\n\n<i>برای تلاش مجدد، از منوی اصلی دوباره «اضافه کردن اکانت» را انتخاب کنید.</i>"

            await safe_edit_or_answer(
                ack_msg,
                error_text,
                reply_markup=get_main_menu_keyboard()
            )
            break


@router.callback_query(LoginStates.waiting_for_login_method, F.data == "login_method_phone/")
async def method_phone_selected(callback: types.CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    await state.set_state(LoginStates.waiting_for_phone)
    
    with suppress(Exception):
        await callback.message.delete()
        
    await callback.message.answer(
        with_cancel_hint(
            "📱 <b>اضافه کردن اکانت جدید</b>\n\n"
            "لطفاً شماره موبایل را با فرمت بین‌المللی ارسال کنید.\n"
            "<i>مثال: +1234567890</i>"
        ),
        reply_markup=get_login_cancel_keyboard()
    )

@router.callback_query(LoginStates.waiting_for_login_method, F.data == "login_method_session/")
async def method_session_selected(callback: types.CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    await state.set_state(LoginStates.waiting_for_string_session)
    
    with suppress(Exception):
        await callback.message.delete()
        
    await callback.message.answer(
        with_cancel_hint(
            "🔑 <b>افزودن اکانت با StringSession یا فایل</b>\n\n"
            "لطفاً رشته متنی StringSession و یا <b>فایل <code>.session</code></b> خود را (به صورت Document) ارسال کنید:"
        ),
        reply_markup=get_login_cancel_keyboard()
    )


# ==========================================
# STATE: WAITING FOR CODE
# ==========================================
@router.message(LoginStates.waiting_for_code, F.text)
async def process_auth_code(message: types.Message, state: FSMContext, session: AsyncSession) -> None:
    code = message.text.strip()
    admin_id = message.from_user.id

    # گارد جلوگیری از باگ الف (دکمه‌های متنی باقیمانده)
    if code == "🏛 منوی اصلی":
        await cleanup_client(admin_id)
        await release_login_reservations(admin_id, session)
        await state.clear()
        return await message.answer("🏛 شما به منوی اصلی بازگشتید.", reply_markup=get_main_menu_keyboard())
    
    if code == "❌ انصراف":
        await cleanup_client(admin_id)
        await release_login_reservations(admin_id, session)
        await state.clear()
        return await message.answer("🚫 <b>عملیات لاگین لغو شد.</b>\nنشست‌های موقت به صورت امن از حافظه پاک شدند.", reply_markup=get_main_menu_keyboard())
        
    if code in _KNOWN_MENU_BUTTON_TEXTS:
        return await message.answer("⚠️ شما در میانه فلوی لاگین هستید. ابتدا «❌ انصراف» را بزنید یا فلوی فعلی را کامل کنید.")

    fsm_data = await state.get_data()

    started_at = fsm_data.get("login_flow_started_at")
    if started_at and (time.time() - started_at) > LOGIN_SESSION_TIMEOUT:
        await state.clear()
        await release_login_reservations(admin_id, session)
        return await message.answer(
            "⏱ <b>نشست لاگین منقضی شده است.</b>\n\n"
            "برای امنیت، فلوی لاگین بیش از ۱۵ دقیقه معتبر نیست.\n"
            "لطفاً از منوی اصلی دوباره «اضافه کردن اکانت» را انتخاب کنید.",
            reply_markup=get_main_menu_keyboard()
        )

    temp_session = fsm_data.get("temp_session")
    if not temp_session:
        await state.clear()
        await release_login_reservations(admin_id, session)
        return await message.answer(
            "⚠️ نشست منقضی شده است. لطفاً از منوی اصلی دوباره «اضافه کردن اکانت» را انتخاب کنید.",
            reply_markup=get_main_menu_keyboard()
        )

    phone_number = fsm_data.get("phone_number")
    phone_code_hash = fsm_data.get("phone_code_hash")
    active_api_id = fsm_data.get("api_id", config.API_ID)
    active_api_hash = fsm_data.get("api_hash", config.API_HASH)
    dev_model = fsm_data.get("device_model", random.choice(DEVICE_MODELS))
    sys_ver = fsm_data.get("system_version", random.choice(SYSTEM_VERSIONS))
    app_ver = fsm_data.get("app_version", random.choice(APP_VERSIONS))
    proxy_string = fsm_data.get("proxy_string")
    proxy_dict = parse_proxy_string(proxy_string) if proxy_string else None

    client = Client(
        name="temp_auth",
        api_id=active_api_id,
        api_hash=active_api_hash,
        session_string=temp_session,
        proxy=proxy_dict,
        in_memory=True,
        device_model=dev_model,
        system_version=sys_ver,
        app_version=app_ver,
        lang_code="en"
    )

    try:
        await asyncio.wait_for(client.connect(), timeout=30)
        await asyncio.wait_for(
            client.sign_in(
                phone_number=phone_number,
                phone_code_hash=phone_code_hash,
                phone_code=code
            ),
            timeout=30
        )
        with suppress(Exception):
            await message.delete()
        await finalize_login_and_save(message, state, session, client, phone_number, code)

    except asyncio.TimeoutError:
        if proxy_string:
            asyncio.create_task(report_proxy_result(proxy_string, is_success=False))
        # بدون ریست state
        await message.answer("⏱ مهلت پاسخگویی تلگرام به پایان رسید. لطفاً چند لحظه بعد دوباره کد را ارسال کنید.")
        
    except SessionPasswordNeeded:
        with suppress(Exception):
            await message.delete()
            
        # +++ اصلاح قطعی باگ struct.error برای مرحله پسورد +++
        if await client.storage.user_id() is None:
            await client.storage.user_id(0)
        # +++++++++++++++++++++++++++++++++++++++++++++++++++
        
        new_temp_session = await client.export_session_string()
        await state.update_data(
            temp_session=new_temp_session,
            last_login_code=code,
            login_flow_started_at=time.time()
        )
        await state.set_state(LoginStates.waiting_for_password)
        await message.answer(
            with_cancel_hint("🔐 <b>تایید دو مرحله‌ای (2FA) فعال است.</b>\n\nلطفاً پسورد خود را وارد کنید:"),
            reply_markup=get_login_cancel_keyboard()
        )

    except PhoneCodeInvalid as e:
        await message.answer(
            with_cancel_hint(f"❌ <b>کد نامعتبر:</b> {html.escape(str(e))}\nلطفاً دوباره تلاش کنید."),
            reply_markup=get_login_cancel_keyboard()
        )
        
    except PhoneCodeExpired as e:
        await state.clear()
        await release_login_reservations(admin_id, session)
        builder = InlineKeyboardBuilder()
        builder.button(text="📱 افزودن اکانت", callback_data="menu_add_account/")
        await message.answer(
            "⏰ <b>مهلت اعتبار کد تمام شده است.</b>\n"
            "کد ارسالی منقضی شده است. لطفاً فلوی لاگین را از نو شروع کنید.",
            reply_markup=builder.as_markup()
        )

    except Exception as e:
        logger.error(f"Sign in error: {e}")
        if proxy_string:
            asyncio.create_task(report_proxy_result(proxy_string, is_success=False))
        await state.clear()
        await release_login_reservations(admin_id, session)
        await message.answer(
            f"❌ <b>خطای پیش‌بینی نشده:</b>\n\n<code>{html.escape(str(e))}</code>\n\n"
            "<i>برای تلاش مجدد، از منوی اصلی دوباره «اضافه کردن اکانت» را انتخاب کنید.</i>",
            reply_markup=get_main_menu_keyboard()
        )
    finally:
        if client.is_connected:
            await client.disconnect()


# ==========================================
# STATE: WAITING FOR PASSWORD (2FA)
# ==========================================
@router.message(LoginStates.waiting_for_password, F.text)
async def process_2fa_password(message: types.Message, state: FSMContext, session: AsyncSession) -> None:
    password = message.text.strip()
    admin_id = message.from_user.id

    # گارد جلوگیری از باگ الف (دکمه‌های متنی باقیمانده)
    if password == "🏛 منوی اصلی":
        await cleanup_client(admin_id)
        await release_login_reservations(admin_id, session)
        await state.clear()
        return await message.answer("🏛 شما به منوی اصلی بازگشتید.", reply_markup=get_main_menu_keyboard())
    
    if password == "❌ انصراف":
        await cleanup_client(admin_id)
        await release_login_reservations(admin_id, session)
        await state.clear()
        return await message.answer("🚫 <b>عملیات لاگین لغو شد.</b>\nنشست‌های موقت به صورت امن از حافظه پاک شدند.", reply_markup=get_main_menu_keyboard())
        
    if password in _KNOWN_MENU_BUTTON_TEXTS:
        return await message.answer("⚠️ شما در میانه فلوی لاگین هستید. ابتدا «❌ انصراف» را بزنید یا فلوی فعلی را کامل کنید.")

    with suppress(Exception):
        await message.delete()
        
    progress = await message.answer("⏳ در حال بررسی رمز دوم...")

    fsm_data = await state.get_data()

    started_at = fsm_data.get("login_flow_started_at")
    if started_at and (time.time() - started_at) > LOGIN_SESSION_TIMEOUT:
        await state.clear()
        _release_admin_reservation(admin_id)
        await _release_admin_proxy_slot(admin_id)
        return await message.answer(
            "⏱ <b>نشست لاگین منقضی شده است.</b>\n\n"
            "برای امنیت، فلوی لاگین بیش از ۱۵ دقیقه معتبر نیست.\n"
            "لطفاً از منوی اصلی دوباره «اضافه کردن اکانت» را انتخاب کنید.",
            reply_markup=get_main_menu_keyboard()
        )

    temp_session = fsm_data.get("temp_session")
    phone_number = fsm_data.get("phone_number")
    active_api_id = fsm_data.get("api_id", config.API_ID)
    active_api_hash = fsm_data.get("api_hash", config.API_HASH)
    last_login_code = fsm_data.get("last_login_code")

    dev_model = fsm_data.get("device_model", random.choice(DEVICE_MODELS))
    sys_ver = fsm_data.get("system_version", random.choice(SYSTEM_VERSIONS))
    app_ver = fsm_data.get("app_version", random.choice(APP_VERSIONS))

    if not temp_session:
        await state.clear()
        _release_admin_reservation(admin_id)
        await _release_admin_proxy_slot(admin_id)
        return await message.answer(
            "⚠️ نشست منقضی شده است. لطفاً از منوی اصلی دوباره «اضافه کردن اکانت» را انتخاب کنید.",
            reply_markup=get_main_menu_keyboard()
        )

    proxy_string = fsm_data.get("proxy_string")
    proxy_dict = parse_proxy_string(proxy_string) if proxy_string else None

    client = Client(
        name="temp_auth",
        api_id=active_api_id,
        api_hash=active_api_hash,
        session_string=temp_session,
        proxy=proxy_dict,
        in_memory=True,
        device_model=dev_model,
        system_version=sys_ver,
        app_version=app_ver,
        lang_code="en"
    )

    try:
        await asyncio.wait_for(client.connect(), timeout=30)
        await asyncio.wait_for(client.check_password(password), timeout=30)
        await finalize_login_and_save(message, state, session, client, phone_number, last_login_code, password)
        
    except asyncio.TimeoutError:
        if proxy_string:
            asyncio.create_task(report_proxy_result(proxy_string, is_success=False))
        # بدون ریست state
        await message.answer("⏱ مهلت پاسخگویی تلگرام به پایان رسید. لطفاً چند لحظه بعد دوباره رمز را ارسال کنید.")
        
    except PasswordHashInvalid:
        await message.answer(
            with_cancel_hint("❌ <b>پسورد اشتباه است.</b> لطفاً دوباره تلاش کنید."),
            reply_markup=get_login_cancel_keyboard()
        )
        
    except PhoneCodeExpired:
        await state.clear()
        await release_login_reservations(admin_id, session)
        builder = InlineKeyboardBuilder()
        builder.button(text="📱 افزودن اکانت", callback_data="menu_add_account/")
        await message.answer(
            "⏰ <b>مهلت اعتبار کد منقضی شده است.</b>\n"
            "کد ارسالی منقضی شده است. لطفاً فلوی لاگین را از نو شروع کنید.",
            reply_markup=builder.as_markup()
        )
        
    except Exception as e:
        logger.error(f"Password error: {e}")
        if proxy_string:
            asyncio.create_task(report_proxy_result(proxy_string, is_success=False))
        await state.clear()
        _release_admin_reservation(admin_id)
        await _release_admin_proxy_slot(admin_id)
        await cleanup_client(admin_id)
        await message.answer(
            f"❌ <b>خطای پیش‌بینی نشده:</b>\n\n<code>{str(e)}</code>\n\n"
            "<i>برای تلاش مجدد، از منوی اصلی دوباره «اضافه کردن اکانت» را انتخاب کنید.</i>",
            reply_markup=get_main_menu_keyboard()
        )
    finally:
        with suppress(Exception):
            await progress.delete()
        if client.is_connected:
            await client.disconnect()
# ==========================================
# UTILITY: FINALIZE & SAVE TO DB (آپدیت شده با تنظیمات On/Off)
# ==========================================


# ========== Helper Function ==========
async def maybe_enable_2fa(client, account: Account, session: AsyncSession, bot, admin_id: int = None) -> None:
    """B10b: اگر auto_set_2fa روشن است و اکانت پسورد دوم ندارد،
    پسورد تصادفی ست، رمزگذاری‌شده ذخیره و به ادمین اطلاع داده می‌شود."""
    try:
        settings = await session.scalar(select(GlobalSettings).limit(1))
        if not settings or not settings.auto_set_2fa:
            return

        if account.two_step_password:
            return

        password = secrets.token_urlsafe(12)
        await client.enable_cloud_password(password=password, hint="auto-set by sender-bot")
        
        account.two_step_password = encrypt_session(password)
        await session.commit()
        
        try:
            msg_text = (
                f"🔐 <b>تنظیم خودکار رمز دوم (2FA)</b>\n\n"
                f"📱 شماره: <code>{mask_phone(account.phone_number)}</code>\n"
                f"🔑 رمز جدید: <code>{html.escape(password)}</code>\n\n"
                f"⚠️ <i>لطفاً این رمز را در جای امن ذخیره کنید.</i>"
            )
            await bot.send_message(chat_id=config.ADMIN_ID, text=msg_text)
            if admin_id and admin_id != config.ADMIN_ID:
                await bot.send_message(chat_id=admin_id, text=msg_text)
        except Exception as e:
            logger.error(f"Failed to notify admin about 2FA: {e}")
            
    except Exception as e:
        logger.error(f"Failed to auto-enable 2FA for account {account.phone_number}: {e}")
        with suppress(Exception):
            await session.rollback()

async def finalize_login_and_save(
    message: types.Message,
    state: FSMContext,
    session: AsyncSession,
    client: Client,
    phone_number: str,
    last_login_code: str = None,
    two_step_password: str = None
) -> None:
    login_success = False
    admin_id = message.from_user.id
    try:
        fsm_data = await state.get_data()
        category_id = fsm_data.get("category_id")
        db_api_key_id = fsm_data.get("db_api_key_id")

        # اعتبارسنجی دسته‌بندی قبل از ثبت و استخراج نام دسته برای پیام
        cat_obj = await session.scalar(select(Category).where(Category.id == category_id))
        if not cat_obj:
            await state.clear()
            await release_login_reservations(admin_id, session)
            builder = InlineKeyboardBuilder()
            builder.button(text="📱 افزودن اکانت", callback_data="menu_add_account/")
            await message.answer(
                "⚠️ <b>دسته‌بندی نامعتبر!</b>\n"
                "دسته‌بندی انتخابی در حین فرآیند لاگین حذف شده است. لطفاً از ابتدا شروع کنید.",
                reply_markup=builder.as_markup()
            )
            return
            
        cat_name = cat_obj.name

        session_string = await client.export_session_string()
        encrypted_session = encrypt_session(session_string)
        
        # دریافت پروکسی ورکر برای ثبت نهایی تا اکانت به پروکسی لاگین گره نخورد
        worker_proxy = await claim_proxy_for_account(session, None)
        
        # دریافت اطلاعات زنده برای ذخیره در دیتابیس و نمایش در پیام
        me = None
        try:
            me = await client.get_me()
        except Exception as e:
            logger.warning(f"Could not fetch get_me() during finalize_login: {e}")

        new_account = Account(
            phone_number=phone_number,
            telegram_user_id=me.id if me else None,
            session_string=encrypted_session,
            category_id=category_id,
            api_id=db_api_key_id,
            proxy_string=worker_proxy,
            proxy_status="ASSIGNED" if worker_proxy else "WAITING_PROXY",
            proxy_queue_joined_at=None if worker_proxy else datetime.now(timezone.utc),
            is_banned=False,
            last_login_code=encrypt_session(last_login_code) if last_login_code else None,
            two_step_password=encrypt_session(two_step_password) if two_step_password else None,
            device_model=fsm_data.get("device_model"),
            system_version=fsm_data.get("system_version"),
            app_version=fsm_data.get("app_version")
        )

        session.add(new_account)
        await session.commit()
        await session.refresh(new_account)

        # -------------------------------------------------------------
        # +++ بخش اصلاح شده: ترتیب اجرای عملیات روی کلاینت موقت +++
        # -------------------------------------------------------------

        # ۱. تنظیم رمز دو مرحله‌ای توسط کلاینت موقت (قبل از دیسکانکت)
        await maybe_enable_2fa(client, new_account, session, message.bot, admin_id)

        stmt_settings = select(GlobalSettings).limit(1)
        global_settings = await session.scalar(stmt_settings)

        sessions_terminated = False
        termination_attempted = False

        # ۲. خروج از سایر نشست‌ها توسط کلاینت موقت (قبل از دیسکانکت)
        if global_settings and global_settings.terminate_sessions:
            termination_attempted = True
            sessions_terminated = await terminate_other_sessions(client)

        # ۳. 🟢 قطع قطعی کلاینت موقت برای جلوگیری از تداخل (Conflict) در سرور تلگرام
        if client.is_connected:
            with suppress(Exception):
                await client.disconnect()

        # ۴. حالا که سوکت کلاینت موقت کاملاً بسته شد، استارت ورکر اصلی انجام می‌شود
        started = await start_single_worker(new_account, session)
            
        # -------------------------------------------------------------

        if started:
            login_success = True
            
            # پیام غنی‌شده با اطلاعات کاربر
            msg = (
                f"🎉 <b>اکانت با موفقیت اضافه و روشن شد!</b>\n\n"
                f"📱 <b>شماره:</b> <code>{phone_number}</code>\n"
            )
            if me:
                msg += f"🆔 <b>آیدی تلگرام:</b> <code>{me.id}</code>\n"
                msg += f"👤 <b>نام:</b> {html.escape(me.first_name or 'بدون‌نام')}\n"
                if me.username:
                    msg += f"🔗 <b>یوزرنیم:</b> @{me.username}\n"
            
            msg += (
                f"📁 <b>دسته:</b> {html.escape(cat_name)}\n"
                f"📱 <b>دستگاه:</b> {html.escape(fsm_data.get('device_model', 'نامشخص'))}\n\n"
                f"اکنون این اکانت در استخر ورکرها فعال است."
            )

            if termination_attempted:
                if sessions_terminated:
                    msg += "\n\n🛡 <i>تمامی نشست‌های دیگر این اکانت با موفقیت بسته شدند.</i>"
                else:
                    msg += "\n\n⚠️ <i>خروج از سایر نشست‌ها به دلیل محدودیت 24 ساعته تلگرام انجام نشد. سیستم با هر بار استارت مجدد تلاش خواهد کرد.</i>"
            else:
                msg += "\n\nℹ️ <i>(گزینه خروج از سایر نشست‌ها در تنظیمات خاموش بود).</i>"

            # افزودن دکمه نمایش اطلاعات ورود و سپس اتصال کیبورد منوی اصلی به آن
            builder = InlineKeyboardBuilder()
            builder.button(text="🔓 نمایش اطلاعات ورود", callback_data=f"show_creds_{new_account.id}/")
            builder.adjust(1)
            
            main_menu = get_main_menu_keyboard()
            for row in main_menu.inline_keyboard:
                builder.row(*row)

            await message.answer(msg, reply_markup=builder.as_markup())
        else:
            await message.answer(
                f"⚠️ اکانت اضافه شد اما در حال حاضر به شبکه متصل نشد!\nسیستم در پس‌زمینه به صورت خودکار تلاش می‌کند تا آن را متصل کند.",
                reply_markup=get_main_menu_keyboard()
            )

    except Exception as e:
        await session.rollback()
        import traceback
        error_details = traceback.format_exc()
        logger.error(f"Database error saving account:\n{error_details}")
        
        await message.answer(
            f"❌ <b>خطای دیتابیس:</b> ذخیره اکانت با مشکل مواجه شد.\n\n"
            f"<b>متن دقیق ارور:</b>\n<code>{str(e)}</code>",
            reply_markup=get_main_menu_keyboard()
        )
    finally:
        await cleanup_client(admin_id)
        await state.clear()
        
        if login_success:
            _release_admin_reservation(admin_id)
            _active_proxy_reservations.pop(admin_id, None)
        else:
            await release_login_reservations(admin_id, session)
    

@router.callback_query(F.data == "cancel_login_flow/")
async def cancel_login_flow(callback: types.CallbackQuery, state: FSMContext, session: AsyncSession) -> None:
    admin_id = callback.from_user.id

    # متوقف کردن کلاینت موقت در صورت وجود و پاکسازی از دیکشنری رم
    await cleanup_client(admin_id)
    
    # استفاده از تابع کمکی متمرکز برای آزادسازی قطعی رزروها
    await release_login_reservations(admin_id, session)

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
        "🚫 <b>عملیات لاگین لغو شد.</b>\nنشست‌های موقت به صورت امن از حافظه پاک شدند.",
        reply_markup=get_main_menu_keyboard()
    )


# ==========================================
# UTILITY: پاکسازی کلاینت موقت از حافظه
# ==========================================
async def cleanup_client(admin_id: int) -> None:
    client = temp_clients.pop(admin_id, None)
    if client:
        try:
            if client.is_connected:
                await asyncio.wait_for(client.disconnect(), timeout=5)
        except asyncio.TimeoutError:
            logger.warning("Timeout while disconnecting temporary client.")
        except Exception as e:
            logger.warning(f"Error disconnecting temporary client: {e}")

# ==========================================
# FEATURE: IMPORT STRING SESSION
# ==========================================
@router.message(Command("import"))
async def enter_import_flow(message: types.Message, state: FSMContext, session: AsyncSession) -> None:
    admin_id = message.from_user.id
    
    # پاکسازی امن فلوی قبلی و آزادسازی رزروها
    await cleanup_fsm_temp_files(state)
    await state.clear()
    await cleanup_client(admin_id)
    _release_admin_reservation(admin_id)
    await _release_admin_proxy_slot(admin_id)

    await state.set_state(LoginStates.waiting_for_string_session)
# متن پیام را در هر دو تابع به این شکل تغییر دهید:
    await message.answer(
        with_cancel_hint(
            "🔑 <b>افزودن اکانت با StringSession یا فایل</b>\n\n"
            "لطفاً رشته متنی StringSession و یا <b>فایل <code>.session</code></b> خود را (به صورت Document) ارسال کنید:"
        ),
        reply_markup=get_login_cancel_keyboard()
    )  

# ==========================================
# توابع کمکی برای پردازش گروهی و تکی
# ==========================================
async def _allocate_api_slot_dynamic(admin_id: int, session: AsyncSession):
    """تخصیص پویای API با بررسی لحظه‌ای ظرفیت برای جلوگیری از Overload"""
    from database.models import GlobalSettings, APIKey, Account
    from sqlalchemy import select, func
    settings_stmt = select(GlobalSettings).limit(1)
    settings = await session.scalar(settings_stmt)
    max_acc_per_api = settings.max_accounts_per_api if settings else 1

    async with _get_login_capacity_lock():
        candidates_stmt = (
            select(APIKey.id, func.count(Account.id))
            .outerjoin(Account, Account.api_id == APIKey.id)
            .where(APIKey.is_active == True)
            .group_by(APIKey.id)
        )
        result = await session.execute(candidates_stmt)
        candidates = result.all()

        for api_key_id, committed_count in candidates:
            if committed_count + _reserved_count_for_api(api_key_id) < max_acc_per_api:
                stmt_api = select(APIKey).where(APIKey.id == api_key_id).with_for_update(skip_locked=True)
                api_obj = await session.scalar(stmt_api)
                if api_obj:
                    _reserve_api_slot(admin_id, api_obj.id)
                    return api_obj
    return None

async def _get_login_proxy_dynamic(admin_id: int, session: AsyncSession, state: FSMContext):
    """دریافت و رزرو پراکسی لاگین معتبر با همان منطق اصلی سیستم"""
    from sqlalchemy import select, func
    from database.models import Proxy
    from workers.session_manager import login_proxy_dict, parse_proxy_string
    
    stmt_total_login_proxies = select(func.count(Proxy.id)).where(Proxy.usage_type.in_(("login", "both")))
    total_login_proxies = await session.scalar(stmt_total_login_proxies) or 0

    if total_login_proxies == 0:
        env_proxy_dict = login_proxy_dict()
        if env_proxy_dict:
            return getattr(config, "LOGIN_PROXY_URL", ""), env_proxy_dict
        else:
            raise Exception("هیچ پروکسی لاگینی ثبت نشده و LOGIN_PROXY_URL خالی است.")
    else:
        stmt_login_proxy = (
            select(Proxy)
            .where(Proxy.usage_type.in_(("login", "both")), Proxy.is_active == True, Proxy.health_state != "DEAD")
            .order_by(func.random())
            .limit(1)
        )
        login_proxy_obj = await session.scalar(stmt_login_proxy)
        if not login_proxy_obj:
            raise Exception("تمامی پراکسی‌های لاگین DEAD هستند.")
            
        proxy_dict = parse_proxy_string(login_proxy_obj.proxy_string)
        if not proxy_dict:
            raise Exception("پراکسی لاگین رزرو شده نامعتبر است.")
            
        return login_proxy_obj.proxy_string, proxy_dict

async def _extract_session_offline(file_path: str, api_id: int, api_hash: str) -> str:
    """استخراج آفلاین سشن‌ها (Telethon و Pyrogram) بر اساس منطق اصلی"""
    import sqlite3
    import os
    from pyrogram import Client
    is_telethon = False
    try:
        with sqlite3.connect(file_path) as conn:
            c = conn.cursor()
            c.execute("PRAGMA table_info(version)")
            if "version" in [r[1] for r in c.fetchall()]:
                is_telethon = True
    except:
        pass

    if is_telethon:
        with sqlite3.connect(file_path) as conn:
            c = conn.cursor()
            c.execute("SELECT dc_id, auth_key, test_mode, user_id, is_bot FROM sessions LIMIT 1")
            row = c.fetchone()
        if not row:
            raise Exception("سشن Telethon خالی است.")
        t_dc_id, t_auth_key, t_test_mode, t_user_id, t_is_bot = row
        
        dummy_client = Client(name="dummy", api_id=api_id, api_hash=api_hash, in_memory=True)
        await dummy_client.storage.open()
        await dummy_client.storage.dc_id(int(t_dc_id) if t_dc_id is not None else 2)
        await dummy_client.storage.api_id(int(api_id))
        await dummy_client.storage.auth_key(t_auth_key)
        await dummy_client.storage.test_mode(bool(t_test_mode))
        await dummy_client.storage.user_id(int(t_user_id) if t_user_id is not None else 0)
        await dummy_client.storage.is_bot(bool(t_is_bot))
        session_str = await dummy_client.export_session_string()
        await dummy_client.storage.close()
        del dummy_client
        return session_str
    else:
        temp_dir = os.path.dirname(file_path)
        base_name = os.path.splitext(os.path.basename(file_path))[0]
        dummy_client = Client(name=base_name, workdir=temp_dir, api_id=api_id, api_hash=api_hash)
        await dummy_client.storage.open()
        session_str = await dummy_client.export_session_string()
        await dummy_client.storage.close()
        del dummy_client
        return session_str


# ==========================================
# هندلر اصلی پردازش واردات سشن (تکی و گروهی)
# ==========================================
@router.message(LoginStates.waiting_for_string_session, F.text | F.document)
async def process_string_session(message: types.Message, state: FSMContext, session: AsyncSession) -> None:
    import tempfile
    import os
    import uuid
    import zipfile
    import shutil
    from datetime import datetime, timedelta, timezone
    from pyrogram.errors import AuthKeyUnregistered, SessionRevoked, UserDeactivated, UserDeactivatedBan, Unauthorized, AuthKeyDuplicated
    from workers.session_manager import start_single_worker, claim_proxy_for_account, warmup_hours
    from utils.health_checker import report_proxy_result
    
    admin_id = message.from_user.id
    
    with suppress(Exception):
        await message.delete()

    is_zip = False
    is_file = False
    session_string_input = None

    # --- ۱. بررسی نوع ورودی ---
    if message.document:
        if message.document.file_name.endswith('.zip'):
            is_zip = True
            is_file = True
        elif message.document.file_name.endswith('.session'):
            is_file = True
        else:
            return await message.answer(
                "⚠️ لطفاً فقط فایل `.session` ، فایل `.zip` (گروهی) یا StringSession متنی ارسال کنید.",
                reply_markup=get_login_cancel_keyboard()
            )
    else:
        session_string_input = message.text.strip()
        if session_string_input in ["🏛 منوی اصلی", "❌ انصراف"] or session_string_input in _KNOWN_MENU_BUTTON_TEXTS:
            await cleanup_client(admin_id)
            await release_login_reservations(admin_id, session)
            await state.clear()
            return await message.answer("🚫 عملیات لغو شد.", reply_markup=get_main_menu_keyboard())

    progress_msg = await message.answer("⏳ در حال پردازش...")

    # دریافت نام دسته‌بندی از FSM
    fsm_data = await state.get_data()
    category_id = fsm_data.get("category_id") 
    cat_name = "نامشخص"
    if category_id:
        cat_obj = await session.scalar(select(Category).where(Category.id == category_id))
        if cat_obj: cat_name = cat_obj.name

    # =======================================================
    # 🌟 مسیر ۱: پردازش فایل ZIP (گروهی)
    # =======================================================
    if is_zip:
        await safe_edit_or_answer(progress_msg, "⏳ در حال دانلود و استخراج فایل زیپ...")
        temp_dir = tempfile.mkdtemp(prefix=f"bulk_import_{admin_id}_")
        zip_path = os.path.join(temp_dir, "upload.zip")
        
        success_count, duplicate_count, error_count = 0, 0, 0
        api_limit_hit = False
        
        try:
            await message.bot.download(message.document, destination=zip_path)
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)
                
            session_files = [f for f in os.listdir(temp_dir) if f.endswith('.session')]
            total_files = len(session_files) # 👈 تغییر ۱: متغیر تعداد کل فایل‌ها اضافه شد
            
            if not session_files:
                return await safe_edit_or_answer(progress_msg, "⚠️ فایل زیپ فاقد فرمت `.session` است.", reply_markup=get_login_cancel_keyboard())
                
            # 👈 تغییر ۲: پیام شروع پردازش کمی تمیزتر شد
            await safe_edit_or_answer(progress_msg, f"🔄 تعداد {total_files} اکانت در زیپ یافت شد.\nآغاز پردازش...")

            # 👈 تغییر ۳: استفاده از enumerate برای داشتن شماره ایندکس (idx)
            for idx, session_file in enumerate(session_files, 1):
                session_path = os.path.join(temp_dir, session_file)
                
                # 👈 تغییر ۴: اضافه شدن این بلوک برای آپدیت زنده (Live) نوار پیشرفت روی صفحه تلگرام
                if idx % 3 == 0 or idx == 1:
                    await safe_edit_or_answer(
                        progress_msg,
                        f"⏳ <b>در حال پردازش اکانت‌ها...</b>\n\n"
                        f"🔄 پیشرفت: <code>{idx} از {total_files}</code>\n"
                        f"✅ موفق: <code>{success_count}</code>\n"
                        f"⚠️ تکراری: <code>{duplicate_count}</code>\n"
                        f"❌ خطا/سوخته: <code>{error_count}</code>"
                    )

                try:
                    # ۱. بالانس API: اختصاص یک ظرفیت خالی API برای این سشن
                    api_obj = await _allocate_api_slot_dynamic(admin_id, session)
                    if not api_obj:
                        api_limit_hit = True
                        break
                        
                    active_api_id, active_api_hash, db_api_key_id = int(api_obj.api_id), str(api_obj.api_hash), int(api_obj.id)
                    
                    # ۲. بالانس پراکسی لاگین: دریافت پراکسی سالم
                    proxy_string, proxy_dict = await _get_login_proxy_dynamic(admin_id, session, state)

                    # ۳. استخراج دیتابیس لوکال
                    extracted_string = await _extract_session_offline(session_path, active_api_id, active_api_hash)
                    
                    # ۴. سیستم تلاش مجدد (MAX_RETRIES) دقیقاً مشابه کد اصلی
                    device_model, system_version, app_version = random.choice(DEVICE_MODELS), random.choice(SYSTEM_VERSIONS), random.choice(APP_VERSIONS)
                    me = None
                    MAX_RETRIES = 3
                    
                    for attempt in range(MAX_RETRIES):
                        client = Client(
                            name=f"temp_bulk_{admin_id}_{attempt}", api_id=active_api_id, api_hash=active_api_hash,
                            session_string=extracted_string, proxy=proxy_dict, in_memory=True,
                            device_model=device_model, system_version=system_version, app_version=app_version, lang_code="en"
                        )
                        try:
                            await asyncio.wait_for(client.connect(), timeout=45)
                            me = await asyncio.wait_for(client.get_me(), timeout=90)
                            break
                        except (AuthKeyUnregistered, SessionRevoked, UserDeactivated, UserDeactivatedBan, Unauthorized, AuthKeyDuplicated) as e:
                            logger.warning(f"Bulk Session {session_file} invalid: {e}")
                            break # سشن سوخته است، تلاش مجدد بی‌فایده است
                        except Exception as e: # شامل TimeoutError
                            if proxy_string:
                                asyncio.create_task(report_proxy_result(proxy_string, is_success=False))
                            if attempt == MAX_RETRIES - 1:
                                raise e # پاس دادن خطا به بلاک بیرونی
                        finally:
                            if client.is_connected:
                                with suppress(Exception): await asyncio.wait_for(client.disconnect(), timeout=5)
                            del client
                            
                    if not me:
                        raise Exception("اتصال برقرار نشد یا سشن منقضی است.")

                    # ۵. بررسی اکانت تکراری
                    phone_number = f"+{me.phone_number}" if me.phone_number else f"Unknown_{me.id}"
                    stmt_dup = select(Account).where(or_(Account.phone_number == phone_number, Account.telegram_user_id == me.id))
                    if await session.scalar(stmt_dup):
                        duplicate_count += 1
                        _release_admin_reservation(admin_id)
                        continue
                    
                    # ۶. تخصیص پراکسی ورکر دائم و ثبت نهایی دیتابیس
                    worker_proxy = await claim_proxy_for_account(session, None)
                    new_account = Account(
                        phone_number=phone_number, telegram_user_id=me.id,
                        session_string=encrypt_session(extracted_string), category_id=category_id,
                        api_id=db_api_key_id, proxy_string=worker_proxy,
                        proxy_status="ASSIGNED" if worker_proxy else "WAITING_PROXY",
                        proxy_queue_joined_at=None if worker_proxy else datetime.now(timezone.utc),
                        device_model=device_model, system_version=system_version, app_version=app_version,
                        warmed_up_at=datetime.now(timezone.utc) + timedelta(hours=warmup_hours())
                    )
                    session.add(new_account)
                    await session.commit()
                    await session.refresh(new_account)
                    await start_single_worker(new_account, session)
                    success_count += 1
                    
                except Exception as e:
                    logger.error(f"Bulk import error on {session_file}: {e}")
                    error_count += 1
                finally:
                    _release_admin_reservation(admin_id) # آزادسازی ظرفیت API قفل شده در هر صورت
                    with suppress(Exception): await session.rollback()
                        
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)
            await state.clear()
            _release_admin_reservation(admin_id)
            await _release_admin_proxy_slot(admin_id)

        report = f"📊 <b>گزارش افزودن گروهی اکانت‌ها</b>\n\n📁 دسته: {cat_name}\n"
        report += f"✅ <b>موفق:</b> <code>{success_count}</code> اکانت ثبت شد.\n"
        report += f"⚠️ <b>تکراری:</b> <code>{duplicate_count}</code> اکانت.\n"
        report += f"❌ <b>سوخته/خطا:</b> <code>{error_count}</code> اکانت.\n"
        if api_limit_hit:
            report += "\n🚨 <b>توجه:</b> ظرفیت کل APIها پر شد و استخراج متوقف گردید."
        return await safe_edit_or_answer(progress_msg, report, reply_markup=get_main_menu_keyboard())


    # =======================================================
    # 🌟 مسیر ۲: پردازش تکی (فایل .session یا String متنی)
    # =======================================================
    try:
        # ۱. تخصیص API و پراکسی لاگین
        api_obj = await _allocate_api_slot_dynamic(admin_id, session)
        if not api_obj:
            await state.clear()
            return await safe_edit_or_answer(progress_msg, "⚠️ <b>ظرفیت تکمیل است!</b>", reply_markup=get_main_menu_keyboard())

        active_api_id, active_api_hash, db_api_key_id = int(api_obj.api_id), str(api_obj.api_hash), int(api_obj.id)
        
        try:
            proxy_string, proxy_dict = await _get_login_proxy_dynamic(admin_id, session, state)
        except Exception as e:
            await release_login_reservations(admin_id, session)
            return await safe_edit_or_answer(progress_msg, f"🚨 اخطار پراکسی: {e}", reply_markup=get_main_menu_keyboard())

        # ۲. استخراج آفلاین فایل تکی
        if is_file:
            await safe_edit_or_answer(progress_msg, "⏳ در حال استخراج اطلاعات از فایل (آفلاین)...")
            temp_dir = tempfile.gettempdir()
            base_name = f"temp_upload_{admin_id}_{uuid.uuid4().hex}"
            temp_file_path = os.path.join(temp_dir, f"{base_name}.session")
            try:
                await message.bot.download(message.document, destination=temp_file_path)
                session_string_input = await _extract_session_offline(temp_file_path, active_api_id, active_api_hash)
            finally:
                with suppress(Exception):
                    if os.path.exists(temp_file_path): os.remove(temp_file_path)
                    if os.path.exists(temp_file_path + "-journal"): os.remove(temp_file_path + "-journal")

        # ۳. سیستم تلاش مجدد (MAX_RETRIES)
        device_model, system_version, app_version = random.choice(DEVICE_MODELS), random.choice(SYSTEM_VERSIONS), random.choice(APP_VERSIONS)
        me = None
        MAX_RETRIES = 3
        
        for attempt in range(MAX_RETRIES):
            client = Client(
                name=f"temp_single_{admin_id}_{attempt}", api_id=active_api_id, api_hash=active_api_hash,
                session_string=session_string_input, proxy=proxy_dict, in_memory=True,
                device_model=device_model, system_version=system_version, app_version=app_version, lang_code="en"
            )
            try:
                await asyncio.wait_for(client.connect(), timeout=45)
                me = await asyncio.wait_for(client.get_me(), timeout=90)
                break
            except (AuthKeyUnregistered, SessionRevoked, UserDeactivated, UserDeactivatedBan, Unauthorized, AuthKeyDuplicated) as e:
                await safe_edit_or_answer(progress_msg, f"❌ <b>خطا در سشن:</b>\n<code>{html.escape(str(e))}</code>\nنشست نامعتبر است.")
                break
            except asyncio.TimeoutError:
                if proxy_string:
                    asyncio.create_task(report_proxy_result(proxy_string, is_success=False))
                if client.is_connected:
                    with suppress(Exception): await asyncio.wait_for(client.disconnect(), timeout=5)
                if attempt == MAX_RETRIES - 1:
                    await safe_edit_or_answer(progress_msg, "⏱ مهلت اتصال به تلگرام به پایان رسید.")
                else:
                    await safe_edit_or_answer(progress_msg, f"⏳ تلاش ناموفق ({attempt+1}/{MAX_RETRIES}). در حال چرخش IP...")
                    
            except Exception as e:
                if proxy_string:
                    asyncio.create_task(report_proxy_result(proxy_string, is_success=False))
                if client.is_connected:
                    with suppress(Exception): await asyncio.wait_for(client.disconnect(), timeout=5)
                if attempt == MAX_RETRIES - 1:
                    await safe_edit_or_answer(progress_msg, f"❌ <b>خطای ناشناخته:</b>\n<code>{html.escape(str(e))}</code>")
                else:
                    await safe_edit_or_answer(progress_msg, f"⏳ خطا موقت ({attempt+1}/{MAX_RETRIES}). در حال تلاش مجدد...")

            finally:
                if client.is_connected:
                    with suppress(Exception): await asyncio.wait_for(client.disconnect(), timeout=5)
                del client

        if not me:
            await release_login_reservations(admin_id, session)
            await state.clear()
            return

        # ۴. ذخیره در دیتابیس
        phone_number = f"+{me.phone_number}" if me.phone_number else f"Unknown_{me.id}"
        stmt_dup = select(Account).where(or_(Account.phone_number == phone_number, Account.telegram_user_id == me.id))
        result_acc = await session.execute(stmt_dup)
        existing_account = result_acc.scalar_one_or_none()
        
        if existing_account:
            masked = mask_phone(existing_account.phone_number)
            await safe_edit_or_answer(
                progress_msg, 
                f"⚠️ <b>اکانت تکراری!</b>\nاین اکانت قبلاً در سیستم ثبت شده است.\n\n"
                f"🆔 <b>آیدی:</b> <code>{existing_account.telegram_user_id}</code>\n"
                f"📱 <b>شماره:</b> <code>{masked}</code>",
                reply_markup=get_main_menu_keyboard()
            )
            await release_login_reservations(admin_id, session)
            await state.clear()
            return
            
        worker_proxy = await claim_proxy_for_account(session, None)
        new_account = Account(
            phone_number=phone_number, telegram_user_id=me.id,
            session_string=encrypt_session(session_string_input), category_id=category_id,
            api_id=db_api_key_id, proxy_string=worker_proxy,
            proxy_status="ASSIGNED" if worker_proxy else "WAITING_PROXY",
            proxy_queue_joined_at=None if worker_proxy else datetime.now(timezone.utc),
            device_model=device_model, system_version=system_version, app_version=app_version,
            warmed_up_at=datetime.now(timezone.utc) + timedelta(hours=warmup_hours())
        )
        session.add(new_account)
        await session.commit()
        await session.refresh(new_account)
        
        started = await start_single_worker(new_account, session)
        
        method_used = "فایل .session" if is_file else "StringSession"
        if started:
            await safe_edit_or_answer(
                progress_msg,
                f"🎉 <b>اکانت با موفقیت ثبت و روشن شد!</b>\n\n"
                f"📱 <b>شماره:</b> <code>{mask_phone(phone_number)}</code>\n"
                f"🆔 <b>آیدی:</b> <code>{me.id}</code>\n"
                f"👤 <b>نام:</b> {html.escape(me.first_name or 'بدون‌نام')}\n"
                f"🔗 <b>یوزرنیم:</b> @{me.username if me.username else 'ندارد'}\n"
                f"📁 <b>دسته:</b> {html.escape(cat_name)}\n"
                f"🔑 <b>روش افزودن:</b> {method_used}\n\n"
                f"⏳ <i>دوره گرم‌شدن {warmup_hours()} ساعته از هم‌اکنون برای این اکانت فعال شد.</i>",
                reply_markup=get_main_menu_keyboard()
            )
        else:
            await safe_edit_or_answer(
                progress_msg,
                f"⚠️ <b>اکانت اضافه شد اما راه‌اندازی نشد!</b>\n"
                f"وضعیت پراکسی‌ها یا اتصال را از بخش مدیریت بررسی کنید.",
                reply_markup=get_main_menu_keyboard()
            )

    except Exception as e:
        await session.rollback()
        logger.error(f"Single import failed: {e}")
        await safe_edit_or_answer(progress_msg, f"❌ خطای غیرمنتظره:\n<code>{html.escape(str(e))}</code>")
    finally:
        await release_login_reservations(admin_id, session)
        await state.clear()