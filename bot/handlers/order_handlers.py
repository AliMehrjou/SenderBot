import asyncio
import html
import logging
import os
import random
import re
import string
import time
import uuid
from contextlib import suppress
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urlparse
from aiogram.types import InlineKeyboardMarkup
import aiofiles
from aiogram import Router, types, F, Bot
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.types import FSInputFile
from aiogram.utils.keyboard import InlineKeyboardBuilder
from pyrogram import Client as PyrogramClient
from pyrogram.enums import ChatType
from pyrogram.errors import (
    ChatAdminRequired,
    UserAlreadyParticipant,
    InviteHashExpired,
    FloodWait,
    RPCError,
    ChannelInvalid,
    ChannelPrivate,
    PeerIdInvalid,
    UsernameInvalid,
    UsernameNotOccupied,
)
import redis.asyncio as aioredis
from config import config
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from bot.handlers.login_handlers import release_login_reservations
from bot.states.confirm_fsm import ConfirmStates
from bot.states.order_fsm import CreateOrderStates
from database.models import Category, Order, OrderLog, OrderStatus, Account, APIKey
from workers.session_manager import worker_pool
from database.engine import async_session
from bot.keyboards.cancel import get_cancel_keyboard, with_cancel_hint
from bot.keyboards.main_menu import (
    get_main_menu_button,
    get_main_menu_keyboard,
    get_main_menu_reply_keyboard,
)
from utils.fsm_cleanup import cleanup_fsm_temp_files
from utils.safe_edit import safe_edit_or_answer
from utils.crypto import decrypt_session
from utils.error_messages import (
    get_download_error_message,
    get_floodwait_message,
    get_generic_error_message,
    get_telegram_api_error_message,
    report_db_error,
)
from utils.telegram_helpers import (
    answer_callback_error,
    safe_callback_answer,
    safe_edit_message,
    send_loading_message,
)
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
router = Router(name="order_fsm_router")
os.makedirs("downloads", exist_ok=True)
os.makedirs("exports", exist_ok=True)


# ==========================================
# ثابت‌های مشترک
# ==========================================
LINK_TARGET_EXAMPLE = (
    "لینک گروه (ها)\n"
    "تعداد ارسال\n\n"
    "مثال تک‌لینک:\n"
    "https://t.me/source\n"
    "100\n\n"
    "مثال چند لینک (هر لینک در یک خط یا با فاصله):\n"
    "https://t.me/source1\n"
    "https://t.me/source2\n"
    "100"
)

MAX_SOURCE_MESSAGES = 10


# ==========================================
# ⌨️ کیبوردهای دکمه‌ای فلوی سفارش (Reply Keyboard)
# قاعده: انتخاب «ایستا» → دکمه‌ای | لیست «پویا» (تأیید/صفحه‌بندی/چندانتخابی) → شیشه‌ای
# ==========================================
#: سطر ناوبری ثابت — در کل فلوی سفارش پایین چت می‌ماند
FLOW_NAV_ROW: list[str] = ["❌ انصراف", "🏛 منوی اصلی"]


def flow_reply_keyboard(choice_rows: Optional[list[list[str]]] = None) -> types.ReplyKeyboardMarkup:
    """⌨️ ردیف(های) انتخاب + ردیف ناوبری ثابت"""
    rows: list[list[str]] = [list(row) for row in (choice_rows or [])]
    rows.append(list(FLOW_NAV_ROW))
    keyboard = [[types.KeyboardButton(text=t) for t in row] for row in rows]
    return types.ReplyKeyboardMarkup(
        keyboard=keyboard,
        resize_keyboard=True,
        is_persistent=True,
    )


def get_flow_nav_keyboard() -> types.ReplyKeyboardMarkup:
    """⌨️ کیبورد Stateهای ورودی متنی (لینک/فایل/کانال/پیام): فقط ناوبری"""
    return flow_reply_keyboard()


#: نگاشت متن دکمه → مقدار ذخیره‌شده در FSM / Order
ORDER_TYPE_BY_TEXT = {"🔗 با لینک گروه": "link", "📄 با فایل اعضا": "list"}
SEND_TYPE_BY_TEXT = {"👥 همه اعضا": "all", "🎯 اعضای خاص": "unique"}
SEND_METHOD_BY_TEXT = {"۱) متن مستقیم": "direct", "۲) کپی از کانال مبدا": "copy"}

BANNER_POOL_YES_TEXT = "✅ بله، از مخزن بنر"
BANNER_POOL_NO_TEXT = "❌ خیر، متن همین سفارش"
SMART_FLOW_YES_TEXT = "✅ بله، فعال شود"
SMART_FLOW_NO_TEXT = "❌ خیر، ارسال عادی"
SMART_FLOW_CONFIRM_TEXT = "✅ تأیید مجدد و ادامه"
SMART_FLOW_NO_CONFIRM_TEXT = "❌ جریان هوشمند فعال نشود"
END_COLLECTION_TEXT = "✅ پایان"

#: فیلتر ارسال + استراتژی سفارش استخراج (هر دو در state مشترک waiting_for_filter)
FILTER_BY_TEXT = {
    "👤 کاربران واقعی": "real",
    "👥 همه کاربران": "all",
    "🟢 آنلاین": "online",
    "👻 فیک": "fake",
    "📱 شماره‌دار": "phone",
    # ⚙️ استراتژی‌های سفارش استخراج
    "👥 همه اعضا": "users",
    "💬 فرستندگان پیام": "messages",
    "🥇 طلایی": "golden",
    "🟢 فقط آنلاین": "online",
}


def get_order_type_keyboard() -> types.ReplyKeyboardMarkup:
    return flow_reply_keyboard([["🔗 با لینک گروه", "📄 با فایل اعضا"]])


def get_send_type_keyboard() -> types.ReplyKeyboardMarkup:
    return flow_reply_keyboard([["👥 همه اعضا", "🎯 اعضای خاص"]])


def get_send_method_keyboard() -> types.ReplyKeyboardMarkup:
    """⌨️ کیبورد دکمه‌ای روش ارسال (متن مستقیم / کپی از کانال مبدا)"""
    return flow_reply_keyboard([["۱) متن مستقیم", "۲) کپی از کانال مبدا"]])


def get_banner_pool_keyboard() -> types.ReplyKeyboardMarkup:
    return flow_reply_keyboard([[BANNER_POOL_YES_TEXT, BANNER_POOL_NO_TEXT]])


def get_smart_flow_keyboard() -> types.ReplyKeyboardMarkup:
    return flow_reply_keyboard([[SMART_FLOW_YES_TEXT, SMART_FLOW_NO_TEXT]])


def get_smart_flow_confirm_keyboard() -> types.ReplyKeyboardMarkup:
    return flow_reply_keyboard([[SMART_FLOW_CONFIRM_TEXT], [SMART_FLOW_NO_CONFIRM_TEXT]])


def get_filter_keyboard() -> types.ReplyKeyboardMarkup:
    return flow_reply_keyboard([
        ["👤 کاربران واقعی", "👥 همه کاربران"],
        ["🟢 آنلاین", "👻 فیک", "📱 شماره‌دار"],
    ])


def get_extract_strategy_keyboard() -> types.ReplyKeyboardMarkup:
    return flow_reply_keyboard([
        ["👥 همه اعضا", "💬 فرستندگان پیام"],
        ["🥇 طلایی", "🟢 فقط آنلاین"],
    ])


def get_end_collection_keyboard() -> types.ReplyKeyboardMarkup:
    """⌨️ کیبورد حلقه‌های جمع‌آوری (پیام سفارش / پیام نمونه کانال مبدا)"""
    return flow_reply_keyboard([[END_COLLECTION_TEXT]])


#: فاز ۵ (تکمیل): این لیست خالی است — هر ۱۳ دکمه‌ی منوی اصلی اکنون
#: هندلر متنی مخصوص به خودشان را دارند (در ادامه‌ی همین فایل). این لیست
#: صرفاً برای حفظ سابقه‌ی طراحی فاز انتقال نگه داشته شده است.
PENDING_MENU_TEXTS: list[str] = []


# ==========================================
# کیبورد شیشه‌ای دسته‌بندی (چندانتخابی با ✅ — با Reply شدنی نیست)
# ==========================================
async def build_category_keyboard(session: AsyncSession, selected_ids: list[int]):
    stmt = select(Category)
    result = await session.execute(stmt)
    categories = result.scalars().all()

    builder = InlineKeyboardBuilder()
    builder.button(text="🏛 منوی اصلی", callback_data="menu_home/")
    builder.button(text="✅ تأیید دسته‌بندی", callback_data="order_skip_category/")
    builder.button(text="❌ انصراف", callback_data="cancel_current_flow/")

    for cat in categories:
        text = f"✅ {cat.name}" if cat.id in selected_ids else cat.name
        builder.button(text=text, callback_data=f"order_cat_{cat.id}/")

    builder.adjust(2, 1)
    return builder.as_markup()


# ==========================================
# 🟣 ورود به فلوی سفارش — مشترک بین دکمه‌ی دکمه‌ای (متن) و شیشه‌ای (callback)
# ⚠️ ترتیب ثبت مهم: هندلرهای «متنیِ بدون state» باید قبل از هندلرهای state ثبت شوند
# ==========================================
async def _start_create_order_flow(
    message: types.Message,
    state: FSMContext,
    session: AsyncSession,
    callback: Optional[types.CallbackQuery] = None,
) -> None:
    await cleanup_fsm_temp_files(state)
    await state.clear()

    try:
        cats_count = await session.scalar(select(func.count(Category.id))) or 0
    except Exception as e:
        await session.rollback()
        err = report_db_error("دسته‌بندی‌ها", e)
        if callback is not None:
            return await answer_callback_error(callback, err, get_main_menu_button())
        return await message.answer(err, reply_markup=get_main_menu_keyboard())
    
    if cats_count == 0:
        builder = InlineKeyboardBuilder()
        builder.button(text="⚙️ رفتن به تنظیمات", callback_data="menu_settings/")
        builder.button(text="🏛 منوی اصلی", callback_data="menu_home/")
        builder.adjust(2)
        return await safe_edit_or_answer(
            message,
            "⚠️ هیچ دسته‌بندی‌ای یافت نشد. لطفاً ابتدا از بخش تنظیمات یک دسته‌بندی ایجاد کنید.",
            reply_markup=builder.as_markup(),
        )

    await state.update_data(selected_categories=[])
    await state.set_state(CreateOrderStates.waiting_for_category)

    try:
        markup = await build_category_keyboard(session, [])
    except Exception as e:
        await session.rollback()
        err = report_db_error("دسته‌بندی‌ها", e)
        if callback is not None:
            return await answer_callback_error(callback, err, get_main_menu_button())
        return await message.answer(err, reply_markup=get_main_menu_keyboard())

    await safe_edit_or_answer(
        message,
        with_cancel_hint("📁 <b>یک دسته‌بندی انتخاب کنید:</b>"),
        reply_markup=markup,
    )


@router.callback_query(F.data == "menu_create_order/")
async def enter_create_order_flow(callback: types.CallbackQuery, state: FSMContext, session: AsyncSession) -> None:
    """ورودی شیشه‌ای (منوهای inline قدیمی / سایر هندلرها)"""
    await callback.answer()
    await _start_create_order_flow(callback.message, state, session, callback=callback)


@router.message(F.text == "🛍 ثبت سفارش 🛍")
async def create_order_text_entry(message: types.Message, state: FSMContext, session: AsyncSession) -> None:
    """⌨️ ورودی دکمه‌ای منوی اصلی — ثبت سفارش"""
    await _start_create_order_flow(message, state, session)


@router.message(F.text == "💾 لیست سفارشات 💾")
async def active_orders_text_entry(message: types.Message, state: FSMContext, session: AsyncSession) -> None:
    """⌨️ ورودی دکمه‌ای منوی اصلی — Kill Switch (خودِ لیست شیشه‌ای می‌ماند)"""
    if await state.get_state() is not None:
        await cleanup_fsm_temp_files(state)
        await state.clear()
    await _render_active_orders(message, session, state=state, page=1)

@router.message(F.text == "🏛 منوی اصلی")
async def main_menu_text_entry(message: types.Message, state: FSMContext, session: AsyncSession) -> None:
    if await state.get_state() is not None:
        await cleanup_fsm_temp_files(state)
        await state.clear()
        
        # آزادسازی رزروهای لاگین رها شده
        await release_login_reservations(message.from_user.id, session)
        
        await message.answer(
            "❌ عملیات لغو شد.\n🏛 شما به منوی اصلی بازگشتید.",
            reply_markup=get_main_menu_reply_keyboard(),
        )
    else:
        await message.answer("🏛 <b>منوی اصلی:</b>", reply_markup=get_main_menu_reply_keyboard())



# ==========================================
# ⌨️ فاز ۵ — هندلرهای متنی ۱۱ دکمه‌ی باقی‌مانده‌ی منوی اصلی
# ==========================================
# راهبرد: هیچ state یا callback_data جدیدی ساخته نمی‌شود. هر دکمه‌ی متنی،
# کاربر را با یک «دکمه‌ی شیشه‌ایِ تک‌گزینه‌ای» به همان callback_data ای که
# از قبل در فایل مربوطه پیاده‌سازی شده هدایت می‌کند. کاربر با یک ضربه‌ی
# اضافی وارد همان فلوی موجود می‌شود.
#
# ⚠️ ترتیب ثبت: این هندلرها روی روتر order_router هستند که قبل از بقیه‌ی
# روترها در main.py ثبت می‌شود. از آنجا که هیچ‌کدام از این متن‌ها در جای
# دیگری به‌عنوان ورودی FSM استفاده نمی‌شوند، تداخلی وجود ندارد.
def _single_route_button(text: str, callback_data: str) -> InlineKeyboardMarkup:
    """دکمه‌ی شیشه‌ای تک‌گزینه‌ای: همان متن دکمه‌ی پایین چت + یک «منوی اصلی»."""
    builder = InlineKeyboardBuilder()
    builder.button(text=text, callback_data=callback_data)
    builder.button(text="🏛 منوی اصلی", callback_data="menu_home/")
    builder.adjust(1)
    return builder.as_markup()


# کمکی مشترک برای همه‌ی دکمه‌های متنیِ فاز ۵:
# ۱) اگر state فعالی هست، ابتدا پاکش می‌کند (هماهنگ با الگوی main_menu_text_entry).
# ۲) پیام راهنما + دکمه‌ی شیشه‌ای را ارسال می‌کند.
async def _route_to_inline(
    message: types.Message,
    state: FSMContext,
    *,
    button_text: str,
    callback_data: str,
    prompt: str = "👇 برای ورود به این بخش، دکمه‌ی زیر را بزنید:",
) -> None:
    if await state.get_state() is not None:
        await cleanup_fsm_temp_files(state)
        await state.clear()
        
        # از آنجایی که این یک متد داخلی است، سشن دیتابیس در لحظه ساخته می‌شود
        from database.engine import async_session
        async with async_session() as temp_session:
            await release_login_reservations(message.from_user.id, temp_session)
            
    await message.answer(
        prompt,
        reply_markup=_single_route_button(button_text, callback_data),
    )

@router.message(F.text == "📱 افزودن اکانت 📱")
async def add_account_text_entry(message: types.Message, state: FSMContext) -> None:
    """⌨️ دکمه‌ی پایین چت → مسیر شیشه‌ای menu_add_account/ (login_handlers)"""
    await _route_to_inline(
        message, state,
        button_text="📱 افزودن اکانت 📱",
        callback_data="menu_add_account/",
    )


@router.message(F.text == "📥 افزودن Api 📥")
async def add_api_text_entry(message: types.Message, state: FSMContext) -> None:
    """⌨️ دکمه‌ی پایین چت → مسیر شیشه‌ای menu_add_api/ (api_handlers)"""
    await _route_to_inline(
        message, state,
        button_text="📥 افزودن Api 📥",
        callback_data="menu_add_api/",
    )


@router.message(F.text == "📄 افزودن دسته‌بندی 📄")
async def add_category_text_entry(message: types.Message, state: FSMContext) -> None:
    """⌨️ دکمه‌ی پایین چت → مسیر شیشه‌ای settings_add_cat/ (settings_handlers)"""
    await _route_to_inline(
        message, state,
        button_text="📄 افزودن دسته‌بندی 📄",
        callback_data="settings_add_cat/",
    )


@router.message(F.text == "📲 لیست اکانت‌ها 📲")
async def list_accounts_text_entry(message: types.Message, state: FSMContext) -> None:
    """⌨️ دکمه‌ی پایین چت → مسیر شیشه‌ای menu_list_accounts/ (stats_handlers)"""
    await _route_to_inline(
        message, state,
        button_text="📲 لیست اکانت‌ها 📲",
        callback_data="menu_list_accounts/",
    )


@router.message(F.text == "📤 لیست Api 📤")
async def list_api_text_entry(message: types.Message, state: FSMContext) -> None:
    """⌨️ دکمه‌ی پایین چت → مسیر شیشه‌ای menu_list_api/ (api_handlers)"""
    await _route_to_inline(
        message, state,
        button_text="📤 لیست Api 📤",
        callback_data="menu_list_api/",
    )


@router.message(F.text == "🗂 لیست دسته‌بندی‌ها 🗂")
async def list_categories_text_entry(message: types.Message, state: FSMContext) -> None:
    """⌨️ دکمه‌ی پایین چت → مسیر شیشه‌ای menu_list_categories/ (settings_handlers)"""
    await _route_to_inline(
        message, state,
        button_text="🗂 لیست دسته‌بندی‌ها 🗂",
        callback_data="menu_list_categories/",
    )


@router.message(F.text == "🌐 آنالیز 🌐")
async def analysis_text_entry(message: types.Message, state: FSMContext) -> None:
    """⌨️ دکمه‌ی پایین چت → مسیر شیشه‌ای menu_analysis/ (extractor_handlers)"""
    await _route_to_inline(
        message, state,
        button_text="🌐 آنالیز 🌐",
        callback_data="menu_analysis/",
    )


@router.message(F.text == "⚙️ تنظیمات ⚙️")
async def settings_text_entry(message: types.Message, state: FSMContext) -> None:
    """⌨️ دکمه‌ی پایین چت → مسیر شیشه‌ای menu_settings/ (settings_handlers)"""
    await _route_to_inline(
        message, state,
        button_text="⚙️ تنظیمات ⚙️",
        callback_data="menu_settings/",
    )


@router.message(F.text == "📚 راهنما 📚")
async def help_text_entry(message: types.Message, state: FSMContext) -> None:
    """⌨️ دکمه‌ی پایین چت → مسیر شیشه‌ای menu_help/ (general_handlers)"""
    await _route_to_inline(
        message, state,
        button_text="📚 راهنما 📚",
        callback_data="menu_help/",
    )


@router.message(F.text == "📊 آمار 📊")
async def stats_text_entry(message: types.Message, state: FSMContext) -> None:
    """⌨️ دکمه‌ی پایین چت → مسیر شیشه‌ای menu_stats/ (stats_handlers)"""
    await _route_to_inline(
        message, state,
        button_text="📊 آمار 📊",
        callback_data="menu_stats/",
    )


@router.message(F.text == "👨‍💻 افزودن ادمین 👨‍💻")
async def add_admin_text_entry(message: types.Message, state: FSMContext) -> None:
    """⌨️ دکمه‌ی پایین چت → مسیر شیشه‌ای menu_add_admin/ (admin_manage)"""
    await _route_to_inline(
        message, state,
        button_text="👨‍💻 افزودن ادمین 👨‍💻",
        callback_data="menu_add_admin/",
    )



# ==========================================
# STATE: CANCEL ORDER CREATION (دکمه‌ی ❌ انصراف + /cancel)
# ==========================================
@router.message(Command("cancel"))
@router.message(F.text == "❌ انصراف")
async def cancel_order_creation(message: types.Message, state: FSMContext) -> None:
    current_state = await state.get_state()
    if current_state is None:
        return await message.answer(
            "هیچ عملیاتی برای لغو کردن وجود ندارد.",
            reply_markup=get_main_menu_reply_keyboard(),
        )

    await cleanup_fsm_temp_files(state)
    await state.clear()
    await message.answer(
        "🚫 عملیات ثبت سفارش لغو شد و تمام فایل‌های موقت با موفقیت پاکسازی شدند.",
        reply_markup=get_main_menu_reply_keyboard(),
    )


# ==========================================
# 2. STATE: Toggle Category (شیشه‌ای — چندانتخابی)
# ==========================================
@router.callback_query(CreateOrderStates.waiting_for_category, F.data.startswith("order_cat_") & F.data.endswith("/"))
async def toggle_category_selection(callback: types.CallbackQuery, state: FSMContext, session: AsyncSession) -> None:
    cat_id_str = callback.data.replace("order_cat_", "").replace("/", "")
    if not cat_id_str.isdigit():
        return await callback.answer("⚠️ داده‌های نامعتبر است.", show_alert=True)

    await callback.answer()
    cat_id = int(cat_id_str)

    fsm_data = await state.get_data()
    selected_categories = fsm_data.get("selected_categories", [])

    if cat_id in selected_categories:
        selected_categories.remove(cat_id)
    else:
        selected_categories.append(cat_id)

    await state.update_data(selected_categories=selected_categories)

    try:
        markup = await build_category_keyboard(session, selected_categories)
    except Exception as e:
        await session.rollback()
        return await answer_callback_error(
            callback, report_db_error("دسته‌بندی‌ها", e), get_main_menu_button()
        )

    await safe_edit_or_answer(
        callback.message,
        with_cancel_hint(
            "✅ <b>انتخاب شد.</b>\n\n"
            "یک مورد دیگر انتخاب کنید یا به مرحله بعد بروید:"
        ),
        reply_markup=markup,
    )


@router.message(CreateOrderStates.waiting_for_category)
async def category_stray_text(message: types.Message) -> None:
    """⌨️ متن ناخواسته در مرحله‌ی دسته‌بندی — این مرحله فقط شیشه‌ای است"""
    await message.answer(
        "⚠️ در این مرحله فقط از دکمه‌های شیشه‌ای پیام بالا (انتخاب دسته‌بندی / ⏭ عبور) استفاده کنید."
    )


# ==========================================
# 3. STATE: عبور از دسته‌بندی → انتخاب نوع سفارش (دکمه‌ای)
# ==========================================
@router.callback_query(CreateOrderStates.waiting_for_category, F.data == "order_skip_category/")
async def skip_category_selection(callback: types.CallbackQuery, state: FSMContext) -> None:
    fsm_data = await state.get_data()
    selected_categories = fsm_data.get("selected_categories", [])

    if not selected_categories:
        return await callback.answer("⚠️ لطفاً حداقل یک دسته‌بندی را انتخاب کنید!", show_alert=True)

    await callback.answer()
    await state.set_state(CreateOrderStates.waiting_for_order_type)

    # حذف کیبورد شیشه‌ای پیام دسته‌بندی برای کاهش شلوغی
    with suppress(TelegramBadRequest):
        await callback.message.edit_reply_markup(reply_markup=None)

    # ⌨️ کیبورد دکمه‌ای فقط با پیام «جدید» قابل نمایش است → answer
    await callback.message.answer(
        with_cancel_hint("📌 <b>نوع سفارش را انتخاب کنید:</b>"),
        reply_markup=get_order_type_keyboard(),
    )


# ==========================================
# 4. ⌨️ STATE: انتخاب نوع سفارش (دکمه‌ای — جایگزین callback شد)
# ==========================================
@router.message(CreateOrderStates.waiting_for_order_type, F.text.in_(ORDER_TYPE_BY_TEXT))
async def process_order_type_selection(message: types.Message, state: FSMContext) -> None:
    order_type = ORDER_TYPE_BY_TEXT[message.text.strip()]
    await state.update_data(order_type=order_type)

    if order_type == "link":
        # حذف سوال نوع ارسال و تنظیم پیش‌فرض برای جهش مستقیم به دریافت تارگت
        await state.update_data(send_type="all")
        await state.set_state(CreateOrderStates.waiting_for_target_data)
        await message.answer(
            with_cancel_hint(
                "📝 <b>اطلاعات را دقیقاً مشابه فرمت زیر ارسال کنید:</b>\n\n" + LINK_TARGET_EXAMPLE
            ),
            reply_markup=get_flow_nav_keyboard(),
        )
    else:
        await state.set_state(CreateOrderStates.waiting_for_target_data)
        await message.answer(
            with_cancel_hint("📁 <b>لیست اعضا را در قالب فایل متنی (txt) ارسال کنید:</b>"),
            reply_markup=get_flow_nav_keyboard(),
        )

@router.message(CreateOrderStates.waiting_for_order_type)
async def order_type_text_fallback(message: types.Message, state: FSMContext) -> None:
    await message.answer(
        with_cancel_hint("⚠️ لطفاً با دکمه‌های زیر نوع سفارش را انتخاب کنید."),
        reply_markup=get_order_type_keyboard(),
    )

@router.message(F.text == "🧰 ابزارها")
async def tools_text_entry(message: types.Message, state: FSMContext) -> None:
    await _route_to_inline(message, state, button_text="🧰 ابزارها", callback_data="menu_txt_generator/")

@router.message(F.text == "🎨 بنرها")
async def banners_text_entry(message: types.Message, state: FSMContext) -> None:
    await _route_to_inline(message, state, button_text="🎨 بنرها", callback_data="menu_banners/")


# ==========================================
# 6. STATE: Process Target Data (ورودی متنی — کیبورد ناوبری)
# ==========================================
from utils.telegram_helpers import normalize_target_line as _normalize_target_line

@router.message(CreateOrderStates.waiting_for_target_data)
async def process_target_data(message: types.Message, state: FSMContext, bot: Bot) -> None:
    fsm_data = await state.get_data()
    order_type = fsm_data.get("order_type")

    if order_type not in ("link", "list"):
        await cleanup_fsm_temp_files(state)
        await state.clear()
        return await message.answer(
            "⚠️ اطلاعات سفارش ناقص است. لطفاً از منوی اصلی دوباره «🛍 ثبت سفارش 🛍» را انتخاب کنید.",
            reply_markup=get_main_menu_reply_keyboard(),
        )

    target_data = None
    target_count = None

    if order_type == "link":
        if not message.text:
            return await message.answer(
                with_cancel_hint(
                    "⚠️ لطفاً متن را دقیقاً طبق فرمت نمونه ارسال کنید.\n\n"
                    "📝 <b>فرمت مورد انتظار:</b>\n" + LINK_TARGET_EXAMPLE
                ),
                reply_markup=get_flow_nav_keyboard(),
            )

        lines = message.text.strip().split('\n')
        if len(lines) < 2:
            return await message.answer(
                with_cancel_hint(
                    "⚠️ فرمت نامعتبر! خط(های) اول باید لینک گروه(ها) و خط آخر تعداد باشد.\n\n"
                    "📝 <b>فرمت مورد انتظار:</b>\n" + LINK_TARGET_EXAMPLE
                ),
                reply_markup=get_flow_nav_keyboard(),
            )

        count_str = lines[-1].strip()

        if not count_str.isdecimal():
            return await message.answer(
                with_cancel_hint(
                    "⚠️ خط آخر (تعداد ارسال) باید فقط شامل اعداد باشد.\n\n"
                    "📝 <b>فرمت مورد انتظار:</b>\n" + LINK_TARGET_EXAMPLE
                ),
                reply_markup=get_flow_nav_keyboard(),
            )

        target_count = int(count_str)
        if target_count < 1:
            return await message.answer(
                with_cancel_hint("⚠️ تعداد ارسال باید حداقل ۱ باشد. عدد ۰ یعنی ارسال به همه اعضا و مجاز نیست."),
                reply_markup=get_flow_nav_keyboard(),
            )
        if target_count > 100000:
            return await message.answer(
                with_cancel_hint("⚠️ حداکثر تعداد ارسال مجاز ۱۰۰٬۰۰۰ است."),
                reply_markup=get_flow_nav_keyboard(),
            )

        valid_lines = []
        invalid_lines_info = []
        
        for i, line in enumerate(lines[:-1], start=1):
            token = line.strip()
            if not token:
                continue
            res = _normalize_target_line(token)
            if res:
                valid_lines.append(res)
            else:
                invalid_lines_info.append(f"خط {i}: <code>{html.escape(token)}</code>")

        if not valid_lines and not invalid_lines_info:
            return await message.answer(
                with_cancel_hint(
                    "⚠️ فرمت نامعتبر! خط(های) اول باید لینک گروه(ها) و خط آخر تعداد باشد.\n\n"
                    "📝 <b>فرمت مورد انتظار:</b>\n" + LINK_TARGET_EXAMPLE
                ),
                reply_markup=get_flow_nav_keyboard(),
            )

        if not valid_lines:
            return await message.answer(
                with_cancel_hint(
                    "⚠️ هیچ لینک معتبری یافت نشد. لینک باید به شکل https://t.me/... ، لینک دعوت، یا @username باشد.\n\n"
                    "📝 <b>فرمت مورد انتظار:</b>\n" + LINK_TARGET_EXAMPLE
                ),
                reply_markup=get_flow_nav_keyboard(),
            )
            
        if invalid_lines_info:
            invalid_text = "\n".join(invalid_lines_info)
            return await message.answer(
                with_cancel_hint(
                    "⚠️ خط(های) زیر معتبر نیستند و نادیده گرفته می‌شوند:\n"
                    f"{invalid_text}\n"
                    "لطفاً مجدداً کل ورودی را با فرمت درست ارسال کنید."
                ),
                reply_markup=get_flow_nav_keyboard(),
            )

        target_data = "\n".join(valid_lines)

    elif order_type == "list":
        if not message.document or not message.document.file_name.endswith('.txt'):
            return await message.answer(
                with_cancel_hint("⚠️ لطفاً فقط یک فایل با پسوند .txt حاوی لیست آیدی‌ها ارسال کنید."),
                reply_markup=get_flow_nav_keyboard(),
            )

        if message.document.file_size and message.document.file_size > 20 * 1024 * 1024:
            return await message.answer(
                with_cancel_hint("⚠️ حجم فایل نباید بیشتر از ۲۰ مگابایت باشد."),
                reply_markup=get_flow_nav_keyboard(),
            )

        file_id = message.document.file_id
        unique_filename = str(uuid.uuid4())
        target_data = f"downloads/target_list_{unique_filename}.txt"

        try:
            file = await bot.get_file(file_id)
            await bot.download_file(file.file_path, destination=target_data)
        except Exception as e:
            logger.error(f"Error downloading target list: {e}", exc_info=True)
            if os.path.exists(target_data):
                try:
                    os.remove(target_data)
                except OSError:
                    logger.warning(f"Failed to remove partial file: {target_data}")
            return await message.answer(
                with_cancel_hint(get_download_error_message()),
                reply_markup=get_flow_nav_keyboard(),
            )
        target_count = 0

    await state.update_data(target_data=target_data, target_count=target_count)
    await state.update_data(order_messages=[])

    await ask_send_method_question(message, state)
# ==========================================
# 📋 ⌨️ STATE: روش ارسال (دکمه‌ای: متن مستقیم / کپی از کانال مبدا)
# ==========================================
async def ask_send_method_question(message: types.Message, state: FSMContext) -> None:
    await state.set_state(CreateOrderStates.waiting_for_send_method)

    await message.answer(
        with_cancel_hint(
            "📤 <b>روش ارسال:</b>\n\n"
            "۱) <b>متن مستقیم</b>\n"
            "ارسال با متن/مدیای همین سفارش — قابل شخصی‌سازی {first_name} و spintax.\n\n"
            "۲) <b>کپی از کانال مبدا</b>\n"
            "پیام از یک کانال مبدا برای هر تارگت <b>کپی</b> می‌شود:\n"
            "• ظاهر کاملاً طبیعی (بدون هدر «فوروارد شده از»)\n"
            "• entityهای پیام مبنا — از جمله ایموجی‌های پریمیوم (custom emoji) — "
            "دقیقاً حفظ می‌شوند\n"
            "• در این حالت متن سفارش اختیاری است"
        ),
        reply_markup=get_send_method_keyboard(),
    )


@router.message(CreateOrderStates.waiting_for_send_method, F.text.in_(SEND_METHOD_BY_TEXT))
async def process_send_method_selection(message: types.Message, state: FSMContext) -> None:
    send_method = SEND_METHOD_BY_TEXT[message.text.strip()]
    await state.update_data(send_method=send_method)

    if send_method == "direct":
        await state.set_state(CreateOrderStates.waiting_for_messages)
        await message.answer(
            with_cancel_hint("💬 <b>پیام خود را ارسال کنید:</b>"),
            reply_markup=get_flow_nav_keyboard(),
        )
    else:
        await state.update_data(source_message_ids=[])
        await state.set_state(CreateOrderStates.waiting_for_source_channel)
        await message.answer(
            with_cancel_hint(
                "📋 <b>کپی از کانال مبدا</b>\n\n"
                "شناسه‌ی عددی یا یوزرنیم کانال مبدا را ارسال کنید:\n\n"
                "✅ یوزرنیم: <code>@mychannel</code>\n"
                "✅ لینک عمومی: <code>https://t.me/mychannel</code>\n"
                "✅ شناسه‌ی عددی: <code>-1001234567890</code>\n"
                "✅ لینک خصوصی: <code>https://t.me/c/1234567890</code>\n\n"
                "⚠️ حداقل یکی از اکانت‌های ورکر باید به این کانال دسترسی داشته باشد "
                "(برای کانال‌های خصوصی یعنی عضویت) — در غیر این صورت کپی ممکن نیست."
            ),
            reply_markup=get_flow_nav_keyboard(),
        )


@router.message(CreateOrderStates.waiting_for_send_method)
async def send_method_text_fallback(message: types.Message, state: FSMContext) -> None:
    await message.answer(
        with_cancel_hint("⚠️ لطفاً با دکمه‌های زیر یکی از روش‌های ارسال را انتخاب کنید."),
        reply_markup=get_send_method_keyboard(),
    )


# ==========================================
# 📋 بررسی دسترسی به کانال مبدا (بدون تغییر)
# ==========================================
_SOURCE_CHANNEL_USERNAME_INVALID_TEXT = (
    "⚠️ یوزرنیم واردشده نامعتبر است یا توسط هیچ کانالی اشغال نشده.\n"
    "لطفاً یوزرنیم را به شکل <code>@channel</code> بررسی و دوباره ارسال کنید."
)

_SOURCE_CHANNEL_NOT_ACCESSIBLE_TEXT = (
    "⚠️ کانال مبدا در دسترس نیست.\n\n"
    "هیچ‌کدام از اکانت‌های ورکرِ بررسی‌شده به این کانال دسترسی ندارند "
    "(برای کانال‌های خصوصی، اکانت ورکر باید عضو کانال باشد) یا شناسه/یوزرنیم اشتباه است."
)


def _normalize_channel_input(raw: str) -> Optional[str]:
    text = raw.strip()

    m = re.fullmatch(r"(?:https?://)?t\.me/c/(\d{4,12})/?", text, re.IGNORECASE)
    if m:
        return f"-100{m.group(1)}"

    m = re.fullmatch(r"(?:https?://)?t\.me/([A-Za-z0-9_]{4,64})/?", text, re.IGNORECASE)
    if m:
        return f"@{m.group(1)}"

    if re.fullmatch(r"@?[A-Za-z0-9_]{4,64}", text):
        return text if text.startswith("@") else f"@{text}"

    if re.fullmatch(r"-?\d{4,}", text):
        return text

    return None


def _parse_proxy_string(proxy_string: Optional[str]) -> Optional[dict]:
    if not proxy_string:
        return None
    try:
        raw = proxy_string.strip()
        if "://" not in raw:
            raw = f"socks5://{raw}"
        parsed = urlparse(raw)
        scheme = (parsed.scheme or "socks5").lower()
        if scheme not in ("socks4", "socks5", "http", "https"):
            scheme = "socks5"
        if not parsed.hostname:
            return None
        return {
            "scheme": scheme,
            "hostname": parsed.hostname,
            "port": parsed.port or 1080,
            "username": parsed.username,
            "password": parsed.password,
        }
    except Exception:
        return None


def _validate_source_chat(chat):
    chat_type = getattr(chat, "type", None)
    if chat_type not in (ChatType.CHANNEL, ChatType.SUPERGROUP):
        return None, (
            "⚠️ شناسه/یوزرنیم واردشده مربوط به یک کانال نیست "
            "(کاربر، بات یا گروه معمولی است). لطفاً کانال مبدا را ارسال کنید."
        )
    return chat, None


def _get_connected_worker_clients() -> list:
    try:
        return [c for c in worker_pool.values() if getattr(c, "is_connected", False)]
    except Exception:
        return []


async def _check_channel_with_worker_sessions(
    channel_input: str,
    session: AsyncSession,
    category_ids: Optional[list[int]] = None,
):
    """فاز ۲ بررسی کانال مبدا: کلاینت موقت (in_memory) از session_string — اولویت با اکانت‌های دسته‌های همین سفارش؛ تا ۳ اکانت."""
    base_conditions = [
        Account.is_banned == False,  # noqa: E712
        Account.session_string.isnot(None),
        Account.session_string != "",
        APIKey.is_active == True,  # noqa: E712
    ]

    def _build_stmt(with_category: bool):
        stmt = (
            select(Account, APIKey)
            .join(APIKey, Account.api_id == APIKey.id)
            .where(*base_conditions)
            .order_by(Account.id)
            .limit(3)
        )
        if with_category and category_ids:
            stmt = stmt.where(Account.category_id.in_(category_ids))
        return stmt

    try:
        rows = (await session.execute(_build_stmt(True))).all()
        if not rows and category_ids:
            rows = (await session.execute(_build_stmt(False))).all()
    except Exception as e:
        await session.rollback()
        return None, report_db_error("اکانت‌های ورکر", e)

    if not rows:
        return None, (
            "⚠️ هیچ اکانت ورکری برای بررسی دسترسی به کانال مبدا در دسترس نیست.\n"
            "لطفاً ابتدا از بخش مدیریت اکانت‌ها، یک اکانت با سشن فعال اضافه کنید."
        )

    access_denied = False
    last_error = None

    for account, api_key in rows:
        # 🛡 (BUG-06): session_string در دیتابیس Fernet-encrypted است — خطای decrypt فقط همین اکانت را رد می‌کند
        try:
            decrypted_session = decrypt_session(account.session_string)
        except Exception as e:
            last_error = e
            logger.warning(
                f"Source-channel check: decrypt_session failed for user_{account.id}/ "
                f"({e.__class__.__name__}: {e}); trying next account."
            )
            continue
        if not decrypted_session:
            last_error = ValueError("empty session_string after decrypt")
            logger.warning(
                f"Source-channel check: empty decrypted session for user_{account.id}/; "
                f"trying next account."
            )
            continue

        temp_client = PyrogramClient(
            name=f"source_channel_check_{account.id}",
            api_id=api_key.api_id,
            api_hash=api_key.api_hash,
            session_string=decrypted_session,
            proxy=_parse_proxy_string(account.proxy_string),
            in_memory=True,
            no_updates=True,
        )
        try:
            await asyncio.wait_for(temp_client.start(), timeout=45)
        except Exception as e:
            last_error = e
            logger.warning(f"Source-channel check: temp client user_{account.id}/ failed to start: {e}")
            with suppress(Exception):
                await temp_client.stop()
            continue

        try:
            chat = await asyncio.wait_for(temp_client.get_chat(channel_input), timeout=30)
            return chat, None
        except (UsernameInvalid, UsernameNotOccupied):
            return None, _SOURCE_CHANNEL_USERNAME_INVALID_TEXT
        except (ChannelInvalid, ChannelPrivate, PeerIdInvalid):
            access_denied = True
            continue
        except Exception as e:
            last_error = e
            logger.warning(f"Source-channel check: get_chat on user_{account.id}/ failed: {e}")
            continue
        finally:
            with suppress(Exception):
                await temp_client.stop()

    if access_denied:
        return None, _SOURCE_CHANNEL_NOT_ACCESSIBLE_TEXT
    return None, (
        "⚠️ بررسی دسترسی به کانال مبدا ناموفق بود (خطای اتصال اکانت‌های ورکر).\n"
        f"<code>{html.escape(str(last_error))}</code>\n"
        "لطفاً دوباره تلاش کنید یا وضعیت پراکسی/سشن اکانت‌ها را بررسی کنید."
    )


async def check_source_channel_access(
    channel_input: str,
    session: AsyncSession,
    category_ids: Optional[list[int]] = None,
):
    """فاز ۱) کلاینت‌های متصل worker_pool (حداکثر ۵) — فاز ۲) کلاینت موقت از دیتابیس"""
    live_clients = _get_connected_worker_clients()[:5]

    for live_client in live_clients:
        try:
            chat = await asyncio.wait_for(live_client.get_chat(channel_input), timeout=30)
        except (UsernameInvalid, UsernameNotOccupied):
            return None, _SOURCE_CHANNEL_USERNAME_INVALID_TEXT
        except (ChannelInvalid, ChannelPrivate, PeerIdInvalid):
            continue
        except Exception as e:
            logger.warning(f"Source-channel check: live worker get_chat failed: {e}")
            continue
        return _validate_source_chat(chat)

    chat, error = await _check_channel_with_worker_sessions(channel_input, session, category_ids)
    if chat is None:
        return None, error
    return _validate_source_chat(chat)


# ==========================================
# 📋 STATE: دریافت شناسه/یوزرنیم کانال مبدا
# ==========================================
@router.message(CreateOrderStates.waiting_for_source_channel)
async def process_source_channel(message: types.Message, state: FSMContext, session: AsyncSession) -> None:
    if not message.text:
        return await message.answer(
            with_cancel_hint("⚠️ لطفاً شناسه‌ی عددی یا یوزرنیم کانال مبدا را به‌صورت متن ارسال کنید."),
            reply_markup=get_flow_nav_keyboard(),
        )

    channel_input = _normalize_channel_input(message.text)

    if channel_input is None:
        return await message.answer(
            with_cancel_hint(
                "⚠️ فرمت ورودی نامعتبر است.\n\n"
                "✅ یوزرنیم: <code>@mychannel</code>\n"
                "✅ لینک عمومی: <code>https://t.me/mychannel</code>\n"
                "✅ شناسه‌ی عددی: <code>-1001234567890</code>\n"
                "✅ لینک خصوصی: <code>https://t.me/c/1234567890</code>"
            ),
            reply_markup=get_flow_nav_keyboard(),
        )

    wait_msg = await message.answer("⏳ در حال بررسی دسترسی به کانال مبدا با یکی از اکانت‌های ورکر...")

    fsm_data = await state.get_data()
    category_ids = fsm_data.get("selected_categories", [])

    chat, error_text = await check_source_channel_access(channel_input, session, category_ids)

    with suppress(TelegramBadRequest):
        await wait_msg.delete()

    if chat is None:
        return await message.answer(
            with_cancel_hint(error_text or "⚠️ بررسی کانال مبدا ناموفق بود. لطفاً دوباره تلاش کنید."),
            reply_markup=get_flow_nav_keyboard(),
        )

    channel_title = chat.title or (f"@{chat.username}" if chat.username else str(chat.id))
    channel_username = f"@{chat.username}" if chat.username else "ندارد"

    protected_warning = ""
    if getattr(chat, "has_protected_content", False):
        protected_warning = (
            "\n\n🚨 <b>هشدار مهم:</b> «حفاظت از محتوا» (Restrict Saving) در این کانال "
            "فعال است و تلگرام کپی پیام‌های آن را با خطای ChatForwardsRestricted "
            "مسدود می‌کند. قویاً توصیه می‌شود کانال دیگری انتخاب کنید."
        )

    await state.update_data(
        source_channel_id=chat.id,
        source_channel_title=channel_title,
        source_message_ids=[],
    )
    await state.set_state(CreateOrderStates.waiting_for_source_messages)

    await message.answer(
        with_cancel_hint(
            f"✅ کانال مبدا تأیید شد: <b>{html.escape(channel_title)}</b>\n"
            f"👤 یوزرنیم: {html.escape(channel_username)}\n"
            f"🆔 شناسه‌ی ذخیره‌شده: <code>{chat.id}</code>"
            f"{protected_warning}\n\n"
            "📥 حالا <b>پیام(های) نمونه</b> را از همین کانال در این چت فوروارد کنید:\n\n"
            "• شناسه‌ی کانال و id پیام به‌صورت خودکار از روی فوروارد استخراج می‌شود\n"
            f"• حداکثر <b>{MAX_SOURCE_MESSAGES}</b> پیام (برای هر تارگت یکی از آن‌ها به‌صورت تصادفی کپی می‌شود)\n"
            "• در حالت کپی، متن سفارش اختیاری است"
        ),
        reply_markup=get_end_collection_keyboard(),
    )


# ==========================================
# 📋 استخراج مبدأ فوروارد (بدون تغییر)
# ==========================================
def _extract_forward_source(message: types.Message):
    origin = getattr(message, "forward_origin", None)
    if origin is not None:
        if getattr(origin, "type", None) == "channel":
            origin_chat = getattr(origin, "chat", None)
            origin_message_id = getattr(origin, "message_id", None)
            if origin_chat is not None and origin_message_id is not None:
                return origin_chat.id, origin_message_id
        return None, None

    forward_from_chat = getattr(message, "forward_from_chat", None)
    forward_from_message_id = getattr(message, "forward_from_message_id", None)
    if forward_from_chat is not None and forward_from_message_id is not None:
        return forward_from_chat.id, forward_from_message_id

    return None, None


# ==========================================
# 📋 ⌨️ STATE: فوروارد پیام‌های نمونه — «✅ پایان» دکمه‌ای شد
# ⚠️ این هندلر باید «قبل از» process_source_forward ثبت شود تا متن «✅ پایان»
# به‌عنوان فوروارد پردازش نشود.
# ==========================================
@router.message(CreateOrderStates.waiting_for_source_messages, F.text == END_COLLECTION_TEXT)
async def end_source_messages_text(message: types.Message, state: FSMContext, bot: Bot) -> None:
    fsm_data = await state.get_data()
    if not fsm_data.get("source_message_ids"):
        return await message.answer(
            with_cancel_hint("⚠️ حداقل یک پیام نمونه فوروارد کنید!"),
            reply_markup=get_end_collection_keyboard(),
        )
    await finish_source_message_collection(message, state, bot)


@router.message(CreateOrderStates.waiting_for_source_messages)
async def process_source_forward(message: types.Message, state: FSMContext) -> None:
    fsm_data = await state.get_data()
    source_channel_id = fsm_data.get("source_channel_id")
    source_message_ids = fsm_data.get("source_message_ids", [])

    if source_channel_id is None:
        await state.clear()
        return await message.answer(
            "⚠️ اطلاعات کانال مبدا از دست رفت. لطفاً از منوی اصلی دوباره «🛍 ثبت سفارش 🛍» را انتخاب کنید.",
            reply_markup=get_main_menu_reply_keyboard(),
        )

    fwd_chat_id, fwd_message_id = _extract_forward_source(message)

    if fwd_chat_id is None:
        return await message.answer(
            with_cancel_hint(
                "⚠️ این پیام فورواردِ یک <b>کانال</b> نیست.\n\n"
                "لطفاً پیام(های) نمونه را مستقیماً از خودِ کانال مبدا فوروارد کنید "
                "(فوروارد از کاربر یا گروه پذیرفته نمی‌شود)."
            ),
            reply_markup=get_end_collection_keyboard(),
        )

    if fwd_chat_id != source_channel_id:
        return await message.answer(
            with_cancel_hint(
                "⚠️ این پیام متعلق به کانال مبدا‌ی ثبت‌شده نیست!\n\n"
                f"🆔 کانال این پیام: <code>{fwd_chat_id}</code>\n"
                f"🆔 کانال مبدا: <code>{source_channel_id}</code>\n\n"
                "لطفاً فقط پیام‌های همان کانال مبدا را فوروارد کنید."
            ),
            reply_markup=get_end_collection_keyboard(),
        )

    if fwd_message_id in source_message_ids:
        return await message.answer(
            with_cancel_hint(f"ℹ️ پیام <code>{fwd_message_id}</code> قبلاً به لیست اضافه شده است."),
            reply_markup=get_end_collection_keyboard(),
        )

    source_message_ids.append(fwd_message_id)
    await state.update_data(source_message_ids=source_message_ids)

    if len(source_message_ids) >= MAX_SOURCE_MESSAGES:
        return await finish_source_message_collection(message, state, message.bot)

    await message.answer(
        with_cancel_hint(
            f"✅ پیام <code>{fwd_message_id}</code> اضافه شد "
            f"({len(source_message_ids)}/{MAX_SOURCE_MESSAGES}).\n\n"
            "📤 پیام بعدی را فوروارد کنید یا برای پایان «✅ پایان» را بزنید."
        ),
        reply_markup=get_end_collection_keyboard(),
    )


async def finish_source_message_collection(message: types.Message, state: FSMContext, bot: Bot) -> None:
    """پایان جمع‌آوری پیام‌های نمونه — در حالت کپی مستقیماً به انتخاب فیلتر می‌رویم"""
    fsm_data = await state.get_data()
    source_channel_id = fsm_data.get("source_channel_id")
    source_message_ids = fsm_data.get("source_message_ids", [])

    await state.update_data(
        use_banner_pool=False,
        smart_flow=False,
        order_messages=[],
    )

    ids_display = ", ".join(f"<code>{mid}</code>" for mid in source_message_ids)

    await message.answer(
        "📋 <b>پیام‌های کانال مبدا ثبت شد.</b>\n\n"
        f"🆔 کانال مبدا: <code>{source_channel_id}</code>\n"
        f"📨 پیام‌ها ({len(source_message_ids)}): {ids_display}\n\n"
        "ℹ️ در حالت کپی:\n"
        "• برای هر تارگت یکی از پیام‌های بالا به‌صورت تصادفی کپی می‌شود\n"
        "• ظاهر پیام طبیعی است (بدون هدر «فوروارد شده از»)\n"
        "• ایموجی‌های پریمیوم و قالب‌بندی پیام مبدا دقیقاً حفظ می‌شوند\n"
        "• شخصی‌سازی {first_name} و جهش متن اعمال نمی‌شود"
    )

    await proceed_to_filter_selection(message, state, bot)


# ==========================================
# ACTIVE ORDERS & KILL SWITCH
# ⌨️ ورودی (دکمه‌ای/کال‌بکی) مشترک — خودِ لیست شیشه‌ای می‌ماند (پویا + صفحه‌بندی)
# ==========================================
async def _render_active_orders(
    message: types.Message,
    session: AsyncSession,
    state: Optional[FSMContext] = None,
    page: int = 1,
    callback: Optional[types.CallbackQuery] = None,
) -> None:
    active_filter = Order.status.in_([OrderStatus.pending, OrderStatus.running])

    try:
        total_orders = (
            await session.scalar(select(func.count(Order.id)).where(active_filter)) or 0
        )
        total_pages = calculate_total_pages(total_orders)
        page = clamp_page(page, total_pages)
        offset = get_page_offset(page)

        current_orders = (
            await session.scalars(
                select(Order)
                .where(active_filter)
                .order_by(Order.id.desc())
                .offset(offset)
                .limit(PAGINATION_SIZE)
            )
        ).all()
    except Exception as e:
        await session.rollback()
        err = report_db_error("سفارشات", e)
        if callback is not None:
            return await answer_callback_error(callback, err, get_main_menu_button())
        return await message.answer(err, reply_markup=get_main_menu_button())

    if state is not None:
        await state.update_data(active_orders_page=page)

    if total_orders == 0:
        return await message.answer(
            "✅ <b>هیچ سفارش فعالی وجود ندارد.</b>\n\nتمامی کمپین‌ها به اتمام رسیده‌اند.",
            reply_markup=get_main_menu_reply_keyboard(),
        )

    builder = InlineKeyboardBuilder()

    for order in current_orders:
        status_emoji = "⏳" if order.status == OrderStatus.pending else "🚀"
        display_code = order.tracking_code if order.tracking_code else f"ID-{order.id}"
        button_text = f"{status_emoji} لغو سفارش {display_code}"
        builder.button(text=button_text, callback_data=f"cancel_order_{order.id}/")

    builder.adjust(1)

    add_pagination_nav_row(builder, page, total_pages, callback_prefix="menu_active_orders_")
    # اضافه شدن دسترسی به داشبورد کل سفارشات به UI
    builder.row(types.InlineKeyboardButton(text="🗂 همه سفارشات", callback_data="menu_list_orders/"))
    add_list_footer(builder, refresh_callback=f"menu_active_orders_page_{page}/")

    await safe_edit_or_answer(
        message,
        f"🛑 <b>مدیریت سفارشات فعال</b>\n"
        f"🔢 مجموع: <b>{total_orders}</b> سفارش فعال\n"
        f"📄 صفحهٔ <b>{page}</b> از <b>{total_pages}</b>\n\n"
        "لیست زیر شامل کمپین‌های در حال اجرا یا در صف انتظار است.\n"
        "<i>برای توقف اضطراری (Kill Switch) روی هر سفارش کلیک کنید:</i>",
        reply_markup=builder.as_markup(),
    )

@router.callback_query(F.data.startswith("menu_active_orders"))
async def list_active_orders(
    callback: types.CallbackQuery,
    session: AsyncSession,
    state: FSMContext = None,
    skip_answer: bool = False,
    page: Optional[int] = None,
) -> None:
    if not skip_answer:
        await callback.answer()

    if page is None:
        page = parse_page_from_callback(callback.data)

    await _render_active_orders(callback.message, session, state=state, page=page, callback=callback)


# ==========================================
# 🔵 CANCEL ORDER: مرحله ۱ (تأیید دو مرحله‌ای — شیشه‌ای، بدون تغییر)
# ==========================================
@router.callback_query(F.data.startswith("cancel_order_") & F.data.endswith("/"))
async def cancel_order_handler(callback: types.CallbackQuery, state: FSMContext, session: AsyncSession) -> None:
    order_id_str = callback.data.replace("cancel_order_", "").replace("/", "")

    if not order_id_str.isdigit():
        return await callback.answer("⚠️ شناسه نامعتبر است.")

    order_id = int(order_id_str)

    try:
        stmt = select(Order).where(Order.id == order_id)
        result = await session.execute(stmt)
        order = result.scalar_one_or_none()
    except Exception as e:
        await session.rollback()
        return await answer_callback_error(
            callback, report_db_error("سفارش", e), get_main_menu_button()
        )

    if not order or order.status not in [OrderStatus.pending, OrderStatus.running]:
        await callback.answer("⚠️ این سفارش قبلاً لغو شده یا وجود ندارد.", show_alert=True)
        fsm_data = await state.get_data()
        return await list_active_orders(
            callback, session, state=state, skip_answer=True,
            page=fsm_data.get("active_orders_page", 1),
        )

    await callback.answer()

    fsm_data = await state.get_data()
    await cleanup_fsm_temp_files(state)
    await state.update_data(
        confirm_action="cancel_order",
        target_id=order_id,
        return_page=fsm_data.get("active_orders_page", 1),
    )
    await state.set_state(ConfirmStates.waiting_for_confirmation)

    display_code = order.tracking_code if order.tracking_code else f"ID-{order.id}"

    target_display = order.target_data or ""
    if order.order_type == "link" and target_display and not target_display.startswith("http"):
        target_display = f"🔗 https://t.me/{target_display.replace('@', '')}"
    elif order.order_type == "link" and target_display:
        target_display = f"🔗 {target_display}"
    elif order.order_type == "list":
        target_display = "📁 فایل متنی (TXT)"
    if not target_display:
        target_display = "نامشخص"

    order_type_display = "🔗 لینک گروه" if order.order_type == "link" else "📄 فایل اعضا"
    status_display = "🚀 در حال اجرا" if order.status == OrderStatus.running else "⏳ در صف انتظار"
    created_date = order.created_at.strftime("%Y/%m/%d %H:%M") if order.created_at else "نامشخص"

    media_files_count = sum(1 for p in [order.media_path, order.media_2_path, order.media_3_path] if p)

    send_method_display = "📋 کپی از کانال مبدا" if order.source_message_ids else "📤 متن مستقیم"

    builder = InlineKeyboardBuilder()
    builder.button(text="✅ بله، لغو کن", callback_data=f"confirm_cancel_order_{order.id}/")
    builder.button(text="❌ انصراف", callback_data="cancel_confirm_cancel_order/")
    builder.adjust(2)

    await safe_edit_message(
        callback.message,
        f"⚠️ <b>تأیید لغو سفارش</b>\n\n"
        f"آیا از لغو سفارش <b>#{order_id}</b> مطمئن هستید؟\n\n"
        f"🆔 شناسه سفارش: <code>{order.id}</code>\n"
        f"🎟 کد رهگیری: <code>{display_code}</code>\n"
        f"📦 نوع سفارش: {order_type_display}\n"
        f"📤 روش ارسال: {send_method_display}\n"
        f"🎯 هدف ارسال: {target_display}\n"
        f"👤 تعداد درخواستی: {order.target_count}\n"
        f"📎 فایل‌های مدیا: {media_files_count} عدد\n"
        f"📍 وضعیت فعلی: {status_display}\n"
        f"📊 داشبورد زنده: <code>/gtg_{order.id}</code>\n"
        f"📅 تاریخ ثبت: {created_date}\n\n"
        f"⚠️ <b>توجه: این عمل قابل بازگشت نیست و فایل‌های مدیا پاک می‌شوند.</b>",
        reply_markup=builder.as_markup(),
        # --- FIX M13 ---
        link_preview_options=types.LinkPreviewOptions(is_disabled=True),
    )

# ==========================================
# 🔵 CANCEL ORDER: مرحله ۲ (اجرای واقعی — بدون تغییر)
# ==========================================
@router.callback_query(F.data.startswith("confirm_cancel_order_") & F.data.endswith("/"))
async def confirm_cancel_order_handler(callback: types.CallbackQuery, state: FSMContext, session: AsyncSession) -> None:
    current_state = await state.get_state()
    fsm_data = await state.get_data()

    if current_state != ConfirmStates.waiting_for_confirmation or fsm_data.get("confirm_action") != "cancel_order":
        return await callback.answer(
            "⚠️ این درخواست تأیید منقضی شده است. لطفاً از ابتدا اقدام کنید.",
            show_alert=True,
        )

    order_id_str = callback.data.replace("confirm_cancel_order_", "").replace("/", "")

    if not order_id_str.isdigit():
        return await callback.answer("⚠️ شناسه نامعتبر است.")

    order_id = int(order_id_str)

    if fsm_data.get("target_id") != order_id:
        return await callback.answer(
            "⚠️ این درخواست تأیید با سفارش نمایش‌داده‌شده مطابقت ندارد. لطفاً از ابتدا اقدام کنید.",
            show_alert=True,
        )

    await safe_callback_answer(callback, "⏳ در حال پردازش...")

    try:
        stmt = select(Order).where(Order.id == order_id)
        result = await session.execute(stmt)
        order = result.scalar_one_or_none()
    except Exception as e:
        await session.rollback()
        return await answer_callback_error(
            callback, report_db_error("سفارش", e), get_main_menu_button()
        )

    if not order or order.status not in [OrderStatus.pending, OrderStatus.running]:
        return_page = fsm_data.get("return_page", 1)
        await state.clear()
        await callback.message.answer("⚠️ این سفارش قبلاً لغو شده یا وجود ندارد.")
        return await list_active_orders(
            callback, session, state=state, skip_answer=True, page=return_page
        )

    media_paths = [order.media_path, order.media_2_path, order.media_3_path]

    order.status = OrderStatus.error
    order.target_data = ""
    order.media_path = None
    order.media_2_path = None
    order.media_3_path = None

    try:
        await session.commit()
    except Exception as e:
        await session.rollback()
        return await answer_callback_error(
            callback, report_db_error("سفارش", e), get_main_menu_button()
        )

    # --- FIX M1: Write the Kill Switch flag to Redis for fast-path cancellation ---
    try:
        r = aioredis.from_url(config.REDIS_URL, decode_responses=True)
        await r.set(f"kill_order:{order_id}", "1", ex=3600 * 24)
        await r.aclose()
    except Exception as e:
        logger.warning(f"Failed to set Redis kill flag for order {order_id}: {e}")
    # ------------------------------------------------------------------------------

    for path in media_paths:
        if path and os.path.exists(path):
            try:
                os.remove(path)
                logger.info(f"Garbage Collection: Deleted media {path} for cancelled order #{order.id}")
            except Exception as e:
                logger.warning(f"Failed to delete media {path}: {e}")

    return_page = fsm_data.get("return_page", 1)
    await state.clear()

    await callback.message.answer(
        f"✅ سفارش <b>#{order_id}</b> با موفقیت متوقف شد و فایل‌های آن پاکسازی گردید."
    )

    return await list_active_orders(
        callback, session, state=state, skip_answer=True, page=return_page
    )

# ==========================================
# 🔵 انصراف از لغو سفارش (بدون تغییر)
# ==========================================
@router.callback_query(F.data == "cancel_confirm_cancel_order/")
async def cancel_order_confirmation_declined(callback: types.CallbackQuery, state: FSMContext, session: AsyncSession) -> None:
    fsm_data = await state.get_data()
    return_page = fsm_data.get("return_page", 1)
    if fsm_data.get("confirm_action") == "cancel_order":
        await state.clear()

    await callback.answer("🚫 عملیات لغو سفارش متوقف شد.")

    return await list_active_orders(
        callback, session, state=state, skip_answer=True, page=return_page
    )


# ==========================================
# 🛍 ORDERS LIST (شیشه‌ای — فیلتر وضعیت + صفحه‌بندی؛ بدون تغییر)
# ==========================================
ORDER_STATUS_BADGES: dict = {
    OrderStatus.pending: "🕒 در صف انتظار",
    OrderStatus.running: "🚀 در حال اجرا",
    OrderStatus.completed: "✅ تکمیل شده",
    OrderStatus.error: "🛑 لغو شده",
}
ORDER_STATUS_BADGES.update({
    getattr(s, "value", s): label for s, label in list(ORDER_STATUS_BADGES.items())
})
ORDER_TYPE_LABELS: dict = {
    "link": "🔗 لینک گروه",
    "list": "📄 فایل اعضا",
    "extract": "⚙️ استخراج",
}
ORDERS_FILTERS: dict = {
    "all": "همه",
    "pending": "در انتظار",
    "running": "در حال اجرا",
    "completed": "تکمیل شده",
    "failed": "متوقف/لغو شده",
}
ORDERS_FILTER_EMOJIS: dict = {
    "all": "📊",
    "pending": "🕒",
    "running": "🚀",
    "completed": "✅",
    "failed": "⛔️",
}


def get_order_status_badge(status) -> str:
    badge = ORDER_STATUS_BADGES.get(status)
    if badge is None and status is not None:
        badge = ORDER_STATUS_BADGES.get(str(status), "❔ نامشخص")
    return badge or "❔ نامشخص"


def _build_orders_filter_condition(filter_type: str):
    if filter_type == "pending":
        return Order.status == OrderStatus.pending
    if filter_type == "running":
        return Order.status == OrderStatus.running
    if filter_type == "completed":
        return Order.status == OrderStatus.completed
    if filter_type == "failed":
        return Order.status == OrderStatus.error
    return None


async def render_orders_list(
    callback: types.CallbackQuery,
    session: AsyncSession,
    state: Optional[FSMContext] = None,
    filter_type: str = "all",
    page: int = 1,
) -> None:
    if filter_type not in ORDERS_FILTERS:
        filter_type = "all"

    filter_cond = _build_orders_filter_condition(filter_type)

    try:
        total_all = await session.scalar(select(func.count(Order.id))) or 0
        pending_count = await session.scalar(
            select(func.count(Order.id)).where(Order.status == OrderStatus.pending)
        ) or 0
        running_count = await session.scalar(
            select(func.count(Order.id)).where(Order.status == OrderStatus.running)
        ) or 0
        completed_count = await session.scalar(
            select(func.count(Order.id)).where(Order.status == OrderStatus.completed)
        ) or 0
        failed_count = await session.scalar(
            select(func.count(Order.id)).where(Order.status == OrderStatus.error)
        ) or 0

        filter_counts = {
            "all": total_all,
            "pending": pending_count,
            "running": running_count,
            "completed": completed_count,
            "failed": failed_count,
        }

        total_count = filter_counts[filter_type]
        total_pages = calculate_total_pages(total_count)
        page = clamp_page(page, total_pages)
        offset = get_page_offset(page)

        stmt = select(Order).order_by(Order.id.desc())
        if filter_cond is not None:
            stmt = stmt.where(filter_cond)
        orders = (await session.scalars(stmt.offset(offset).limit(PAGINATION_SIZE))).all()

        sent_map: dict = {}
        if orders:
            progress_rows = (
                await session.execute(
                    select(OrderLog.order_id, func.count(OrderLog.id))
                    .where(
                        OrderLog.order_id.in_([o.id for o in orders]),
                        OrderLog.status == "success",
                    )
                    .group_by(OrderLog.order_id)
                )
            ).all()
            sent_map = dict(progress_rows)
    except Exception as e:
        await session.rollback()
        return await answer_callback_error(
            callback, report_db_error("سفارشات", e), get_main_menu_button()
        )

    if state is not None:
        await state.update_data(orders_list_filter=filter_type, orders_list_page=page)

    filter_label = ORDERS_FILTERS[filter_type]
    filter_emoji = ORDERS_FILTER_EMOJIS[filter_type]

    text = (
        "🛍 <b>لیست سفارشات</b>\n"
        f"🔎 فیلتر فعلی: {filter_emoji} <b>{filter_label}</b>\n"
        f"🔢 مجموع در این فیلتر: <b>{total_count}</b>\n"
        f"📄 صفحهٔ <b>{page}</b> از <b>{total_pages}</b>\n\n"
    )

    builder = InlineKeyboardBuilder()

    filter_buttons = []
    for f_key in ("all", "pending", "running", "completed", "failed"):
        count = filter_counts[f_key]
        if f_key == filter_type:
            btn_text = f"📍 {ORDERS_FILTERS[f_key]} ({count})"
        else:
            btn_text = f"{ORDERS_FILTER_EMOJIS[f_key]} {ORDERS_FILTERS[f_key]} ({count})"
        filter_buttons.append(
            types.InlineKeyboardButton(
                text=btn_text,
                callback_data=f"list_orders_filter_{f_key}_page_1/",
            )
        )

    builder.row(filter_buttons[0], filter_buttons[1])
    builder.row(filter_buttons[2], filter_buttons[3])
    builder.row(filter_buttons[4])

    if total_count == 0 or not orders:
        text += "⚠️ موردی یافت نشد.\n\n<i>برای تغییر فیلتر از دکمه‌های بالا استفاده کنید.</i>"
        add_list_footer(builder, refresh_callback=f"list_orders_filter_{filter_type}_page_{page}/")
        return await safe_edit_or_answer(callback.message, text, reply_markup=builder.as_markup())

    order_buttons = []
    for idx, order in enumerate(orders, start=offset + 1):
        badge = get_order_status_badge(order.status)
        type_label = ORDER_TYPE_LABELS.get(order.order_type, "❔ نامشخص")

        sent_count = sent_map.get(order.id, 0)
        if order.target_count:
            progress = f"👤 {sent_count}/{order.target_count}"
        else:
            progress = f"👤 {sent_count}"

        created_date = order.created_at.strftime("%Y/%m/%d") if order.created_at else "نامشخص"

        details = []
        if order.tracking_code:
            details.append(f"🎟 <code>{html.escape(order.tracking_code)}</code>")
        details.append(progress)
        details.append(f"📅 {created_date}")

        text += (
            f"<b>{idx}.</b> 🆔 <code>#{order.id}</code> · {badge} · {type_label}\n"
            + " · ".join(details)
            + "\n\n"
        )

        order_buttons.append(
            types.InlineKeyboardButton(
                text=f"📊 سفارش {order.id}",
                callback_data=f"view_order_{order.id}/",
            )
        )

    for i in range(0, len(order_buttons), 2):
        builder.row(*order_buttons[i:i + 2])

    text += "👇 برای مشاهدهٔ داشبورد هر سفارش، روی دکمهٔ مربوطه کلیک کنید:"

    add_pagination_nav_row(
        builder, page, total_pages,
        callback_prefix=f"list_orders_filter_{filter_type}_",
    )
    add_list_footer(
        builder,
        refresh_callback=f"list_orders_filter_{filter_type}_page_{page}/",
    )

    await safe_edit_or_answer(callback.message, text, reply_markup=builder.as_markup())


@router.callback_query(F.data == "menu_list_orders/")
async def show_orders_list_handler(callback: types.CallbackQuery, state: FSMContext, session: AsyncSession) -> None:
    await safe_callback_answer(callback)
    await cleanup_fsm_temp_files(state)
    await state.clear()
    await render_orders_list(callback, session, state=state, filter_type="all", page=1)


@router.callback_query(F.data.startswith("list_orders_filter_"))
async def list_orders_filter_handler(callback: types.CallbackQuery, state: FSMContext, session: AsyncSession) -> None:
    await safe_callback_answer(callback)

    match = re.match(r"^list_orders_filter_([a-z]+)_page_(\d+)/$", callback.data)
    if not match:
        return await render_orders_list(callback, session, state=state)

    await render_orders_list(
        callback, session, state=state,
        filter_type=match.group(1),
        page=int(match.group(2)),
    )


@router.callback_query(F.data.startswith("view_order_") & F.data.endswith("/"))
async def view_order_dashboard_handler(callback: types.CallbackQuery, session: AsyncSession) -> None:
    order_id_str = callback.data.replace("view_order_", "").replace("/", "")
    if not order_id_str.isdigit():
        return await callback.answer("⚠️ شناسه نامعتبر است.", show_alert=True)

    order_id = int(order_id_str)
    await safe_callback_answer(callback)

    try:
        text, markup = await generate_dashboard_data(
            order_id, session, back_callback="orders_back_to_list/"
        )
    except Exception as e:
        await session.rollback()
        return await answer_callback_error(
            callback, report_db_error("سفارش", e), get_main_menu_button()
        )

    if not text:
        return await callback.answer("⚠️ سفارش مورد نظر یافت نشد!", show_alert=True)

    await safe_edit_message(
        callback.message, text, reply_markup=markup, disable_web_page_preview=True
    )


@router.callback_query(F.data == "orders_back_to_list/")
async def orders_back_to_list_handler(callback: types.CallbackQuery, state: FSMContext, session: AsyncSession) -> None:
    await safe_callback_answer(callback)
    fsm_data = await state.get_data()
    await render_orders_list(
        callback, session, state=state,
        filter_type=fsm_data.get("orders_list_filter", "all"),
        page=fsm_data.get("orders_list_page", 1),
    )


# ==========================================
# 7. ⌨️ STATE: دریافت پیام‌های سفارش (تا ۳ پیام) — «✅ پایان» دکمه‌ای شد
# ⚠️ این هندلر باید «قبل از» process_order_messages ثبت شود.
# ==========================================
@router.message(CreateOrderStates.waiting_for_messages, F.text == END_COLLECTION_TEXT)
async def end_messages_text(message: types.Message, state: FSMContext) -> None:
    fsm_data = await state.get_data()
    if not fsm_data.get("order_messages"):
        return await message.answer(
            with_cancel_hint("⚠️ لطفاً حداقل یک پیام ارسال کنید!"),
            reply_markup=get_end_collection_keyboard(),
        )
    await ask_banner_pool_question(message, state)


@router.message(CreateOrderStates.waiting_for_messages)
async def process_order_messages(message: types.Message, state: FSMContext, bot: Bot) -> None:
    fsm_data = await state.get_data()
    order_messages = fsm_data.get("order_messages", [])

    msg_text = message.text or message.caption or ""
    media_path = None
    media_type = None

    if not message.photo and not message.video and not message.document and not msg_text:
        return await message.answer(
            with_cancel_hint("⚠️ لطفاً یک پیام متنی یا دارای رسانه (عکس/ویدیو/فایل) ارسال کنید."),
            reply_markup=get_end_collection_keyboard(),
        )

    os.makedirs("downloads", exist_ok=True)
    unique_filename = str(uuid.uuid4())

    try:
        if message.photo:
            media_type = "photo"
            file_id = message.photo[-1].file_id
            file = await bot.get_file(file_id)
            media_path = f"downloads/{unique_filename}.jpg"
            await bot.download_file(file.file_path, destination=media_path)

        elif message.video:
            if message.video.file_size and message.video.file_size > 20 * 1024 * 1024:
                return await message.answer(
                    with_cancel_hint("⚠️ حجم ویدیو نباید بیشتر از ۲۰ مگابایت باشد."),
                    reply_markup=get_end_collection_keyboard(),
                )
            media_type = "video"
            file_id = message.video.file_id
            file = await bot.get_file(file_id)
            media_path = f"downloads/{unique_filename}.mp4"
            await bot.download_file(file.file_path, destination=media_path)

        elif message.document:
            if message.document.file_size and message.document.file_size > 20 * 1024 * 1024:
                return await message.answer(
                    with_cancel_hint("⚠️ حجم فایل نباید بیشتر از ۲۰ مگابایت باشد."),
                    reply_markup=get_end_collection_keyboard(),
                )
            media_type = "document"
            file_id = message.document.file_id
            ext = os.path.splitext(message.document.file_name)[1] if message.document.file_name else ".dat"
            file = await bot.get_file(file_id)
            media_path = f"downloads/{unique_filename}{ext}"
            await bot.download_file(file.file_path, destination=media_path)

    except Exception as e:
        logger.error(f"Error downloading media in process_order_messages: {e}", exc_info=True)
        if media_path and os.path.exists(media_path):
            try:
                os.remove(media_path)
            except OSError:
                logger.warning(f"Failed to remove partial media file: {media_path}")
        return await message.answer(
            with_cancel_hint(get_download_error_message()),
            reply_markup=get_end_collection_keyboard(),
        )

    order_messages.append({
        "text": msg_text,
        "media_path": media_path,
        "media_type": media_type,
    })

    await state.update_data(order_messages=order_messages)
    msg_count = len(order_messages)

    if msg_count < 3:
        await message.answer(
            with_cancel_hint(
                f"💬 <b>پیام {msg_count + 1} را ارسال کنید:</b>\n\n"
                f"❕ تا ۳ پیام می‌توانید ارسال کنید."
            ),
            reply_markup=get_end_collection_keyboard(),
        )
    else:
        await ask_banner_pool_question(message, state)


# ==========================================
# 🎨 ⌨️ BANNER POOL: سوال بله/خیر (دکمه‌ای)
# ==========================================
async def ask_banner_pool_question(message: types.Message, state: FSMContext) -> None:
    await state.set_state(CreateOrderStates.waiting_for_banner_pool)

    await message.answer(
        with_cancel_hint(
            "🎨 <b>استفاده از مخزن بنر</b>\n\n"
            "استفاده از مخزن بنر (به‌جای متن این سفارش)؟\n\n"
            "🟢 <b>بله:</b> متن و مدیای این سفارش نادیده گرفته می‌شود و هر اکانت برای "
            "هر دسته از تارگت‌ها یک بنر تصادفی از مخزن دریافت می‌کند.\n"
            "⚪️ <b>خیر:</b> ارسال با متن و مدیای همین سفارش انجام می‌شود.\n\n"
            "<i>نکته: اگر هنگام ارسال هیچ بنر فعالی در مخزن نباشد، سفارش به‌صورت خودکار "
            "با متن خودش ادامه می‌دهد و به شما هشدار داده می‌شود.</i>"
        ),
        reply_markup=get_banner_pool_keyboard(),
    )


@router.message(CreateOrderStates.waiting_for_banner_pool, F.text == BANNER_POOL_YES_TEXT)
async def banner_pool_yes_handler(message: types.Message, state: FSMContext) -> None:
    await state.update_data(use_banner_pool=True)
    await ask_smart_flow_question(message, state)


@router.message(CreateOrderStates.waiting_for_banner_pool, F.text == BANNER_POOL_NO_TEXT)
async def banner_pool_no_handler(message: types.Message, state: FSMContext) -> None:
    await state.update_data(use_banner_pool=False)
    await ask_smart_flow_question(message, state)


@router.message(CreateOrderStates.waiting_for_banner_pool)
async def banner_pool_text_fallback(message: types.Message, state: FSMContext) -> None:
    await message.answer(
        with_cancel_hint("⚠️ لطفاً با دکمه‌های زیر انتخاب کنید."),
        reply_markup=get_banner_pool_keyboard(),
    )


# ==========================================
# 🧠 ⌨️ SMART FLOW: سوال فعال‌سازی جریان هوشمند (دکمه‌ای)
# ==========================================
async def ask_smart_flow_question(message: types.Message, state: FSMContext) -> None:
    fsm_data = await state.get_data()
    msg_count = len(fsm_data.get("order_messages", []))

    await state.set_state(CreateOrderStates.waiting_for_smart_flow)

    banner_hint = ""
    if msg_count < 2:
        banner_hint = (
            "\n⚠️ <b>توجه:</b> این سفارش هنوز پیام دوم (بنر) ندارد؛ "
            "در صورت فعال‌سازی، هشدار و تأیید مجدد دریافت خواهید کرد."
        )

    await message.answer(
        with_cancel_hint(
            "🧠 <b>جریان هوشمند (Smart Flow)</b>\n\n"
            "جریان هوشمند برای این سفارش فعال شود؟\n\n"
            f"📩 پیام‌های ثبت‌شده: <b>{msg_count}</b>\n"
            "🔁 روند ارسال برای هر تارگت:\n"
            "۱️⃣ ارسال پیام اول (یخ‌شکن)\n"
            "۲️⃣ انتظار برای «سین» تارگت (حداکثر ۲ تا ۷ دقیقه)\n"
            "۳️⃣ تاخیر انسانی ۳۰ تا ۱۸۰ ثانیه\n"
            "۴️⃣ ارسال بنر اصلی (پیام دوم)\n"
            f"{banner_hint}\n\n"
            "⚠️ این حالت سرعت ارسال را به‌شدت کاهش می‌دهد و برای کمپین‌های حجیم مناسب نیست."
        ),
        reply_markup=get_smart_flow_keyboard(),
    )


@router.message(CreateOrderStates.waiting_for_smart_flow, F.text == SMART_FLOW_YES_TEXT)
async def smart_flow_yes_handler(message: types.Message, state: FSMContext, bot: Bot) -> None:
    fsm_data = await state.get_data()
    order_messages = fsm_data.get("order_messages", [])

    # سفارش تک‌پیامی → هشدار + تأیید مجدد (کیبورد تأیید دکمه‌ای)
    if len(order_messages) < 2:
        return await message.answer(
            with_cancel_hint(
                "⚠️ <b>هشدار: بنر (پیام دوم) خالی است!</b>\n\n"
                "این سفارش فقط <b>۱ پیام</b> دارد. با جریان هوشمندِ فعال و بدون بنر، "
                "تارگت‌ها فقط پیام اول را دریافت می‌کنند و مرحله‌ی بنر اجرا نخواهد شد.\n\n"
                "برای ادامه با این شرایط، تأیید مجدد لازم است:"
            ),
            reply_markup=get_smart_flow_confirm_keyboard(),
        )

    await state.update_data(smart_flow=True)
    await proceed_to_filter_selection(message, state, bot)


@router.message(CreateOrderStates.waiting_for_smart_flow, F.text == SMART_FLOW_CONFIRM_TEXT)
async def smart_flow_yes_confirm_handler(message: types.Message, state: FSMContext, bot: Bot) -> None:
    """تأیید مجدد: فعال‌سازی جریان هوشمند با بنرِ خالی (سفارش تک‌پیامی)"""
    await state.update_data(smart_flow=True)
    await proceed_to_filter_selection(message, state, bot)


@router.message(CreateOrderStates.waiting_for_smart_flow, F.text.in_([SMART_FLOW_NO_TEXT, SMART_FLOW_NO_CONFIRM_TEXT]))
async def smart_flow_no_handler(message: types.Message, state: FSMContext, bot: Bot) -> None:
    await state.update_data(smart_flow=False)
    await proceed_to_filter_selection(message, state, bot)


@router.message(CreateOrderStates.waiting_for_smart_flow)
async def smart_flow_text_fallback(message: types.Message, state: FSMContext) -> None:
    await message.answer(
        with_cancel_hint("⚠️ لطفاً با دکمه‌های زیر انتخاب کنید."),
        reply_markup=get_smart_flow_keyboard(),
    )


async def proceed_to_filter_selection(message: types.Message, state: FSMContext, bot: Bot):
    await state.set_state(CreateOrderStates.waiting_for_filter)
    wait_msg = await message.answer("⏳ در حال بررسی...")

    fsm_data = await state.get_data()
    order_type = fsm_data.get("order_type")
    target_data = fsm_data.get("target_data")

    total_users, real_users, fake_users, online_users, phone_users = 0, 0, 0, 0, 0
    stats_unavailable = False
    elapsed_time = 0

    async def _finish_flow_with_error(text: str) -> None:
        await cleanup_fsm_temp_files(state)
        await state.clear()
        with suppress(TelegramBadRequest):
            await wait_msg.delete()
        await message.answer(text, reply_markup=get_main_menu_reply_keyboard())

    if order_type == "extract":
        with suppress(TelegramBadRequest):
            await wait_msg.delete()
        return await message.answer(
            with_cancel_hint(
                "⚙️ <b>سفارش استخراج — استراتژی را انتخاب کنید:</b>\n\n"
                "👥 <b>همه اعضا:</b> لیست اعضای گروه (سقف ۱۰,۰۰۰)\n"
                "💬 <b>فرستندگان پیام:</b> کاربران فعال در تاریخچه اخیر\n"
                "🥇 <b>طلایی:</b> تقاطع اعضا و فعالیت واقعی در پیام‌ها\n"
                "🟢 <b>فقط آنلاین:</b> فقط اعضای آنلاین/اخیراً فعال\n"
                "<i>(UserStatus.ONLINE / RECENTLY)</i>"
            ),
            reply_markup=get_extract_strategy_keyboard(),
        )

    if order_type == "link":
        active_workers = [c for c in worker_pool.values() if c.is_connected]
        if not active_workers:
            return await _finish_flow_with_error(
                "❌ <b>خطا</b>\n\nهیچ اکانت فعالی برای بررسی لینک یافت نشد.\n"
                "<i>لطفاً پس از اتصال اکانت‌ها، دوباره از منوی اصلی شروع کنید.</i>"
            )

        client = random.choice(active_workers)
        start_time = time.time()

        link_target = (target_data or "").strip()
        is_private_invite = bool(re.search(r"t\.me/(?:\+|joinchat/)", link_target, re.IGNORECASE))
        m_link = re.fullmatch(r"(?:https?://)?t\.me/([A-Za-z0-9_]{4,64})/?", link_target, re.IGNORECASE)
        resolve_input = f"@{m_link.group(1)}" if m_link else link_target

        try:
            chat = None
            if not is_private_invite:
                try:
                    chat = await asyncio.wait_for(client.get_chat(resolve_input), timeout=30)
                except FloodWait:
                    raise
                except Exception:
                    chat = None

            if chat is None:
                stats_unavailable = True
            else:
                member_count = getattr(chat, "member_count", None)
                if member_count is None:
                    stats_unavailable = True
                else:
                    total_users = int(member_count) or 0

        except FloodWait as e:
            return await _finish_flow_with_error(
                get_floodwait_message(e.value)
                + "\n<i>پس از پایان این مدت، دوباره از منوی اصلی شروع کنید.</i>"
            )

        except ChatAdminRequired:
            return await _finish_flow_with_error(
                "❌ <b>خطای دسترسی</b>\n\n"
                "ربات در این گروه ادمین نیست یا دسترسی لازم را ندارد.\n"
                "لطفاً اطمینان حاصل کنید ربات عضو گروه است و دسترسی‌های کافی دارد."
            )

        except UserAlreadyParticipant:
            return await _finish_flow_with_error("❌ اکانت قبلاً عضو این گروه است.")

        except InviteHashExpired:
            return await _finish_flow_with_error(
                "❌ لینک دعوت منقضی شده است. لطفاً لینک جدیدی دریافت کنید."
            )

        except RPCError as e:
            logger.error(f"Telegram RPC error in proceed_to_filter_selection: {e}", exc_info=True)
            return await _finish_flow_with_error(get_telegram_api_error_message())

        except Exception as e:
            logger.error(f"Unexpected error in proceed_to_filter_selection: {e}", exc_info=True)
            return await _finish_flow_with_error(get_generic_error_message())

        elapsed_time = int(time.time() - start_time) or 1

    elif order_type == "list":
        try:
            with open(target_data, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            total_users = len(lines)
            real_users = total_users
        except Exception as e:
            logger.error(f"Error reading target list file {target_data}: {e}", exc_info=True)
            return await _finish_flow_with_error(
                "❌ خطا در خواندن فایل لیست اعضا.\n"
                "ممکن است فایل حذف شده باشد. لطفاً دوباره از منوی اصلی شروع کنید."
            )
        
        await state.update_data(
            stats_total=total_users,
            stats_real=real_users,
            stats_fake=fake_users,
            stats_online=online_users,
            stats_phone=phone_users,
        )

        with suppress(TelegramBadRequest):
            await wait_msg.delete()
        
        # پرش مستقیم از فیلتر با ایجاد یک سشن موقت برای فینالایزِ دیتابیس
        async with async_session() as db_session:
            return await _finalize_order(message, state, db_session, filter_type=None)

    await state.update_data(
        stats_total=total_users,
        stats_real=real_users,
        stats_fake=fake_users,
        stats_online=online_users,
        stats_phone=phone_users,
    )

    if order_type == "link":
        total_display = "نامشخص" if stats_unavailable else str(total_users)
        stats_body = (
            f"🔻 همه کاربران: {total_display}\n"
            "🔻 تفکیک (واقعی/فیک/آنلاین/شماره‌دار): —\n"
            "<i>آمار تفکیکی نیازمند عضویت است و join آماری حذف شد؛ "
            "فیلتر انتخابی هنگام استخراج اعضا (رزولور) اعمال می‌شود.</i>\n"
        )
    else:
        stats_body = (
            f"🔻 همه کاربران: {total_users}\n"
            f"🔻 کاربران واقعی: {real_users}\n"
            f"🔻 کاربران فیک: {fake_users}\n"
            f"🔻 کاربران آنلاین: {online_users}\n"
            f"🔻 کاربران شماره‌دار: {phone_users}\n"
        )

    with suppress(TelegramBadRequest):
        await wait_msg.delete()

    await message.answer(
        with_cancel_hint(
            "📊 <b>نوع کاربران را انتخاب کنید:</b>\n\n"
            + stats_body +
            f"⏰ زمان بررسی: {elapsed_time} ثانیه"
        ),
        reply_markup=get_filter_keyboard(),
    )


# ==========================================
# 9. ⌨️ Finalize Order (ثبت در دیتابیس) — ورودی دکمه‌ای + callback سازگار
# ==========================================
async def _finalize_order(message: types.Message, state: FSMContext, session: AsyncSession, filter_type: Optional[str]) -> None:
    fsm_data = await state.get_data()
    cat_ids = fsm_data.get("selected_categories", [])
    order_type = fsm_data.get("order_type")
    target_data = fsm_data.get("target_data")
    target_count = fsm_data.get("target_count", 0)
    
    list_source_path: Optional[str] = None
    if order_type == "list":
        list_source_path = target_data
        try:
            with open(target_data, "r", encoding="utf-8") as f:
                file_targets = [line.strip() for line in f if line.strip()]
        except Exception as e:
            logger.error(f"Error reading target list file {target_data}: {e}", exc_info=True)
            await cleanup_fsm_temp_files(state)
            await state.clear()
            return await message.answer(
                "❌ خطا در خواندن فایل لیست اعضا.\n"
                "ممکن است فایل حذف شده باشد. لطفاً دوباره از منوی اصلی شروع کنید.",
                reply_markup=get_main_menu_reply_keyboard(),
            )
        if not file_targets:
            await cleanup_fsm_temp_files(state)
            await state.clear()
            return await message.answer(
                "⚠️ فایل لیست اعضا خالی است یا هیچ شناسه‌ی معتبری ندارد.\n"
                "لطفاً فایلی حاوی شناسه‌ها (هر خط یک شناسه) ارسال کنید و دوباره شروع کنید.",
                reply_markup=get_main_menu_reply_keyboard(),
            )
        target_data = "\n".join(file_targets)
        target_count = len(file_targets)
        filter_type = None
        
    messages = fsm_data.get("order_messages", [])
    smart_flow = fsm_data.get("smart_flow", False)
    use_banner_pool = fsm_data.get("use_banner_pool", False)

    source_channel_id = fsm_data.get("source_channel_id")
    source_message_ids_list = fsm_data.get("source_message_ids") or []
    source_message_ids_str = ",".join(str(mid) for mid in source_message_ids_list) or None

    if not messages and not source_message_ids_list and not use_banner_pool:
        await cleanup_fsm_temp_files(state)
        await state.clear()
        return await message.answer(
            "⚠️ هیچ محتوایی برای این سفارش ثبت نشده است "
            "(نه پیام مستقیم، نه پیام کانال مبدا).\n"
            "لطفاً از منوی اصلی دوباره «🛍 ثبت سفارش 🛍» را انتخاب کنید.",
            reply_markup=get_main_menu_reply_keyboard(),
        )

    # سفارش جدید به صورت پیش‌فرض is_approved=False است
    new_order = Order(
        order_type=order_type,
        target_data=target_data,
        target_count=target_count,
        filter_type=filter_type,
        status=OrderStatus.pending,
        smart_flow=smart_flow,
        use_banner_pool=use_banner_pool,
        source_channel_id=source_channel_id,
        source_message_ids=source_message_ids_str,
        is_approved=False
    )

    if len(messages) > 0:
        new_order.message_text = messages[0].get("text")
        new_order.media_path = messages[0].get("media_path")
        new_order.media_type = messages[0].get("media_type")
    if len(messages) > 1:
        new_order.message_2_text = messages[1].get("text")
        new_order.media_2_path = messages[1].get("media_path")
        new_order.media_2_type = messages[1].get("media_type")
    if len(messages) > 2:
        new_order.message_3_text = messages[2].get("text")
        new_order.media_3_path = messages[2].get("media_path")
        new_order.media_3_type = messages[2].get("media_type")

    try:
        chars = string.ascii_uppercase + string.digits
        for _ in range(5):
            candidate_code = f"ORD-{''.join(random.choices(chars, k=6))}"
            if not await session.scalar(
                select(Order.id).where(Order.tracking_code == candidate_code)
            ):
                break
        else:
            candidate_code = f"ORD-{uuid.uuid4().hex[:6].upper()}"
        new_order.tracking_code = candidate_code

        stmt = select(Category).where(Category.id.in_(cat_ids))
        selected_cats = (await session.scalars(stmt)).all()
        new_order.categories.extend(selected_cats)

        session.add(new_order)
        await session.commit()
        await session.refresh(new_order)
    except Exception as e:
        await session.rollback()
        await cleanup_fsm_temp_files(state)
        await state.clear()
        return await message.answer(
            report_db_error("سفارش", e),
            reply_markup=get_main_menu_reply_keyboard(),
        )

    if list_source_path and os.path.exists(list_source_path):
        with suppress(OSError):
            os.remove(list_source_path)

    tracking_cmd = f"/gtg_{new_order.id}"
    cat_names = ", ".join([cat.name for cat in selected_cats]) if selected_cats else "بدون دسته"
    order_type_display = "🔗 لینک گروه" if order_type == "link" else ("📄 فایل اعضا" if order_type == "list" else "⚙️ استخراج")
    
    target_display = target_data if order_type == "link" else "📁 فایل متنی (TXT)"
    if order_type == "link" and target_display and not target_display.startswith("http"):
        target_display = f"🔗 https://t.me/{target_display.replace('@', '')}"
        
    banner_pool_note = "\n🎨 استفاده از مخزن بنر: <b>فعال ✅</b>" if use_banner_pool else ""
    copy_source_note = ""
    if source_message_ids_str:
        copy_source_note = (
            f"\n📋 روش ارسال: <b>کپی از کانال مبدا</b> "
            f"<code>{source_channel_id}</code> ({len(source_message_ids_list)} پیام)"
        )

    await state.clear()
    
    # 📩 ۱. پیام تایید برای کاربری که در حال ثبت است
    await message.answer(
        f"✅ <b>سفارش با موفقیت ثبت شد و در انتظار تایید است.</b>\n"
        f"🎟 کد رهگیری: <code>{new_order.tracking_code}</code>\n"
        f"{banner_pool_note}"
        f"{copy_source_note}\n\n"
        f"♻️ مشاهده داشبورد زنده: {tracking_cmd}",
        reply_markup=get_main_menu_reply_keyboard(),
    )

    # 📩 ۲. ارسال پیام درخواست تایید برای ادمین (Admin Notification)
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ تایید و شروع", callback_data=f"approve_order_{new_order.id}/")
    builder.button(text="❌ رد سفارش", callback_data=f"reject_order_{new_order.id}/")
    builder.adjust(2)

    admin_summary = (
        f"🛎 <b>سفارش جدید نیازمند تایید دیسپچ</b>\n\n"
        f"🆔 شناسه: <code>{new_order.id}</code>\n"
        f"🎟 کد رهگیری: <code>{new_order.tracking_code}</code>\n"
        f"📦 نوع ارسال: {order_type_display}\n"
        f"🎯 هدف: {target_display}\n"
        f"👤 تعداد تارگت: <code>{target_count}</code>\n"
        f"🗂 دسته‌ها: {cat_names}\n"
        f"📊 داشبورد: {tracking_cmd}\n\n"
        f"<i>لطفاً جهت ورود این سفارش به صف اجرا (Task Queue) آن را تایید کنید.</i>"
    )
    
    try:
        await message.bot.send_message(
            chat_id=config.ADMIN_ID,
            text=admin_summary,
            reply_markup=builder.as_markup(),
            disable_web_page_preview=True
        )
    except Exception as e:
        logger.error(f"Failed to send approval request to admin for order {new_order.id}: {e}")


@router.message(CreateOrderStates.waiting_for_filter, F.text.in_(FILTER_BY_TEXT))
async def finalize_order_creation_text(message: types.Message, state: FSMContext, session: AsyncSession) -> None:
    """⌨️ انتخاب فیلتر/استراتژی با دکمه‌ی دکمه‌ای"""
    filter_type = FILTER_BY_TEXT[message.text.strip()]
    await _finalize_order(message, state, session, filter_type)


@router.message(CreateOrderStates.waiting_for_filter)
async def filter_text_fallback(message: types.Message, state: FSMContext) -> None:
    await message.answer(
        with_cancel_hint("⚠️ لطفاً یکی از گزینه‌ها را با دکمه‌های زیر انتخاب کنید."),
        reply_markup=get_filter_keyboard(),
    )


@router.callback_query(CreateOrderStates.waiting_for_filter, F.data.startswith("filter_"))
async def finalize_order_creation(callback: types.CallbackQuery, state: FSMContext, session: AsyncSession) -> None:
    """🔄 سازگاری: callback شیشه‌ای filter_ حفظ شد (ممکن است extractor_handlers هم بفرستد)"""
    await callback.answer()
    filter_type = callback.data.replace("filter_", "").replace("/", "")
    await _finalize_order(callback.message, state, session, filter_type)


# ==========================================
# 10. Live Dashboard (/gtg_id) — شیشه‌ای، بدون تغییر
# ==========================================
async def generate_dashboard_data(
    order_id: int,
    session: AsyncSession,
    back_callback: Optional[str] = None,
):
    from sqlalchemy.orm import selectinload
    from sqlalchemy import case, func, select
    import html

    # ۱. بارگذاری سفارش به همراه دسته‌بندی‌ها (حل مشکل N+1 برای رابطه‌ی Many-to-Many)
    stmt = select(Order).options(selectinload(Order.categories)).where(Order.id == order_id)
    order = await session.scalar(stmt)
    if not order:
        return None, None

    # ۲. دریافت تجمیعی آمار و زمان‌ها تنها با یک کوئری (Performance Boost)
    logs_stmt = select(
        OrderLog.status,
        func.count(OrderLog.id),
        func.min(OrderLog.created_at),
        func.max(OrderLog.created_at)
    ).where(OrderLog.order_id == order_id).group_by(OrderLog.status)
    logs_result = await session.execute(logs_stmt)

    status_counts = {}
    min_date = None
    max_date = None

    for row in logs_result:
        status, count, r_min, r_max = row[0], row[1], row[2], row[3]
        status_counts[status] = count
        if min_date is None or (r_min and r_min < min_date):
            min_date = r_min
        if max_date is None or (r_max and r_max > max_date):
            max_date = r_max

    sent_count = status_counts.get("success", 0)
    error_count = status_counts.get("error", 0) + status_counts.get("spam", 0) # سازگاری با وضعیت قدیمی
    flood_count = status_counts.get("flood", 0)
    restricted_count = status_counts.get("restricted", 0)

    checked_count = sum(status_counts.values())

    # ۳. محاسبات سرعت و زمان باقیمانده (ETA) - گارد تقسیم بر صفر رعایت شده
    speed_per_minute = 0
    duration_minutes = 0
    if min_date and max_date:
        duration_seconds = (max_date - min_date).total_seconds()
        duration_minutes = duration_seconds / 60.0
        if duration_minutes > 0:
            speed_per_minute = checked_count / duration_minutes

    progress_percent = 0
    if order.target_count and order.target_count > 0:
        progress_percent = round((sent_count / order.target_count) * 100, 1)

    # ۴. عملکرد اکانت‌ها (تجمیع شده، بدون N+1)
    acc_stmt = (
        select(
            Account.phone_number,
            Category.name,
            func.sum(case((OrderLog.status == 'success', 1), else_=0)),
            func.sum(case((OrderLog.status != 'success', 1), else_=0))
        )
        .select_from(OrderLog)
        .join(Account, OrderLog.account_id == Account.id)
        .outerjoin(Category, Account.category_id == Category.id)
        .where(OrderLog.order_id == order_id)
        .group_by(Account.id, Account.phone_number, Category.name)
        .order_by(func.count(OrderLog.id).desc())
    )
    acc_result = await session.execute(acc_stmt)
    accounts_data = acc_result.all()
    total_accounts = len(accounts_data)

    accounts_lines = []
    for row in accounts_data[:10]:
        phone, cat, succ, err = row[0], row[1], row[2] or 0, row[3] or 0
        cat_disp = cat if cat else "بدون دسته"
        
        phone_str = str(phone)
        masked = f"{phone_str[:4]}***{phone_str[-2:]}" if len(phone_str) > 6 else "***"
        accounts_lines.append(f"▫️ <code>{masked}</code> ({cat_disp}): ✅ {succ} | ❌ {err}")

    if total_accounts > 10:
        accounts_lines.append(f"➕ و {total_accounts - 10} اکانت دیگر...")

    # ۵. آخرین ۳ خطای سفارش
    err_stmt = (
        select(OrderLog.target, OrderLog.error_message)
        .where(OrderLog.order_id == order_id, OrderLog.status.in_(['error', 'flood', 'restricted']))
        .order_by(OrderLog.id.desc())
        .limit(3)
    )
    err_result = await session.execute(err_stmt)
    errors_data = err_result.all()

    error_lines = []
    for tgt, msg in errors_data:
        msg_str = str(msg or "نامشخص")
        if len(msg_str) > 80:
            msg_str = msg_str[:80] + "..."
        safe_msg = html.escape(msg_str)
        safe_tgt = html.escape(str(tgt)[:20])
        error_lines.append(f"⚠️ <code>{safe_tgt}</code>: {safe_msg}")

    # ================= تولید متن نهایی بخش‌ها =================
    
    # --- بخش ۱ ---
    order_type_labels = {
        "link": "🔗 لینک گروه",
        "list": "📄 فایل اعضا",
        "extract": "🧲 استخراج/آنالیز"
    }
    type_label = order_type_labels.get(order.order_type, f"❔ {order.order_type}")
    tracking_display = order.tracking_code if order.tracking_code else f"ID-{order.id}"

    cats = [c.name for c in order.categories]
    cat_names = "، ".join(cats) if cats else "بدون دسته"

    target_display = order.target_data or ""
    if order.order_type == "link":
        if not target_display.startswith("http") and not target_display.startswith("@"):
            target_display = f"🔗 https://t.me/{target_display.replace('@', '')}"
        else:
            target_display = f"🔗 {target_display}"
    elif order.order_type == "list":
        target_display = "📁 فایل متنی (TXT)"
    else: # استخراج و مدیریت سقف رشته
        if len(target_display) > 40:
            target_display = target_display[:40] + "..."
        target_display = f"🧲 {target_display}"

    if order.status == OrderStatus.completed:
        status_text = "✅ تکمیل شده"
    elif order.status == OrderStatus.running:
        status_text = "🚀 در حال اجرا"
    elif order.status == OrderStatus.error:
        if not order.is_approved and order.reject_reason:
            status_text = f"❌ رد شده\n💬 علت: <i>{html.escape(order.reject_reason)}</i>"
        else:
            status_text = "🛑 متوقف / لغو شده"
    else:
        if order.is_approved:
            status_text = "🕒 در صف انتظار دیسپچ"
        else:
            status_text = "⏳ در انتظار تایید ادمین"

    send_method_line = ""
    if order.source_message_ids:
        src_count = len([p for p in order.source_message_ids.split(",") if p.strip().isdigit()])
        send_method_line = f"📋 روش ارسال: کپی از مبدا <code>{order.source_channel_id}</code> ({src_count} پیام)\n"

    sec1 = [
        "🔰 <b>بخش ۱ — شناسایی سفارش</b>",
        f"🆔 شناسه: <code>{order.id}</code> | 🎟 کد رهگیری: <code>{tracking_display}</code>",
        f"📦 نوع سفارش: {type_label}",
        f"🎯 تارگت: {target_display}",
        f"🗂 دسته‌ها: {cat_names}"
    ]
    if order.scheduled_for:
        sec1.append(f"📅 زمان‌بندی: {order.scheduled_for.strftime('%Y/%m/%d %H:%M')}")
    if order.retry_count > 0:
        sec1.append(f"🔄 تلاش مجدد: {order.retry_count}")
    sec1.append(f"📍 وضعیت: <b>{status_text}</b>")
    if send_method_line:
        sec1.append(send_method_line.strip())

    # --- بخش ۲ ---
    sec2 = [
        "📈 <b>بخش ۲ — پیشرفت و سرعت</b>",
        f"👤 ارسال شده/درخواستی: {sent_count} / {order.target_count or 0} ({progress_percent}%)",
        f"🔍 بررسی شده کل: {checked_count}",
        f"🚀 سرعت ارسال: {int(speed_per_minute)} در دقیقه"
    ]
    if order.status == OrderStatus.running and speed_per_minute > 0 and order.target_count:
        remaining_targets = max(0, order.target_count - sent_count)
        eta_m = remaining_targets / speed_per_minute
        sec2.append(f"⏳ زمان باقیمانده (ETA): ~{int(eta_m)} دقیقه")

    st_str = min_date.strftime("%H:%M:%S") if min_date else "نامشخص"
    la_str = max_date.strftime("%Y/%m/%d %H:%M:%S") if max_date else "نامشخص"
    sec2.append(f"⌚️ شروع: {st_str} | ⏳ مدت اجرا: {int(duration_minutes)} دقیقه")
    sec2.append(f"🔄 آخرین بروزرسانی: {la_str}")

    # --- بخش ۳ ---
    sec3_base = [
        "📊 <b>بخش ۳ — تفکیک وضعیت‌ها و اکانت‌ها</b>",
        f"✅ موفق: {sent_count} | ❌ خطا: {error_count}"
    ]
    if flood_count > 0:
        sec3_base.append(f"🌊 فلاد (FloodWait): {flood_count}")
    if restricted_count > 0:
        sec3_base.append(f"⛔️ محدود (Restricted): {restricted_count}")

    sec3_base.append("")
    sec3_base.append(f"📱 عملکرد اکانت‌ها (کل: {total_accounts}):")

    core_text = "\n".join(sec1) + "\n\n" + "\n".join(sec2) + "\n\n" + "\n".join(sec3_base)
    acc_text = "\n".join(accounts_lines) if accounts_lines else "موردی یافت نشد."
    err_text = ("\n\n🚨 آخرین خطاها:\n" + "\n".join(error_lines)) if error_lines else ""

    dashboard_text = core_text + "\n" + acc_text + err_text

    # گارد هوشمند طول کل پیام برای جلوگیری از BadRequest (سقف ~۴۰۹۶ کاراکتر)
    if len(dashboard_text) > 4000:
        if err_text:
            dashboard_text = core_text + "\n" + acc_text + "\n\n🚨 <i>خطاها به دلیل محدودیت طول پیام پنهان شدند.</i>"
        if len(dashboard_text) > 4000:
            dashboard_text = core_text + "\n" + "<i>لیست اکانت‌ها به دلیل محدودیت طول پیام پنهان شد.</i>"

    # === ساخت کیبورد ===
    from aiogram.utils.keyboard import InlineKeyboardBuilder
    builder = InlineKeyboardBuilder()

    if order.status == OrderStatus.pending and not order.is_approved:
        builder.button(text="✅ تایید و شروع", callback_data=f"approve_order_{order.id}/")
        builder.button(text="❌ رد سفارش", callback_data=f"reject_order_{order.id}/")

    builder.button(text="🔄 بروزرسانی", callback_data=f"update_order_{order.id}/")
    
    # دکمه خروجی برای سفارش استخراج فقط اگر تکمیل شده باشد اضافه می‌شود
    has_export_btn = False
    if order.order_type != "extract" or order.status == OrderStatus.completed:
        builder.button(text="📥 خروجی", callback_data=f"export_order_{order.id}/")
        has_export_btn = True
        
    if back_callback:
        builder.button(text="🔙 بازگشت به لیست سفارشات", callback_data=back_callback)
    builder.button(text="🏛 منوی اصلی", callback_data="menu_home/")

    # چیدمان داینامیک بر اساس وجود یا عدم وجود دکمه خروجی
    if order.status == OrderStatus.pending and not order.is_approved:
        if has_export_btn:
            builder.adjust(2, 2, 1, 1) if back_callback else builder.adjust(2, 2, 1)
        else:
            builder.adjust(2, 1, 1, 1) if back_callback else builder.adjust(2, 1, 1)
    else:
        if has_export_btn:
            builder.adjust(2, 1, 1) if back_callback else builder.adjust(2, 1)
        else:
            builder.adjust(1, 1, 1) if back_callback else builder.adjust(1, 1)

    return dashboard_text, builder.as_markup()

@router.message(F.text.regexp(r"^/gtg_(\d+)$"))
async def show_order_dashboard(message: types.Message, session: AsyncSession) -> None:
    order_id = int(message.text.split("_")[1])

    try:
        text, markup = await generate_dashboard_data(order_id, session)
    except Exception as e:
        await session.rollback()
        return await message.answer(
            report_db_error("سفارش", e),
            reply_markup=get_main_menu_keyboard(),
        )

    if not text:
        return await message.answer(
            "⚠️ سفارش مورد نظر یافت نشد.",
            reply_markup=get_main_menu_keyboard(),
        )

    # --- FIX M13 ---
    await message.answer(text, reply_markup=markup, link_preview_options=types.LinkPreviewOptions(is_disabled=True))

# ========== اضافه کردن هندلر جدید پس از show_order_dashboard ==========

@router.message(F.text.regexp(r"^(?:/gtg_)?((?:ORD|EXT)-[A-Za-z0-9]{1,20})$", flags=re.IGNORECASE))
async def track_by_tracking_code(message: types.Message, session: AsyncSession) -> None:
    match = re.match(r"^(?:/gtg_)?((?:ORD|EXT)-[A-Za-z0-9]{1,20})$", message.text.strip(), re.IGNORECASE)
    if not match:
        return
    
    code = match.group(1).upper()
    
    try:
        order = await session.scalar(select(Order).where(Order.tracking_code == code))
        if not order:
            return await message.answer(f"❌ سفارشی با کد رهگیری <code>{html.escape(code)}</code> یافت نشد.")
        
        if code.startswith("ORD-"):
            text, markup = await generate_dashboard_data(order.id, session)
            if not text:
                return await message.answer(f"❌ سفارشی با کد رهگیری <code>{html.escape(code)}</code> یافت نشد.")
            
            await message.answer(text, reply_markup=markup, disable_web_page_preview=True)
            
        elif code.startswith("EXT-"):
            from bot.handlers.extractor_handlers import build_extraction_dashboard_view
            
            text, markup = await build_extraction_dashboard_view(session, code)
            if not text:
                return await message.answer(f"❌ سفارشی با کد رهگیری <code>{html.escape(code)}</code> یافت نشد.")
            
            await message.answer(text, reply_markup=markup, disable_web_page_preview=True)
            
    except Exception as e:
        await session.rollback()
        await message.answer(
            report_db_error("رهگیری سفارش", e),
            reply_markup=get_main_menu_keyboard()
        )


@router.callback_query(F.data.startswith("update_order_"))
async def refresh_order_dashboard(callback: types.CallbackQuery, state: FSMContext, session: AsyncSession) -> None:
    # 🛡 فاز ۳: گارد parse (قبلاً int() بدون بررسی)
    order_id_str = callback.data.replace("update_order_", "").replace("/", "")
    if not order_id_str.isdigit():
        return await callback.answer("⚠️ شناسه نامعتبر است.", show_alert=True)

    order_id = int(order_id_str)

    # 🛡 فاز ۳: خطای DB جدا از «سفارش یافت نشد» هندل می‌شود
    try:
        # 📄 فاز ۳: اگر داشبورد از داخل لیست سفارشات باز شده باشد، دکمهٔ
        # «بازگشت به لیست» بعد از بروزرسانی هم حفظ می‌شود
        fsm_data = await state.get_data()
        back_callback = "orders_back_to_list/" if "orders_list_filter" in fsm_data else None

        text, markup = await generate_dashboard_data(order_id, session, back_callback=back_callback)
    except Exception as e:
        await session.rollback()
        return await answer_callback_error(
            callback, report_db_error("سفارش", e), get_main_menu_button()
        )

    if not text:
        return await callback.answer("⚠️ سفارش یافت نشد!", show_alert=True)

    edited = await safe_edit_message(
        callback.message, text,
        reply_markup=markup,
        # --- FIX M13 ---
        link_preview_options=types.LinkPreviewOptions(is_disabled=True)
    )
    if edited:
        await safe_callback_answer(callback, "✅ آمار با موفقیت بروزرسانی شد.")
    else:
        await safe_callback_answer(callback, "🔄 آمار تغییری نکرده است.")


@router.callback_query(F.data == "status_btn_ignore/")
async def ignore_status_button(callback: types.CallbackQuery) -> None:
    await callback.answer("وضعیت سفارش اکنون در متن پیام نمایش داده می‌شود.", show_alert=False)


# ==========================================
# هندلر دکمه Export برای خروجی گرفتن
# ==========================================
@router.callback_query(F.data.startswith("export_order_"))
async def export_order_results(callback: types.CallbackQuery, session: AsyncSession) -> None:
    order_id_str = callback.data.replace("export_order_", "").replace("/", "")
    if not order_id_str.isdigit():
        return await callback.answer("⚠️ شناسه نامعتبر است.", show_alert=True)

    order_id = int(order_id_str)

    wait_msg = await send_loading_message(callback.message, "⏳ در حال تولید فایل گزارش...")

    file_path = None
    try:
        # --- بخش اول: ارزیابی سفارش و خروجی استخراج ---
        stmt = select(Order).where(Order.id == order_id)
        order = await session.scalar(stmt)
        
        if order and order.order_type == "extract":
            # گارد منطقی: اگر سفارش استخراج هنوز تکمیل نشده است
            if order.status != OrderStatus.completed:
                return await safe_edit_message(
                    wait_msg,
                    "⚠️ <b>فایل استخراج هنوز آماده نیست.</b>\n"
                    "لطفاً تا تکمیل شدن سفارش منتظر بمانید.",
                    reply_markup=get_main_menu_button()
                )
            
            # ارسال فایل استخراج شده
            if order.media_path and os.path.exists(order.media_path):
                document = FSInputFile(order.media_path)
                try:
                    await callback.message.answer_document(
                        document=document,
                        caption=f"📥 <b>گزارش خروجی سفارش #{order_id}</b>\n(فایل استخراج شده)",
                    )
                except Exception as e:
                    logger.error(f"Error sending stored extract file for order {order_id}: {e}")
                    return await safe_edit_message(
                        wait_msg,
                        "❌ خطا در ارسال فایل گزارش.\nلطفاً دوباره تلاش کنید.",
                        reply_markup=get_main_menu_button()
                    )
                with suppress(Exception):
                    await wait_msg.delete()
                return
            else:
                return await safe_edit_message(
                    wait_msg,
                    "❌ <b>خطا:</b> فایل خروجی استخراج روی سرور یافت نشد یا حذف شده است.",
                    reply_markup=get_main_menu_button()
                )
        # -------------------------------------------------------------------------
        
        # --- بخش دوم: تولید گزارش لاگ‌ها برای سفارشات ارسال انبوه ---
        try:
            stmt = select(OrderLog.status, OrderLog.target).where(OrderLog.order_id == order_id)
            result = await session.execute(stmt)
            logs = result.all() 
        except Exception as e:
            await session.rollback()
            return await safe_edit_message(
                wait_msg,
                report_db_error("گزارش سفارش", e),
                reply_markup=get_main_menu_button()
            )

        if not logs:
            return await safe_edit_message(
                wait_msg,
                "⚠️ <b>هیچ گزارش ارسالی برای این سفارش ثبت نشده است.</b>\n"
                "ممکن است سفارش هنوز شروع نشده یا در صف انتظار باشد.",
                reply_markup=get_main_menu_button()
            )

        os.makedirs("exports", exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        unique_id = uuid.uuid4().hex[:6]
        file_path = f"exports/order_{order_id}_results_{timestamp}_{unique_id}.txt"

        successful_targets = 0

        try:
            async with aiofiles.open(file_path, 'w', encoding='utf-8') as f:
                for status, target in logs:
                    if status == "success" and target:
                        await f.write(f"{target}\n")
                        successful_targets += 1
        except Exception as e:
            logger.error(f"Error writing export file for order {order_id}: {e}", exc_info=True)
            return await safe_edit_message(
                wait_msg,
                "❌ خطا در تولید فایل گزارش.\n"
                "لطفاً دوباره تلاش کنید.",
                reply_markup=get_main_menu_button()
            )

        if successful_targets == 0:
            return await safe_edit_message(
                wait_msg,
                "⚠️ <b>هیچ ارسال موفقی برای این سفارش ثبت نشده است.</b>",
                reply_markup=get_main_menu_button()
            )

        document = FSInputFile(file_path)
        try:
            await callback.message.answer_document(
                document=document,
                caption=f"📥 <b>گزارش خروجی سفارش #{order_id}</b>\n"
                        f"تعداد ارسال‌های موفق: <code>{successful_targets}</code>",
            )
        except Exception as e:
            logger.error(f"Error sending export file for order {order_id}: {e}", exc_info=True)
            return await safe_edit_message(
                wait_msg,
                "❌ خطا در ارسال فایل گزارش.\n"
                "لطفاً دوباره تلاش کنید.",
                reply_markup=get_main_menu_button()
            )

        with suppress(Exception):
            await wait_msg.delete()

    finally:
        if file_path and os.path.exists(file_path):
            try:
                os.remove(file_path)
            except OSError as e:
                logger.warning(f"Failed to remove export file {file_path}: {e}")

# ==========================================
# 🟢 هندلرهای تایید و رد سفارش (Idempotent)
# ==========================================
@router.callback_query(F.data.startswith("approve_order_") & F.data.endswith("/"))
async def action_approve_order(callback: types.CallbackQuery, session: AsyncSession) -> None:
    order_id = int(callback.data.replace("approve_order_", "").replace("/", ""))
    
    try:
        stmt = select(Order).where(Order.id == order_id)
        order = await session.scalar(stmt)
        
        if not order:
            return await callback.answer("⚠️ سفارش مورد نظر یافت نشد.", show_alert=True)
            
        # بررسی Idempotent: اطمینان از وضعیت در دیتابیس
        if order.is_approved or order.status != OrderStatus.pending:
            await callback.answer("ℹ️ این سفارش قبلاً تعیین تکلیف شده است.", show_alert=True)
            # بروزرسانی داشبورد برای حذف دکمه‌های تایید
            text, markup = await generate_dashboard_data(order_id, session)
            return await safe_edit_or_answer(callback.message, text, reply_markup=markup, disable_web_page_preview=True)
            
        order.is_approved = True
        await session.commit()
        
        await callback.answer("✅ سفارش با موفقیت تایید و به صف دیسپچ اضافه شد.", show_alert=True)
        
        text, markup = await generate_dashboard_data(order_id, session)
        await safe_edit_or_answer(
            callback.message, 
            text, 
            reply_markup=markup, 
            disable_web_page_preview=True
        )
        
    except Exception as e:
        await session.rollback()
        await answer_callback_error(callback, report_db_error("تایید سفارش", e), get_main_menu_button())


@router.callback_query(F.data.startswith("reject_order_") & F.data.endswith("/"))
async def action_reject_order(callback: types.CallbackQuery, session: AsyncSession) -> None:
    order_id = int(callback.data.replace("reject_order_", "").replace("/", ""))
    
    try:
        stmt = select(Order).where(Order.id == order_id)
        order = await session.scalar(stmt)
        
        if not order:
            return await callback.answer("⚠️ سفارش مورد نظر یافت نشد.", show_alert=True)
            
        # بررسی Idempotent
        if order.is_approved or order.status != OrderStatus.pending:
            await callback.answer("ℹ️ این سفارش قبلاً تعیین تکلیف شده است.", show_alert=True)
            text, markup = await generate_dashboard_data(order_id, session)
            return await safe_edit_or_answer(callback.message, text, reply_markup=markup, disable_web_page_preview=True)
            
        # تنظیم وضعیت نهایی به Error و درج دلیل رد
        order.is_approved = False
        order.status = OrderStatus.error
        order.reject_reason = "توسط ادمین سیستم رد شد."
        order.target_data = ""  # پاکسازی تارگت‌ها
        
        # پاکسازی فایل‌های مدیا
        for path in [order.media_path, order.media_2_path, order.media_3_path]:
            if path and os.path.exists(path):
                with suppress(Exception):
                    os.remove(path)
                    
        order.media_path = None
        order.media_2_path = None
        order.media_3_path = None
        
        await session.commit()
        
        await callback.answer("❌ سفارش رد و لغو شد.", show_alert=True)
        
        text, markup = await generate_dashboard_data(order_id, session)
        await safe_edit_or_answer(
            callback.message, 
            text, 
            reply_markup=markup, 
            disable_web_page_preview=True
        )
        
    except Exception as e:
        await session.rollback()
        await answer_callback_error(callback, report_db_error("رد سفارش", e), get_main_menu_button())

