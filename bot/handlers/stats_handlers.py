import asyncio
import html
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from aiogram import Router, types, F
from aiogram.fsm.context import FSMContext
from aiogram.utils.keyboard import InlineKeyboardBuilder
from bot.handlers.login_handlers import release_login_reservations
from sqlalchemy import select, update, delete, func, or_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from utils.safe_edit import safe_edit_or_answer
from workers.session_manager import worker_pool, remove_account_from_system, release_proxy_slot
from pyrogram.raw.functions.account import GetAuthorizations
from typing import Optional, List, Dict
from database.models import Account, Category, Order, OrderLog, OrderStatus, APIKey
from bot.states.confirm_fsm import ConfirmStates
from workers.session_manager import worker_pool
from utils.advanced_anti_ban import terminate_other_sessions
from utils.crypto import decrypt_session, build_session_file, mask_phone
from datetime import datetime, timedelta, timezone
from sqlalchemy import case, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from database.models import OrderLog

# 🟣 فاز ۷ (رفع بن‌بست FSM): helper های قبلی
from bot.keyboards.main_menu import get_main_menu_keyboard, get_main_menu_button
from utils.fsm_cleanup import cleanup_fsm_temp_files
from utils.safe_edit import safe_edit_or_answer

# 🛡 فاز ۴ (مدیریت خطا): helper های فاز ۱ و ۲
from utils.error_messages import (
    get_telegram_api_error_message,
    report_db_error,
)
from utils.telegram_helpers import (
    answer_callback_error,
    safe_callback_answer,
    safe_edit_message,
    send_loading_message,
)

# 📄 فاز ۴ (صفحه‌بندی استاندارد): زیرساخت مشترک paginate (فاز ۱)
from utils.pagination import (
    PAGINATION_SIZE,
    add_pagination_nav_row,
    calculate_total_pages,
    clamp_page,
    get_page_offset,
)

# 🔍 فاز ۴ (جستجو): stateها و helperهای استاندارد جستجو (فاز ۱)
from utils.search_helpers import (
    SearchStates,
    build_like_pattern,
    normalize_phone_query,
)

# 🟣 فاز ۴: راهنمای استاندارد انصراف (برای پیام‌های FSM فلوی جستجو)
from bot.keyboards.cancel import with_cancel_hint

logger = logging.getLogger(__name__)
router = Router(name="stats_handlers_router")


def _as_utc(dt: Optional[datetime]) -> Optional[datetime]:
    """نرمال‌سازی مقدار datetime برگشتی از MySQL (naive UTC) به aware UTC."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def get_back_keyboard():
    """کیبورد بازگشت به منوی اصلی"""
    builder = InlineKeyboardBuilder()
    builder.button(text="🏛 منوی اصلی", callback_data="menu_home/")
    return builder.as_markup()


# ==========================================
# 🟣 فاز ۷: کیبورد بازگشت بعد از تغییر دسته‌بندی اکانت
# ==========================================
def get_accounts_return_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="🔙 بازگشت به لیست اکانت‌ها", callback_data="menu_list_accounts/")
    builder.button(text="🏛 منوی اصلی", callback_data="menu_home/")
    builder.adjust(2)
    return builder.as_markup()


# ==========================================
# 📄 فاز ۴: شرط SQL فیلترهای لیست اکانت‌ها
# ==========================================
def _build_accounts_filter_conditions(filter_type: str, now_naive: datetime):
    """
    📄 فاز ۴: شرایط SQL هر فیلتر به‌صورت «لیست» (chain با AND).
    """
    if filter_type == "notreg":
        return [Account.session_string.is_(None)]
    if filter_type == "limited":
        return [
            Account.is_banned == False, 
            Account.session_string.is_not(None), 
            or_(Account.flood_wait_until > now_naive, Account.restricted_until > now_naive)
        ]
    if filter_type == "active":
        return [Account.is_banned == False, Account.session_string.is_not(None)]
    if filter_type == "ability":
        return [
            Account.is_banned == False,
            Account.session_string.is_not(None),
            or_(Account.flood_wait_until.is_(None), Account.flood_wait_until <= now_naive),
            or_(Account.restricted_until.is_(None), Account.restricted_until <= now_naive)
        ]
    return None  # all


# ==========================================
# REPORTS: FILTERED ACCOUNTS LIST (با صفحه‌بندی)
# ==========================================
@router.callback_query(F.data.startswith("list_acc_filter_"))
@router.callback_query(F.data.startswith("list_acc_filter_"))
async def render_filtered_account_list(
    callback: types.CallbackQuery,
    session: AsyncSession,
    state: FSMContext = None,
    filter_type: str = None,
    page: int = None,
    skip_answer: bool = False,
) -> None:
    if not skip_answer:
        await callback.answer()

    if filter_type is None or page is None:
        match = re.match(r"list_acc_filter_([a-z]+)_page_(\d+)/", callback.data)
        if not match:
            return await callback.message.answer("⚠️ داده‌های کال‌بک نامعتبر است.")
        filter_type = match.group(1)
        page = int(match.group(2))

    now = datetime.now(timezone.utc)
    
    filter_titles = {
        "all": "💢 تمام اکانت‌ها",
        "notreg": "⛔️ ثبت‌نام نشده",
        "limited": "🚫 مسدود/محدود",
        "disconnected": "⚠️ قطع اتصال",
        "ready": "✅ آماده ارسال",
        "cooldown": "💤 در حال استراحت"
    }
    display_title = filter_titles.get(filter_type, "لیست اکانت‌ها")

    from workers.sender import _get_redis
    from utils.account_display import get_all_accounts_stats, get_account_display_status
    from workers.session_manager import worker_pool
    
    redis_client = _get_redis()
    stats, account_categories = await get_all_accounts_stats(session, redis_client, worker_pool)

    filter_map = {
        "notreg": "NOT_REG",
        "limited": "BLOCKED",
        "cooldown": "COOLDOWN",
        "disconnected": "DISCONNECTED",
        "ready": "READY",
        "all": "ALL"
    }
    
    target_cat = filter_map.get(filter_type, "ALL")
    if target_cat != "ALL":
        valid_ids = [aid for aid, cat in account_categories.items() if cat == target_cat]
        if not valid_ids:
            conditions = [Account.id == -1]
        else:
            conditions = [Account.id.in_(valid_ids)]
    else:
        conditions = None

    try:
        count_stmt = select(func.count(Account.id))
        list_stmt = select(Account).options(selectinload(Account.category)).order_by(Account.id.asc())
        
        if conditions is not None:
            count_stmt = count_stmt.where(*conditions)
            list_stmt = list_stmt.where(*conditions)

        total_accounts = await session.scalar(count_stmt) or 0
        total_pages = calculate_total_pages(total_accounts)
        page = clamp_page(page, total_pages)
        offset = get_page_offset(page)

        current_accounts = (await session.scalars(list_stmt.offset(offset).limit(PAGINATION_SIZE))).all()
    except Exception as e:
        await session.rollback()
        return await answer_callback_error(
            callback, report_db_error("اکانت‌ها", e), get_main_menu_keyboard()
        )

    if state is not None:
        await state.update_data(list_filter=filter_type, list_page=page)

    builder = InlineKeyboardBuilder()

    if total_accounts == 0:
        builder.button(text="🔍 جستجو با شماره", callback_data="search_accounts/")
        builder.button(text="🔙 بازگشت به داشبورد", callback_data="menu_list_accounts/")
        builder.adjust(1)
        return await safe_edit_or_answer(
            callback.message,
            f"📋 <b>{display_title}</b>\n\n⚠️ هیچ اکانتی در این فیلتر یافت نشد.",
            reply_markup=builder.as_markup()
        )

    text = (
        f"📋 <b>{display_title}</b>\n"
        f"🔢 مجموع: <b>{total_accounts}</b> اکانت\n"
        f"📄 صفحهٔ <b>{page}</b> از <b>{total_pages}</b>\n\n"
    )

    pipe = redis_client.pipeline()
    for acc in current_accounts:
        pipe.exists(f"chunk_cooldown:{acc.id}")
    redis_results = await pipe.execute()

    for idx, acc in enumerate(current_accounts):
        is_conn = acc.id in worker_pool and getattr(worker_pool[acc.id], "is_connected", False)
        has_redis = bool(redis_results[idx])
        disp = get_account_display_status(acc, is_conn, has_redis, now)
        status_badge = disp["badge"]

        cat_name = html.escape(acc.category.name) if acc.category else "بدون دسته"
        
        display_idx = offset + idx + 1
        text += (
            f"{display_idx}. شماره: <code>{acc.phone_number}</code> {status_badge}\n"
            f"📁 دسته‌بندی: /category_{acc.id} ({cat_name})\n"
            f"🔸 وضعیت: /status_{acc.id}\n\n"
        )

        phone_display = str(acc.phone_number) if acc.phone_number else f"ID-{acc.id}"
        btn_label = phone_display if len(phone_display) <= 16 else phone_display[:15] + "…"
        builder.button(text=f"🗑 حذف {btn_label}", callback_data=f"delete_acc_{acc.id}/")

    builder.adjust(2)
    text += "👇 برای حذف هر اکانت، روی دکمه مربوطه کلیک کنید:"

    add_pagination_nav_row(builder, page, total_pages, callback_prefix=f"list_acc_filter_{filter_type}_")
    builder.row(
        types.InlineKeyboardButton(text="🔄 بروزرسانی", callback_data=f"list_acc_filter_{filter_type}_page_{page}/"),
        types.InlineKeyboardButton(text="🔍 جستجو", callback_data="search_accounts/"),
    )
    builder.row(types.InlineKeyboardButton(text="🔙 بازگشت به داشبورد", callback_data="menu_list_accounts/"))

    await safe_edit_or_answer(callback.message, text, reply_markup=builder.as_markup())


# ==========================================
# 🔍 فاز ۴: جستجوی اکانت با شماره موبایل
# ==========================================
def get_account_search_prompt_keyboard():
    """
    🔍 فاز ۴: کیبورد انصراف فلوی جستجوی اکانت — کاملاً محلی (بدون وابستگی به
    هندلر عمومی cancel) تا خروج از state جستجو تضمینی باشد و به داشبورد
    اکانت‌ها (نه منوی اصلی) برگردد.
    """
    builder = InlineKeyboardBuilder()
    builder.button(text="❌ انصراف", callback_data="cancel_account_search/")
    builder.button(text="🔙 بازگشت به داشبورد", callback_data="menu_list_accounts/")
    builder.button(text="🏛 منوی اصلی", callback_data="menu_home/")
    builder.adjust(1)
    return builder.as_markup()


async def build_accounts_search_view(
    session: AsyncSession,
    query: str,
    page: int,
) -> tuple[str, types.InlineKeyboardMarkup, int]:
    """
    🔍 فاز ۴: ساخت متن و کیبورد «نتایج جستجوی اکانت» (بدون ارسال/ویرایش پیام).

    - جستجوی جزئی (LIKE %query%) روی شماره موبایل با کوئری‌های efficient:
      یک COUNT + یک OFFSET/LIMIT (۱۰ نتیجه در هر صفحه)
    - Stateless: کوئری جستجو (فقط ارقام) در خود callback حمل می‌شود
      (acc_search_{query}_page_{n}/) → ناوبری و 🔄 بروزرسانی بدون نیاز به FSM
      و حتی بعد از ری‌استارت ربات کار می‌کند

    Returns:
        (text, markup, actual_page) — صفحهٔ واقعی بعد از clamp

    Raises:
        Exception: خطای دیتابیس (بعد از rollback مجدداً raise می‌شود)
    """
    # نرمال‌سازی دفاعی (idempotent — ارقام فارسی/عربی + حذف +، فاصله، خط تیره)
    query = normalize_phone_query(query)

    try:
        like_cond = Account.phone_number.like(build_like_pattern(query), escape="\\")

        total_count = (
            await session.scalar(select(func.count(Account.id)).where(like_cond)) or 0
        )
        total_pages = calculate_total_pages(total_count)
        page = clamp_page(page, total_pages)
        offset = get_page_offset(page)

        accounts = (
            await session.scalars(
                select(Account)
                .options(selectinload(Account.category))
                .where(like_cond)
                .order_by(Account.id.asc())
                .offset(offset)
                .limit(PAGINATION_SIZE)
            )
        ).all()
    except Exception:
        await session.rollback()
        raise

    now = datetime.now(timezone.utc)
    builder = InlineKeyboardBuilder()

    text = (
        "🔍 <b>نتایج جستجوی اکانت</b>\n"
        f"📱 عبارت جستجو: <code>{html.escape(query)}</code>\n"
        f"🔢 نتایج یافت‌شده: <b>{total_count}</b>\n"
        f"📄 صفحهٔ <b>{page}</b> از <b>{total_pages}</b>\n\n"
    )

    # ── بدون نتیجه ──
    if total_count == 0:
        text += "⚠️ هیچ اکانتی با این شماره یافت نشد.\n\n<i>برای جستجوی مجدد از دکمهٔ زیر استفاده کنید:</i>"
        builder.row(types.InlineKeyboardButton(text="🔍 جستجوی جدید", callback_data="search_accounts/"))
        builder.row(types.InlineKeyboardButton(text="🔙 بازگشت به داشبورد", callback_data="menu_list_accounts/"))
        return text, builder.as_markup(), page

    # ── آیتم‌های صفحه (قالب یکسان با لیست فیلتردار) ──
    from workers.sender import _get_redis
    from utils.account_display import get_account_display_status
    from workers.session_manager import worker_pool
    redis_client = _get_redis()
    
    pipe = redis_client.pipeline()
    for acc in accounts:
        pipe.exists(f"chunk_cooldown:{acc.id}")
    redis_results = await pipe.execute()

    for idx, acc in enumerate(accounts, start=offset + 1):
        is_conn = acc.id in worker_pool and getattr(worker_pool[acc.id], "is_connected", False)
        has_redis = bool(redis_results[idx - offset - 1])
        disp = get_account_display_status(acc, is_conn, has_redis, now)
        status_badge = disp["badge"]

        cat_name = html.escape(acc.category.name) if acc.category else "بدون دسته"

        text += (
            f"{idx}. شماره: <code>{acc.phone_number}</code> {status_badge}\n"
            f"📁 دسته‌بندی: /category_{acc.id} ({cat_name})\n"
            f"🔸 وضعیت: /status_{acc.id}\n\n"
        )

        phone_display = str(acc.phone_number) if acc.phone_number else f"ID-{acc.id}"
        btn_label = phone_display if len(phone_display) <= 16 else phone_display[:15] + "…"
        builder.button(text=f"🗑 حذف {btn_label}", callback_data=f"delete_acc_{acc.id}/")

    builder.adjust(2)

    text += "👇 برای حذف هر اکانت، روی دکمه مربوطه کلیک کنید:"

    # ── ناوبری صفحات (کوئری در callback حمل می‌شود) ──
    add_pagination_nav_row(builder, page, total_pages, callback_prefix=f"acc_search_{query}_")

    # ── 🔄 بروزرسانی (رفرش همان جستجو/صفحه) + 🔍 جستجوی جدید ──
    builder.row(
        types.InlineKeyboardButton(
            text="🔄 بروزرسانی",
            callback_data=f"acc_search_{query}_page_{page}/",
        ),
        types.InlineKeyboardButton(text="🔍 جستجوی جدید", callback_data="search_accounts/"),
    )

    builder.row(
        types.InlineKeyboardButton(text="🔙 بازگشت به داشبورد", callback_data="menu_list_accounts/")
    )

    return text, builder.as_markup(), page


@router.callback_query(F.data == "search_accounts/")
async def start_account_search(callback: types.CallbackQuery, state: FSMContext) -> None:
    """🔍 فاز ۴: شروع فلوی جستجوی اکانت با شماره موبایل."""
    await safe_callback_answer(callback)

    # 🟣 الگوی استاندارد ورود به فلوی جدید: پاکسازی state قبلی
    await cleanup_fsm_temp_files(state)
    await state.clear()

    await state.set_state(SearchStates.waiting_for_phone_query)

    await safe_edit_or_answer(
        callback.message,
        with_cancel_hint(
            "🔍 <b>جستجوی اکانت</b>\n\n"
            "لطفاً شماره موبایل (یا بخشی از آن) را ارسال کنید:\n"
            "<i>مثال: <code>0912</code> یا <code>989123456789</code></i>"
        ),
        reply_markup=get_account_search_prompt_keyboard()
    )


@router.message(SearchStates.waiting_for_phone_query, F.text)
async def perform_account_search(message: types.Message, state: FSMContext, session: AsyncSession) -> None:
    """
    🔍 فاز ۴: اجرای جستجو و نمایش صفحهٔ اول نتایج.

    اعتبارسنجی: فقط ارقام (پس از نرمال‌سازی ارقام فارسی/عربی و حذف جداسازها)،
    ۳ تا ۲۰ رقم — این محدودیت، حمل امن کوئری در callback (سقف ۶۴ بایت) را
    هم تضمین می‌کند.
    """
    query = normalize_phone_query(message.text or "")

    if not query.isdigit() or not (3 <= len(query) <= 20):
        return await message.answer(
            with_cancel_hint(
                "⚠️ فرمت نامعتبر!\n"
                "لطفاً فقط شماره موبایل (یا بخشی عددی از آن) را ارسال کنید.\n"
                "<i>مثال: <code>0912</code> یا <code>989123456789</code> — حداقل ۳ و حداکثر ۲۰ رقم.</i>"
            ),
            reply_markup=get_account_search_prompt_keyboard()
        )

    try:
        text, markup, actual_page = await build_accounts_search_view(session, query, page=1)
    except Exception as e:
        return await message.answer(
            report_db_error("اکانت‌ها", e),
            reply_markup=get_accounts_return_keyboard()
        )

    # خروج از state جستجو اما «حفظ داده‌ها» — زمینهٔ فعلی برای بازگشت فلوی
    # حذف از نتایج (همان الگوی list_filter/list_page لیست فیلتردار)
    await state.set_state(None)
    await state.update_data(list_filter="search", list_page=actual_page, acc_search_query=query)

    await message.answer(text, reply_markup=markup)


@router.callback_query(F.data.startswith("acc_search_") & F.data.endswith("/"))
async def accounts_search_page_handler(callback: types.CallbackQuery, state: FSMContext, session: AsyncSession) -> None:
    """
    🔍 فاز ۴: هندلر مشترک «صفحه بعد/قبل» و «🔄 بروزرسانی» نتایج جستجو.
    (کوئری جستجو در خود callback حمل می‌شود → stateless)
    """
    match = re.match(r"^acc_search_(\d+)_page_(\d+)/$", callback.data)
    if not match:
        return await safe_callback_answer(callback, "⚠️ داده‌های کال‌بک نامعتبر است.", show_alert=True)

    await safe_callback_answer(callback)

    query = match.group(1)
    page = int(match.group(2))

    try:
        text, markup, actual_page = await build_accounts_search_view(session, query, page)
    except Exception as e:
        return await answer_callback_error(
            callback, report_db_error("اکانت‌ها", e), get_main_menu_keyboard()
        )

    await state.update_data(list_filter="search", list_page=actual_page, acc_search_query=query)

    await safe_edit_or_answer(callback.message, text, reply_markup=markup)


@router.callback_query(F.data == "cancel_account_search/")
async def cancel_account_search(callback: types.CallbackQuery, state: FSMContext, session: AsyncSession) -> None:
    """🔍 فاز ۴: انصراف از جستجو → بازگشت به داشبورد اکانت‌ها."""
    await safe_callback_answer(callback, "🚫 جستجوی اکانت لغو شد.")

    # show_accounts_dashboard خودش state را پاک می‌کند
    return await show_accounts_dashboard(callback, session, state=state, skip_answer=True)


# ==========================================
# 📄 فاز ۴: بازگشت هوشمند بعد از حذف اکانت
# (لیست فیلتردار یا نتایج جستجو — بسته به محل شروع عملیات)
# ==========================================
def _current_view_context(fsm_data: dict) -> dict:
    """
    🔍 فاز ۴: ساخت زمینهٔ بازگشت از کلیدهای «نمای فعلی»
    (list_filter / list_page / acc_search_query).

    اگر کاربر الان در نتایج جستجو است → return_search؛
    در غیر این صورت → return_filter/return_page (لیست فیلتردار).
    """
    if fsm_data.get("list_filter") == "search" and fsm_data.get("acc_search_query"):
        return {
            "return_search": fsm_data.get("acc_search_query"),
            "return_page": fsm_data.get("list_page", 1),
        }
    return {
        "return_filter": fsm_data.get("list_filter", "all"),
        "return_page": fsm_data.get("list_page", 1),
    }


async def _return_to_accounts_view(
    callback: types.CallbackQuery,
    session: AsyncSession,
    state: FSMContext,
    fsm_data: dict,
) -> None:
    """
    🔍 فاز ۴: بازگشت به «نمای درست» اکانت‌ها بعد از عملیات حذف/انصراف.

    - اگر عملیات از «نتایج جستجو» شروع شده باشد (return_search مقدار دارد) →
      بازگشت به همان نتایج (همان کوئری و صفحه)
    - در غیر این صورت → لیست فیلتردار قبلی (الگوی فازهای قبل)
    """
    return_search = fsm_data.get("return_search")
    return_page = fsm_data.get("return_page", 1)

    if return_search:
        try:
            text, markup, actual_page = await build_accounts_search_view(session, return_search, return_page)
        except Exception as e:
            return await answer_callback_error(
                callback, report_db_error("اکانت‌ها", e), get_main_menu_keyboard()
            )
        # بازیابی زمینه برای حذف‌های بعدی از همان نتایج
        await state.update_data(list_filter="search", list_page=actual_page, acc_search_query=return_search)
        return await safe_edit_or_answer(callback.message, text, reply_markup=markup)

    return await render_filtered_account_list(
        callback, session, state,
        filter_type=fsm_data.get("return_filter", "all"),
        page=return_page,
        skip_answer=True,
    )


# ==========================================
# REPORTS: ORDER & DELIVERY ANALYTICS
# ==========================================
@router.callback_query(F.data == "menu_delivery_stats/")
async def show_order_analysis(callback: types.CallbackQuery, session: AsyncSession, state: FSMContext) -> None:
    await callback.answer()

    await cleanup_fsm_temp_files(state)
    await state.clear()

    # 🛡 فاز ۴ (مشکل ۱): هر ۵ خواندن دیتابیس این هندلر بدون حفاظ بود
    try:
        # 1. Queue Analytics
        pending_stmt = select(func.count(Order.id)).where(Order.status == OrderStatus.pending)
        running_stmt = select(func.count(Order.id)).where(Order.status == OrderStatus.running)
        completed_stmt = select(func.count(Order.id)).where(Order.status == OrderStatus.completed)

        pending_orders = (await session.execute(pending_stmt)).scalar() or 0
        running_orders = (await session.execute(running_stmt)).scalar() or 0
        completed_orders = (await session.execute(completed_stmt)).scalar() or 0

        # 2. Delivery Analytics
        success_stmt = select(func.count(OrderLog.id)).where(OrderLog.status == "success")
        error_stmt = select(func.count(OrderLog.id)).where(OrderLog.status == "error")
        flood_stmt = select(func.count(OrderLog.id)).where(OrderLog.status == "flood")
        restricted_stmt = select(func.count(OrderLog.id)).where(OrderLog.status == "restricted")

        total_success = (await session.execute(success_stmt)).scalar() or 0
        total_error = (await session.execute(error_stmt)).scalar() or 0
        total_flood = (await session.execute(flood_stmt)).scalar() or 0
        total_restricted = (await session.execute(restricted_stmt)).scalar() or 0
        total_logs = total_success + total_error + total_flood + total_restricted

        # 3. Top Error Causes
        top_errors_stmt = (
            select(OrderLog.error_message, func.count(OrderLog.id))
            .where(OrderLog.status == "error")
            .group_by(OrderLog.error_message)
            .order_by(func.count(OrderLog.id).desc())
            .limit(3)
        )
        top_errors_result = await session.execute(top_errors_stmt)
        top_errors = top_errors_result.all()
    except Exception as e:
        await session.rollback()
        return await answer_callback_error(
            callback, report_db_error("آمار", e), get_back_keyboard()
        )

    delivery_rate = round((total_success / total_logs * 100), 2) if total_logs > 0 else 0.0

    error_report = ""
    if top_errors:
        error_report = "\n\n⚠️ <b>علت‌های اصلی خطا:</b>\n"
        for err, count in top_errors:
            # 🛡 فاز ۴: escape — پیام خطای ذخیره‌شده می‌تواند شامل کاراکترهای HTML باشد
            clean_err = err.split(":")[0] if err else "نامشخص"
            error_report += f"├ <i>{html.escape(clean_err)}</i> : <code>{count}</code>\n"

    report_text = (
        "📈 <b>آمار سفارشات و ارسال</b>\n\n"
        "📋 <b>وضعیت صف:</b>\n"
        f"├ در حال اجرا: <code>{running_orders}</code>\n"
        f"├ در صف انتظار: <code>{pending_orders}</code>\n"
        f"└ تکمیل شده: <code>{completed_orders}</code>\n\n"
        "🚀 <b>عملکرد ارسال:</b>\n"
        f"├ تحویل شده: <code>{total_success}</code>\n"
        f"├ ناموفق: <code>{total_error}</code>\n"
        f"├ محدود شده (FloodWait): <code>{total_flood}</code>\n"
        f"├ ریزتریکتیو شده: <code>{total_restricted}</code>\n"
        f"└ نرخ موفقیت: <b>{delivery_rate}%</b>"
        f"{error_report}"
    )

    # 🛡 فاز ۴: ویرایش امن
    await safe_edit_or_answer(callback.message, report_text, reply_markup=get_back_keyboard())

# ==========================================
# HOME ROUTING
# ==========================================
@router.callback_query(F.data == "menu_home/")
async def return_to_home(callback: types.CallbackQuery, state: FSMContext, session: AsyncSession) -> None:
    await callback.answer()

    await cleanup_fsm_temp_files(state)
    await state.clear()
    
    # آزادسازی رزروهای لاگین رها شده
    await release_login_reservations(callback.from_user.id, session)

    await safe_edit_or_answer(
        callback.message,
        "🎛 <b>پنل کنترل اصلی</b>\n\nلطفاً یک گزینه را انتخاب کنید:",
        reply_markup=get_main_menu_keyboard()
    )

# ==========================================
# REPORTS: ACCOUNTS LIST & PAGINATION
# ==========================================
@router.callback_query(F.data == "menu_list_accounts/")
async def show_accounts_dashboard(callback: types.CallbackQuery, session: AsyncSession, state: FSMContext = None, skip_answer: bool = False) -> None:
    if not skip_answer:
        await callback.answer()

    if state is not None:
        await cleanup_fsm_temp_files(state)
        await state.clear()

    now_aware = datetime.now(timezone.utc)
    now_naive = now_aware.replace(tzinfo=None)

    try:
        # ۱. محاسبه آمار کل اکانت‌ها
        stmt_all = select(func.count(Account.id))
        total_all = (await session.execute(stmt_all)).scalar() or 0

        # ۲. محاسبه اکانت‌های ثبت‌نام نشده
        stmt_not_reg = select(func.count(Account.id)).where(Account.session_string.is_(None))
        total_not_reg = (await session.execute(stmt_not_reg)).scalar() or 0

        # ۳. محاسبه اکانت‌های لیمیت شده
        stmt_limited = select(func.count(Account.id)).where(
            Account.is_banned == False, 
            Account.session_string.is_not(None),
            or_(Account.flood_wait_until > now_naive, Account.restricted_until > now_naive)
        )
        total_limited = (await session.execute(stmt_limited)).scalar() or 0

        # ۴. محاسبه اکانت‌های فعال
        stmt_active = select(func.count(Account.id)).where(
            Account.is_banned == False,
            Account.session_string.is_not(None)
        )
        total_active = (await session.execute(stmt_active)).scalar() or 0

        # ۵. دریافت آیدی اکانت‌های آماده ارسال برای بررسی در Redis
        stmt_ability = select(Account.id).where(
            Account.is_banned == False,
            Account.session_string.is_not(None),
            or_(Account.flood_wait_until.is_(None), Account.flood_wait_until <= now_naive),
            or_(Account.restricted_until.is_(None), Account.restricted_until <= now_naive)
        )
        ability_ids = (await session.execute(stmt_ability)).scalars().all()
        
        # 🟢 خواندن وضعیت Cooldown (استراحت) از Redis
        total_cooldown = 0
        if ability_ids:
            from workers.sender import _get_redis
            redis_client = _get_redis()
            pipe = redis_client.pipeline()
            for aid in ability_ids:
                pipe.exists(f"chunk_cooldown:{aid}")
            
            results = await pipe.execute()
            total_cooldown = sum(1 for res in results if res)

        # 🟢 محاسبه تعداد واقعی اکانت‌های آماده ارسال (کسر استراحت‌کننده‌ها)
        total_ability = len(ability_ids) - total_cooldown
        if total_ability < 0:
            total_ability = 0
        
    except Exception as e:
        await session.rollback()
        return await answer_callback_error(
            callback, report_db_error("اکانت‌ها", e), get_main_menu_keyboard()
        )

    text = (
        "📲 <b>لیست اکانت‌ها</b> 📲\n\n"
        "🗑 پاکسازی همه اکانت‌های ثبت‌نام نشده: /DelUnregistered\n"
        "🔍 برای یافتن سریع یک اکانت specific از جستجو استفاده کنید.\n"
    )

    builder = InlineKeyboardBuilder()

    builder.button(text=f"💢 همه ({total_all})", callback_data="list_acc_filter_all_page_1/")
    builder.button(text=f"⛔️ ثبت‌نام نشده ({total_not_reg})", callback_data="list_acc_filter_notreg_page_1/")
    builder.button(text=f"❌ محدود شده ({total_limited})", callback_data="list_acc_filter_limited_page_1/")
    builder.button(text=f"♻️ آماده ارسال ({total_ability})", callback_data="list_acc_filter_ability_page_1/")
    builder.button(text=f"✅ فعال ({total_active})", callback_data="list_acc_filter_active_page_1/")
    
    # 🟢 دکمه جدید برای اکانت‌های در حال استراحت
    builder.button(text=f"💤 استراحت ({total_cooldown})", callback_data="list_acc_filter_cooldown_page_1/")
    
    builder.button(text="🔍 جستجو با شماره", callback_data="search_accounts/")
    builder.button(text="🏛 منوی اصلی", callback_data="menu_home/")

    # تنظیم چیدمان جدید دکمه‌ها
    builder.adjust(1, 2, 2, 1, 1, 1)

    await safe_edit_or_answer(callback.message, text, reply_markup=builder.as_markup())


# ==========================================
# COMMAND HANDLERS: ACCOUNT ACTIONS
# ==========================================

# ==========================================
# ۱. هندلر پاکسازی گروهیِ اکانت‌های ثبت‌نام نشده
# ==========================================
@router.message(F.text == "/DelUnregistered")
async def delete_unregistered_accounts(message: types.Message, state: FSMContext, session: AsyncSession) -> None:
    # 🛡 فاز ۴ (مشکل ۳): کوئری شمارش بدون حفاظ بود — قطع دیتابیس = کرش بی‌صدای هندلر
    try:
        count_stmt = select(func.count(Account.id)).where(Account.session_string.is_(None))
        unregistered_count = await session.scalar(count_stmt) or 0
    except Exception as e:
        await session.rollback()
        return await message.answer(
            report_db_error("اکانت‌ها", e),
            reply_markup=get_back_keyboard()
        )

    if unregistered_count == 0:
        return await message.answer(
            "✅ <b>سیستم بهینه است.</b>\n\nهیچ اکانت ثبت‌نام‌نشده‌ای برای پاکسازی وجود ندارد.",
            reply_markup=get_back_keyboard()
        )

    await state.update_data(confirm_action="del_unregistered", pending_count=unregistered_count)
    await state.set_state(ConfirmStates.waiting_for_confirmation)

    builder = InlineKeyboardBuilder()
    builder.button(text="✅ بله، پاکسازی کن", callback_data="confirm_del_unregistered/")
    builder.button(text="❌ انصراف", callback_data="cancel_confirm_del_unregistered/")
    builder.adjust(2)

    await message.answer(
        "⚠️ <b>تأیید پاکسازی انبوه</b>\n\n"
        f"آیا از پاکسازی <b>{unregistered_count}</b> اکانت ثبت‌نام‌نشده مطمئن هستید؟\n\n"
        "⚠️ <b>توجه: این عمل قابل بازگشت نیست.</b>\n"
        "تمام اکانت‌های بدون سشن به‌طور دائمی از سیستم حذف می‌شوند.",
        reply_markup=builder.as_markup()
    )


# ==========================================
# 🟤 فاز ۵ — مرحله ۲ (اجرای واقعی): پاکسازی انبوه
# ==========================================
@router.callback_query(F.data == "confirm_del_unregistered/")
async def confirm_delete_unregistered_handler(callback: types.CallbackQuery, state: FSMContext, session: AsyncSession) -> None:
    current_state = await state.get_state()
    fsm_data = await state.get_data()

    if current_state != ConfirmStates.waiting_for_confirmation or fsm_data.get("confirm_action") != "del_unregistered":
        return await callback.answer(
            "⚠️ این درخواست تأیید منقضی شده است. لطفاً از ابتدا با دستور /DelUnregistered اقدام کنید.",
            show_alert=True
        )

    await safe_callback_answer(callback)

    # پیام انتظار واقعی — این عملیات می‌تواند طولانی باشد
    wait_msg = await send_loading_message(
        callback.message,
        "⏳ در حال پاکسازی اکانت‌های ثبت‌نام‌نشده از دیتابیس و حافظه..."
    )

    # ── ۱. واکشی شناسه‌های اکانت‌های ثبت‌نام‌نشده ──
    try:
        ids_stmt = select(Account.id).where(Account.session_string.is_(None))
        ids_result = await session.execute(ids_stmt)
        unregistered_ids = ids_result.scalars().all()
    except Exception as e:
        await session.rollback()
        # state حفظ می‌شود تا دکمه «✅ بله، پاکسازی کن» قابل استفاده مجدد باشد
        return await safe_edit_message(
            wait_msg,
            report_db_error("اکانت‌ها", e),
            reply_markup=get_accounts_return_keyboard()
        )

    if not unregistered_ids:
        await state.clear()
        await safe_edit_message(
            wait_msg,
            "✅ هیچ اکانت ثبت‌نام‌نشده‌ای برای پاکسازی باقی نمانده است."
        )
        return await show_accounts_dashboard(callback, session, state=state, skip_answer=True)

    # ── ۲. توقف ورکرها و حذف از RAM (best-effort) ──
    for acc_id in unregistered_ids:
        client = worker_pool.pop(acc_id, None)
        if client:
            try:
                if client.is_connected:
                    await client.stop()
                logger.info(f"Worker {acc_id} stopped and removed from RAM during bulk cleanup.")
            except Exception as e:
                logger.warning(f"Error stopping worker {acc_id} during bulk cleanup: {e}")

    # ── ۳. حذف انبوه از دیتابیس ──
    try:
        stmt = delete(Account).where(Account.session_string.is_(None))
        result = await session.execute(stmt)
        await session.commit()
    except Exception as e:
        await session.rollback()
        # state حفظ می‌شود تا کاربر بتواند همان دکمه تأیید را دوباره بزند
        return await safe_edit_message(
            wait_msg,
            report_db_error("اکانت‌ها", e),
            reply_markup=get_accounts_return_keyboard()
        )

    deleted_count = result.rowcount
    await state.clear()

    await safe_edit_message(
        wait_msg,
        f"✅ تعداد <b>{deleted_count}</b> اکانت ثبت‌نام نشده با موفقیت از <b>دیتابیس</b> و <b>حافظه (RAM)</b> پاکسازی شدند."
    )

    return await show_accounts_dashboard(callback, session, state=state, skip_answer=True)


# ==========================================
# 🟤 فاز ۵ — انصراف از پاکسازی انبوه
# ==========================================
@router.callback_query(F.data == "cancel_confirm_del_unregistered/")
async def cancel_delete_unregistered_confirmation(callback: types.CallbackQuery, state: FSMContext, session: AsyncSession) -> None:
    fsm_data = await state.get_data()

    if fsm_data.get("confirm_action") == "del_unregistered":
        await state.clear()

    await callback.answer("🚫 عملیات پاکسازی لغو شد.")

    return await show_accounts_dashboard(callback, session, state=state, skip_answer=True)


# ==========================================
# 🟣 فاز ۴ — DELETE ACCOUNT: توابع کمکی مشترک
# ==========================================
def build_acc_delete_confirmation_text(acc: Account, cat_name: str, is_worker_online: bool, has_redis: bool = False) -> str:
    now = datetime.now(timezone.utc)
    from utils.account_display import get_account_display_status
    
    disp = get_account_display_status(acc, is_worker_online, has_redis, now)
    status_display = f"{disp['badge']} ({disp['desc']})"

    worker_display = "🟢 متصل (در حال اجرا)" if is_worker_online else "⚪️ آفلاین"
    safe_cat_name = html.escape(cat_name)
    phone_display = str(acc.phone_number) if acc.phone_number else "نامشخص"

    return (
        "⚠️ <b>تأیید حذف اکانت</b>\n\n"
        "آیا از حذف اکانت زیر مطمئن هستید؟\n\n"
        f"🆔 شناسه: <code>{acc.id}</code>\n"
        f"📱 شماره: <code>{phone_display}</code>\n"
        f"📍 وضعیت اکانت: {status_display}\n"
        f"🔌 وضعیت ورکر: {worker_display}\n"
        f"📁 دسته‌بندی: {safe_cat_name}\n\n"
        "⚠️ <b>توجه: این عمل قابل بازگشت نیست.</b>\n"
        "اکانت هم از <b>دیتابیس</b> و هم از <b>حافظه (RAM)</b> به‌طور کامل پاکسازی می‌شود."
    )


def build_acc_delete_confirmation_keyboard(acc_id: int) -> types.InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ بله، حذف کن", callback_data=f"confirm_delete_acc_{acc_id}/")
    builder.button(text="❌ انصراف", callback_data="cancel_confirm_delete_acc/")
    builder.adjust(2)
    return builder.as_markup()


@router.callback_query(F.data.startswith("delete_acc_") & F.data.endswith("/"))
async def delete_account_handler(callback: types.CallbackQuery, state: FSMContext, session: AsyncSession) -> None:
    acc_id_str = callback.data.replace("delete_acc_", "").replace("/", "")

    if not acc_id_str.isdigit():
        return await callback.answer("⚠️ آیدی نامعتبر.", show_alert=True)

    acc_id = int(acc_id_str)

    try:
        stmt = select(Account).options(selectinload(Account.category)).where(Account.id == acc_id)
        acc = await session.scalar(stmt)
    except Exception as e:
        await session.rollback()
        return await answer_callback_error(
            callback, report_db_error("اکانت", e), get_main_menu_button()
        )

    if not acc:
        await callback.answer("⚠️ این اکانت وجود ندارد یا قبلاً حذف شده است.", show_alert=True)
        fsm_data = await state.get_data()
        return await _return_to_accounts_view(
            callback, session, state, _current_view_context(fsm_data)
        )

    await callback.answer()

    fsm_data = await state.get_data()
    await state.update_data(
        confirm_action="delete_acc",
        target_id=acc_id,
        **_current_view_context(fsm_data),
    )
    await state.set_state(ConfirmStates.waiting_for_confirmation)

    cat_name = acc.category.name if acc.category else "بدون دسته"
    client = worker_pool.get(acc_id)
    is_worker_online = bool(client and client.is_connected)

    # تبدیل به safe_edit_or_answer برای جلوگیری از Exceptionهای Unhandled
    await safe_edit_or_answer(
        callback.message,
        build_acc_delete_confirmation_text(acc, cat_name, is_worker_online),
        reply_markup=build_acc_delete_confirmation_keyboard(acc_id)
    )


# === bot/handlers/stats_handlers.py | show_global_statistics ===


# ==========================================
# 🟣 فاز ۴ — مرحله ۲ (اجرای واقعی)
# (🔍 فاز ۴: بازگشت به همان لیست/نتایج جستجوی قبلی)
# ==========================================
@router.callback_query(F.data.startswith("confirm_delete_acc_") & F.data.endswith("/"))
async def confirm_delete_account_handler(callback: types.CallbackQuery, state: FSMContext, session: AsyncSession) -> None:
    current_state = await state.get_state()
    fsm_data = await state.get_data()

    if current_state != ConfirmStates.waiting_for_confirmation or fsm_data.get("confirm_action") != "delete_acc":
        return await callback.answer(
            "⚠️ این درخواست تأیید منقضی شده است. لطفاً از ابتدا اقدام کنید.",
            show_alert=True
        )

    acc_id_str = callback.data.replace("confirm_delete_acc_", "").replace("/", "")

    if not acc_id_str.isdigit():
        return await callback.answer("⚠️ آیدی نامعتبر.", show_alert=True)

    acc_id = int(acc_id_str)

    if fsm_data.get("target_id") != acc_id:
        return await callback.answer(
            "⚠️ این درخواست تأیید با اکانت نمایش‌داده‌شده مطابقت ندارد. لطفاً از ابتدا اقدام کنید.",
            show_alert=True
        )

    # 🛡 فاز ۴: safe_callback_answer — اگر پردازش طولانی شد و مهلت پاسخ تمام
    # شد، خطای QUERY_ID_INVALID باعث کرش هندلرِ خطا نمی‌شود
    await safe_callback_answer(callback, "⏳ در حال پردازش...")

    # 🛡 فاز ۴: خواندن اکانت از دیتابیس داخل حفاظ
    # (state عمداً پاک نمی‌شود — در خطای گذرا دکمه تأیید قابل استفاده مجدد می‌ماند)
    try:
        stmt = select(Account).where(Account.id == acc_id)
        acc = await session.scalar(stmt)
    except Exception as e:
        await session.rollback()
        return await answer_callback_error(
            callback, report_db_error("اکانت", e), get_main_menu_button()
        )

    if not acc:
        await state.clear()
        await callback.message.answer("⚠️ این اکانت در دیتابیس وجود ندارد یا قبلاً حذف شده است.")
        # 🔍 فاز ۴: بازگشت به نمای درست (fsm_data اسنپ‌شات قبل از clear است)
        return await _return_to_accounts_view(callback, session, state, fsm_data)

    phone_display = str(acc.phone_number) if acc.phone_number else f"ID-{acc.id}"

    try:
        old_proxy = acc.proxy_string
        if old_proxy:
            await release_proxy_slot(session, old_proxy)
            
        await session.delete(acc)
        await session.commit()
        
        # 🔥 فراخوانی پاکسازی عمیق (توقف ورکر از RAM و حذف فایل سشن از هارد)
        await remove_account_from_system(acc_id)
        
    except Exception as e:
        # 🛡 فاز ۴ (مشکل ۱): پیام بر اساس نوع خطا
        await session.rollback()
        await state.clear()
        await callback.message.answer(report_db_error("اکانت", e))
        # 🔍 فاز ۴: بازگشت به نمای درست
        return await _return_to_accounts_view(callback, session, state, fsm_data)

    await state.clear()

    await callback.message.answer(
        f"✅ اکانت شماره <code>{phone_display}</code> با موفقیت از <b>دیتابیس</b>، <b>حافظه (RAM)</b> و <b>سرور</b> پاکسازی شد."
    )

    # 🔍 فاز ۴: بازگشت به نمای درست
    return await _return_to_accounts_view(callback, session, state, fsm_data)

async def render_throughput_stats(session: AsyncSession) -> str:
    """
    L-04 (Phase 6 / T6) — REAL throughput measured from the OrderLog table.

    Counts actually-delivered messages (status='success') over the last 1h and
    24h in ONE index-friendly query:

        WHERE status = 'success' AND created_at >= <now - 24h>
        + SUM(CASE WHEN created_at >= <now - 1h> THEN 1 ELSE 0 END)

    Replaces the old cooldown-derived estimate (3600 / avg(SEND_DELAY_*)),
    which was a guess, not a measurement.
    """
    # MySQL DATETIME columns come back naive; the system stores UTC everywhere,
    # so build naive-UTC bounds to keep the comparison correct.
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    one_hour_ago = now - timedelta(hours=1)
    one_day_ago = now - timedelta(hours=24)

    stmt = select(
        func.sum(case((OrderLog.created_at >= one_hour_ago, 1), else_=0)),
        func.count(OrderLog.id),
    ).where(
        OrderLog.status == "success",
        OrderLog.created_at >= one_day_ago,
    )

    row = (await session.execute(stmt)).one()
    last_1h = int(row[0] or 0)
    last_24h = int(row[1] or 0)

    return (
        "🚀 <b>توان ارسال واقعی</b>\n"
        f"├ ۱ ساعت گذشته: <b>{last_1h:,}</b> پیام\n"
        f"└ ۲۴ ساعت گذشته: <b>{last_24h:,}</b> پیام\n"
    )


# ==========================================
# 🟣 فاز ۴ — انصراف از حذف اکانت
# (🔍 فاز ۴: بازگشت به همان لیست/نتایج جستجوی قبلی)
# ==========================================
@router.callback_query(F.data == "cancel_confirm_delete_acc/")
async def cancel_delete_account_confirmation(callback: types.CallbackQuery, state: FSMContext, session: AsyncSession) -> None:
    fsm_data = await state.get_data()

    if fsm_data.get("confirm_action") == "delete_acc":
        await state.clear()

    await callback.answer("🚫 عملیات حذف اکانت لغو شد.")

    # 🔍 فاز ۴: بازگشت به نمای درست (لیست فیلتردار یا نتایج جستجو)
    return await _return_to_accounts_view(callback, session, state, fsm_data)


# ==========================================
# ۲. هندلر حذف تکی اکانت (مسیر کامند متنی)
# ==========================================
@router.message(F.text.regexp(r"^/delete_(\d+)$"))
async def delete_single_account(message: types.Message, state: FSMContext, session: AsyncSession) -> None:
    match = re.match(r"^/delete_(\d+)$", message.text)
    if not match:
        return

    acc_id = int(match.group(1))

    # 🛡 فاز ۴ (مشکل ۱): خواندن اکانت بدون حفاظ بود
    try:
        stmt = select(Account).options(selectinload(Account.category)).where(Account.id == acc_id)
        acc = await session.scalar(stmt)
    except Exception as e:
        await session.rollback()
        return await message.answer(
            report_db_error("اکانت", e),
            reply_markup=get_back_keyboard()
        )

    if not acc:
        return await message.answer(
            "⚠️ این اکانت در دیتابیس وجود ندارد یا قبلاً حذف شده است.",
            reply_markup=get_back_keyboard()
        )

    # 🔍 فاز ۴: return_search=None صریح — مسیر کامند همیشه به لیست «همه» برمی‌گردد
    await state.update_data(
        confirm_action="delete_acc",
        target_id=acc_id,
        return_filter="all",
        return_page=1,
        return_search=None,
    )
    await state.set_state(ConfirmStates.waiting_for_confirmation)

    cat_name = acc.category.name if acc.category else "بدون دسته"
    client = worker_pool.get(acc_id)
    is_worker_online = bool(client and client.is_connected)

    await message.answer(
        build_acc_delete_confirmation_text(acc, cat_name, is_worker_online),
        reply_markup=build_acc_delete_confirmation_keyboard(acc_id)
    )


# ۳. هندلر بررسی وضعیت دقیق اکانت
# ۳. هندلر بررسی وضعیت دقیق اکانت (پشتیبانی از /status_id و /user_id)
@router.message(F.text.regexp(r"^/(?:status|user)_(\d+)(?:/)?$"))
async def show_account_status(message: types.Message, session: AsyncSession) -> None:
    # 🛡 فاز ۱۰ (SEC-5): این دستور اطلاعات حساس اکانت را نشان می‌دهد ← فقط ادمین اصلی.
    if not _is_main_admin(message.from_user):
        if _MAIN_ADMIN_ID is None:
            return await message.answer(
                "⛔️ دسترسی محدود: شناسهٔ ادمین اصلی (ADMIN_ID) در تنظیمات پروژه یافت نشد."
            )
        return await message.answer("⛔️ فقط ادمین اصلی اجازهٔ استفاده از این دستور را دارد.")

    # دریافت آیدی از هر دو فرمت status و user
    match = re.match(r"^/(?:status|user)_(\d+)(?:/)?$", message.text)
    if not match:
        return

    acc_id = int(match.group(1))

    # 🛡 فاز ۴ (مشکل ۱): خواندن اکانت بدون حفاظ بود
    try:
        stmt = select(Account).options(selectinload(Account.category)).where(Account.id == acc_id)
        acc = await session.scalar(stmt)
    except Exception as e:
        await session.rollback()
        return await message.answer(
            report_db_error("اکانت", e),
            reply_markup=get_back_keyboard()
        )

    if not acc:
        return await message.answer(
            "⚠️ این اکانت در سیستم وجود ندارد.",
            reply_markup=get_back_keyboard()
        )

    now = datetime.now(timezone.utc)

    first_name, last_name, username, has_photo = "ندارد", "ندارد", "ندارد", "ندارد ❌"
    current_session_text = ""
    other_sessions_text = "📱 <b>سایر نشست‌ها: 0</b>\n"
    is_live = False

    client = worker_pool.get(acc_id)

    if client and client.is_connected:
        try:
            me = await client.get_me()
            first_name = me.first_name or "ندارد"
            last_name = me.last_name or "ندارد"
            username = f"@{me.username}" if me.username else "ندارد"
            has_photo = "دارد ✅" if me.photo else "ندارد ❌"
            is_live = True

            auths = await client.invoke(GetAuthorizations())
            authorizations = auths.authorizations

            other_sessions_count = 0

            for auth in authorizations:
                if getattr(auth, 'current', False):
                    from utils.timezone_helpers import to_tehran_time
                    date_created = to_tehran_time(datetime.fromtimestamp(auth.date_created, timezone.utc))
                    date_active = to_tehran_time(datetime.fromtimestamp(auth.date_active, timezone.utc))

                    # 🛡 فاز ۴: escape رشته‌های سمت کلاینت تلگرام
                    current_session_text = (
                        f"💻 <b>نشست فعلی:</b>\n"
                        f"🔻 آی‌پی: <code>{auth.ip or 'نامشخص'}</code>\n"
                        f"🔻 کشور: {html.escape(auth.country or 'نامشخص')}\n"
                        f"🔻 دستگاه: {html.escape(auth.device_model or 'نامشخص')}\n"
                        f"🔻 پلتفرم: {html.escape(auth.platform or 'نامشخص')}\n"
                        f"🔻 سیستم عامل: {html.escape(auth.system_version or 'نامشخص')}\n"
                        f"🔻 شناسه API: <code>{auth.api_id or 'نامشخص'}</code>\n"
                        f"🔻 نام برنامه: {html.escape(auth.app_name or 'نامشخص')}\n"
                        f"🔻 نسخه برنامه: {html.escape(auth.app_version or 'نامشخص')}\n"
                        f"🔻 تاریخ ایجاد: {date_created}\n"
                        f"🔻 آخرین فعالیت: {date_active}\n\n"
                    )
                else:
                    other_sessions_count += 1

            if other_sessions_count > 0:
                other_sessions_text = (
                    f"📱 <b>سایر نشست‌ها: {other_sessions_count}</b>\n"
                    f"🔻 مدیریت و خروج: /sessions_{acc.id}\n\n"
                )
            else:
                other_sessions_text = "📱 <b>سایر نشست‌ها: 0</b>\n\n"

        except Exception as e:
            logger.error(f"Error fetching live session data for acc {acc_id}: {e}", exc_info=True)
            current_session_text = "⚠️ <i>خطا در دریافت اطلاعات نشست از تلگرام.</i>\n\n"

    from workers.sender import _get_redis
    from utils.account_display import get_account_display_status
    try:
        has_redis = await _get_redis().exists(f"chunk_cooldown:{acc.id}")
    except Exception:
        has_redis = False
        
    disp = get_account_display_status(acc, is_live, bool(has_redis), now)
    status_text = f"{disp['badge']} — {disp['desc']}"

    cat_name = acc.category.name if acc.category else "بدون دسته"
    
    # +++ دریافت آیدی تلگرام در صورت وجود +++
    user_id_line = f"🔻 آیدی تلگرام: <code>{acc.telegram_user_id}</code>\n" if getattr(acc, 'telegram_user_id', None) else ""

    text = (
        f"{status_text}\n\n"
        f"👤 <b>اطلاعات عمومی:</b>\n"
        f"🔻 شماره: <code>{html.escape(str(acc.phone_number))}</code>\n"  # ماسک برداشته شد
        f"{user_id_line}"
        f"🔻 نام: {html.escape(first_name)}\n"
        f"🔻 نام خانوادگی: {html.escape(last_name)}\n"
        f"🔻 یوزرنیم: {html.escape(username)}\n"
        f"🔻 عکس پروفایل: {has_photo}\n\n"
        f"{current_session_text}"
        f"{other_sessions_text}"
        # بخش security_text از اینجا کاملا حذف شد
        f"📁 دسته‌بندی: /category_{acc.id} ({html.escape(cat_name)})\n"
        f"❌ حذف اکانت: /delete_{acc.id}\n"
        f"⬇️ برای دانلود فایل سشن: /dl_session_{acc.id}"
    )

    # +++ اضافه شدن دکمه شیشه‌ای برای نمایش اطلاعات ورود +++
    builder = InlineKeyboardBuilder()
    builder.button(text="🔓 نمایش اطلاعات ورود", callback_data=f"show_creds_{acc.id}/")
    builder.adjust(1)

    await message.answer(text, reply_markup=builder.as_markup())

# ==========================================
# ⬇️ دانلود فایل .session اکانت (فقط ادمین اصلی)
# ==========================================

# 🕒 مدت نمایش پیامِ حاوی فایل سشن (ثانیه) — بعد از آن پیام خودکار حذف می‌شود
SESSION_FILE_MESSAGE_TTL = 60

# 📁 پوشهٔ ساخت فایل موقت سشن (نسبت به ریشهٔ پروژه)
SESSIONS_DOWNLOAD_DIR = "sessions"


# --- File: stats_handlers.py ---

# حوالی خط ۵۷۰، در تابع _resolve_main_admin_id:
def _resolve_main_admin_id() -> int | None:
    """..."""
    try:
        # --- FIX M4: Use actual config mechanism used elsewhere ---
        from config import config  # noqa: PLC0415
        if hasattr(config, 'ADMIN_ID') and config.ADMIN_ID:
            return int(config.ADMIN_ID)
        # ----------------------------------------------------------
    except Exception:
        pass

    raw = os.getenv("MAIN_ADMIN_ID") or os.getenv("ADMIN_ID")
    if raw and raw.lstrip("-").isdigit():
        return int(raw)
    return None


_MAIN_ADMIN_ID = _resolve_main_admin_id()


def _is_main_admin(user: types.User | None) -> bool:
    """🛡 چک دسترسی ادمین اصلی — Fail-Closed (بدون تنظیم، هیچ‌کس مجاز نیست)."""
    return _MAIN_ADMIN_ID is not None and user is not None and user.id == _MAIN_ADMIN_ID


async def _delete_message_later(msg: types.Message, delay: float = SESSION_FILE_MESSAGE_TTL) -> None:
    """🕒 حذف خودکار پیام حساس بعد از تأخیر مشخص — خطای «قبلاً حذف شده» بی‌صدا نادیده گرفته می‌شود."""
    try:
        await asyncio.sleep(delay)
        await msg.delete()
    except Exception:
        pass


@router.message(F.text.regexp(r"^/dl_session_(\d+)$"))
async def download_account_session_file(message: types.Message, session: AsyncSession) -> None:
    """
    ⬇️ ساخت و ارسال فایل .session استاندارد Pyrogram برای اکانت.

    جریان:
      ۱) فقط ادمین اصلی مجاز است
      ۲) سشن رمزنگاری‌شده از دیتابیس خوانده و با Fernet رمزگشایی می‌شود
      ۳) فایل در sessions/{phone}.session ساخته می‌شود
      ۴) فایل به‌صورت document ارسال و پیام بعد از ۶۰ ثانیه خودکار حذف می‌شود
      ۵) فایل موقت در finally (حتی در صورت خطا) از دیسک پاک می‌شود

    ⚠️ امنیت: محتوای سشن هرگز در لاگ یا پیام‌ها نوشته نمی‌شود.
    """
    # ── ۱) چک دسترسی: فقط ادمین اصلی ──
    if not _is_main_admin(message.from_user):
        if _MAIN_ADMIN_ID is None:
            return await message.answer(
                "⛔️ دسترسی محدود: شناسهٔ ادمین اصلی (ADMIN_ID) در تنظیمات پروژه یافت نشد."
            )
        return await message.answer("⛔️ فقط ادمین اصلی اجازهٔ استفاده از این دستور را دارد.")

    match = re.match(r"^/dl_session_(\d+)$", message.text or "")
    if not match:
        return

    acc_id = int(match.group(1))

    # ── ۲) خواندن اکانت از دیتابیس (🛡 فاز ۴: داخل حفاظ) ──
    try:
        stmt = select(Account).where(Account.id == acc_id)
        acc = await session.scalar(stmt)
    except Exception as e:
        await session.rollback()
        return await message.answer(
            report_db_error("اکانت", e),
            reply_markup=get_back_keyboard()
        )

    if not acc:
        return await message.answer(
            "⚠️ این اکانت در سیستم وجود ندارد.",
            reply_markup=get_back_keyboard()
        )

    if not acc.session_string:
        return await message.answer(
            "⛔️ این اکانت ثبت‌نام نشده است و سشنی برای دانلود ندارد.",
            reply_markup=get_back_keyboard()
        )

    # ── ۳) رمزگشایی سشن (⚠️ خروجی هرگز لاگ نمی‌شود) ──
    try:
        session_string = decrypt_session(acc.session_string)
    except Exception:
        logger.error(f"dl_session: رمزگشایی سشن اکانت {acc_id} ناموفق بود (محتوا لاگ نمی‌شود).")
        return await message.answer(
            "❌ رمزگشایی سشن این اکانت ناموفق بود (FERNET_KEY نامعتبر یا دادهٔ خراب).",
            reply_markup=get_back_keyboard()
        )

    if not session_string:
        return await message.answer(
            "⚠️ سشن این اکانت خالی است.",
            reply_markup=get_back_keyboard()
        )

    # ── ۴) نام‌گذاری امن فایل — فقط ارقام شماره (جلوگیری از path traversal) ──
    safe_phone = re.sub(r"\D", "", str(acc.phone_number or ""))
    file_stem = safe_phone if safe_phone else f"account_{acc.id}"
    out_path = os.path.join(SESSIONS_DOWNLOAD_DIR, f"{file_stem}.session")

    # ── ۵) ساخت فایل، ارسال، حذف خودکار پیام و پاکسازی قطعی فایل ──
    try:
        build_session_file(session_string, out_path)

        caption = (
            f"📦 <b>فایل سشن اکانت</b>\n\n"
            f"🆔 شناسه: <code>{acc.id}</code>\n"
            f"📱 شماره: <code>{html.escape(str(acc.phone_number))}</code>\n\n"
            "⚠️ <b>این فایل به‌شدت محرمانه است و معادلِ دسترسی کامل به اکانت است.</b>\n"
            f"🕒 این پیام تا {SESSION_FILE_MESSAGE_TTL} ثانیهٔ دیگر به‌صورت خودکار حذف می‌شود — "
            "فایل را همین حالا در جای امن ذخیره کنید.\n"
            "🧹 فایل موقت روی سرور بلافاصله پس از ارسال حذف شده است."
        )

        sent_msg = await message.answer_document(
            document=types.FSInputFile(path=out_path, filename=f"{file_stem}.session"),
            caption=caption,
        )

        # 🕒 حذف خودکار پیام حساس بعد از ۶۰ ثانیه (asyncio.create_task)
        asyncio.create_task(_delete_message_later(sent_msg))
    except Exception as e:
        # ⚠️ امنیت: فقط نوع خطا لاگ می‌شود — نه محتوای سشن
        logger.error(f"dl_session: خطا در ساخت/ارسال فایل سشن اکانت {acc_id}: {type(e).__name__}: {e}")
        await message.answer(
            "❌ خطا در ساخت یا ارسال فایل سشن. لطفاً لاگ سرور را بررسی کنید.",
            reply_markup=get_back_keyboard()
        )
    finally:
        # 🧹 پاکسازی قطعی فایل از دیسک — حتی در صورت خطا (تضمین باقی‌نماندن فایل)
        try:
            if os.path.exists(out_path):
                os.remove(out_path)
                logger.info(f"dl_session: فایل موقت سشن اکانت {acc_id} از دیسک پاک شد.")
        except OSError as e:
            logger.warning(f"dl_session: حذف فایل موقت سشن اکانت {acc_id} ناموفق بود: {e}")

@router.callback_query(F.data.startswith("show_creds_") & F.data.endswith("/"))
async def show_account_credentials(callback: types.CallbackQuery, session: AsyncSession) -> None:
    """
    🔓 نمایش اطلاعات حساس لاگین اکانت (فقط برای ادمین اصلی).
    مقادیر واقعی در این پیام دیکد و ارسال می‌شوند و بعد از ۶۰ ثانیه به صورت خودکار حذف می‌شوند.
    """
    if not _is_main_admin(callback.from_user):
        return await callback.answer("⛔️ فقط ادمین اصلی اجازه دسترسی به این بخش را دارد.", show_alert=True)
    
    acc_id_str = callback.data.replace("show_creds_", "").replace("/", "")
    if not acc_id_str.isdigit():
        return await callback.answer("⚠️ آیدی نامعتبر.", show_alert=True)
    
    acc_id = int(acc_id_str)

    try:
        stmt = select(Account).where(Account.id == acc_id)
        acc = await session.scalar(stmt)
    except Exception as e:
        await session.rollback()
        return await answer_callback_error(callback, report_db_error("اکانت", e), get_back_keyboard())

    if not acc:
        return await callback.answer("⚠️ این اکانت در سیستم وجود ندارد.", show_alert=True)

    await callback.answer()

    # ۱. استخراج User ID
    uid = acc.telegram_user_id
    if not uid and acc.session_string:
        try:
            # Fallback 1: تلاش برای استخراج مستقیم از ساختار StringSession
            decrypted = decrypt_session(acc.session_string)
            if decrypted:
                from utils.crypto import _parse_string_session
                _, _, uid_parsed = _parse_string_session(decrypted)
                if uid_parsed:
                    uid = uid_parsed
        except Exception:
            pass
            
    if not uid:
        # Fallback 2: استخراج از ورکر زنده در صورت اتصال
        client = worker_pool.get(acc.id)
        if client and client.is_connected:
            try:
                me = await client.get_me()
                uid = me.id
            except Exception:
                pass

    # ۲. رمزگشایی اطلاعات حساس (این مقادیر هرگز در لاگ چاپ نمی‌شوند)
    login_code = decrypt_session(acc.last_login_code) if acc.last_login_code else "نامشخص"
    two_step_password = decrypt_session(acc.two_step_password) if acc.two_step_password else "تنظیم نشده"
    
    # کامنت مستندات امنیتی: به دلیل اینکه پیام‌های تلگرام E2E (رمزنگاری سرتاسری) نیستند، 
    # برای حفظ امنیتِ Credentials، از TTL شصت ثانیه‌ای استفاده می‌کنیم تا ردپایی در چت ربات باقی نماند.
    msg_text = (
        "🔓 <b>اطلاعات محرمانه ورود</b>\n\n"
        f"📱 شماره: <code>{html.escape(str(acc.phone_number))}</code>\n"
        f"🆔 آیدی تلگرام: <code>{uid or 'نامشخص'}</code>\n\n"
        f"🔑 <b>رمز دوم (2FA):</b> <code>{html.escape(two_step_password)}</code>\n\n"
        f"💬 <b>کد آخرین ورود:</b> <code>{html.escape(login_code)}</code>\n"
        "<i>(توجه: کد ورود یک‌بارمصرف است و ممکن است منقضی شده باشد)</i>\n\n"
        "⏱ <i>امنیت: به دلیل عدم رمزنگاری E2E در تلگرام، این پیام تا ۶۰ ثانیه دیگر خودکار حذف می‌شود.</i>"
    )
    
    sent_msg = await callback.message.answer(msg_text)
    
    # حذف خودکار پیام پس از ۶۰ ثانیه
    asyncio.create_task(_delete_message_later(sent_msg, SESSION_FILE_MESSAGE_TTL))


# ۴. هندلر تغییر دسته‌بندی
@router.message(F.text.regexp(r"^/category_(\d+)$"))
async def change_account_category(message: types.Message, session: AsyncSession) -> None:
    match = re.match(r"^/category_(\d+)$", message.text)
    if not match:
        return

    acc_id = int(match.group(1))

    # 🛡 فاز ۴ (مشکل ۱): هر دو خواندن دیتابیس بدون حفاظ بودند
    try:
        stmt_acc = select(Account).where(Account.id == acc_id)
        acc = await session.scalar(stmt_acc)
    except Exception as e:
        await session.rollback()
        return await message.answer(
            report_db_error("اکانت", e),
            reply_markup=get_back_keyboard()
        )

    if not acc:
        return await message.answer(
            "⚠️ این اکانت در سیستم وجود ندارد.",
            reply_markup=get_back_keyboard()
        )

    try:
        stmt_cats = select(Category)
        result = await session.execute(stmt_cats)
        categories = result.scalars().all()
    except Exception as e:
        await session.rollback()
        return await message.answer(
            report_db_error("دسته‌بندی‌ها", e),
            reply_markup=get_back_keyboard()
        )

    if not categories:
        return await message.answer("⚠️ هیچ دسته‌بندی در سیستم یافت نشد.")

    builder = InlineKeyboardBuilder()

    for cat in categories:
        btn_text = f"✅ {cat.name}" if acc.category_id == cat.id else cat.name
        builder.button(text=btn_text, callback_data=f"setcat_{acc_id}_{cat.id}/")

    builder.adjust(2)

    builder.row(types.InlineKeyboardButton(text="❌ انصراف", callback_data="menu_list_accounts/"))

    await message.answer(
        f"🔄 <b>تغییر دسته‌بندی اکانت</b>\n\n"
        f"📱 شماره: <code>{acc.phone_number}</code>\n\n"
        f"لطفاً دسته‌بندی جدید را از لیست زیر انتخاب کنید:",
        reply_markup=builder.as_markup()
    )

# ==========================================
# 🟣 فاز ۷ (اصلاح اصلی این هندلر)
# ==========================================
@router.callback_query(F.data.startswith("setcat_") & F.data.endswith("/"))
async def process_category_change(callback: types.CallbackQuery, session: AsyncSession) -> None:
    parts = callback.data.replace("setcat_", "").replace("/", "").split("_")

    if len(parts) != 2 or not parts[0].isdigit() or not parts[1].isdigit():
        return await callback.answer("⚠️ دیتای نامعتبر.", show_alert=True)

    acc_id = int(parts[0])
    new_cat_id = int(parts[1])

    # 🛡 فاز ۴ (مشکل ۱ + باگ ترتیب): نام دسته «قبل از» commit خوانده می‌شود
    try:
        cat_stmt = select(Category).where(Category.id == new_cat_id)
        new_cat = await session.scalar(cat_stmt)

        stmt = update(Account).where(Account.id == acc_id).values(category_id=new_cat_id)
        await session.execute(stmt)
        await session.commit()
    except Exception as e:
        await session.rollback()
        return await answer_callback_error(
            callback, report_db_error("اکانت", e), get_accounts_return_keyboard()
        )

    cat_name = new_cat.name if new_cat else "نامشخص"

    await safe_edit_or_answer(
        callback.message,
        f"✅ <b>عملیات موفق</b>\n\n"
        f"اکانت مورد نظر با موفقیت به دسته‌بندی <b>{html.escape(cat_name)}</b> منتقل شد.",
        reply_markup=get_accounts_return_keyboard()
    )


# --- File: stats_handlers.py ---

# حوالی خط ۷۵۸:
@router.message(F.text.regexp(r"^/sessions_(\d+)$"))
async def terminate_account_sessions(message: types.Message, session: AsyncSession) -> None:
    # --- FIX M3: Add main-admin gate for destructive action ---
    if not _is_main_admin(message.from_user):
        if _MAIN_ADMIN_ID is None:
            return await message.answer(
                "⛔️ دسترسی محدود: شناسهٔ ادمین اصلی (ADMIN_ID) در تنظیمات پروژه یافت نشد."
            )
        return await message.answer("⛔️ فقط ادمین اصلی اجازهٔ استفاده از این دستور را دارد.")
    # ----------------------------------------------------------

    match = re.match(r"^/sessions_(\d+)$", message.text)
    if not match:
        return

    acc_id = int(match.group(1))

    # 🛡 فاز ۴ (مشکل ۱): خواندن اکانت بدون حفاظ بود
    try:
        stmt = select(Account).where(Account.id == acc_id)
        acc = await session.scalar(stmt)
    except Exception as e:
        await session.rollback()
        return await message.answer(
            report_db_error("اکانت", e),
            reply_markup=get_back_keyboard()
        )

    if not acc:
        return await message.answer(
            "⚠️ این اکانت در سیستم وجود ندارد.",
            reply_markup=get_back_keyboard()
        )

    client = worker_pool.get(acc_id)

    if not client or not client.is_connected:
        return await message.answer(
            f"⚠️ <b>عملیات ناموفق:</b>\n\n"
            f"اکانت شماره <code>{acc.phone_number}</code> در حال حاضر به سرور تلگرام متصل نیست (آفلاین). "
            f"برای خروج از سایر نشست‌ها، اکانت حتماً باید روشن و متصل باشد.",
            reply_markup=get_back_keyboard()
        )

    wait_msg = await send_loading_message(message, "⏳ در حال ارسال درخواست امنیتی به سرورهای تلگرام...")

    # 🛡 فاز ۴: فراخوانی terminate_other_sessions کاملاً بی‌حفاظ بود
    try:
        success = await terminate_other_sessions(client)
    except Exception as e:
        logger.error(f"Error terminating sessions for acc {acc_id}: {e}", exc_info=True)
        return await safe_edit_message(
            wait_msg,
            get_telegram_api_error_message(),
            reply_markup=get_back_keyboard()
        )

    if success:
        await safe_edit_message(
            wait_msg,
            f"✅ <b>عملیات موفق:</b>\n\n"
            f"تمامی نشست‌های دیگر برای اکانت <code>{acc.phone_number}</code> با موفقیت بسته شدند. "
            f"اکنون تنها سرورِ این ربات به اکانت دسترسی دارد.",
            reply_markup=get_back_keyboard()
        )
    else:
        await safe_edit_message(
            wait_msg,
            f"🛡 <b>محدودیت امنیتی تلگرام:</b>\n\n"
            f"شما به تازگی وارد اکانت <code>{acc.phone_number}</code> شده‌اید.\n"
            f"تلگرام برای جلوگیری از دسترسی غیرمجاز، اجازه خروج سایر نشست‌ها را تا <b>۲۴ ساعت پس از لاگین جدید</b> نمی‌دهد.\n\n"
            f"<i>لطفاً فردا مجدداً این دستور را امتحان کنید.</i>",
            reply_markup=get_back_keyboard()
        )

# ==========================================
# 13. HANDLER: Global Stats Dashboard
# ==========================================
@router.callback_query(F.data == "menu_stats/")
async def show_global_statistics(callback: types.CallbackQuery, session: AsyncSession, state: FSMContext) -> None:
    await safe_callback_answer(callback, "⏳ در حال محاسبه آمار...", show_alert=False)

    await cleanup_fsm_temp_files(state)
    await state.clear()

    from utils.timezone_helpers import get_current_tehran_time
    now_tehran = get_current_tehran_time()
    start_of_today = now_tehran.replace(hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc)
    start_of_yesterday = start_of_today - timedelta(days=1)

    try:
        async def get_order_stats(time_filter=None, end_time=None):
            total_stmt = select(func.count(Order.id))
            comp_stmt = select(func.count(Order.id)).where(Order.status == OrderStatus.completed)

            if time_filter:
                if end_time:
                    total_stmt = total_stmt.where(Order.created_at >= time_filter, Order.created_at < end_time)
                    comp_stmt = comp_stmt.where(Order.created_at >= time_filter, Order.created_at < end_time, Order.status == OrderStatus.completed)
                else:
                    total_stmt = total_stmt.where(Order.created_at >= time_filter)
                    comp_stmt = comp_stmt.where(Order.created_at >= time_filter, Order.status == OrderStatus.completed)

            total = await session.scalar(total_stmt) or 0
            comp = await session.scalar(comp_stmt) or 0
            return comp, total

        today_comp, today_total = await get_order_stats(start_of_today)
        yesterday_comp, yesterday_total = await get_order_stats(start_of_yesterday, start_of_today)
        all_comp, all_total = await get_order_stats()

        from workers.sender import _get_redis
        from utils.account_display import get_all_accounts_stats
        from workers.session_manager import worker_pool
        
        redis_client = _get_redis()
        stats, _ = await get_all_accounts_stats(session, redis_client, worker_pool)

        try:
            total_api = await session.scalar(select(func.count(APIKey.id))) or 0
        except Exception as e:
            logger.error(f"Error counting APIKeys: {e}")
            total_api = 0
            
        throughput_text = await render_throughput_stats(session)

    except Exception as e:
        await session.rollback()
        return await callback.message.answer(
            report_db_error("آمار", e),
            reply_markup=get_back_keyboard()
        )

    stats_text = (
        "📊 <b>آمار سیستم</b>\n\n"

        "🛍 <b>سفارشات:</b>\n"
        f"🟢 امروز: {today_comp} از {today_total}\n"
        f"⚪️ دیروز: {yesterday_comp} از {yesterday_total}\n"
        f"🔴 کل: {all_comp} از {all_total}\n\n"

        "🤖 <b>اکانت‌ها:</b>\n"
        f"💢 کل: {stats['TOTAL']}\n"
        f"✅ آماده ارسال: {stats['READY']}\n"
        f"🚫 مسدود/محدود: {stats['BLOCKED']}\n"
        f"💤 در حال استراحت: {stats['COOLDOWN']}\n"
        f"⚠️ قطع اتصال: {stats['DISCONNECTED']}\n"
        f"⛔️ ثبت نشده: {stats['NOT_REG']}\n\n"

        f"🔘 <b>تعداد APIها:</b> {total_api}\n\n"
        
        f"{throughput_text}"
    )

    builder = InlineKeyboardBuilder()
    builder.button(text="📈 تحویل‌سنجی سفارشات", callback_data="menu_delivery_stats/")
    builder.button(text="🔄 بروزرسانی", callback_data="menu_stats/")
    builder.button(text="🏛 منوی اصلی", callback_data="menu_home/")
    builder.adjust(1, 2)

    await safe_edit_message(callback.message, stats_text, reply_markup=builder.as_markup())

# کد جدید (به انتهای فایل stats_handlers.py اضافه شود)
@router.message(F.text.regexp(r"^/orderstats_(\d+)$"))
async def show_detailed_order_stats(message: types.Message, session: AsyncSession) -> None:
    """
    فاز ۳: گزارش‌گیری دقیق آمار هر سفارش و سهم هر اکانت مستقیماً از روی ردیف‌های OrderLog.
    این هندلر شمارنده‌های کش‌شده را نادیده می‌گیرد و واقعیت دیتابیس را بازسازی می‌کند.
    """
    # 🛡 دسترسی فقط برای ادمین اصلی
    if not _is_main_admin(message.from_user):
        if _MAIN_ADMIN_ID is None:
            return await message.answer("⛔️ دسترسی محدود: شناسهٔ ادمین اصلی تنظیم نشده است.")
        return await message.answer("⛔️ فقط ادمین اصلی اجازه دسترسی به این گزارش را دارد.")

    match = re.match(r"^/orderstats_(\d+)$", message.text)
    if not match:
        return
        
    order_id = int(match.group(1))
    
    wait_msg = await message.answer("⏳ در حال استخراج و بازسازی آمار مستقیم از OrderLog...")

    try:
        # ۱. بررسی وجود سفارش
        order = await session.get(Order, order_id)
        if not order:
            return await safe_edit_message(wait_msg, "⚠️ سفارشی با این شناسه در سیستم یافت نشد.")

        # ۲. محاسبه آمار کلی سفارش با Group By روی status
        status_stmt = (
            select(OrderLog.status, func.count(OrderLog.id))
            .where(OrderLog.order_id == order_id)
            .group_by(OrderLog.status)
        )
        status_rows = (await session.execute(status_stmt)).all()

        stats = {"success": 0, "error": 0, "flood": 0, "restricted": 0, "partial": 0}
        total_logs = 0
        for st, cnt in status_rows:
            # مپ کردن مقادیر برای اطمینان
            safe_st = st if st in stats else "error"
            stats[safe_st] += cnt
            total_logs += cnt

        # ۳. محاسبه آمار تفکیکی هر اکانت با Group By روی account_id و status
        acc_stmt = (
            select(OrderLog.account_id, OrderLog.status, func.count(OrderLog.id))
            .where(OrderLog.order_id == order_id)
            .group_by(OrderLog.account_id, OrderLog.status)
        )
        acc_rows = (await session.execute(acc_stmt)).all()

        acc_stats = {}
        for acc_id, st, cnt in acc_rows:
            if acc_id not in acc_stats:
                acc_stats[acc_id] = {"success": 0, "error": 0, "flood": 0, "restricted": 0, "partial": 0, "total": 0}
            safe_st = st if st in acc_stats[acc_id] else "error"
            acc_stats[acc_id][safe_st] += cnt
            acc_stats[acc_id]["total"] += cnt

        # ۴. قالب‌بندی گزارش خروجی
        status_val = order.status.value if hasattr(order.status, 'value') else order.status
        
        report = (
            f"📊 <b>گزارش دقیق و بازسازی‌شده سفارش #{order_id}</b>\n\n"
            f"🔸 <b>وضعیت فعلی سفارش:</b> <code>{status_val}</code>\n"
            f"🔢 <b>مجموع تلاش‌های ثبت‌شده:</b> <b>{total_logs}</b>\n\n"
            f"📈 <b>آمار کل (مبتنی بر لاگ):</b>\n"
            f"✅ موفق کامل: <code>{stats['success']}</code>\n"
            f"⚠️ موفق جزئی: <code>{stats['partial']}</code>\n"
            f"❌ خطای ارسال: <code>{stats['error']}</code>\n"
            f"⏳ محدودیت (FloodWait): <code>{stats['flood']}</code>\n"
            f"🚫 محدودیت اسپم (PeerFlood): <code>{stats['restricted']}</code>\n\n"
            f"🤖 <b>تفکیک عملکرد اکانت‌ها (Workers):</b>\n"
        )

        if not acc_stats:
            report += "<i>هیچ رکوردی برای این سفارش در لاگ ثبت نشده است.</i>"
        else:
            for acc_id, ast in acc_stats.items():
                acc_label = f"user_{acc_id}/" if acc_id else "سیستم"
                report += (
                    f"▫️ <b>{acc_label}</b> ➜ "
                    f"موفق: <code>{ast['success']}</code> | "
                    f"جزئی: <code>{ast['partial']}</code> | "
                    f"خطا: <code>{ast['error']}</code> | "
                    f"محدودیت: <code>{ast['flood']}</code> | "
                    f"اسپم: <code>{ast['restricted']}</code> "
                    f"(کل: <b>{ast['total']}</b>)\n"
                )

        await safe_edit_message(wait_msg, report)

    except Exception as e:
        await session.rollback()
        logger.error(f"Error generating detailed order stats for #{order_id}: {e}", exc_info=True)
        await safe_edit_message(wait_msg, f"❌ خطا در تولید گزارش: {e}")