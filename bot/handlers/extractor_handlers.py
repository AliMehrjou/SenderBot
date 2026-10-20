import asyncio
import html
import logging
import os
import random
import re
import string
import uuid
from datetime import datetime
from typing import Optional
import re
import aiofiles

from aiogram import Router, types, F, Bot
from aiogram.fsm.context import FSMContext
from aiogram.types import FSInputFile
from aiogram.utils.keyboard import InlineKeyboardBuilder
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from database.models import Order, OrderLog, OrderStatus
from workers.session_manager import worker_pool
from bot.states.extractor_fsm import ExtractorStates
from bot.keyboards.cancel import get_cancel_keyboard, with_cancel_hint
from bot.keyboards.main_menu import get_main_menu_button, get_main_menu_keyboard, get_main_menu_reply_keyboard
from utils.fsm_cleanup import cleanup_fsm_temp_files
from config import config
from database.models import Order, OrderLog, OrderStatus
from workers.session_manager import worker_pool

# 📄 فاز ۵: helperهای استاندارد پروژه (ویرایش امن + مدیریت خطا)
from utils.safe_edit import safe_edit_or_answer
from utils.error_messages import report_db_error
from utils.telegram_helpers import (
    answer_callback_error,
    safe_callback_answer,
    safe_edit_message,
)

# 📄 فاز ۵ (صفحه‌بندی استاندارد): زیرساخت مشترک paginate (فاز ۱)
from utils.pagination import (
    PAGINATION_SIZE,
    add_pagination_nav_row,
    calculate_total_pages,
    clamp_page,
    get_page_offset,
    parse_page_from_callback,
)

logger = logging.getLogger(__name__)

router = Router(name="extractor_handlers_router")

# اطمینان از وجود پوشه خروجی
os.makedirs("exports", exist_ok=True)


# ==========================================
# 📄 فاز ۵: ثابت‌ها و هلپرهای داشبورد/لیست استخراج
# ==========================================

#: نشان فارسی وضعیت سفارش استخراج
EXTRACTION_STATUS_BADGES: dict = {
    OrderStatus.pending: "🕒 در صف انتظار",
    OrderStatus.running: "🚀 در حال اجرا",
    OrderStatus.completed: "✅ تکمیل شده",
    OrderStatus.error: "🛑 لغو شده",
}
# پوشش هر دو حالت Enum و رشته (بسته به نوع ستون status در دیتابیس)
EXTRACTION_STATUS_BADGES.update({
    getattr(s, "value", s): label for s, label in list(EXTRACTION_STATUS_BADGES.items())
})

#: برچسب فارسی الگوریتم استخراج (filter_type سفارش‌های extract)
#: برچسب فارسی الگوریتم استخراج (filter_type سفارش‌های extract)
EXTRACTION_TYPE_LABELS: dict = {
    "users": "👥 کاربران (ساده)",
    "messages": "💬 پیام‌ها (تارگت‌های فعال)",
    "golden": "🌟 طلایی (دقیق و تقاطعی)",
    "online": "🟢 فقط آنلاین",
}

EXT_TRACKING_CODE_PATTERN = re.compile(r"EXT-[A-Z0-9]{1,20}")

EXTRACTION_STRATEGY_TEXT = (
    "⚙️ <b>سفارش استخراج — استراتژی را انتخاب کنید:</b>\n\n"
    "👥 <b>همه اعضا:</b> لیست اعضای گروه (سقف ۱۰,۰۰۰)\n"
    "   ⚠️ اگر ادمین گروه لیست اعضا را مخفی کرده، این گزینه نتیجه نمی‌دهد.\n\n"
    "💬 <b>فرستندگان پیام:</b> کاربران فعال در تاریخچه اخیر\n"
    "   ✅ حتی اگر لیست اعضا مخفی باشد، این گزینه کار می‌کند.\n\n"
    "🥇 <b>طلایی:</b> تقاطع اعضا و فعالیت واقعی در پیام‌ها\n"
    "   ⚠️ نیاز به دسترسی به لیست اعضا دارد.\n\n"
    "🟢 <b>فقط آنلاین:</b> فقط اعضای آنلاین/اخیراً فعال\n"
    "   ⚠️ نیاز به دسترسی به لیست اعضا دارد.\n\n"
    "💡 <b>توصیه:</b> اگر مطمئن نیستید، ابتدا «همه اعضا» را امتحان کنید. اگر نتیجه نداد، ربات به‌طور خودکار «فرستندگان پیام» را پیشنهاد می‌دهد."
)


def get_extraction_status_badge(status) -> str:
    """📄 فاز ۵: نشان فارسی وضعیت سفارش استخراج (مقاوم در برابر Enum/str)."""
    badge = EXTRACTION_STATUS_BADGES.get(status)
    if badge is None and status is not None:
        badge = EXTRACTION_STATUS_BADGES.get(str(status), "❔ نامشخص")
    return badge or "❔ نامشخص"

def get_wizard_filter_label(filter_str: str) -> str:
    """تبدیل فرمت قدیمی و JSON جدید به لیبل فارسی خوانا"""
    if not filter_str:
        return "❔ نامشخص"
    
    # اگر فرمت JSON ویزارد جدید باشد
    if filter_str.startswith("{"):
        import json
        try:
            filters = json.loads(filter_str)
            labels = []
            if filters.get("online_only"): labels.append("آنلاین")
            if filters.get("has_photo"): labels.append("عکس‌دار")
            if filters.get("no_bots"): labels.append("بدون ربات")
            return " + ".join(labels) if labels else "بدون فیلتر"
        except:
            return "فیلتر سفارشی"
            
    # اگر فرمت قدیمی باشد (users, messages, golden)
    return EXTRACTION_TYPE_LABELS.get(filter_str, "❔ نامشخص")


# ==========================================
# BACKGROUND TASK: اجرای سناریوی استخراج در پس‌زمینه
# ⚠️ نکته: اگر در فایل واقعی شما کدی زیر این عنوان وجود دارد (بخش BACKGROUND TASK
# در نسخهٔ ارسالی خالی بود)، آن کد را عیناً در همین جایگاه حفظ کنید.
# ==========================================

# ==========================================
# ⚙️ جریان استخراج کلاسیک (لینک + پیام‌های ربات)
# ==========================================
from aiogram.exceptions import TelegramBadRequest
from contextlib import suppress
from bot.handlers.order_handlers import get_extract_strategy_keyboard, FILTER_BY_TEXT


@router.callback_query(F.data == "menu_analysis/")
async def enter_classic_extraction(callback: types.CallbackQuery, state: FSMContext, session: AsyncSession) -> None:
    await safe_callback_answer(callback)
    await cleanup_fsm_temp_files(state)
    await state.clear()

    # 🟢 گارد امنیتی: بررسی وضعیت ورکرها قبل از شروع ثبت سفارش استخراج
    from database.models import Account, AccountStatus
    from sqlalchemy import or_, select
    from datetime import datetime, timezone
    from contextlib import suppress
    from aiogram.exceptions import TelegramBadRequest
    
    connected_ids = [acc_id for acc_id, c in worker_pool.items() if getattr(c, "is_connected", False)]
    available_workers = 0
    
    if connected_ids:
        now_naive = datetime.now(timezone.utc).replace(tzinfo=None)
        # 🟢 تغییر: دریافت آی‌دی اکانت‌های معتبر
        stmt = select(Account.id).where(
            Account.id.in_(connected_ids),
            Account.is_banned == False,
            Account.status == AccountStatus.active,
            or_(Account.flood_wait_until.is_(None), Account.flood_wait_until <= now_naive),
            or_(Account.restricted_until.is_(None), Account.restricted_until <= now_naive)
        )
        valid_account_ids = (await session.scalars(stmt)).all()
        
        if valid_account_ids:
            try:
                # 🟢 کسر اکانت‌های در حال استراحت
                from workers.sender import _get_redis
                redis_client = _get_redis()
                pipe = redis_client.pipeline()
                for aid in valid_account_ids:
                    pipe.exists(f"chunk_cooldown:{aid}")
                cooldown_results = await pipe.execute()
                
                available_workers = len(valid_account_ids) - sum(1 for res in cooldown_results if res)
            except Exception as e:
                import logging
                logging.getLogger(__name__).error(f"Redis check failed in extractor preflight: {e}")
                available_workers = len(valid_account_ids)
        
    if available_workers <= 0:
        with suppress(TelegramBadRequest):
            await callback.message.edit_reply_markup(reply_markup=None)
            
        return await safe_edit_or_answer(
            callback.message,
            "❌ <b>امکان ثبت سفارش آنالیز وجود ندارد</b>\n\n"
            "در حال حاضر هیچ اکانتِ سالم و آزادی در سیستم یافت نشد.\n"
            "(تمام ورکرها ممکن است در حال استراحت دوره‌ای باشند، یا مسدود و دارای محدودیت باشند)\n\n"
            "<i>لطفاً اکانت جدیدی اضافه کنید یا منتظر پایان استراحت اکانت‌های فعلی بمانید.</i>",
            reply_markup=get_main_menu_keyboard()
        )

    # ۱. تنظیم نوع سفارش و هدایت به استیت دریافت لینک
    await state.update_data(order_type="extract")
    await state.set_state(ExtractorStates.waiting_for_link)
    
    with suppress(TelegramBadRequest):
        await callback.message.edit_reply_markup(reply_markup=None)

    text = (
        "🌐 <b>سفارش استخراج (آنالیز)</b>\n\n"
        "لطفاً لینک گروه مورد نظر خود را ارسال کنید:\n"
        "<i>(مثال: t.me/groupname یا @groupname)</i>"
    )
    
    await callback.message.answer(
        with_cancel_hint(text),
        reply_markup=get_cancel_keyboard() 
    )


@router.message(ExtractorStates.waiting_for_link, F.text)
async def classic_process_link(message: types.Message, state: FSMContext) -> None:
    link = message.text.strip()
    from utils.telegram_helpers import parse_target_links
    valid_links, _ = parse_target_links(link)

    if not valid_links or len(valid_links) > 1:
        return await message.answer(
            with_cancel_hint("⚠️ لطفاً دقیقاً **یک لینک گروه معتبر** ارسال کنید."),
            reply_markup=get_cancel_keyboard()
        )

    # ۲. ذخیره لینک و نمایش کیبورد متنی استراتژی‌ها
    await state.update_data(target_link=valid_links[0])
    await state.set_state(ExtractorStates.waiting_for_strategy)
    
    await message.answer(
        with_cancel_hint(EXTRACTION_STRATEGY_TEXT),
        reply_markup=get_extract_strategy_keyboard()
    )

@router.message(ExtractorStates.waiting_for_strategy, F.text.in_(FILTER_BY_TEXT))
async def classic_confirm_and_start(message: types.Message, state: FSMContext, session: AsyncSession) -> None:
    # ۳. نگاشت انتخاب کاربر به استراتژی استخراج (مثل "messages", "users", و ...)
    filter_type = FILTER_BY_TEXT[message.text.strip()]
    
    data = await state.get_data()
    target_link = data.get("target_link")
    await state.clear()

    import string
    import random
    from config import config
    
    chars = string.ascii_uppercase + string.digits
    tracking_code = f"EXT-{''.join(random.choices(chars, k=6))}"

    # ۴. ساخت و درج سفارش در دیتابیس با مقدار استراتژی کلاسیک
    new_order = Order(
        order_type="extract",
        target_data=target_link,
        filter_type=filter_type, 
        status=OrderStatus.pending,
        tracking_code=tracking_code,
        user_id=message.from_user.id,
        is_approved=False
    )
    session.add(new_order)
    await session.commit()

    type_fa = EXTRACTION_TYPE_LABELS.get(filter_type, "نامشخص")

    # --- پیام تایید نهایی برای کاربر ---
    text = (
        f"✅ <b>سفارش استخراج در صف قرار گرفت!</b>\n\n"
        f"🆔 کد رهگیری: <code>{tracking_code}</code>\n"
        f"🎯 تارگت: {target_link}\n"
        f"⚙️ استراتژی: <b>{type_fa}</b>\n\n"
        "⏳ سفارش در انتظار تایید ادمین است؛ پس از تایید به‌طور خودکار آغاز می‌شود."
    )
    
    # جایگزینی کیبورد ربات با کیبورد اصلی
    await message.answer(text, reply_markup=get_main_menu_reply_keyboard())
    
    # --- ارسال پیام تایید/رد برای ادمین ---
    admin_builder = InlineKeyboardBuilder()
    admin_builder.button(text="✅ تایید و شروع", callback_data=f"approve_order_{new_order.id}/")
    admin_builder.button(text="❌ رد سفارش", callback_data=f"reject_order_{new_order.id}/")
    admin_builder.adjust(2)
    
    try:
        from utils.admin_broadcast import broadcast_to_admins_with_keyboard
        await broadcast_to_admins_with_keyboard(
            bot=message.bot,
            text=(
                f"🛎 <b>سفارش استخراج جدید نیازمند تایید</b>\n\n"
                f"🆔 شناسه: <code>{new_order.id}</code>\n"
                f"🎟 کد رهگیری: <code>{new_order.tracking_code}</code>\n"
                f"📊 استراتژی: <b>{type_fa}</b>\n"
                f"🎯 تارگت: <code>{html.escape(target_link)}</code>\n\n"
                f"<i>لطفاً جهت ورود این سفارش به صف اجرا آن را تایید کنید.</i>"
            ),
            keyboard=admin_builder.as_markup()
        )
    except Exception as e:
        logger.error(f"Failed to send extraction approval request to admin: {e}")

@router.message(ExtractorStates.waiting_for_strategy)
async def classic_strategy_fallback(message: types.Message) -> None:
    await message.answer(
        with_cancel_hint("⚠️ لطفاً استراتژی استخراج را از طریق دکمه‌های کیبورد پایین صفحه انتخاب کنید."),
        reply_markup=get_extract_strategy_keyboard()
    )

# ==========================================
# 📄 فاز ۵: لیست سفارشات استخراج (صفحه‌بندی استاندارد)
# ==========================================
async def render_extraction_orders_list(
    callback: types.CallbackQuery,
    session: AsyncSession,
    state: Optional[FSMContext] = None,
    page: int = 1,
    send_new: bool = False,
) -> None:
    try:
        total_count = await session.scalar(
            select(func.count(Order.id)).where(Order.order_type == "extract")
        ) or 0
        total_pages = calculate_total_pages(total_count)
        page = clamp_page(page, total_pages)
        offset = get_page_offset(page)

        orders = (
            await session.scalars(
                select(Order)
                .where(Order.order_type == "extract")
                .order_by(Order.id.desc())
                .offset(offset)
                .limit(PAGINATION_SIZE)
            )
        ).all()

        # 🟢 فیکس فاز دوم: منطق اشتباهِ شمردن OrderLog از این قسمت کاملاً حذف شد.
        # تعداد استخراجی فقط زمانی معتبر است که ورکر آن را صراحتاً در extracted_count ثبت کرده باشد.
    except Exception as e:
        await session.rollback()
        return await answer_callback_error(
            callback, report_db_error("سفارشات استخراج", e), get_main_menu_button()
        )

    if state is not None:
        await state.update_data(ext_list_page=page)

    builder = InlineKeyboardBuilder()

    if total_count == 0:
        builder.row(types.InlineKeyboardButton(text="🔙 بازگشت به آنالیز", callback_data="menu_analysis/"))
        builder.row(types.InlineKeyboardButton(text="🏛 منوی اصلی", callback_data="menu_home/"))
        text_empty = (
            "📋 <b>لیست سفارشات استخراج</b>\n\n"
            "⚠️ موردی یافت نشد.\n\n"
            "برای شروع، از منوی آنالیز یکی از انواع استخراج را انتخاب کنید:"
        )
        if send_new:
            return await callback.message.answer(text_empty, reply_markup=builder.as_markup())
        return await safe_edit_or_answer(callback.message, text_empty, reply_markup=builder.as_markup())

    text = (
        "📋 <b>لیست سفارشات استخراج</b>\n"
        f"🔢 مجموع: <b>{total_count}</b> سفارش\n"
        f"📄 صفحهٔ <b>{page}</b> از <b>{total_pages}</b>\n\n"
    )

    for idx, order in enumerate(orders, start=offset + 1):
        badge = get_extraction_status_badge(order.status)
        type_label = get_wizard_filter_label(order.filter_type)
        # 🟢 فیکس فاز دوم: اگر ثبت نشده باشد (درحال اجرا)، صرفا 0 نشان داده می‌شود
        extracted = order.extracted_count or 0 
        created_date = order.created_at.strftime("%Y/%m/%d") if order.created_at else "نامشخص"

        code = order.tracking_code
        if code:
            text += (
                f"<b>{idx}.</b> 🎟 <code>{code}</code> · {badge}\n"
                f"🧮 {type_label} · 👥 {extracted} استخراج‌شده · 📅 {created_date}\n\n"
            )
            builder.button(text=f"📊 {code}", callback_data=f"ext_dashboard_{code}/")
        else:
            text += (
                f"<b>{idx}.</b> 🆔 <code>#{order.id}</code> · {badge}\n"
                f"🧮 {type_label} · 👥 {extracted} استخراج‌شده · 📅 {created_date}\n"
                f"<i>(بدون کد رهگیری — داشبورد: /gtg_{order.id})</i>\n\n"
            )

    builder.adjust(2)

    text += "👇 برای مشاهدهٔ داشبورد هر استخراج، روی دکمهٔ مربوطه کلیک کنید:"

    add_pagination_nav_row(builder, page, total_pages, callback_prefix="ext_list_")
    builder.row(types.InlineKeyboardButton(text="🔄 بروزرسانی", callback_data=f"ext_list_page_{page}/"))
    builder.row(
        types.InlineKeyboardButton(text="🔙 بازگشت به آنالیز", callback_data="menu_analysis/"),
        types.InlineKeyboardButton(text="🏛 منوی اصلی", callback_data="menu_home/"),
    )

    if send_new:
        await callback.message.answer(text, reply_markup=builder.as_markup())
    else:
        await safe_edit_or_answer(callback.message, text, reply_markup=builder.as_markup())

@router.callback_query(F.data.startswith("ext_list_page_"))
async def extraction_orders_list_handler(callback: types.CallbackQuery, state: FSMContext, session: AsyncSession) -> None:
    """
    📄 فاز ۵: هندلر مشترک ورود به لیست، «صفحه بعد/قبل» و «🔄 بروزرسانی» —
    دکمهٔ بروزرسانی دقیقاً همان callback صفحهٔ فعلی را صدا می‌زند.
    """
    await safe_callback_answer(callback)

    # 🟣 الگوی استاندارد: خروج از state آنالیز هنگام ورود به لیست
    await cleanup_fsm_temp_files(state)
    await state.clear()

    page = parse_page_from_callback(callback.data)
    await render_extraction_orders_list(callback, session, state=state, page=page)


async def build_extraction_dashboard_view(
    session: AsyncSession, tracking_code: str, back_page: int = 1
) -> tuple[Optional[str], Optional[types.InlineKeyboardMarkup]]:
    try:
        extraction = await session.scalar(
            select(Order).where(
                Order.tracking_code == tracking_code,
                Order.order_type == "extract",
            )
        )
        if not extraction:
            return None, None

        extracted_count = extraction.extracted_count or 0

    except Exception as e:
        logger.error(f"Error building extraction dashboard for {tracking_code}: {e}", exc_info=True)
        return None, None

    # 🟢 تشخیص وضعیت دقیق‌تر برای داشبورد استخراج
    if extraction.status == OrderStatus.pending:
        if extraction.is_approved:
            badge = "🕒 در صف انتظار دیسپچ"
        else:
            badge = "⏳ در انتظار تایید ادمین"
    elif extraction.status == OrderStatus.running:
        badge = "🚀 در حال اجرا"
    elif extraction.status == OrderStatus.completed:
        badge = "✅ تکمیل شده"
        if extraction.reject_reason == "fallback_messages":
            badge = "✅ تکمیل شده (فال‌بک پیام‌ها)"
    else:
        if not extraction.is_approved and extraction.reject_reason:
            badge = f"❌ رد شده\n💬 علت: <i>{html.escape(extraction.reject_reason)}</i>"
        else:
            badge = "🛑 متوقف/لغو شده"

    type_label = get_wizard_filter_label(extraction.filter_type)
    source_display = html.escape(extraction.target_data or "نامشخص")
    created_date = extraction.created_at.strftime("%Y/%m/%d %H:%M") if extraction.created_at else "نامشخص"

    text = (
        "📊 <b>داشبورد استخراج</b>\n\n"
        f"🔑 کد پیگیری: <code>{tracking_code}</code>\n"
        f"🆔 شناسه سفارش: <code>#{extraction.id}</code>\n"
        f"📍 منبع: <code>{source_display}</code>\n"
        f"🧮 الگوریتم: {type_label}\n"
        f"📈 وضعیت: {badge}\n"
        f"👥 تعداد استخراج‌شده: <b>{extracted_count}</b>\n"
        f"🕐 شروع: {created_date}\n"
    )

    if extraction.status == OrderStatus.completed:
        text += "\n📁 فایل نتیجه آماده است — برای دریافت آن از دکمهٔ «📥 خروجی» استفاده کنید.\n"
        if extraction.reject_reason == "fallback_messages":
            text += "\n⚠️ <i>توجه: به دلیل مخفی بودن لیست اعضای این گروه، استخراج به صورت خودکار از میان «پیام‌دهندگان اخیر» انجام شد تا سفارش با موفقیت تکمیل گردد.</i>\n"
    elif extraction.status == OrderStatus.error and extracted_count > 0:
        text += "\n📁 عملیات متوقف شد، اما فایل استخراج تا این لحظه آماده است.\n"

    builder = InlineKeyboardBuilder()
    
    # 🟢 دکمه‌های تایید برای وضعیت در انتظار تایید
    if extraction.status == OrderStatus.pending and not extraction.is_approved:
        builder.button(text="✅ تایید و شروع", callback_data=f"approve_order_{extraction.id}/")
        builder.button(text="❌ رد سفارش", callback_data=f"reject_order_{extraction.id}/")

    builder.button(text="🔄 بروزرسانی", callback_data=f"ext_dashboard_{tracking_code}/")
    
    # 🟢 دکمه توقف برای تمامی عملیات‌های فعال (حتی قبل از تایید و در صف انتظار)
    if extraction.status in [OrderStatus.pending, OrderStatus.running]:
        builder.button(text="🛑 توقف عملیات / لغو", callback_data=f"cancel_order_{extraction.id}/")
    
    # 🟢 دکمه خروجی برای عملیات تمام شده یا متوقف شده (در صورت وجود دیتا)
    if extraction.status in [OrderStatus.completed, OrderStatus.error] and getattr(extraction, "media_path", None):
        builder.button(text="📥 خروجی", callback_data=f"export_order_{extraction.id}/")
    
    
    builder.adjust(1)

    return text, builder.as_markup()


@router.callback_query(F.data.startswith("ext_dashboard_new_") & F.data.endswith("/"))
async def show_extraction_dashboard_new(callback: types.CallbackQuery, state: FSMContext, session: AsyncSession) -> None:
    await safe_callback_answer(callback)
    tracking_code = callback.data.replace("ext_dashboard_new_", "").replace("/", "")

    if not EXT_TRACKING_CODE_PATTERN.fullmatch(tracking_code):
        return await callback.message.answer("⚠️ کد پیگیری نامعتبر است.")
    
    fsm_data = await state.get_data()
    back_page = fsm_data.get("ext_list_page", 1)

    try:
        text, markup = await build_extraction_dashboard_view(session, tracking_code, back_page)
    except Exception as e:
        await session.rollback()
        return await callback.message.answer(report_db_error("سفارش استخراج", e))

    if not text:
        return await safe_callback_answer(callback, "❌ سفارش استخراج یافت نشد.", show_alert=True)

    await callback.message.answer(
        text,
        reply_markup=markup,
        link_preview_options=types.LinkPreviewOptions(is_disabled=True),
    )

@router.callback_query(F.data.startswith("ext_list_new_page_"))
async def extraction_orders_list_new_handler(callback: types.CallbackQuery, state: FSMContext, session: AsyncSession) -> None:
    await safe_callback_answer(callback)
    await cleanup_fsm_temp_files(state)
    await state.clear()
    page = parse_page_from_callback(callback.data.replace("_new", ""))
    await render_extraction_orders_list(callback, session, state=state, page=page, send_new=True)

@router.callback_query(F.data.startswith("ext_dashboard_") & F.data.endswith("/"))
async def show_extraction_dashboard(callback: types.CallbackQuery, state: FSMContext, session: AsyncSession) -> None:
    """
    📊 فاز ۵/۶: داشبورد زندهٔ سفارش استخراج با قابلیت بروزرسانی خودکار در پیام جدید
    """
    await safe_callback_answer(callback)

    tracking_code = callback.data.replace("ext_dashboard_", "").replace("/", "")
    # هندل کردن دکمه‌های قدیمی ثبت‌شده که ممکن است هنوز new_ داشته باشند
    if tracking_code.startswith("new_"):
        tracking_code = tracking_code[4:]

    if not EXT_TRACKING_CODE_PATTERN.fullmatch(tracking_code):
        return await safe_edit_message(callback.message, "⚠️ کد پیگیری نامعتبر است.")
    
    fsm_data = await state.get_data()
    back_page = fsm_data.get("ext_list_page", 1)

    try:
        order = await session.scalar(
            select(Order).where(
                Order.tracking_code == tracking_code,
                Order.order_type == "extract",
            )
        )
        if not order:
            return await safe_edit_message(callback.message, "❌ سفارش استخراج یافت نشد.")

        text, markup = await build_extraction_dashboard_view(session, tracking_code, back_page)
    except Exception as e:
        await session.rollback()
        return await answer_callback_error(
            callback, report_db_error("سفارش استخراج", e), get_main_menu_button()
        )

    if not text:
        return await safe_edit_message(callback.message, "❌ سفارش استخراج یافت نشد.")

    # 🟢 پاک کردن پیام قدیمی برای ارسال داشبورد در پیام جدید طبق نیاز کارفرما
    from contextlib import suppress
    with suppress(Exception):
        await callback.message.delete()
        
    is_active = order.status in [OrderStatus.pending, OrderStatus.running]
    indicator = "\n\n🟢 <b>لایو</b> (به‌روزرسانی خودکار فعال)" if is_active else ""

    sent_message = await callback.message.answer(
        text + indicator,
        reply_markup=markup,
        link_preview_options=types.LinkPreviewOptions(is_disabled=True),
    )

    # 🟢 اتصال به سیستم تسک‌های پس‌زمینهٔ داشبورد /gtg_ برای رفرش خودکار
    if is_active:
        from bot.handlers.order_handlers import _active_refresh_tasks, _auto_refresh_dashboard_task
        task_key = f"{sent_message.chat.id}_{sent_message.message_id}"
        _active_refresh_tasks[task_key] = asyncio.create_task(
            _auto_refresh_dashboard_task(sent_message, order.id, callback.bot)
        )


# ==========================================
# 📄 فاز ۵/۶: هندلر درخواست استخراج از طریق پیام‌ها (Fallback دستی)
# ==========================================
@router.callback_query(F.data.startswith("new_extract_messages_") & F.data.endswith("/"))
async def confirm_new_extract_messages(callback: types.CallbackQuery, session: AsyncSession) -> None:
    await safe_callback_answer(callback)
    order_id_str = callback.data.replace("new_extract_messages_", "").replace("/", "")
    
    if not order_id_str.isdigit():
        return await safe_edit_message(callback.message, "❌ شناسه سفارش نامعتبر است.")
        
    order_id = int(order_id_str)
    
    old_order = await session.get(Order, order_id)
    if not old_order:
        return await callback.message.answer("❌ سفارش قبلی یافت نشد.")
        
    import string
    import random
    from config import config
    
    chars = string.ascii_uppercase + string.digits
    tracking_code = f"EXT-{''.join(random.choices(chars, k=6))}"
    
    new_order = Order(
        order_type="extract",
        target_data=old_order.target_data,
        filter_type="messages",
        status=OrderStatus.pending,
        tracking_code=tracking_code,
        user_id=callback.from_user.id,
        is_approved=False,
        speed_mode=old_order.speed_mode,
    )
    session.add(new_order)
    await session.commit()
    
    await callback.message.answer(
        f"✅ <b>سفارش استخراج جایگزین ثبت شد!</b>\n\n"
        f"🆔 کد رهگیری: <code>{tracking_code}</code>\n"
        f"🎯 تارگت: {html.escape(old_order.target_data or '')}\n"
        f"⚙️ استراتژی: <b>پیام‌ها (messages)</b>\n\n"
        "⏳ سفارش در انتظار تایید ادمین است."
    )
    
    admin_builder = InlineKeyboardBuilder()
    admin_builder.button(text="✅ تایید و شروع", callback_data=f"approve_order_{new_order.id}/")
    admin_builder.button(text="❌ رد سفارش", callback_data=f"reject_order_{new_order.id}/")
    admin_builder.adjust(2)
    
    try:
        from utils.admin_broadcast import broadcast_to_admins_with_keyboard
        await broadcast_to_admins_with_keyboard(
            bot=callback.bot,
            text=(
                f"🛎 <b>سفارش استخراج جدید (فال‌بک دستی) نیازمند تایید</b>\n\n"
                f"🆔 شناسه: <code>{new_order.id}</code>\n"
                f"🎟 کد رهگیری: <code>{new_order.tracking_code}</code>\n"
                f"📊 استراتژی: <b>پیام‌ها</b>\n"
                f"🎯 تارگت: <code>{html.escape(old_order.target_data or '')}</code>\n\n"
                f"<i>این سفارش به عنوان جایگزین سفارش مخفیِ ({old_order.id}) ثبت شده است.</i>"
            ),
            keyboard=admin_builder.as_markup()
        )
    except Exception as e:
        logger.error(f"Failed to send manual fallback extraction approval request to admin: {e}")
        
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass

