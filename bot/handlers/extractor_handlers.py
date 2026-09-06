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
from bot.keyboards.main_menu import get_main_menu_button, get_main_menu_keyboard
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
EXTRACTION_TYPE_LABELS: dict = {
    "users": "👥 کاربران (ساده)",
    "messages": "💬 پیام‌ها (تارگت‌های فعال)",
    "golden": "🌟 طلایی (دقیق و تقاطعی)",
}
EXT_TRACKING_CODE_PATTERN = re.compile(r"EXT-[A-Z0-9]{1,20}")


def get_extraction_status_badge(status) -> str:
    """📄 فاز ۵: نشان فارسی وضعیت سفارش استخراج (مقاوم در برابر Enum/str)."""
    badge = EXTRACTION_STATUS_BADGES.get(status)
    if badge is None and status is not None:
        badge = EXTRACTION_STATUS_BADGES.get(str(status), "❔ نامشخص")
    return badge or "❔ نامشخص"


# ==========================================
# BACKGROUND TASK: اجرای سناریوی استخراج در پس‌زمینه
# ⚠️ نکته: اگر در فایل واقعی شما کدی زیر این عنوان وجود دارد (بخش BACKGROUND TASK
# در نسخهٔ ارسالی خالی بود)، آن کد را عیناً در همین جایگاه حفظ کنید.
# ==========================================

# ==========================================
# UI HANDLERS: تعامل با ادمین در ربات (منوی آنالیز)
# ==========================================
@router.callback_query(F.data == "menu_analysis/")
async def enter_analysis_menu(callback: types.CallbackQuery, state: FSMContext) -> None:
    await safe_callback_answer(callback)

    # 🟣 فاز ۱: پاکسازی امن state و فایل‌های موقت فلوی قبلی
    # (اگر کاربر وسط فلوی دیگری مثل ثبت سفارش بوده باشد، فایل‌های موقتش
    # orphan نمی‌شوند و بلافاصله از روی هارد پاک می‌شوند)
    await cleanup_fsm_temp_files(state)
    await state.clear()

    # تنظیم استیت روی انتظار برای انتخاب نوع آنالیز
    await state.set_state(ExtractorStates.waiting_for_analysis_type)

    # 📄 فاز ۵: دکمهٔ «📋 سفارشات استخراج» اضافه شد — راه پیگیری سفارش‌های
    # استخراج بدون نیاز به حفظ کردن کد رهگیری EXT-XXXXXX
    builder = InlineKeyboardBuilder()
    builder.button(text="👥 کاربران", callback_data="analysis_users/")
    builder.button(text="💬 پیام‌ها", callback_data="analysis_messages/")
    builder.button(text="🌟 استخراج طلایی", callback_data="analysis_golden/")
    builder.button(text="📋 سفارشات استخراج", callback_data="ext_list_page_1/")
    builder.button(text="❌ انصراف", callback_data="cancel_current_flow/")
    builder.button(text="🏛 منوی اصلی", callback_data="menu_home/")

    # چیدمان: دو دکمه در ردیف اول، «طلایی» و «لیست» هرکدام یک ردیف، انصراف+منو در آخر
    builder.adjust(2, 1, 1, 2)

    await safe_edit_or_answer(
        callback.message,
        with_cancel_hint("🌐 <b>نوع آنالیز را انتخاب کنید:</b>"),
        reply_markup=builder.as_markup()
    )


@router.callback_query(ExtractorStates.waiting_for_analysis_type, F.data.in_(["analysis_users/", "analysis_messages/", "analysis_golden/"]))
async def ask_for_analysis_link(callback: types.CallbackQuery, state: FSMContext) -> None:
    await safe_callback_answer(callback)

    # استخراج نوع آنالیز از کال‌بک (users یا messages یا golden) و ذخیره آن در FSM
    analysis_type = callback.data.replace("analysis_", "").replace("/", "")
    await state.update_data(analysis_type=analysis_type)

    # انتقال به استیت دریافت لینک
    await state.set_state(ExtractorStates.waiting_for_link)

    # درخواست لینک از کاربر
    text_type = ""
    if analysis_type == "users": text_type = "استخراج کاربران"
    elif analysis_type == "messages": text_type = "استخراج پیام‌ها"
    elif analysis_type == "golden": text_type = "استخراج طلایی"

    # 🟣 فاز ۱: پیام prompt کیبورد انصراف و راهنمای /cancel دارد
    # 📄 فاز ۵: ویرایش امن (پیام حذف‌شده/قدیمی → fallback به answer)
    await safe_edit_or_answer(
        callback.message,
        with_cancel_hint(f"🔗 <b>لینک گروه را برای {text_type} ارسال کنید:</b>"),
        reply_markup=get_cancel_keyboard()
    )

@router.message(ExtractorStates.waiting_for_link, F.text)
async def process_extraction_link(message: types.Message, state: FSMContext, session: AsyncSession) -> None:
    link = message.text.strip()
    
    from utils.telegram_helpers import parse_target_links
    valid_links, invalid_lines = parse_target_links(link)

    if not valid_links and invalid_lines:
        invalid_text = "\n".join([f"خط {num}: <code>{html.escape(txt)}</code>" for num, txt in invalid_lines])
        return await message.answer(
            with_cancel_hint(
                "⚠️ فرمت لینک نامعتبر است. خط(های) زیر معتبر نیستند:\n"
                f"{invalid_text}\n\n"
                "لطفاً فقط یکی از الگوهای مجاز را ارسال کنید:\n"
                "• <code>t.me/username</code>\n"
                "• <code>t.me/joinchat/...</code> یا <code>t.me/+...</code>\n"
                "• <code>@username</code>"
            ),
            reply_markup=get_cancel_keyboard()
        )
        
    if valid_links and invalid_lines:
        invalid_text = "\n".join([f"خط {num}: <code>{html.escape(txt)}</code>" for num, txt in invalid_lines])
        return await message.answer(
            with_cancel_hint(
                "⚠️ برخی خطوط معتبر نیستند:\n"
                f"{invalid_text}\n\n"
                "همه خطوط باید لینک معتبر باشند. لطفاً مجدداً کل ورودی را با فرمت درست ارسال کنید."
            ),
            reply_markup=get_cancel_keyboard()
        )

    if not valid_links:
        return await message.answer(
            with_cancel_hint(
                "⚠️ هیچ لینک معتبری یافت نشد. لطفاً فقط یکی از الگوهای مجاز را ارسال کنید."
            ),
            reply_markup=get_cancel_keyboard()
        )
        
    # 🟢 فیکس فاز اول: جلوگیری از ورود چند لینک برای جلوگیری از اوررایت شدن خروجی استخراج
    if len(valid_links) > 1:
        return await message.answer(
            with_cancel_hint(
                "⚠️ <b>خطا: استخراج هم‌زمان مجاز نیست.</b>\n\n"
                "برای جلوگیری از تداخل داده‌ها، سفارشات استخراج فقط باید شامل <b>یک لینک</b> باشند.\n"
                "لطفاً فقط یک گروه را برای استخراج ارسال کنید."
            ),
            reply_markup=get_cancel_keyboard()
        )
        
    # حالا مطمئنیم که دقیقاً یک لینک معتبر داریم
    target_link = valid_links[0]

    fsm_data = await state.get_data()
    analysis_type = fsm_data.get("analysis_type", "users")
    await state.clear()

    chars = string.ascii_uppercase + string.digits
    random_str = ''.join(random.choices(chars, k=6))
    tracking_code = f"EXT-{random_str}"

    type_fa = "استخراج کاربران (ساده)"
    if analysis_type == "messages":
        type_fa = "استخراج پیام‌ها (تارگت‌های فعال)"
    elif analysis_type == "golden":
        type_fa = "استخراج طلایی (دقیق و تقاطعی)"

    # 🟢 فقط همان یک لینک به عنوان تارگت ذخیره می‌شود
    new_order = Order(
        order_type="extract",
        target_data=target_link,
        filter_type=analysis_type,
        status=OrderStatus.pending,
        tracking_code=tracking_code
    )

    session.add(new_order)

    try:
        await session.commit()
    except Exception as e:
        await session.rollback()
        return await message.answer(
            report_db_error("سفارش استخراج", e),
            reply_markup=get_main_menu_keyboard()
        )

    builder = InlineKeyboardBuilder()
    builder.button(text="📊 داشبورد استخراج", callback_data=f"ext_dashboard_{tracking_code}/")
    builder.button(text="📋 سفارشات استخراج", callback_data="ext_list_page_1/")
    builder.button(text="🏛 منوی اصلی", callback_data="menu_home/")
    builder.adjust(1)

    await message.answer(
        f"✅ <b>سفارش استخراج با موفقیت در صف قرار گرفت.</b>\n\n"
        f"🆔 کد رهگیری: <code>{tracking_code}</code>\n"
        f"📊 الگوریتم: <b>{type_fa}</b>\n\n"
        f"⏳ سفارش در انتظار تایید ادمین است؛ پس از تایید، به‌طور خودکار وارد صف اجرا می‌شود و فایل خروجی ارسال خواهد شد.\n\n"
        f"<i>برای پیگیری زندهٔ وضعیت از دکمهٔ زیر استفاده کنید:</i>",
        reply_markup=builder.as_markup()
    )

    admin_builder = InlineKeyboardBuilder()
    admin_builder.button(text="✅ تایید و شروع", callback_data=f"approve_order_{new_order.id}/")
    admin_builder.button(text="❌ رد سفارش", callback_data=f"reject_order_{new_order.id}/")
    admin_builder.adjust(2)
    
    target_summary = html.escape(target_link)
        
    try:
        await message.bot.send_message(
            chat_id=config.ADMIN_ID,
            text=(
                f"🛎 <b>سفارش استخراج جدید نیازمند تایید</b>\n\n"
                f"🆔 شناسه: <code>{new_order.id}</code>\n"
                f"🎟 کد رهگیری: <code>{new_order.tracking_code}</code>\n"
                f"📊 الگوریتم: <b>{type_fa}</b>\n"
                f"🎯 تارگت: <code>{target_summary}</code>\n\n"
                f"<i>لطفاً جهت ورود این سفارش به صف اجرا آن را تایید کنید.</i>"
            ),
            reply_markup=admin_builder.as_markup(),
            disable_web_page_preview=True
        )
    except Exception as e:
        logger.error(f"Failed to send extraction approval request to admin for order {new_order.id}: {e}")


# ==========================================
# 📄 فاز ۵: لیست سفارشات استخراج (صفحه‌بندی استاندارد)
# ==========================================
async def render_extraction_orders_list(
    callback: types.CallbackQuery,
    session: AsyncSession,
    state: Optional[FSMContext] = None,
    page: int = 1,
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
        return await safe_edit_or_answer(
            callback.message,
            "📋 <b>لیست سفارشات استخراج</b>\n\n"
            "⚠️ موردی یافت نشد.\n\n"
            "برای شروع، از منوی آنالیز یکی از انواع استخراج را انتخاب کنید:",
            reply_markup=builder.as_markup()
        )

    text = (
        "📋 <b>لیست سفارشات استخراج</b>\n"
        f"🔢 مجموع: <b>{total_count}</b> سفارش\n"
        f"📄 صفحهٔ <b>{page}</b> از <b>{total_pages}</b>\n\n"
    )

    for idx, order in enumerate(orders, start=offset + 1):
        badge = get_extraction_status_badge(order.status)
        type_label = EXTRACTION_TYPE_LABELS.get(order.filter_type, "❔ نامشخص")
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

        # 🟢 فیکس فاز دوم: حذف کوئری غلط روی جدول OrderLog
        extracted_count = extraction.extracted_count or 0

    except Exception as e:
        # 🟢 فیکس ضدالگوی پرتاب عریان خطا
        logger.error(f"Error building extraction dashboard for {tracking_code}: {e}", exc_info=True)
        return None, None

    badge = get_extraction_status_badge(extraction.status)
    type_label = EXTRACTION_TYPE_LABELS.get(extraction.filter_type, "❔ نامشخص")
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

    builder = InlineKeyboardBuilder()
    builder.button(text="🔄 بروزرسانی", callback_data=f"ext_dashboard_{tracking_code}/")
    
    if extraction.status == OrderStatus.completed:
        builder.button(text="📥 خروجی", callback_data=f"export_order_{extraction.id}/")
        builder.button(text="🔙 بازگشت به لیست استخراج", callback_data=f"ext_list_page_{back_page}/")
        builder.button(text="🏛 منوی اصلی", callback_data="menu_home/")
        builder.adjust(1, 2, 1)
    else:
        builder.button(text="🔙 بازگشت به لیست استخراج", callback_data=f"ext_list_page_{back_page}/")
        builder.button(text="🏛 منوی اصلی", callback_data="menu_home/")
        builder.adjust(1, 1, 1)

    return text, builder.as_markup()


@router.callback_query(F.data.startswith("ext_dashboard_") & F.data.endswith("/"))
async def show_extraction_dashboard(callback: types.CallbackQuery, state: FSMContext, session: AsyncSession) -> None:
    """
    📊 فاز ۵: داشبورد زندهٔ سفارش استخراج — وضعیت، تعداد استخراج‌شده، 🔄 بروزرسانی
    و 📥 خروجی (که توسط هندلر export_order در order_fsm_router پردازش می‌شود).
    """
    tracking_code = callback.data.replace("ext_dashboard_", "").replace("/", "")

    if not EXT_TRACKING_CODE_PATTERN.fullmatch(tracking_code):
        return await safe_callback_answer(callback, "⚠️ کد پیگیری نامعتبر است.", show_alert=True)

    await safe_callback_answer(callback)
    
    fsm_data = await state.get_data()
    back_page = fsm_data.get("ext_list_page", 1)

    try:
        text, markup = await build_extraction_dashboard_view(session, tracking_code, back_page)
    except Exception as e:
        await session.rollback()
        return await answer_callback_error(
            callback, report_db_error("سفارش استخراج", e), get_main_menu_button()
        )

    if not text:
        return await safe_callback_answer(callback, "❌ سفارش استخراج یافت نشد.", show_alert=True)

    await safe_edit_message(
        callback.message, text,
        reply_markup=markup,
        link_preview_options=types.LinkPreviewOptions(is_disabled=True),
    )