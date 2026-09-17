import logging
from typing import Optional

from aiogram import Router, types, F
from aiogram.fsm.context import FSMContext
from aiogram.utils.keyboard import InlineKeyboardBuilder
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError

from database.models import Admin
from bot.states.admin_fsm import AdminManageStates, AdminMessageStates
from bot.states.confirm_fsm import ConfirmStates
from config import config

# 🟣 فاز ۵ (رفع بن‌بست FSM): helper های قبلی
from bot.keyboards.cancel import with_cancel_hint
from utils.fsm_cleanup import cleanup_fsm_temp_files
from utils.safe_edit import safe_edit_or_answer

# 🛡 فاز ۵ (مدیریت خطا): helper های فاز ۱ و ۲ + کیبورد fallback خطا
from bot.keyboards.main_menu import get_main_menu_button
from utils.error_messages import report_db_error
from utils.telegram_helpers import (
    answer_callback_error,
    safe_callback_answer,
    safe_edit_message,
)

# 📄 فاز ۱ (صفحه‌بندی استاندارد): زیرساخت مشترک paginate
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
router = Router(name="admin_manage_router")


# ==========================================
# 🟣 فاز ۵: کیبوردهای استاندارد فلوی افزودن ادمین
# ==========================================
def get_admin_add_cancel_keyboard():
    """
    کیبورد انصراف برای پیام‌های FSM فلوی افزودن ادمین.
    دکمه انصراف به هندلر عمومی cancel_current_flow/ متصل است که پاک کردن
    state تضمینی است (رفع بن‌بست).
    """
    builder = InlineKeyboardBuilder()
    builder.button(text="❌ انصراف", callback_data="cancel_current_flow/")
    builder.button(text="👥 مشاهده لیست ادمین‌ها", callback_data="menu_list_admins/")
    builder.button(text="🏛 منوی اصلی", callback_data="menu_home/")
    builder.adjust(1)
    return builder.as_markup()


def get_admin_add_finish_keyboard():
    """کیبورد پایان کار (موفقیت) فلوی افزودن ادمین."""
    builder = InlineKeyboardBuilder()
    builder.button(text="➕ افزودن ادمین دیگر", callback_data="menu_add_admin/")
    builder.button(text="👥 مشاهده لیست ادمین‌ها", callback_data="menu_list_admins/")
    builder.button(text="🏛 منوی اصلی", callback_data="menu_home/")
    builder.adjust(2, 1)
    return builder.as_markup()

# ==========================================
# ADD ADMIN FLOW
# ==========================================
@router.callback_query(F.data == "menu_add_admin/")
async def add_admin_start(callback: types.CallbackQuery, state: FSMContext) -> None:
    if callback.from_user.id != config.ADMIN_ID:
        return await safe_callback_answer(
            callback,
            "⛔️ فقط ادمین اصلی سیستم مجاز به مدیریت ادمین‌های فرعی است.",
            show_alert=True
        )

    await safe_callback_answer(callback)

    await cleanup_fsm_temp_files(state)
    await state.clear()

    await state.set_state(AdminManageStates.waiting_for_admin_id)
    await safe_edit_or_answer(
        callback.message,
        with_cancel_hint(
            "👨‍💻 <b>افزودن ادمین جدید</b>\n\n"
            "لطفاً <code>آیدی عددی (Chat ID)</code> شخص مورد نظر را ارسال کنید:"
        ),
        reply_markup=get_admin_add_cancel_keyboard()
    )

@router.message(AdminManageStates.waiting_for_admin_id, F.text)
async def process_admin_id(message: types.Message, state: FSMContext, session: AsyncSession) -> None:
    admin_id_text = message.text.strip()

    if not admin_id_text.isdigit():
        return await message.answer(
            with_cancel_hint("⚠️ فرمت نامعتبر! آیدی باید فقط شامل اعداد باشد."),
            reply_markup=get_admin_add_cancel_keyboard()
        )

    new_admin_id = int(admin_id_text)

    if new_admin_id == config.ADMIN_ID:
        return await message.answer(
            with_cancel_hint("⚠️ این آیدی متعلق به ادمین اصلی سیستم است. لطفاً آیدی دیگری ارسال کنید."),
            reply_markup=get_admin_add_cancel_keyboard()
        )

    new_admin = Admin(telegram_id=new_admin_id)

    try:
        session.add(new_admin)
        await session.commit()
    except IntegrityError:
        await session.rollback()
        logger.info(f"Duplicate admin add attempt skipped: telegram_id={new_admin_id}")
        return await message.answer(
            with_cancel_hint("⚠️ این کاربر از قبل ادمین سیستم است. لطفاً آیدی دیگری ارسال کنید."),
            reply_markup=get_admin_add_cancel_keyboard()
        )
    except Exception as e:
        await session.rollback()
        return await message.answer(
            with_cancel_hint(report_db_error("ادمین", e)),
            reply_markup=get_admin_add_cancel_keyboard()
        )

    await state.clear()
    await message.answer(
        f"🎉 <b>ادمین جدید با موفقیت اضافه شد!</b>\n\n"
        f"آیدی: <code>{new_admin_id}</code>\n"
        f"<i>این کاربر اکنون به پنل کنترل دسترسی دارد.</i>",
        reply_markup=get_admin_add_finish_keyboard()
    )


# ==========================================
# LIST & DELETE ADMINS FLOW
# (📄 فاز ۱: صفحه‌بندی استاندارد ۱۰ ادمین در هر صفحه + 🔄 بروزرسانی)
# ==========================================
# REWRITTEN
async def render_admins_list(
    callback: types.CallbackQuery,
    session: AsyncSession,
    state: Optional[FSMContext] = None,
    page: int = 1,
) -> None:
    """
    📄 فاز ۱: رندر لیست ادمین‌های فرعی با صفحه‌بندی استاندارد.
    """
    try:
        total_count = await session.scalar(select(func.count(Admin.id))) or 0
        total_pages = calculate_total_pages(total_count)

        page = clamp_page(page, total_pages)
        offset = get_page_offset(page)

        admins = (
            await session.scalars(
                select(Admin)
                .order_by(Admin.id.asc())
                .offset(offset)
                .limit(PAGINATION_SIZE)
            )
        ).all()
    except Exception as e:
        await session.rollback()
        return await answer_callback_error(
            callback, report_db_error("ادمین‌ها", e), get_main_menu_button()
        )

    if state is not None:
        await state.update_data(admins_list_page=page)

    builder = InlineKeyboardBuilder()

    if total_count == 0:
        builder.button(text="🔙 بازگشت", callback_data="menu_add_admin/")
        builder.button(text="🏛 منوی اصلی", callback_data="menu_home/")
        builder.adjust(1)
        return await safe_edit_or_answer(
            callback.message,
            "⚠️ هیچ ادمین فرعی در سیستم ثبت نشده است.",
            reply_markup=builder.as_markup()
        )

    text = (
        "👥 <b>لیست ادمین‌های فرعی سیستم</b>\n"
        f"🔢 مجموع: <b>{total_count}</b> ادمین\n"
        f"📄 صفحهٔ <b>{page}</b> از <b>{total_pages}</b>\n\n"
    )

    # دکمه ارسال پیام در بالا قرار می‌گیرد
    builder.button(text="📨 پیام به ادمین‌ها", callback_data="menu_admin_message/")

    for idx, adm in enumerate(admins, start=offset + 1):
        text += f"{idx}. 🆔 <code>{adm.telegram_id}</code>\n"

        id_label = str(adm.telegram_id)
        if len(id_label) > 12:
            id_label = id_label[:11] + "…"
        builder.button(text=f"🗑 حذف {id_label}", callback_data=f"del_admin_{adm.id}/")

    # تنظیم چیدمان: ردیف اول 1 دکمه (پیام)، ردیف‌های بعد 2 دکمه (حذف)
    builder.adjust(1, 2)

    text += "\n👇 برای حذف هر ادمین، روی دکمهٔ مربوطه کلیک کنید:"

    add_pagination_nav_row(builder, page, total_pages, callback_prefix="list_admins_")
    add_list_footer(builder, refresh_callback=f"list_admins_page_{page}/")

    await safe_edit_or_answer(callback.message, text, reply_markup=builder.as_markup())

@router.callback_query(F.data == "menu_list_admins/")
async def list_admins_handler(callback: types.CallbackQuery, state: FSMContext, session: AsyncSession) -> None:
    if callback.from_user.id != config.ADMIN_ID:
        return await safe_callback_answer(callback, "⛔️ دسترسی غیرمجاز.", show_alert=True)

    await safe_callback_answer(callback)

    await cleanup_fsm_temp_files(state)
    await state.clear()

    # 📄 فاز ۱: ورود به لیست همیشه از صفحهٔ ۱
    await render_admins_list(callback, session, state=state, page=1)


# ==========================================
# 📄 فاز ۱: ناوبری صفحات + 🔄 بروزرسانی لیست ادمین‌ها
# دکمهٔ «بروزرسانی» دقیقاً همان کال‌بک صفحهٔ فعلی را صدا می‌زند،
# بنابراین این هندلر هم «صفحه بعد/قبل» و هم «بروزرسانی» را پوشش می‌دهد.
# ==========================================
@router.callback_query(F.data.startswith("list_admins_page_"))
async def list_admins_paginated_handler(callback: types.CallbackQuery, state: FSMContext, session: AsyncSession) -> None:
    if callback.from_user.id != config.ADMIN_ID:
        return await safe_callback_answer(callback, "⛔️ دسترسی غیرمجاز.", show_alert=True)

    await safe_callback_answer(callback)

    page = parse_page_from_callback(callback.data)
    await render_admins_list(callback, session, state=state, page=page)


# ==========================================
# 🟤 فاز ۵ — هندلر سازگاری برای دکمه‌های تأیید قدیمی
# ==========================================
@router.callback_query(F.data.startswith("del_admin_confirm_") & F.data.endswith("/"))
async def legacy_admin_confirm_button_compat(callback: types.CallbackQuery) -> None:
    if callback.from_user.id != config.ADMIN_ID:
        return await safe_callback_answer(callback, "⛔️ دسترسی غیرمجاز.", show_alert=True)

    await safe_callback_answer(
        callback,
        "⚠️ این دکمه متعلق به نسخه قدیمی ربات است. لطفاً از لیست ادمین‌ها مجدداً اقدام کنید.",
        show_alert=True
    )


# ==========================================
# 🟤 فاز ۵ — DELETE ADMIN FLOW: توابع کمکی مشترک
# ==========================================
def build_admin_delete_confirmation_text(admin_obj: Admin) -> str:
    return (
        "⚠️ <b>تأیید حذف ادمین</b>\n\n"
        f"آیا از حذف ادمین <code>{admin_obj.telegram_id}</code> مطمئن هستید؟\n\n"
        f"🆔 شناسه دیتابیس: <code>{admin_obj.id}</code>\n"
        f"👤 آیدی تلگرام: <code>{admin_obj.telegram_id}</code>\n\n"
        "⚠️ <b>توجه: این عمل قابل بازگشت نیست.</b>\n"
        "این کاربر بلافاصله دسترسی خود را به پنل کنترل از دست می‌دهد."
    )


def build_admin_delete_confirmation_keyboard(admin_db_id: int) -> types.InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ بله، حذف کن", callback_data=f"confirm_delete_admin_{admin_db_id}/")
    builder.button(text="❌ انصراف", callback_data="cancel_confirm_delete_admin/")
    builder.adjust(2)
    return builder.as_markup()


# ==========================================
# 🟤 فاز ۵ — مرحله ۱: DELETE ADMIN (نمایش تأیید)
# (📄 فاز ۱: ذخیره صفحهٔ فعلی برای بازگشت بعد از حذف/انصراف)
# ==========================================
@router.callback_query(F.data.startswith("del_admin_") & F.data.endswith("/"))
async def ask_delete_admin_confirmation(callback: types.CallbackQuery, state: FSMContext, session: AsyncSession) -> None:
    if callback.from_user.id != config.ADMIN_ID:
        return await safe_callback_answer(callback, "⛔️ دسترسی غیرمجاز.", show_alert=True)

    admin_id_str = callback.data.replace("del_admin_", "").replace("/", "")
    if not admin_id_str.isdigit():
        return await safe_callback_answer(callback, "⚠️ آیدی نامعتبر.", show_alert=True)

    admin_db_id = int(admin_id_str)

    try:
        stmt = select(Admin).where(Admin.id == admin_db_id)
        admin_obj = await session.scalar(stmt)
    except Exception as e:
        await session.rollback()
        return await answer_callback_error(
            callback, report_db_error("ادمین", e), get_main_menu_button()
        )

    if not admin_obj:
        await safe_callback_answer(callback, "⚠️ این ادمین قبلاً حذف شده است.", show_alert=True)
        # 📄 فاز ۱: رفرش لیست از همان صفحه‌ای که کاربر در آن بوده
        fsm_data = await state.get_data()
        return await render_admins_list(
            callback, session, state=state,
            page=fsm_data.get("admins_list_page", 1)
        )

    await safe_callback_answer(callback)

    # 🟤 ذخیره اطلاعات عملیات در انتظار تأیید در FSM (الگوی استاندارد)
    # 📄 فاز ۱: return_page هم ذخیره می‌شود تا بعد از حذف به همان صفحه برگردیم
    fsm_data = await state.get_data()
    await state.update_data(
        confirm_action="delete_admin",
        target_id=admin_db_id,
        return_page=fsm_data.get("admins_list_page", 1),
    )
    await state.set_state(ConfirmStates.waiting_for_confirmation)

    # 🛡 فاز ۵: ویرایش امن (fallback به answer تا دکمه‌های تأیید همیشه در دسترس بمانند)
    await safe_edit_message(
        callback.message,
        build_admin_delete_confirmation_text(admin_obj),
        reply_markup=build_admin_delete_confirmation_keyboard(admin_db_id)
    )


# ==========================================
# 🟤 فاز ۵ — مرحله ۲ (اجرای واقعی)
# (📄 فاز ۱: بازگشت به همان صفحهٔ قبلی بعد از حذف)
# ==========================================
@router.callback_query(F.data.startswith("confirm_delete_admin_") & F.data.endswith("/"))
async def confirm_delete_admin_handler(callback: types.CallbackQuery, state: FSMContext, session: AsyncSession) -> None:
    if callback.from_user.id != config.ADMIN_ID:
        return await safe_callback_answer(callback, "⛔️ دسترسی غیرمجاز.", show_alert=True)

    # 🔒 بررسی امنیتی state
    current_state = await state.get_state()
    fsm_data = await state.get_data()

    if current_state != ConfirmStates.waiting_for_confirmation or fsm_data.get("confirm_action") != "delete_admin":
        return await safe_callback_answer(
            callback,
            "⚠️ این درخواست تأیید منقضی شده است. لطفاً از ابتدا اقدام کنید.",
            show_alert=True
        )

    admin_id_str = callback.data.replace("confirm_delete_admin_", "").replace("/", "")

    if not admin_id_str.isdigit():
        return await safe_callback_answer(callback, "⚠️ آیدی نامعتبر.", show_alert=True)

    admin_db_id = int(admin_id_str)

    # 🔒 تطابق آیدی ادمین درخواستی با آیدی ذخیره‌شده در state
    if fsm_data.get("target_id") != admin_db_id:
        return await safe_callback_answer(
            callback,
            "⚠️ این درخواست تأیید با ادمین نمایش‌داده‌شده مطابقت ندارد. لطفاً از ابتدا اقدام کنید.",
            show_alert=True
        )

    # 📄 فاز ۱: صفحهٔ بازگشت (قبل از هر state.clear ذخیره شده)
    return_page = fsm_data.get("return_page", 1)

    try:
        stmt = select(Admin).where(Admin.id == admin_db_id)
        admin_obj = await session.scalar(stmt)
    except Exception as e:
        await session.rollback()
        return await answer_callback_error(
            callback, report_db_error("ادمین", e), get_main_menu_button()
        )

    if not admin_obj:
        await state.clear()
        await safe_callback_answer(callback, "⚠️ این ادمین قبلاً حذف شده است.", show_alert=True)
        return await render_admins_list(callback, session, state=state, page=return_page)

    try:
        await session.delete(admin_obj)
        await session.commit()
    except Exception as e:
        await session.rollback()
        await state.clear()
        await answer_callback_error(
            callback, report_db_error("ادمین", e), get_main_menu_button()
        )
        return await render_admins_list(callback, session, state=state, page=return_page)

    # 🟤 پاک کردن state بعد از اجرای موفق عملیات
    await state.clear()

    await safe_callback_answer(callback, "✅ ادمین با موفقیت حذف شد.", show_alert=False)
    await render_admins_list(callback, session, state=state, page=return_page)


# ==========================================
# 🟤 فاز ۵ — انصراف از حذف ادمین
# (📄 فاز ۱: بازگشت به همان صفحهٔ قبلی)
# ==========================================
@router.callback_query(F.data == "cancel_confirm_delete_admin/")
async def cancel_delete_admin_confirmation(callback: types.CallbackQuery, state: FSMContext, session: AsyncSession) -> None:
    if callback.from_user.id != config.ADMIN_ID:
        return await safe_callback_answer(callback, "⛔️ دسترسی غیرمجاز.", show_alert=True)

    fsm_data = await state.get_data()
    return_page = fsm_data.get("return_page", 1)

    if fsm_data.get("confirm_action") == "delete_admin":
        await state.clear()

    await safe_callback_answer(callback, "🚫 عملیات حذف ادمین لغو شد.")

    return await render_admins_list(callback, session, state=state, page=return_page)


# NEW
# ==========================================
# SEND MESSAGE TO ADMINS FLOW
# ==========================================

@router.callback_query(F.data == "menu_admin_message/")
async def admin_message_menu(callback: types.CallbackQuery, state: FSMContext) -> None:
    if callback.from_user.id != config.ADMIN_ID:
        return await safe_callback_answer(callback, "⛔️ دسترسی غیرمجاز.", show_alert=True)
        
    await safe_callback_answer(callback)
    await state.set_state(AdminMessageStates.waiting_for_recipient_selection)
    
    builder = InlineKeyboardBuilder()
    builder.button(text="📢 پیام به همه ادمین‌ها", callback_data="msg_admin_all/")
    builder.button(text="👤 پیام به یک ادمین خاص", callback_data="msg_admin_specific/")
    builder.button(text="🔙 بازگشت", callback_data="menu_list_admins/")
    builder.adjust(1)
    
    await safe_edit_or_answer(
        callback.message,
        "📨 <b>پنل ارسال پیام به ادمین‌ها</b>\n\nلطفاً گیرنده پیام را انتخاب کنید:",
        reply_markup=builder.as_markup()
    )

@router.callback_query(AdminMessageStates.waiting_for_recipient_selection, F.data == "msg_admin_all/")
async def admin_message_all(callback: types.CallbackQuery, state: FSMContext) -> None:
    if callback.from_user.id != config.ADMIN_ID:
        return await safe_callback_answer(callback, "⛔️ دسترسی غیرمجاز.", show_alert=True)
        
    await safe_callback_answer(callback)
    await state.update_data(recipient="all")
    await state.set_state(AdminMessageStates.waiting_for_message_content)
    
    builder = InlineKeyboardBuilder()
    builder.button(text="❌ انصراف", callback_data="menu_list_admins/")
    
    await safe_edit_or_answer(
        callback.message,
        "📢 <b>ارسال پیام به همه ادمین‌ها</b>\n\nلطفاً متن پیام خود را ارسال کنید (امکان استفاده از تگ‌های HTML وجود دارد):",
        reply_markup=builder.as_markup()
    )

async def render_message_admins_list(callback: types.CallbackQuery, session: AsyncSession, page: int = 1) -> None:
    """تابع کمکی برای رندر کردن لیست ادمین‌ها جهت انتخاب گیرنده پیام"""
    try:
        total_count = await session.scalar(select(func.count(Admin.id))) or 0
        total_pages = calculate_total_pages(total_count)

        page = clamp_page(page, total_pages)
        offset = get_page_offset(page)

        admins = (
            await session.scalars(
                select(Admin)
                .order_by(Admin.id.asc())
                .offset(offset)
                .limit(PAGINATION_SIZE)
            )
        ).all()
    except Exception as e:
        await session.rollback()
        return await answer_callback_error(
            callback, report_db_error("ادمین‌ها", e), get_main_menu_button()
        )

    builder = InlineKeyboardBuilder()

    if total_count == 0:
        builder.button(text="🔙 بازگشت", callback_data="menu_admin_message/")
        return await safe_edit_or_answer(
            callback.message,
            "⚠️ هیچ ادمین فرعی برای دریافت پیام وجود ندارد.",
            reply_markup=builder.as_markup()
        )

    text = (
        "👤 <b>انتخاب ادمین برای ارسال پیام</b>\n"
        f"📄 صفحهٔ <b>{page}</b> از <b>{total_pages}</b>\n\n"
    )

    for idx, adm in enumerate(admins, start=offset + 1):
        text += f"{idx}. 🆔 <code>{adm.telegram_id}</code>\n"
        id_label = str(adm.telegram_id)
        if len(id_label) > 12:
            id_label = id_label[:11] + "…"
        builder.button(text=f"📨 پیام {id_label}", callback_data=f"msg_admin_{adm.id}/")

    builder.adjust(2)
    text += "\n👇 برای ارسال پیام، روی دکمهٔ ادمین مورد نظر کلیک کنید:"

    add_pagination_nav_row(builder, page, total_pages, callback_prefix="msg_list_admins_page_")
    builder.row(types.InlineKeyboardButton(text="🔙 بازگشت", callback_data="menu_admin_message/"))

    await safe_edit_or_answer(callback.message, text, reply_markup=builder.as_markup())

@router.callback_query(AdminMessageStates.waiting_for_recipient_selection, F.data == "msg_admin_specific/")
async def admin_message_specific(callback: types.CallbackQuery, session: AsyncSession) -> None:
    if callback.from_user.id != config.ADMIN_ID:
        return await safe_callback_answer(callback, "⛔️ دسترسی غیرمجاز.", show_alert=True)
        
    await safe_callback_answer(callback)
    await render_message_admins_list(callback, session, page=1)

@router.callback_query(AdminMessageStates.waiting_for_recipient_selection, F.data.startswith("msg_list_admins_page_"))
async def msg_list_admins_paginated(callback: types.CallbackQuery, session: AsyncSession) -> None:
    if callback.from_user.id != config.ADMIN_ID:
        return await safe_callback_answer(callback, "⛔️ دسترسی غیرمجاز.", show_alert=True)
        
    await safe_callback_answer(callback)
    page = parse_page_from_callback(callback.data)
    await render_message_admins_list(callback, session, page=page)

@router.callback_query(AdminMessageStates.waiting_for_recipient_selection, F.data.startswith("msg_admin_") & F.data.endswith("/"))
async def select_specific_admin_for_msg(callback: types.CallbackQuery, state: FSMContext, session: AsyncSession) -> None:
    if callback.from_user.id != config.ADMIN_ID:
        return await safe_callback_answer(callback, "⛔️ دسترسی غیرمجاز.", show_alert=True)
        
    if callback.data in ("msg_admin_all/", "msg_admin_specific/"):
        return
        
    admin_id_str = callback.data.replace("msg_admin_", "").replace("/", "")
    if not admin_id_str.isdigit():
        return await safe_callback_answer(callback, "⚠️ آیدی نامعتبر.", show_alert=True)
        
    admin_db_id = int(admin_id_str)
    try:
        admin_obj = await session.scalar(select(Admin).where(Admin.id == admin_db_id))
    except Exception as e:
        await session.rollback()
        return await answer_callback_error(callback, report_db_error("ادمین", e), get_main_menu_button())
        
    if not admin_obj:
        return await safe_callback_answer(callback, "⚠️ ادمین یافت نشد.", show_alert=True)
        
    await safe_callback_answer(callback)
    await state.update_data(recipient=admin_db_id, recipient_tg_id=admin_obj.telegram_id)
    await state.set_state(AdminMessageStates.waiting_for_message_content)
    
    builder = InlineKeyboardBuilder()
    builder.button(text="❌ انصراف", callback_data="menu_list_admins/")
    
    await safe_edit_or_answer(
        callback.message,
        f"👤 <b>ارسال پیام به ادمین <code>{admin_obj.telegram_id}</code></b>\n\nلطفاً متن پیام خود را ارسال کنید (امکان استفاده از تگ‌های HTML وجود دارد):",
        reply_markup=builder.as_markup()
    )

@router.message(AdminMessageStates.waiting_for_message_content, F.text)
async def msg_content_received(message: types.Message, state: FSMContext) -> None:
    if message.from_user.id != config.ADMIN_ID:
        return
        
    text = message.text
    await state.update_data(message_text=text)
    
    fsm_data = await state.get_data()
    recipient = fsm_data.get("recipient")
    
    if recipient == "all":
        target_str = "همه ادمین‌ها"
    else:
        target_str = f"ادمین <code>{fsm_data.get('recipient_tg_id')}</code>"
        
    preview = (
        "📋 <b>پیش‌نمایش پیام:</b>\n\n"
        f"{text}\n\n"
        f"📨 <b>گیرنده:</b> {target_str}\n\n"
        "آیا از ارسال این پیام اطمینان دارید؟"
    )
    
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ بله، ارسال کن", callback_data="confirm_send_msg/")
    builder.button(text="❌ انصراف", callback_data="menu_list_admins/")
    builder.button(text="✏️ ویرایش متن", callback_data="edit_msg_text/")
    builder.adjust(1, 2)
    
    await message.answer(preview, reply_markup=builder.as_markup(), parse_mode="HTML")
    await state.set_state(AdminMessageStates.waiting_for_send_confirmation)

@router.callback_query(AdminMessageStates.waiting_for_send_confirmation, F.data == "edit_msg_text/")
async def edit_msg_text(callback: types.CallbackQuery, state: FSMContext) -> None:
    if callback.from_user.id != config.ADMIN_ID:
        return await safe_callback_answer(callback, "⛔️ دسترسی غیرمجاز.", show_alert=True)
        
    await safe_callback_answer(callback)
    await state.set_state(AdminMessageStates.waiting_for_message_content)
    
    builder = InlineKeyboardBuilder()
    builder.button(text="❌ انصراف", callback_data="menu_list_admins/")
    
    await safe_edit_or_answer(
        callback.message,
        "✏️ لطفاً متن جدید پیام را ارسال کنید:",
        reply_markup=builder.as_markup()
    )

@router.callback_query(AdminMessageStates.waiting_for_send_confirmation, F.data == "confirm_send_msg/")
async def execute_send_msg(callback: types.CallbackQuery, state: FSMContext, session: AsyncSession) -> None:
    if callback.from_user.id != config.ADMIN_ID:
        return await safe_callback_answer(callback, "⛔️ دسترسی غیرمجاز.", show_alert=True)
        
    await safe_callback_answer(callback, "⏳ در حال ارسال...")
    
    fsm_data = await state.get_data()
    recipient = fsm_data.get("recipient")
    text = fsm_data.get("message_text", "")
    
    # هدر مخصوص سیستم
    final_text = f"📨 پیام از ادمین اصلی:\n\n{text}"
    
    success = 0
    failed_ids = []
    
    if recipient == "all":
        admins = (await session.scalars(select(Admin.telegram_id))).all()
        targets = admins
    else:
        targets = [fsm_data.get("recipient_tg_id")]
        
    for tg_id in targets:
        try:
            await callback.bot.send_message(chat_id=tg_id, text=final_text, parse_mode="HTML")
            success += 1
        except Exception as e:
            logger.error(f"Failed to send admin message to {tg_id}: {e}")
            failed_ids.append(str(tg_id))
            
    report = f"✅ پیام به <b>{success}</b> از <b>{len(targets)}</b> ادمین ارسال شد.\n"
    if failed_ids:
        report += f"\n❌ <b>{len(failed_ids)}</b> مورد ناموفق (احتمالاً ربات را بلاک کرده‌اند):\n<code>{', '.join(failed_ids)}</code>"
        
    builder = InlineKeyboardBuilder()
    builder.button(text="🔙 بازگشت به لیست ادمین‌ها", callback_data="menu_list_admins/")
    builder.adjust(1)
    
    await safe_edit_or_answer(callback.message, report, reply_markup=builder.as_markup())
    await state.clear()

from database.models import Account, AccountStatus

@router.callback_query(F.data == "menu_account_health/")
async def account_health_panel(callback: types.CallbackQuery, session: AsyncSession) -> None:
    """🩺 فاز ۱۰: نمایش وضعیت اکانت‌های در استراحت یا مسدود شده"""
    if callback.from_user.id != config.ADMIN_ID:
        return await safe_callback_answer(callback, "⛔️ دسترسی غیرمجاز.", show_alert=True)
        
    await safe_callback_answer(callback)
    
    # واکشی اکانت‌هایی که در حالت Active نیستند (Blocked یا Cooldown)
    stmt = select(Account).where(Account.status.in_([AccountStatus.blocked, AccountStatus.cooldown]))
    problem_accounts = (await session.scalars(stmt)).all()
    
    builder = InlineKeyboardBuilder()
    
    if not problem_accounts:
        text = "🟢 <b>وضعیت سلامت سیستم</b>\n\nهیچ اکانتی در وضعیت Blocked یا Cooldown وجود ندارد. تمام اکانت‌ها در سلامت کامل هستند!"
    else:
        text = "🔴 <b>اکانت‌های نیازمند توجه</b>\n\n"
        for acc in problem_accounts:
            status_emoji = "⏳" if acc.status == AccountStatus.cooldown else "🚫"
            
            # تبدیل زمان بازگشت به تایم‌زون محلی (تهران) برای نمایش به ادمین
            return_str = "دستی (نیازمند بررسی)"
            if acc.expected_return_time:
                from zoneinfo import ZoneInfo
                tehran_tz = ZoneInfo("Asia/Tehran")
                local_time = acc.expected_return_time.astimezone(tehran_tz)
                return_str = local_time.strftime("%H:%M")
                
            status_val = acc.status.value if hasattr(acc.status, 'value') else str(acc.status)
            text += (
                f"{status_emoji} <b>آیدی {acc.id}</b> | وضعیت: {status_val}\n"
                f"▫️ دلیل: <i>{acc.status_reason or 'نامشخص'}</i>\n"
                f"▫️ زمان بازگشت: <code>{return_str}</code>\n\n"
            )
            
            # فقط اکانت‌های Blocked نیاز به دکمه رفع بلاک دستی دارند (Cooldownها خودکار برمی‌گردند)
            if acc.status == AccountStatus.blocked:
                builder.button(text=f"✅ رفع محدودیت آیدی {acc.id}", callback_data=f"unblock_acc_{acc.id}/")

    builder.button(text="🔄 بروزرسانی", callback_data="menu_account_health/")
    builder.button(text="🏛 منوی اصلی", callback_data="menu_home/")
    builder.adjust(1)
    
    await safe_edit_or_answer(callback.message, text, reply_markup=builder.as_markup())


@router.callback_query(F.data.startswith("unblock_acc_") & F.data.endswith("/"))
async def unblock_account_handler(callback: types.CallbackQuery, session: AsyncSession) -> None:
    """🩺 فاز ۱۰: بازگردانی دستی اکانت از حالت مسدود به اکتیو"""
    if callback.from_user.id != config.ADMIN_ID:
        return await safe_callback_answer(callback, "⛔️ دسترسی غیرمجاز.", show_alert=True)
        
    acc_id = int(callback.data.replace("unblock_acc_", "").replace("/", ""))
    
    # استفاده از تابع استاندارد change_account_status برای تغییر اتمیک و ثبت لاگ
    try:
        from workers.sender import change_account_status
        await change_account_status(
            session=session, 
            account_id=acc_id, 
            new_status=AccountStatus.active, 
            reason="Manual Unblock by Admin"
        )
        await safe_callback_answer(callback, f"✅ اکانت {acc_id} با موفقیت به چرخه Active بازگشت.", show_alert=True)
    except Exception as e:
        logger.error(f"Failed to unblock account {acc_id}: {e}")
        await safe_callback_answer(callback, "❌ خطا در بازگردانی اکانت. لاگ سرور را بررسی کنید.", show_alert=True)
        
    # رفرش پنل وضعیت
    await account_health_panel(callback, session)