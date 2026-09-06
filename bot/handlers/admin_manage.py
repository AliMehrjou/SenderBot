import logging
from typing import Optional

from aiogram import Router, types, F
from aiogram.fsm.context import FSMContext
from aiogram.utils.keyboard import InlineKeyboardBuilder
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError

from database.models import Admin
from bot.states.admin_fsm import AdminManageStates
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
async def render_admins_list(
    callback: types.CallbackQuery,
    session: AsyncSession,
    state: Optional[FSMContext] = None,
    page: int = 1,
) -> None:
    """
    📄 فاز ۱: رندر لیست ادمین‌های فرعی با صفحه‌بندی استاندارد.

    - ۱۰ ادمین در هر صفحه (PAGINATION_SIZE) با کوئری‌های efficient:
      یک COUNT برای کل تعداد + یک OFFSET/LIMIT برای صفحهٔ فعلی
      (قبلاً همهٔ ادمین‌ها یک‌جا واکشی می‌شدند و با ۱۰۰+ ادمین،
      پیام از سقف ۴۰۹۶ کاراکتر عبور می‌کرد و کرش می‌کرد)
    - دکمه حذف برای هر ادمین (فرمت callback قدیمی del_admin_{id}/
      حفظ شده تا فلوی تأیید دو مرحله‌ای بدون تغییر کار کند)
    - ردیف ناوبری صفحات + 🔄 بروزرسانی + 🏛 منوی اصلی

    نکته: این تابع خودش callback را answer نمی‌کند؛ همهٔ فراخوانی‌کننده‌ها
    باید قبل از فراخوانی، callback را پاسخ داده باشند (الگوی فاز ۵).
    """
    # 🛡 فاز ۵ (مشکل ۱): این تابع از چند هندلر مختلف صدا زده می‌شود،
    # پس حفاظ دیتابیس داخل خودش انجام می‌شود
    try:
        total_count = await session.scalar(select(func.count(Admin.id))) or 0
        total_pages = calculate_total_pages(total_count)

        # اصلاح هوشمند شماره صفحه (مثلاً بعد از حذف آخرین ادمینِ صفحهٔ آخر)
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

    # 📄 فاز ۱: ثبت صفحهٔ فعلی در FSM برای بازگشت درست بعد از حذف/انصراف
    if state is not None:
        await state.update_data(admins_list_page=page)

    builder = InlineKeyboardBuilder()

    # ── لیست خالی ──
    if total_count == 0:
        builder.button(text="🔙 بازگشت", callback_data="menu_add_admin/")
        builder.button(text="🏛 منوی اصلی", callback_data="menu_home/")
        builder.adjust(1)
        return await safe_edit_or_answer(
            callback.message,
            "⚠️ هیچ ادمین فرعی در سیستم ثبت نشده است.",
            reply_markup=builder.as_markup()
        )

    # ── متن لیست (حداکثر ۱۰ خط → دیگر خطر عبور از سقف ۴۰۹۶ کاراکتر وجود ندارد) ──
    text = (
        "👥 <b>لیست ادمین‌های فرعی سیستم</b>\n"
        f"🔢 مجموع: <b>{total_count}</b> ادمین\n"
        f"📄 صفحهٔ <b>{page}</b> از <b>{total_pages}</b>\n\n"
    )

    for idx, adm in enumerate(admins, start=offset + 1):
        text += f"{idx}. 🆔 <code>{adm.telegram_id}</code>\n"

        # برچسب دکمه حذف: آیدی تلگرام (کوتاه‌سازی برای چیدمان دو ستونی)
        id_label = str(adm.telegram_id)
        if len(id_label) > 12:
            id_label = id_label[:11] + "…"
        builder.button(text=f"🗑 حذف {id_label}", callback_data=f"del_admin_{adm.id}/")

    builder.adjust(2)

    text += "\n👇 برای حذف هر ادمین، روی دکمهٔ مربوطه کلیک کنید:"

    # ── ردیف ناوبری صفحات (فقط وقتی بیش از یک صفحه باشد) ──
    add_pagination_nav_row(builder, page, total_pages, callback_prefix="list_admins_")

    # ── دکمه‌های پایانی: 🔄 بروزرسانی (رفرش همان صفحه) + 🏛 منوی اصلی ──
    add_list_footer(builder, refresh_callback=f"list_admins_page_{page}/")

    # 🛡 فاز ۵: ویرایش امن
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