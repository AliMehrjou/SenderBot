import logging
import html
import re
from sqlalchemy import select
from aiogram import Router, types, F
from aiogram.fsm.context import FSMContext
from aiogram.utils.keyboard import InlineKeyboardBuilder
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from database.models import APIKey, Account
from bot.states.api_fsm import APIStates
from bot.states.confirm_fsm import ConfirmStates

# 🟣 فاز ۳ (رفع بن‌بست FSM): helper های قبلی
from bot.keyboards.cancel import get_cancel_keyboard, with_cancel_hint
from bot.keyboards.main_menu import get_main_menu_keyboard
from utils.fsm_cleanup import cleanup_fsm_temp_files
from utils.safe_edit import safe_edit_or_answer

# 🛡 فاز ۵ (مدیریت خطا): helper های فاز ۱ و ۲
from utils.error_messages import report_db_error
from utils.telegram_helpers import (
    answer_callback_error,
    safe_callback_answer,
    safe_edit_message,
)

# 📄 فاز ۴ (صفحه‌بندی استاندارد): زیرساخت مشترک paginate (فاز ۱)
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
router = Router(name="api_handlers_router")

# ==========================================
# ADD API FLOW
# ==========================================
@router.callback_query(F.data == "menu_add_api/")
async def add_api_start(callback: types.CallbackQuery, state: FSMContext) -> None:
    # 🛡 فاز ۵: safe_callback_answer در همهٔ نقاط (محافظ QUERY_ID_INVALID)
    await safe_callback_answer(callback)

    await cleanup_fsm_temp_files(state)
    await state.clear()

    await state.set_state(APIStates.waiting_for_api_credentials)

    text = with_cancel_hint(
        "📥 <b>افزودن API جدید</b>\n\n"
        "لطفاً اطلاعات API (آیدی و هش) را در دو خط مجزا ارسال کنید:\n\n"
        "مثال:\n"
        "<code>1234567</code>\n"
        "<code>abcdef1234567890abcdef1234567890</code>"
    )
    await safe_edit_or_answer(callback.message, text, reply_markup=get_cancel_keyboard())

@router.message(APIStates.waiting_for_api_credentials, F.text)
async def process_api_credentials(message: types.Message, state: FSMContext, session: AsyncSession) -> None:
    # حذف خطوط خالی احتمالی و اسپلیت کردن متن
    lines = [line.strip() for line in message.text.strip().split('\n') if line.strip()]

    # 🛡 فاز ۵ (مشکل ۵ — تأیید): خطاهای validation کیبورد انصراف + راهنمای /cancel دارند
    if len(lines) < 2:
        return await message.answer(
            with_cancel_hint(
                "⚠️ فرمت نامعتبر است!\n"
                "لطفاً API ID و API HASH را دقیقاً در دو خط مجزا بفرستید."
            ),
            reply_markup=get_cancel_keyboard()
        )

    api_id_text = lines[0]
    api_hash = lines[1]

    if not api_id_text.isdigit():
        return await message.answer(
            with_cancel_hint("⚠️ فرمت نامعتبر! API ID باید فقط شامل اعداد باشد."),
            reply_markup=get_cancel_keyboard()
        )

    # 🛡 باگ ۲: اعتبارسنجی api_hash
    if not re.fullmatch(r"[0-9a-fA-F]{32}", api_hash):
        return await message.answer(
            with_cancel_hint("⚠️ فرمت نامعتبر! API Hash باید ۳۲ کاراکتر هگز باشد."),
            reply_markup=get_cancel_keyboard()
        )

    api_id_value = int(api_id_text)

    # 🛡 باگ ۱: بررسی تکراری بودن API ID قبل از Insert
    exists = await session.scalar(select(APIKey).where(APIKey.api_id == api_id_value).limit(1))
    if exists:
        return await message.answer(
            with_cancel_hint("⚠️ این API قبلاً ثبت شده است."),
            reply_markup=get_cancel_keyboard()
        )

    new_api = APIKey(api_id=api_id_value, api_hash=api_hash, is_active=True)

    try:
        session.add(new_api)
        await session.commit()
    except Exception as e:
        # 🛡 فاز ۵ (مشکل ۱): پیام عمومی «خطایی در ذخیره رخ داد» → پیام بر اساس نوع
        # خطا. رایج‌ترین حالت: کلید تکراری (IntegrityError) → «این API قبلاً ثبت
        # شده است» — دقیقاً همان اطلاعی که کاربر لازم دارد.
        # state حفظ می‌شود تا کاربر همان‌جا API دیگری ارسال کند.
        await session.rollback()
        return await message.answer(
            with_cancel_hint(report_db_error("API", e)),
            reply_markup=get_cancel_keyboard()
        )

    # 🛡 فاز ۵: پاک کردن state و پیام موفقیت «فقط» در مسیر موفق — قبلاً داخل try
    # بودند و شکستِ خودِ ارسال پیامِ موفقیت، بعد از commitِ موفق rollback و
    # پیام خطا تولید می‌کرد (خطای خلاف واقع)
    await state.clear()

    builder = InlineKeyboardBuilder()
    builder.button(text="➕ افزودن API دیگر", callback_data="menu_add_api/")
    builder.button(text="✅ پایان", callback_data="api_add_finish/")
    builder.adjust(2)

    await message.answer(
        f"✅ <b>API با موفقیت اضافه شد!</b>\n"
        f"🆔 <code>{api_id_text}</code>\n\n"
        "<i>برای افزودن API بعدی از دکمه زیر استفاده کنید:</i>",
        reply_markup=builder.as_markup()
    )


# ==========================================
# 🟣 فاز ۳ — پایان افزودن API
# ==========================================
@router.callback_query(F.data == "api_add_finish/")
async def finish_adding_apis(callback: types.CallbackQuery, state: FSMContext) -> None:
    await safe_callback_answer(callback)

    await cleanup_fsm_temp_files(state)
    await state.clear()

    await safe_edit_or_answer(
        callback.message,
        "✅ <b>فرآیند افزودن API به پایان رسید.</b>\nشما به منوی اصلی بازگشتید.",
        reply_markup=get_main_menu_keyboard()
    )


async def render_api_list(callback: types.CallbackQuery, session: AsyncSession, page: int = 1):
    """
    🗂 لیست APIها — 📄 فاز ۴:

    - دکمهٔ «🔄 بروزرسانی» اضافه شد (رفرش همان صفحه — callback همان
      api_page_{page}/ ناوبری است، پس هندلر موجود هر دو را پوشش می‌دهد)
    - رفع N+1: قبلاً برای «هر» API یک کوئری COUNT جدا زده می‌شد (۱۱ کوئری در
      صفحهٔ پر)؛ حالا شمارش اکانت‌های متصلِ کل صفحه با «یک» کوئری تجمعی
      (IN + GROUP BY) واکشی می‌شود
    - صفحه‌بندی با زیرساخت مشترک فاز ۱ (clamp صفحه + ناوبری استاندارد؛
      اصلاح تایپوی جهت فلش «صفحه بعد ⬅️» → «صفحه بعد ➡️»)
    """
    builder = InlineKeyboardBuilder()

    # 🛡 فاز ۵ (مشکل ۱): خواندن‌های دیتابیس داخل حفاظ — این تابع از چند هندلر
    # صدا زده می‌شود. answer_callback_error هم مسیر alert و هم فراخوانی داخلی
    # (پیام جدید با کیبورد fallback) را پوشش می‌دهد.
    try:
        # ── ۱. شمارش کل + اصلاح هوشمند شماره صفحه ──
        total_apis = await session.scalar(select(func.count(APIKey.id))) or 0
        total_pages = calculate_total_pages(total_apis)
        page = clamp_page(page, total_pages)
        offset = get_page_offset(page)

        # ── ۲. APIهای صفحهٔ فعلی (جدیدترین اول) ──
        apis = (
            await session.scalars(
                select(APIKey)
                .order_by(APIKey.id.desc())
                .offset(offset)
                .limit(PAGINATION_SIZE)
            )
        ).all()

        # ── ۳. 📄 فاز ۴ (رفع N+1): شمارش اکانت‌های متصلِ کل صفحه با یک کوئری ──
        acc_counts: dict = {}
        if apis:
            rows = (
                await session.execute(
                    select(Account.api_id, func.count(Account.id))
                    .where(Account.api_id.in_([a.id for a in apis]))
                    .group_by(Account.api_id)
                )
            ).all()
            acc_counts = dict(rows)
    except Exception as e:
        await session.rollback()
        return await answer_callback_error(
            callback, report_db_error("APIها", e), get_main_menu_keyboard()
        )

    # ── لیست خالی ──
    if total_apis == 0:
        builder.button(text="➕ افزودن API جدید", callback_data="menu_add_api/")
        builder.button(text="🏛 منوی اصلی", callback_data="menu_home/")
        builder.adjust(1)
        return await safe_edit_or_answer(
            callback.message,
            "🗂 <b>لیست APIهای ثبت‌شده</b>\n\n"
            "⚠️ هیچ API در دیتابیس یافت نشد.\n\n"
            "برای شروع، یک API جدید اضافه کنید:",
            reply_markup=builder.as_markup()
        )

    text = (
        "🗂 <b>لیست APIهای ثبت‌شده</b>\n"
        f"🔢 مجموع: <b>{total_apis}</b> API\n"
        f"📄 صفحهٔ <b>{page}</b> از <b>{total_pages}</b>\n\n"
    )

    for api in apis:
        acc_count = acc_counts.get(api.id, 0)
        
        masked_hash = html.escape(api.api_hash[:6]) + "••••••••" if api.api_hash else "نامشخص"
        status_display = "✅ فعال" if api.is_active else "❌ غیرفعال"

        text += (
            f"🆔 شناسه API: <code>{api.api_id}</code>\n"
            f"🔏 هش API: <code>{masked_hash}</code>\n"
            f"📍 وضعیت: {status_display}\n"
            f"👥 اکانت‌های متصل: {acc_count}\n"
            f"🗑 برای حذف کامند زیر را بفرستید:\n"
            f"<code>/DeleteApi_{api.id}</code>\n"
            "------------------------\n"
        )

        builder.button(text=f"❌ حذف {api.api_id}", callback_data=f"del_api_{api.id}_{page}/")

    # چیدمان دکمه‌های حذف دو ستونه
    builder.adjust(2)

    # ── ناوبری صفحات (📄 فاز ۴: زیرساخت مشترک — فرمت api_page_{n}/ حفظ شده) ──
    add_pagination_nav_row(builder, page, total_pages, callback_prefix="api_")

    # ── 📄 فاز ۴: دکمهٔ 🔄 بروزرسانی (رفرش همان صفحه) + 🏛 منوی اصلی ──
    add_list_footer(builder, refresh_callback=f"api_page_{page}/")

    # 🛡 فاز ۵: ویرایش امن به جای edit_text خام
    await safe_edit_or_answer(callback.message, text, reply_markup=builder.as_markup())

# --- File: api_handlers.py ---

# حوالی خط ۲۰۸:
@router.callback_query(F.data == "menu_list_api/")
@router.callback_query(F.data.startswith("api_page_"))
# --- FIX M6: Add state to signature and align with standard cleanup pattern ---
async def list_apis_handler(callback: types.CallbackQuery, state: FSMContext, session: AsyncSession) -> None:
# ------------------------------------------------------------------------------
    """
    📄 فاز ۴: هندلر مشترک ورود به لیست، «صفحه بعد/قبل» و «🔄 بروزرسانی» —
    دکمهٔ بروزرسانی دقیقاً همان callback صفحهٔ فعلی را صدا می‌زند.
    """
    await safe_callback_answer(callback)

    # --- FIX M6: Clear previous FSM state ---
    await cleanup_fsm_temp_files(state)
    await state.clear()
    # ----------------------------------------

    # menu_list_api/ → صفحهٔ ۱ | api_page_N/ → N
    page = parse_page_from_callback(callback.data)

    await render_api_list(callback, session, page)


# ==========================================
# 🟢 فاز ۲ (تأیید دو مرحله‌ای) — DELETE API FLOW
# ==========================================
def build_api_delete_confirmation_text(api_obj: APIKey, linked_accounts_count: int) -> str:
    # 🛡 باگ ۳: استفاده از html.escape برای جلوگیری از تخریب پارسه در صورت شروع هش با کاراکترهای خطرناک
    masked_hash = html.escape(api_obj.api_hash[:6]) + "••••••••" if api_obj.api_hash else "نامشخص"
    status_display = "فعال ✅" if api_obj.is_active else "غیرفعال ❌"

    return (
        "⚠️ <b>تأیید حذف API</b>\n\n"
        f"آیا از حذف API با آیدی <code>{api_obj.api_id}</code> مطمئن هستید؟\n\n"
        f"🆔 شناسه دیتابیس: <code>{api_obj.id}</code>\n"
        f"🔑 شناسه API: <code>{api_obj.api_id}</code>\n"
        f"🔏 هش API: <code>{masked_hash}</code>\n"
        f"📍 وضعیت: {status_display}\n"
        f"👥 تعداد اکانت‌های متصل: <b>{linked_accounts_count}</b>\n\n"
        "⚠️ <b>توجه: این عمل قابل بازگشت نیست.</b>"
    )


def build_api_delete_confirmation_keyboard(api_db_id: int) -> types.InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ بله، حذف کن", callback_data=f"confirm_delete_api_{api_db_id}/")
    builder.button(text="❌ انصراف", callback_data="cancel_confirm_delete_api/")
    builder.adjust(2)
    return builder.as_markup()


# ==========================================
# 🟢 فاز ۲ — مرحله ۱ (مسیر دکمه inline)
# ==========================================
@router.callback_query(F.data.startswith("del_api_") & F.data.endswith("/"))
async def delete_api_handler(callback: types.CallbackQuery, state: FSMContext, session: AsyncSession) -> None:
    data_parts = callback.data.replace("del_api_", "").replace("/", "").split("_")

    if not data_parts[0].isdigit():
        return await safe_callback_answer(callback, "⚠️ آیدی نامعتبر.", show_alert=True)

    api_db_id = int(data_parts[0])
    page = int(data_parts[1]) if len(data_parts) > 1 and data_parts[1].isdigit() else 1

    # 🛡 فاز ۵ (مشکل ۱): هر دو خواندن دیتابیس (گارد وابستگی + واکشی API) داخل حفاظ
    try:
        count_stmt = select(func.count(Account.id)).where(Account.api_id == api_db_id)
        linked_accounts_count = await session.scalar(count_stmt) or 0

        stmt = select(APIKey).where(APIKey.id == api_db_id)
        result = await session.execute(stmt)
        api_obj = result.scalar_one_or_none()
    except Exception as e:
        await session.rollback()
        return await answer_callback_error(
            callback, report_db_error("API", e), get_main_menu_keyboard()
        )

    # 🔴 گارد بررسی وابستگی اکانت‌ها
    if linked_accounts_count > 0:
        return await safe_callback_answer(
            callback,
            f"⛔️ خطر اورلود API!\n\n"
            f"تعداد {linked_accounts_count} اکانت به این API متصل هستند. ابتدا اکانت‌ها را حذف کنید.",
            show_alert=True
        )

    if not api_obj:
        await safe_callback_answer(callback, "⚠️ این API قبلاً حذف شده است.", show_alert=True)
        return await render_api_list(callback, session, page)

    await safe_callback_answer(callback)

    await state.update_data(confirm_action="delete_api", target_id=api_db_id, return_page=page)
    await state.set_state(ConfirmStates.waiting_for_confirmation)

    # 🛡 فاز ۵: ویرایش امن (پیام حذف‌شده/غیرقابل ویرایش → fallback به answer
    # تا دکمه‌های تأیید/انصراف همیشه در دسترس کاربر بمانند)
    await safe_edit_message(
        callback.message,
        build_api_delete_confirmation_text(api_obj, linked_accounts_count),
        reply_markup=build_api_delete_confirmation_keyboard(api_db_id)
    )


# ==========================================
# 🟢 فاز ۲ — مرحله ۲ (اجرای واقعی)
# ==========================================
@router.callback_query(F.data.startswith("confirm_delete_api_") & F.data.endswith("/"))
async def confirm_delete_api_handler(callback: types.CallbackQuery, state: FSMContext, session: AsyncSession) -> None:
    # 🔒 بررسی امنیتی state
    current_state = await state.get_state()
    fsm_data = await state.get_data()

    if current_state != ConfirmStates.waiting_for_confirmation or fsm_data.get("confirm_action") != "delete_api":
        return await safe_callback_answer(
            callback,
            "⚠️ این درخواست تأیید منقضی شده است. لطفاً از ابتدا اقدام کنید.",
            show_alert=True
        )

    api_id_str = callback.data.replace("confirm_delete_api_", "").replace("/", "")

    if not api_id_str.isdigit():
        return await safe_callback_answer(callback, "⚠️ آیدی نامعتبر.", show_alert=True)

    api_db_id = int(api_id_str)

    # 🔒 تطابق آیدی API درخواستی با آیدی ذخیره‌شده در state
    if fsm_data.get("target_id") != api_db_id:
        return await safe_callback_answer(
            callback,
            "⚠️ این درخواست تأیید با API نمایش‌داده‌شده مطابقت ندارد. لطفاً از ابتدا اقدام کنید.",
            show_alert=True
        )

    return_page = fsm_data.get("return_page", 1)

    # 🛡 فاز ۵ (مشکل ۱): هر دو خواندن مجدد (گارد Race Condition + واکشی API)
    # داخل حفاظ. در خطای خواندن، state حفظ می‌شود و پیام تأیید دست‌نخورده می‌ماند.
    try:
        count_stmt = select(func.count(Account.id)).where(Account.api_id == api_db_id)
        linked_accounts_count = await session.scalar(count_stmt) or 0

        stmt = select(APIKey).where(APIKey.id == api_db_id)
        result = await session.execute(stmt)
        api_obj = result.scalar_one_or_none()
    except Exception as e:
        await session.rollback()
        return await answer_callback_error(
            callback, report_db_error("API", e), get_main_menu_keyboard()
        )

    # 🔁 گارد وابستگی (بررسی مجدد — جلوگیری از Race Condition)
    if linked_accounts_count > 0:
        await state.clear()
        await safe_callback_answer(
            callback,
            f"⛔️ حذف متوقف شد!\n{linked_accounts_count} اکانت به این API متصل شده‌اند. ابتدا اکانت‌ها را حذف کنید.",
            show_alert=True
        )
        return await render_api_list(callback, session, return_page)

    if not api_obj:
        await state.clear()
        await safe_callback_answer(callback, "⚠️ این API قبلاً حذف شده است.", show_alert=True)
        return await render_api_list(callback, session, return_page)

    try:
        await session.delete(api_obj)
        await session.commit()
    except Exception as e:
        # 🛡 فاز ۵ (مشکل ۱): alert عمومی «❌ خطای دیتابیس.» → پیام بر اساس نوع خطا
        await session.rollback()
        await state.clear()
        await answer_callback_error(
            callback, report_db_error("API", e), get_main_menu_keyboard()
        )
        return await render_api_list(callback, session, return_page)

    # 🟢 پاک کردن state بعد از اجرای موفق عملیات
    await state.clear()

    await safe_callback_answer(callback, "✅ API با موفقیت حذف شد.", show_alert=False)

    await render_api_list(callback, session, return_page)


# ==========================================
# 🟢 فاز ۲ — انصراف از حذف API
# ==========================================
@router.callback_query(F.data == "cancel_confirm_delete_api/")
async def cancel_delete_api_confirmation(callback: types.CallbackQuery, state: FSMContext, session: AsyncSession) -> None:
    fsm_data = await state.get_data()
    return_page = fsm_data.get("return_page", 1)

    if fsm_data.get("confirm_action") == "delete_api":
        await state.clear()

    await safe_callback_answer(callback, "🚫 عملیات حذف API لغو شد.")

    # render_api_list از داخل خودش حفاظ‌شده است
    return await render_api_list(callback, session, return_page)


# ==========================================
# 🟢 فاز ۲ — مرحله ۱ (مسیر کامند متنی)
# ==========================================
@router.message(F.text.regexp(r"^/DeleteApi_(\d+)$"))
async def delete_api_command_handler(message: types.Message, state: FSMContext, session: AsyncSession) -> None:
    match = re.match(r"^/DeleteApi_(\d+)$", message.text)
    if not match:
        return

    api_db_id = int(match.group(1))

    # 🛡 فاز ۵ (مشکل ۱): هر دو خواندن (گارد وابستگی + واکشی API) داخل حفاظ
    try:
        count_stmt = select(func.count(Account.id)).where(Account.api_id == api_db_id)
        linked_accounts_count = await session.scalar(count_stmt) or 0

        stmt = select(APIKey).where(APIKey.id == api_db_id)
        result = await session.execute(stmt)
        api_obj = result.scalar_one_or_none()
    except Exception as e:
        await session.rollback()
        return await message.answer(
            report_db_error("API", e),
            reply_markup=get_main_menu_keyboard()
        )

    # 🔴 گارد بررسی وابستگی در حالت کامند
    if linked_accounts_count > 0:
        return await message.answer(
            f"⛔️ <b>خطر اورلود API!</b>\n\n"
            f"تعداد {linked_accounts_count} اکانت به این API متصل هستند. ابتدا اکانت‌ها را حذف کنید.",
            reply_markup=get_main_menu_keyboard()
        )

    if not api_obj:
        return await message.answer(
            "⚠️ این API قبلاً حذف شده است.",
            reply_markup=get_main_menu_keyboard()
        )

    # 🟢 ذخیره اطلاعات عملیات در انتظار تأیید در FSM
    await state.update_data(confirm_action="delete_api", target_id=api_db_id, return_page=1)
    await state.set_state(ConfirmStates.waiting_for_confirmation)

    await message.answer(
        build_api_delete_confirmation_text(api_obj, linked_accounts_count),
        reply_markup=build_api_delete_confirmation_keyboard(api_db_id)
    )