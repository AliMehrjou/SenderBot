import html
import logging
import os
import uuid
from typing import Optional

from aiogram import Bot, Router, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.utils.keyboard import InlineKeyboardBuilder
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from utils.fsm_cleanup import cleanup_fsm_temp_files
from bot.keyboards.cancel import get_cancel_keyboard, with_cancel_hint
from bot.keyboards.main_menu import get_main_menu_button
from database.models import Banner
from utils.error_messages import (
    get_download_error_message,
    get_generic_error_message,
    report_db_error,
)
from utils.fsm_cleanup import cleanup_fsm_temp_files
from utils.safe_edit import safe_edit_or_answer
from utils.telegram_helpers import answer_callback_error, safe_callback_answer

logger = logging.getLogger(__name__)
router = Router(name="banner_router")

# 🎨 سقف بنرهای فعالِ هم‌زمان — بنر یازدهم فقط وقتی پذیرفته می‌شود که یکی خاموش/حذف شده باشد
MAX_ACTIVE_BANNERS = 10

# پوشه‌ی نگهداری فایل‌های مدیای بنرها
BANNERS_DIR = "banners"
os.makedirs(BANNERS_DIR, exist_ok=True)


class BannerStates(StatesGroup):
    """🎨 FSM پنل مدیریت بنر (متن → مدیای اختیاری)"""
    waiting_for_text = State()
    waiting_for_media = State()


# ==========================================
# 🎨 helper های مشترک
# ==========================================

def _remove_banner_file(path: Optional[str]) -> None:
    """حذف فیزیکی فایل مدیای بنر — فقط و فقط اگر داخل پوشه‌ی banners باشد (حفاظ مسیر)"""
    if path and isinstance(path, str) and path.startswith(f"{BANNERS_DIR}/") and os.path.exists(path):
        try:
            os.remove(path)
            logger.info(f"Garbage Collection: deleted banner media {path}")
        except OSError:
            logger.warning(f"Failed to delete banner media: {path}")


def _media_step_keyboard():
    """کیبورد مرحله‌ی مدیا: «بدون مدیا» + انصراف"""
    builder = InlineKeyboardBuilder()
    builder.button(text="💬 بدون مدیا (فقط متن)", callback_data="banner_skip_media/")
    builder.button(text="❌ انصراف", callback_data="cancel_current_flow/")
    builder.button(text="🏛 منوی اصلی", callback_data="menu_home/")
    builder.adjust(1, 2)
    return builder.as_markup()


def _back_to_banners_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="🎨 بازگشت به بنرها", callback_data="menu_banners/")
    builder.button(text="🏛 منوی اصلی", callback_data="menu_home/")
    builder.adjust(2)
    return builder.as_markup()


async def _fetch_banners(session: AsyncSession):
    """واکشی همه‌ی بنرها + تعداد بنرهای فعال"""
    banners = (await session.scalars(select(Banner).order_by(Banner.id.asc()))).all()
    active_count = await session.scalar(
        select(func.count(Banner.id)).where(Banner.is_active == True)  # noqa: E712
    ) or 0
    return list(banners), int(active_count)


def _build_banner_panel(banners: list, active_count: int):
    """ساخت متن + کیبورد پنل بنرها (لیست، وضعیت، مصرف، پیش‌نمایش)"""
    text = (
        "🎨 <b>مدیریت بنرها (مخزن چرخش بنر)</b>\n"
        f"🟢 بنرهای فعال: <b>{active_count}</b> از <b>{MAX_ACTIVE_BANNERS}</b>\n\n"
    )

    builder = InlineKeyboardBuilder()

    if not banners:
        text += (
            "⚠️ هنوز هیچ بنری ثبت نشده است.\n\n"
            "<i>بنرها در سفارش‌هایی که گزینه‌ی «مخزن بنر» را انتخاب کرده‌اند، "
            "به‌صورت تصادفی بین chunk های ارسال (اکانت‌ها) چرخش می‌خورند.</i>"
        )
    else:
        for idx, banner in enumerate(banners, start=1):
            status = "🟢 فعال" if banner.is_active else "🔴 غیرفعال"
            media_label = {"photo": "📷 عکس", "video": "🎬 ویدیو"}.get(banner.media_type or "", "💬 فقط متن")
            raw_text = (banner.text or "").strip()
            preview = html.escape(raw_text[:80]) + ("…" if len(raw_text) > 80 else "")

            text += (
                f"<b>{idx}.</b> {status} | 🔁 مصرف: <b>{banner.usage_count}</b> | {media_label}\n"
                f"   📝 «{preview}»\n\n"
            )

            # هر بنر یک ردیف: روشن/خاموش + حذف
            builder.row(
                types.InlineKeyboardButton(
                    text=("💡 خاموش" if banner.is_active else "🔌 روشن"),
                    callback_data=f"banner_toggle_{banner.id}/",
                ),
                types.InlineKeyboardButton(
                    text="🗑 حذف",
                    callback_data=f"banner_del_{banner.id}/",
                ),
            )

    builder.row(types.InlineKeyboardButton(text="➕ افزودن بنر", callback_data="banner_add/"))
    builder.row(
        types.InlineKeyboardButton(text="🔄 بروزرسانی", callback_data="menu_banners/"),
        types.InlineKeyboardButton(text="🏛 منوی اصلی", callback_data="menu_home/"),
    )

    return text, builder.as_markup()


async def _show_banner_panel(
    callback: types.CallbackQuery,
    session: AsyncSession,
    skip_answer: bool = False,
) -> None:
    """رندر پنل بنرها روی پیام کال‌بک (الگوی رفرش امن پروژه)"""
    if not skip_answer:
        await callback.answer()
    try:
        banners, active_count = await _fetch_banners(session)
    except Exception as e:
        await session.rollback()
        return await answer_callback_error(callback, report_db_error("بنرها", e), get_main_menu_button())

    text, markup = _build_banner_panel(banners, active_count)
    await safe_edit_or_answer(callback.message, text, reply_markup=markup)


async def _send_banner_panel_message(message: types.Message, session: AsyncSession) -> None:
    """رندر پنل بنرها به‌صورت پیام جدید (برای انتهای فلوی افزودن)"""
    try:
        banners, active_count = await _fetch_banners(session)
        text, markup = _build_banner_panel(banners, active_count)
        await message.answer(text, reply_markup=markup)
    except Exception as e:
        logger.error(f"Failed to render banner panel message: {e}")


# ==========================================
# 🎨 ورود به پنل (دکمه‌ی منوی ادمین + دستور /banners)
# ==========================================

@router.callback_query(F.data == "menu_banners/")
async def banner_menu_handler(callback: types.CallbackQuery, state: FSMContext, session: AsyncSession) -> None:
    """🎨 ورود به پنل مدیریت بنرها"""
    await cleanup_fsm_temp_files(state)
    await state.clear()
    await _show_banner_panel(callback, session)


@router.message(Command("banners"))
async def banners_command_handler(message: types.Message, state: FSMContext, session: AsyncSession) -> None:
    """🎨 ورود سریع به پنل بنرها با دستور /banners"""
    await cleanup_fsm_temp_files(state)
    await state.clear()
    try:
        banners, active_count = await _fetch_banners(session)
    except Exception as e:
        await session.rollback()
        return await message.answer(get_generic_error_message(), reply_markup=get_main_menu_button())

    text, markup = _build_banner_panel(banners, active_count)
    await message.answer(text, reply_markup=markup)


# ==========================================
# 🎨 افزودن بنر: متن → مدیا (اختیاری) → ثبت
# ==========================================

@router.callback_query(F.data == "banner_add/")
async def banner_add_start(callback: types.CallbackQuery, state: FSMContext, session: AsyncSession) -> None:
    # پاکسازی state قبلی برای جلوگیری از نشت دیتا به فلوی ساخت بنر
    await cleanup_fsm_temp_files(state)
    await state.clear()
    
    try:
        active_count = await session.scalar(
            select(func.count(Banner.id)).where(Banner.is_active == True)  # noqa: E712
        ) or 0
    except Exception as e:
        await session.rollback()
        return await answer_callback_error(callback, report_db_error("بنرها", e), get_main_menu_button())

    if active_count >= MAX_ACTIVE_BANNERS:
        return await callback.answer(
            f"⚠️ سقف {MAX_ACTIVE_BANNERS} بنر فعال پر است!\n"
            "ابتدا یکی از بنرها را خاموش یا حذف کنید.",
            show_alert=True,
        )

    await callback.answer()
    
    await state.set_state(BannerStates.waiting_for_text)

    builder = InlineKeyboardBuilder()
    builder.button(text="❌ انصراف", callback_data="cancel_current_flow/")
    builder.button(text="🏛 منوی اصلی", callback_data="menu_home/")
    builder.adjust(2)

    await safe_edit_or_answer(
        callback.message,
        with_cancel_hint(
            "📝 <b>متن بنر را ارسال کنید:</b>\n\n"
            "<i>Spintax و پلیس‌هولدرهای {first_name} و {username} در بنرها هم پشتیبانی می‌شوند.</i>"
        ),
        reply_markup=builder.as_markup(),
    )

@router.message(BannerStates.waiting_for_text, F.text)
async def banner_process_text(message: types.Message, state: FSMContext) -> None:
    banner_text = (message.text or "").strip()

    if not banner_text:
        return await message.answer(
            with_cancel_hint("⚠️ متن بنر نمی‌تواند خالی باشد. لطفاً متن را ارسال کنید:"),
            reply_markup=get_cancel_keyboard(),
        )

    await state.update_data(banner_text=banner_text)
    await state.set_state(BannerStates.waiting_for_media)

    await message.answer(
        with_cancel_hint(
            "🖼 <b>مدیای بنر را ارسال کنید (اختیاری):</b>\n\n"
            "📷 عکس یا 🎬 ویدیو ارسال کنید، یا با دکمه‌ی «بدون مدیا» فقط متن را ثبت کنید.\n"
            "<i>حداکثر حجم ویدیو: ۲۰ مگابایت.</i>"
        ),
        reply_markup=_media_step_keyboard(),
    )


@router.message(BannerStates.waiting_for_media, F.photo | F.video)
async def banner_process_media(
    message: types.Message,
    state: FSMContext,
    bot: Bot,
    session: AsyncSession,
) -> None:
    fsm_data = await state.get_data()
    banner_text = fsm_data.get("banner_text")

    if not banner_text:
        await state.clear()
        return await message.answer(
            "⚠️ داده‌های بنر ناقص است. لطفاً از «🎨 مدیریت بنرها» دوباره شروع کنید.",
            reply_markup=_back_to_banners_keyboard(),
        )

    media_path = None
    media_type = None

    # 🛡 الگوی استاندارد دانلود پروژه: حفاظ کامل + پاکسازی فایل نصفه‌کاره
    try:
        if message.photo:
            media_type = "photo"
            file_id = message.photo[-1].file_id
            file = await bot.get_file(file_id)
            media_path = f"{BANNERS_DIR}/{uuid.uuid4()}.jpg"
            await bot.download_file(file.file_path, destination=media_path)

        elif message.video:
            if message.video.file_size and message.video.file_size > 20 * 1024 * 1024:
                return await message.answer(
                    with_cancel_hint("⚠️ حجم ویدیو نباید بیشتر از ۲۰ مگابایت باشد."),
                    reply_markup=_media_step_keyboard(),
                )
            media_type = "video"
            file_id = message.video.file_id
            file = await bot.get_file(file_id)
            media_path = f"{BANNERS_DIR}/{uuid.uuid4()}.mp4"
            await bot.download_file(file.file_path, destination=media_path)

    except Exception as e:
        logger.error(f"Error downloading banner media: {e}", exc_info=True)
        _remove_banner_file(media_path)
        return await message.answer(
            with_cancel_hint(get_download_error_message()),
            reply_markup=_media_step_keyboard(),
        )

    await _save_banner(session, message, state, banner_text, media_path, media_type)


@router.message(BannerStates.waiting_for_media)
async def banner_invalid_media(message: types.Message) -> None:
    """محتوای غیرمجاز در مرحله‌ی مدیا (مثلاً فایل یا ویس)"""
    await message.answer(
        with_cancel_hint("⚠️ فقط 📷 عکس یا 🎬 ویدیو پذیرفته می‌شود؛ یا دکمه‌ی «💬 بدون مدیا» را بزنید."),
        reply_markup=_media_step_keyboard(),
    )


@router.callback_query(BannerStates.waiting_for_media, F.data == "banner_skip_media/")
async def banner_skip_media(callback: types.CallbackQuery, state: FSMContext, session: AsyncSession) -> None:
    """ثبت بنر فقط با متن (بدون مدیا)"""
    await callback.answer()

    fsm_data = await state.get_data()
    banner_text = fsm_data.get("banner_text")

    if not banner_text:
        await state.clear()
        return await callback.message.answer(
            "⚠️ داده‌های بنر ناقص است. لطفاً از «🎨 مدیریت بنرها» دوباره شروع کنید.",
            reply_markup=_back_to_banners_keyboard(),
        )

    await _save_banner(session, callback.message, state, banner_text, None, None)


async def _save_banner(
    session: AsyncSession,
    message: types.Message,
    state: FSMContext,
    banner_text: str,
    media_path: Optional[str],
    media_type: Optional[str],
) -> None:
    """
    ثبت نهایی بنر در دیتابیس:
    - چک مجدد سقف MAX_ACTIVE_BANNERS بنر فعال (ضد رقابت — شاید در همین لحظه بنر دیگری فعال شده)
    - در صورت خطا/سقف پر: پاکسازی فایل مدیای دانلودشده
    - در صورت موفقیت: پاکسازی FSM + نمایش پنل به‌روز
    """
    try:
        active_count = await session.scalar(
            select(func.count(Banner.id)).where(Banner.is_active == True)  # noqa: E712
        ) or 0

        if active_count >= MAX_ACTIVE_BANNERS:
            await session.rollback()
            _remove_banner_file(media_path)
            return await message.answer(
                f"⚠️ <b>سقف {MAX_ACTIVE_BANNERS} بنر فعال پر است!</b>\n\n"
                "بنر جدید ثبت نشد. ابتدا یکی از بنرهای موجود را خاموش یا حذف کنید و دوباره تلاش کنید.",
                reply_markup=_back_to_banners_keyboard(),
            )

        new_banner = Banner(
            text=banner_text,
            media_path=media_path,
            media_type=media_type,
            is_active=True,
            usage_count=0,
        )
        session.add(new_banner)
        await session.commit()

    except Exception as e:
        await session.rollback()
        _remove_banner_file(media_path)
        logger.error(f"Error saving banner: {e}", exc_info=True)
        return await message.answer(get_generic_error_message(), reply_markup=_back_to_banners_keyboard())

    await state.clear()
    await message.answer(f"✅ بنر <b>#{new_banner.id}</b> با موفقیت ثبت و فعال شد.")
    await _send_banner_panel_message(message, session)


# ==========================================
# 🎨 روشن/خاموش کردن بنر
# ==========================================

@router.callback_query(F.data.regexp(r"^banner_toggle_(\d+)/$"))
async def banner_toggle_handler(callback: types.CallbackQuery, session: AsyncSession) -> None:
    banner_id_str = callback.data.replace("banner_toggle_", "").replace("/", "")

    if not banner_id_str.isdigit():
        return await callback.answer("⚠️ شناسه نامعتبر است.")

    try:
        banner = await session.get(Banner, int(banner_id_str))

        if banner is None:
            await session.rollback()
            await callback.answer("⚠️ این بنر وجود ندارد.", show_alert=True)
            return await _show_banner_panel(callback, session, skip_answer=True)

        if banner.is_active:
            banner.is_active = False
            notice = "🔴 بنر خاموش شد."
        else:
            # 🎨 چک سقف هنگام روشن کردن: یازدهمین بنر فعال پذیرفته نمی‌شود
            active_count = await session.scalar(
                select(func.count(Banner.id)).where(Banner.is_active == True)  # noqa: E712
            ) or 0
            if active_count >= MAX_ACTIVE_BANNERS:
                await session.rollback()
                return await callback.answer(
                    f"⚠️ سقف {MAX_ACTIVE_BANNERS} بنر فعال پر است! ابتدا یکی را خاموش یا حذف کنید.",
                    show_alert=True,
                )
            banner.is_active = True
            notice = "🟢 بنر روشن شد."

        await session.commit()

    except Exception as e:
        await session.rollback()
        return await answer_callback_error(callback, report_db_error("بنر", e), get_main_menu_button())

    await safe_callback_answer(callback, notice)
    await _show_banner_panel(callback, session, skip_answer=True)


# ==========================================
# 🎨 حذف بنر (تأید دو مرحله‌ای + حذف فیزیکی فایل)
# ==========================================

@router.callback_query(F.data.regexp(r"^banner_del_(\d+)/$"))
async def banner_delete_confirm_handler(callback: types.CallbackQuery, session: AsyncSession) -> None:
    banner_id_str = callback.data.replace("banner_del_", "").replace("/", "")

    if not banner_id_str.isdigit():
        return await callback.answer("⚠️ شناسه نامعتبر است.")

    try:
        banner = await session.get(Banner, int(banner_id_str))
    except Exception as e:
        await session.rollback()
        return await answer_callback_error(callback, report_db_error("بنر", e), get_main_menu_button())

    if banner is None:
        await session.rollback()
        await callback.answer("⚠️ این بنر وجود ندارد.", show_alert=True)
        return await _show_banner_panel(callback, session, skip_answer=True)

    await callback.answer()

    raw_text = (banner.text or "").strip()
    preview = html.escape(raw_text[:120]) + ("…" if len(raw_text) > 120 else "")

    builder = InlineKeyboardBuilder()
    builder.button(text="🗑 بله، حذف کن", callback_data=f"banner_del_ok_{banner.id}/")
    builder.button(text="❌ انصراف", callback_data="menu_banners/")
    builder.adjust(2)

    await safe_edit_or_answer(
        callback.message,
        f"⚠️ <b>تأیید حذف بنر #{banner.id}</b>\n\n"
        f"📝 «{preview}»\n\n"
        "آیا از حذف این بنر مطمئن هستید؟\n"
        "<i>فایل مدیای آن هم از سرور پاک خواهد شد. این عمل قابل بازگشت نیست.</i>",
        reply_markup=builder.as_markup(),
    )


@router.callback_query(F.data.regexp(r"^banner_del_ok_(\d+)/$"))
async def banner_delete_execute_handler(callback: types.CallbackQuery, session: AsyncSession) -> None:
    banner_id_str = callback.data.replace("banner_del_ok_", "").replace("/", "")

    if not banner_id_str.isdigit():
        return await callback.answer("⚠️ شناسه نامعتبر است.")

    try:
        banner = await session.get(Banner, int(banner_id_str))

        if banner is None:
            await session.rollback()
            await callback.answer("⚠️ این بنر قبلاً حذف شده است.", show_alert=True)
            return await _show_banner_panel(callback, session, skip_answer=True)

        # مسیر فایل قبل از حذف ردیف ذخیره می‌شود؛ حذف فیزیکی فقط بعد از commit موفق
        media_path = banner.media_path
        await session.delete(banner)
        await session.commit()

    except Exception as e:
        await session.rollback()
        return await answer_callback_error(callback, report_db_error("حذف بنر", e), get_main_menu_button())

    _remove_banner_file(media_path)

    await safe_callback_answer(callback, "🗑 بنر حذف شد.")
    await _show_banner_panel(callback, session, skip_answer=True)