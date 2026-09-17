"""
🖼 پنل مدیریت «پکیج‌های ۳ تایی عکس پروفایل»

فلوی ادمین:
  • ساخت پکیج: نام → عکس ۱ → عکس ۲ → عکس ۳ (فایل‌ها در profile_photos/{package_id}/)
  • لیست پکیج‌ها + تعداد اکانت‌های متصل به هر پکیج
  • حذف پکیج (با هشدار در صورت اتصال اکانت + پاک‌سازی فایل‌ها از دیسک)
  • اتصال دستی پکیج به اکانت (لیست اکانت‌ها با صفحه‌بندی)
  • تخصیص خودکار: هر اکانتِ بدون پکیج → کم‌استفاده‌ترین پکیج

اعمال عکس‌ها روی پروفایل: هنگام استارت ورکر (workers/session_manager.py)
و فقط با روشن بودن auto_set_photo در GlobalSettings.
"""
import asyncio
import logging
import os
import random
import shutil
import time
from typing import Union

from aiogram import Router, types, F
from aiogram.filters import BaseFilter, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.utils.keyboard import InlineKeyboardBuilder
import html
from aiogram.filters import StateFilter
from utils.error_messages import report_db_error
from utils.fsm_cleanup import cleanup_fsm_temp_files
from sqlalchemy import select, func, update
from sqlalchemy.orm import selectinload

from config import config
from database.models import Account, Admin, ProfilePhoto, ProfilePhotoPackage, GlobalSettings

# ⚠️ فقط این یک خط را با مسیر session factory پروژه‌ی خودت هماهنگ کن
# (هر ماژولی که async_sessionmaker / AsyncSessionLocal را export می‌کند)
from database.engine import async_session as async_session_maker

from bot.keyboards.cancel import get_cancel_keyboard, with_cancel_hint
from utils.safe_edit import safe_edit_or_answer

logger = logging.getLogger(__name__)
router = Router(name="photo_handlers_router")

PHOTO_ROOT = "profile_photos"
PACKAGE_PHOTO_COUNT = config.PACKAGE_PHOTO_COUNT
ACCOUNTS_PAGE_SIZE = config.ACCOUNTS_PAGE_SIZE

os.makedirs(PHOTO_ROOT, exist_ok=True)


# ==========================================
# FSM STATES
# ==========================================
class PhotoPackageStates(StatesGroup):
    waiting_for_name = State()
    waiting_for_photo_1 = State()
    waiting_for_photo_2 = State()
    waiting_for_photo_3 = State()
    waiting_for_rename = State()
    waiting_for_replace_photo = State()


# ==========================================
# ADMIN FILTER
# ==========================================
class IsAdmin(BaseFilter):
    """
    فیلتر دسترسی ادمین:
      • ادمین اصلی از config (اولین فیلد موجود بررسی می‌شود):
        ADMIN_IDS / ADMIN_ID / OWNER_ID / SUPER_ADMIN_ID
      • ادمین‌های فرعی از جدول Admin (telegram_id)
    اگر ادمین اصلی شما فقط در جدول Admin ثبت شده، بخش config عملاً بی‌اثر و بی‌ضرر است.
    """
    async def __call__(self, event: Union[types.Message, types.CallbackQuery]) -> bool:
        user_id = event.from_user.id

        for attr in ("ADMIN_IDS", "ADMIN_ID", "OWNER_ID", "SUPER_ADMIN_ID"):
            value = getattr(config, attr, None)
            if value is None:
                continue
            ids = [value] if isinstance(value, int) else list(value)
            if user_id in ids:
                return True

        async with async_session_maker() as session:
            row = await session.execute(
                select(Admin.telegram_id).where(Admin.telegram_id == user_id)
            )
            return row.scalar_one_or_none() is not None


# ==========================================
# KEYBOARDS & HELPERS
# ==========================================
def get_photo_panel_keyboard() -> types.InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="➕ ساخت پکیج جدید", callback_data="photo_pkg_create/")
    builder.button(text="📋 لیست پکیج‌ها", callback_data="photo_pkg_list/")
    builder.button(text="🔗 اتصال پکیج به اکانت", callback_data="photo_pkg_assign/")
    builder.button(text="⚡️ تخصیص خودکار", callback_data="photo_pkg_auto/")
    builder.button(text="🚀 اعمال فوری روی همه", callback_data="photo_pkg_apply_all/")
    builder.adjust(2, 2, 1)
    builder.button(text="🏛 منوی اصلی", callback_data="menu_home/")
    return builder.as_markup()

def get_photo_finish_keyboard() -> types.InlineKeyboardMarkup:
    """کیبورد پایان کار (الگوی فاز ۵ ابزارها): «ساخت پکیج دیگر» هندلر ورود را صدا
    می‌زند که state را پاک و مجدداً تنظیم می‌کند → امن و idempotent است."""
    builder = InlineKeyboardBuilder()
    builder.button(text="➕ پکیج دیگر", callback_data="photo_pkg_create/")
    builder.button(text="📋 لیست پکیج‌ها", callback_data="photo_pkg_list/")
    builder.adjust(2)
    builder.button(text="🏛 منوی اصلی", callback_data="menu_home/")
    return builder.as_markup()


async def _drop_incomplete_flow(state: FSMContext) -> None:
    """حذف فایل‌های موقتِ فلوی نیمه‌کاره ساخت پکیج (در صورت وجود) و پاک‌سازی state.
    idempotent است؛ در ورود به هر فلوی جدید صدا زده می‌شود."""
    data = await state.get_data()
    tmp_dir = data.get("photo_tmp_dir")
    if tmp_dir and os.path.isdir(tmp_dir):
        await asyncio.to_thread(shutil.rmtree, tmp_dir, True)
    await state.clear()


async def _remove_package_folder(package_id: int) -> None:
    folder = os.path.join(PHOTO_ROOT, str(package_id))
    if os.path.isdir(folder):
        await asyncio.to_thread(shutil.rmtree, folder, True)


# ==========================================
# 🏠 PANEL
# ==========================================
@router.callback_query(F.data == "photo_pkg_panel/", IsAdmin())
async def photo_packages_panel(callback: types.CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    await _drop_incomplete_flow(state)
    
    auto_set_photo = False
    try:
        async with async_session_maker() as session:
            settings = await session.scalar(select(GlobalSettings).limit(1))
            if settings:
                auto_set_photo = settings.auto_set_photo
    except Exception as e:
        logger.error(f"Error reading settings: {e}")
        
    status_indicator = (
        "✅ <b>Auto Set Photo فعال</b> — پکیج‌ها به‌طور خودکار اعمال می‌شوند." 
        if auto_set_photo else 
        "⚠️ <b>توجه: تنظیم Auto Set Photo در تنظیمات خاموش است.</b> پکیج‌ها ساخته می‌شوند اما روی اکانت‌ها اعمال نخواهند شد. برای فعال‌سازی به ⚙️ تنظیمات بروید."
    )

    await safe_edit_or_answer(
        callback.message,
        f"🖼 <b>مدیریت پکیج‌های عکس پروفایل</b>\n\n"
        f"{status_indicator}\n\n"
        "هر پکیج دقیقاً ۳ عکس دارد که هنگام استارت ورکر، جایگزین عکس‌های قبلی اکانت می‌شوند.",
        reply_markup=get_photo_panel_keyboard(),
    )


# ==========================================
# ➕ CREATE PACKAGE (FSM: name → photo 1 → 2 → 3)
# ==========================================
@router.callback_query(F.data == "photo_pkg_create/", IsAdmin())
async def start_package_creation(callback: types.CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    await _drop_incomplete_flow(state)
    await state.set_state(PhotoPackageStates.waiting_for_name)
    await safe_edit_or_answer(
        callback.message,
        with_cancel_hint(
            "➕ <b>ساخت پکیج عکس (مرحله ۱ از ۴)</b>\n\n"
            "لطفاً <b>نام پکیج</b> را ارسال کنید (حداکثر ۱۰۰ کاراکتر):"
        ),
        reply_markup=get_cancel_keyboard(),
    )


@router.message(PhotoPackageStates.waiting_for_name, F.text & ~F.text.startswith("/"))
async def process_package_name(message: types.Message, state: FSMContext) -> None:
    name = message.text.strip()
    if not name or len(name) > 100:
        return await message.answer(
            with_cancel_hint("⚠️ نام نامعتبر است (۱ تا ۱۰۰ کاراکتر). لطفاً دوباره ارسال کنید:"),
            reply_markup=get_cancel_keyboard(),
        )

    safe_name = html.escape(name)

    try:
        async with async_session_maker() as session:
            duplicate = await session.scalar(
                select(ProfilePhotoPackage).where(ProfilePhotoPackage.name == name)
            )
    except Exception as e:
        logger.error(f"DB Error in process_package_name: {e}")
        return await report_db_error(message, e)

    if duplicate:
        return await message.answer(
            with_cancel_hint(f"⚠️ پکیجی با نام «{safe_name}» از قبل وجود دارد. نام دیگری بفرستید:"),
            reply_markup=get_cancel_keyboard(),
        )

    tmp_dir = os.path.join(
        PHOTO_ROOT, f"_tmp_{message.chat.id}_{int(time.time())}_{random.randint(100, 999)}"
    )
    os.makedirs(tmp_dir, exist_ok=True)

    await state.update_data(package_name=name, photo_tmp_dir=tmp_dir, photo_step=1)
    await state.set_state(PhotoPackageStates.waiting_for_photo_1)
    
    try:
        await message.answer(
            f"✅ نام «{safe_name}» ثبت شد.\n\n🖼 <b>عکس ۱ از {PACKAGE_PHOTO_COUNT}</b> را ارسال کنید:",
            reply_markup=get_cancel_keyboard(),
        )
    except Exception as e:
        logger.error(f"Failed to send answer in process_package_name: {e}")
        await _drop_incomplete_flow(state)
        await message.answer("⚠️ خطا در ارسال پیام. لطفاً عملیات را از ابتدا آغاز کنید و در نام پکیج از کاراکترهای ایمن استفاده کنید.")



@router.message(
    StateFilter(
        PhotoPackageStates.waiting_for_photo_1,
        PhotoPackageStates.waiting_for_photo_2,
        PhotoPackageStates.waiting_for_photo_3
    ),
    F.photo
)
async def process_package_photo(message: types.Message, state: FSMContext) -> None:
    data = await state.get_data()
    tmp_dir = data.get("photo_tmp_dir")
    name = data.get("package_name")
    position = int(data.get("photo_step", 1))

    if not tmp_dir or not name:
        await _drop_incomplete_flow(state)
        return await message.answer(
            "⚠️ داده‌های فلوی ساخت پکیج از دست رفته است؛ لطفاً از نو شروع کنید.",
            reply_markup=get_photo_finish_keyboard(),
        )

    file_path = os.path.join(tmp_dir, f"{position}.jpg")
    try:
        # بزرگ‌ترین کیفیت: photo[-1]
        await message.bot.download(message.photo[-1], destination=file_path)
    except Exception as e:
        logger.error(f"Failed to download package photo: {e}")
        return await message.answer(
            with_cancel_hint(f"⚠️ دانلود عکس {position} ناموفق بود؛ لطفاً دوباره ارسال کنید:"),
            reply_markup=get_cancel_keyboard(),
        )

    if position < PACKAGE_PHOTO_COUNT:
        next_state = {
            1: PhotoPackageStates.waiting_for_photo_2,
            2: PhotoPackageStates.waiting_for_photo_3,
        }[position]
        await state.set_state(next_state)
        await state.update_data(photo_step=position + 1)
        await message.answer(
            f"✅ عکس {position} از {PACKAGE_PHOTO_COUNT} ذخیره شد.\n\n"
            f"🖼 <b>عکس {position + 1} از {PACKAGE_PHOTO_COUNT}</b> را ارسال کنید:",
            reply_markup=get_cancel_keyboard(),
        )
    else:
        await _finalize_package_creation(message, state)


@router.message(PhotoPackageStates.waiting_for_name)
async def invalid_package_name(message: types.Message, state: FSMContext) -> None:
    await message.answer(
        with_cancel_hint("⚠️ لطفاً فقط <b>متن نام پکیج</b> را ارسال کنید:"),
        reply_markup=get_cancel_keyboard(),
    )


@router.message(
    StateFilter(
        PhotoPackageStates.waiting_for_photo_1,
        PhotoPackageStates.waiting_for_photo_2,
        PhotoPackageStates.waiting_for_photo_3
    ),
    F.text | F.document | F.video | F.animation | F.voice | F.audio,
)
async def invalid_package_photo(message: types.Message, state: FSMContext) -> None:
    await message.answer(
        with_cancel_hint("⚠️ لطفاً فقط <b>عکس</b> ارسال کنید یا /cancel بزنید (نه فایل یا متن):"),
        reply_markup=get_cancel_keyboard(),
    )


async def _finalize_package_creation(message: types.Message, state: FSMContext) -> None:
    data = await state.get_data()
    name = data["package_name"]
    safe_name = html.escape(name)
    tmp_dir = data["photo_tmp_dir"]
    final_dir: str = ""

    try:
        async with async_session_maker() as session:
            package = ProfilePhotoPackage(name=name)
            session.add(package)
            await session.flush() 

            final_dir = os.path.join(PHOTO_ROOT, str(package.id))
            os.makedirs(final_dir, exist_ok=True)

            for i in range(1, PACKAGE_PHOTO_COUNT + 1):
                src = os.path.join(tmp_dir, f"{i}.jpg")
                dst = os.path.join(final_dir, f"{i}.jpg")
                if not os.path.isfile(src):
                    raise FileNotFoundError(f"photo {i} not found in temp dir")
                await asyncio.to_thread(shutil.move, src, dst)
                session.add(ProfilePhoto(package_id=package.id, file_path=dst, position=i))

            await session.commit()
    except Exception as e:
        logger.error(f"Failed to finalize photo package: {e}")
        if final_dir and os.path.isdir(final_dir):
            await asyncio.to_thread(shutil.rmtree, final_dir, True)
        await _drop_incomplete_flow(state)
        return await message.answer(
            "❌ خطا در ثبت نهایی پکیج؛ فایل‌های موقت پاک شدند. لطفاً دوباره تلاش کنید.",
            reply_markup=get_photo_finish_keyboard(),
        )

    await _drop_incomplete_flow(state) 
    await message.answer(
        f"✅ <b>پکیج «{safe_name}» با {PACKAGE_PHOTO_COUNT} عکس ساخته شد.</b>\n\n"
        f"📁 مسیر فایل‌ها: <code>{final_dir}/</code>\n\n"
        "برای اعمال روی اکانت، از «اتصال پکیج به اکانت» یا «تخصیص خودکار» استفاده کنید.",
        reply_markup=get_photo_finish_keyboard(),
    )



# ==========================================
# 📋 LIST / 🗑 DELETE
# ==========================================
@router.callback_query(F.data == "photo_pkg_list/", IsAdmin())
async def list_photo_packages(callback: types.CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    await _drop_incomplete_flow(state)

    try:
        async with async_session_maker() as session:
            total_packages = await session.scalar(select(func.count(ProfilePhotoPackage.id)))
            
            packages = (
                await session.execute(
                    select(ProfilePhotoPackage)
                    .options(
                        selectinload(ProfilePhotoPackage.photos),
                        selectinload(ProfilePhotoPackage.accounts),
                    )
                    .order_by(ProfilePhotoPackage.id)
                    .limit(50)
                )
            ).scalars().all()
    except Exception as e:
        logger.error(f"DB Error in list_photo_packages: {e}")
        return await report_db_error(callback, e)

    if not packages:
        return await safe_edit_or_answer(
            callback.message,
            "📋 هنوز هیچ پکیجی ساخته نشده است.",
            reply_markup=get_photo_panel_keyboard(),
        )

    lines = ["🖼 <b>پکیج‌های عکس پروفایل</b>\n"]
    builder = InlineKeyboardBuilder()
    for pkg in packages:
        safe_name = html.escape(pkg.name)
        photo_count = len(pkg.photos)
        acc_count = len(pkg.accounts)
        status = f"✅ {PACKAGE_PHOTO_COUNT} عکس" if photo_count == PACKAGE_PHOTO_COUNT else f"⚠️ {photo_count} عکس (ناقص)"
        lines.append(f"• <b>#{pkg.id} — «{safe_name}»</b>\n    {status} | 🔗 {acc_count} اکانت")
        builder.row(
            types.InlineKeyboardButton(
                text=f"✏️ ویرایش",
                callback_data=f"photo_pkg_edit:{pkg.id}",
            ),
            types.InlineKeyboardButton(
                text=f"🗑 حذف",
                callback_data=f"photo_pkg_del:{pkg.id}",
            )
        )
        
    if total_packages and total_packages > 50:
        lines.append(f"\n... و {total_packages - 50} پکیج دیگر")

    builder.row(types.InlineKeyboardButton(text="⚡️ تخصیص خودکار", callback_data="photo_pkg_auto/"))
    builder.row(types.InlineKeyboardButton(text="🖼 پنل پکیج‌ها", callback_data="photo_pkg_panel/"))

    await safe_edit_or_answer(
        callback.message,
        "\n".join(lines),
        reply_markup=builder.as_markup(),
    )


@router.callback_query(F.data.startswith("photo_pkg_del:"), IsAdmin())
async def delete_package_prompt(callback: types.CallbackQuery) -> None:
    await callback.answer()
    pkg_id = int(callback.data.split(":")[1])

    try:
        async with async_session_maker() as session:
            pkg = await session.get(
                ProfilePhotoPackage,
                pkg_id,
                options=(selectinload(ProfilePhotoPackage.accounts),),
            )
    except Exception as e:
        logger.error(f"DB Error in delete_package_prompt: {e}")
        return await report_db_error(callback, e)
        
    if not pkg:
        return await callback.answer("⚠️ پکیج یافت نشد.", show_alert=True)
        
    acc_count = len(pkg.accounts)
    safe_name = html.escape(pkg.name)

    if acc_count == 0:
        return await _do_delete_package(callback.message, pkg_id, pkg.name)

    builder = InlineKeyboardBuilder()
    builder.row(
        types.InlineKeyboardButton(text="✅ حذف کن", callback_data=f"photo_pkg_del_confirm:{pkg_id}")
    )
    builder.row(types.InlineKeyboardButton(text="↩️ انصراف", callback_data="photo_pkg_list/"))
    
    await safe_edit_or_answer(
        callback.message,
        f"⚠️ <b>هشدار!</b> پکیج «{safe_name}» به <b>{acc_count}</b> اکانت متصل است.\n\n"
        "با حذف این پکیج:\n"
        "  • تخصیص این اکانت‌ها پاک می‌شود (خودِ اکانت‌ها حذف نمی‌شوند)\n"
        "  • فایل‌های عکس از دیسک پاک خواهند شد\n\nمطمئنی؟",
        reply_markup=builder.as_markup(),
    )


@router.callback_query(F.data.startswith("photo_pkg_del_confirm:"), IsAdmin())
async def delete_package_confirm(callback: types.CallbackQuery) -> None:
    await callback.answer()
    pkg_id = int(callback.data.split(":")[1])
    
    try:
        async with async_session_maker() as session:
            pkg = await session.get(ProfilePhotoPackage, pkg_id)
    except Exception as e:
        logger.error(f"DB Error in delete_package_confirm: {e}")
        return await report_db_error(callback, e)
        
    if not pkg:
        return await callback.answer("⚠️ پکیج یافت نشد.", show_alert=True)
        
    name = pkg.name
    await _do_delete_package(callback.message, pkg_id, name)


async def _do_delete_package(message: types.Message, pkg_id: int, name: str) -> None:
    safe_name = html.escape(name)
    try:
        async with async_session_maker() as session:
            await session.execute(
                update(Account)
                .where(Account.photo_package_id == pkg_id)
                .values(photo_package_id=None)
            )
            pkg = await session.get(ProfilePhotoPackage, pkg_id)
            if pkg:
                await session.delete(pkg)
            await session.commit()
    except Exception as e:
        logger.error(f"DB Error in _do_delete_package: {e}")
        return await report_db_error(message, e)

    await _remove_package_folder(pkg_id)
    await safe_edit_or_answer(
        message,
        f"🗑 پکیج «{safe_name}» به‌همراه فایل‌هایش حذف شد؛ تخصیص اکانت‌های متصل (در صورت وجود) پاک شد.",
        reply_markup=get_photo_panel_keyboard(),
    )


# ==========================================
# 🔗 ASSIGN: accounts list (paginated) → pick package
# ==========================================
@router.callback_query(F.data == "photo_pkg_assign/", IsAdmin())
async def show_assign_accounts(callback: types.CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    await _drop_incomplete_flow(state)
    await _send_accounts_page(callback.message, page=0)


@router.callback_query(F.data.startswith("photo_pkg_assign_page:"), IsAdmin())
async def show_assign_accounts_page(callback: types.CallbackQuery) -> None:
    await callback.answer()
    page = max(0, int(callback.data.split(":")[1]))
    await _send_accounts_page(callback.message, page=page)


async def _send_accounts_page(message: types.Message, page: int) -> None:
    async with async_session_maker() as session:
        total = await session.scalar(select(func.count(Account.id)))
        if not total:
            return await safe_edit_or_answer(
                message, "📱 هیچ اکانتی ثبت نشده است.", reply_markup=get_photo_panel_keyboard()
            )

        max_page = (total - 1) // ACCOUNTS_PAGE_SIZE
        page = min(page, max_page)

        accounts = (
            await session.execute(
                select(Account)
                .order_by(Account.id)
                .limit(ACCOUNTS_PAGE_SIZE)
                .offset(page * ACCOUNTS_PAGE_SIZE)
            )
        ).scalars().all()

        pkg_names = dict(
            (await session.execute(select(ProfilePhotoPackage.id, ProfilePhotoPackage.name))).all()
        )

    builder = InlineKeyboardBuilder()
    for acc in accounts:
        pkg_label = pkg_names.get(acc.photo_package_id, "بدون پکیج")
        builder.row(
            types.InlineKeyboardButton(
                text=f"📱 {acc.phone_number} | {pkg_label}",
                callback_data=f"photo_pkg_acc:{acc.id}:{page}",
            )
        )

    nav = []
    if page > 0:
        nav.append(
            types.InlineKeyboardButton(text="⬅️ قبلی", callback_data=f"photo_pkg_assign_page:{page - 1}")
        )
    if page < max_page:
        nav.append(
            types.InlineKeyboardButton(text="بعدی ➡️", callback_data=f"photo_pkg_assign_page:{page + 1}")
        )
    if nav:
        builder.row(*nav)
    builder.row(types.InlineKeyboardButton(text="⚡️ تخصیص خودکار به همه", callback_data="photo_pkg_auto/"))
    builder.row(types.InlineKeyboardButton(text="🖼 پنل پکیج‌ها", callback_data="photo_pkg_panel/"))

    await safe_edit_or_answer(
        message,
        f"🔗 <b>اتصال پکیج به اکانت</b>\n"
        f"صفحه {page + 1} از {max_page + 1} — یک اکانت را انتخاب کنید:",
        reply_markup=builder.as_markup(),
    )


@router.callback_query(F.data.startswith("photo_pkg_acc:"), IsAdmin())
async def choose_package_for_account(callback: types.CallbackQuery) -> None:
    await callback.answer()
    _, acc_id_str, page_str = callback.data.split(":")
    acc_id, page = int(acc_id_str), int(page_str)

    try:
        async with async_session_maker() as session:
            account = await session.get(Account, acc_id)
            if not account:
                return await safe_edit_or_answer(callback.message, "⚠️ اکانت یافت نشد.")

            packages = (
                await session.execute(
                    select(ProfilePhotoPackage)
                    .options(selectinload(ProfilePhotoPackage.photos))
                    .order_by(ProfilePhotoPackage.id)
                )
            ).scalars().all()
            
            complete_packages = [p for p in packages if len(p.photos) == PACKAGE_PHOTO_COUNT]

            usage = dict(
                (
                    await session.execute(
                        select(Account.photo_package_id, func.count(Account.id))
                        .where(Account.photo_package_id.isnot(None))
                        .group_by(Account.photo_package_id)
                    )
                ).all()
            )

            current = next((p for p in packages if p.id == account.photo_package_id), None)
            current_name = current.name if current else None
            has_package = account.photo_package_id is not None
            
    except Exception as e:
        logger.error(f"DB Error in choose_package_for_account: {e}")
        return await report_db_error(callback, e)

    if not complete_packages:
        return await safe_edit_or_answer(
            callback.message,
            f"⚠️ هیچ پکیج کاملی ({PACKAGE_PHOTO_COUNT} عکسی) وجود ندارد؛ اول یک پکیج بسازید.",
            reply_markup=get_photo_panel_keyboard(),
        )

    builder = InlineKeyboardBuilder()
    for pkg in complete_packages:
        safe_pkg_name = html.escape(pkg.name)
        builder.row(
            types.InlineKeyboardButton(
                text=f"🖼 «{safe_pkg_name}» — 🔗 {usage.get(pkg.id, 0)} اکانت",
                callback_data=f"photo_pkg_set:{acc_id}:{pkg.id}",
            )
        )
    if has_package:
        builder.row(
            types.InlineKeyboardButton(text="🔴 حذف تخصیص فعلی", callback_data=f"photo_pkg_unset:{acc_id}")
        )
    builder.row(
        types.InlineKeyboardButton(text="↩️ بازگشت به لیست اکانت‌ها", callback_data=f"photo_pkg_assign_page:{page}")
    )

    current_label = f"«{html.escape(current_name)}»" if current_name else "ندارد"
    await safe_edit_or_answer(
        callback.message,
        f"📱 اکانت: <code>{account.phone_number}</code>\n"
        f"🖼 پکیج فعلی: {current_label}\n\nکدام پکیج به این اکانت متصل شود؟",
        reply_markup=builder.as_markup(),
    )


@router.callback_query(F.data.startswith("photo_pkg_set:"), IsAdmin())
async def assign_package_to_account(callback: types.CallbackQuery) -> None:
    _, acc_id_str, pkg_id_str = callback.data.split(":")
    acc_id, pkg_id = int(acc_id_str), int(pkg_id_str)

    try:
        async with async_session_maker() as session:
            account = await session.get(Account, acc_id)
            package = await session.get(ProfilePhotoPackage, pkg_id)
            if not account or not package:
                return await callback.answer("⚠️ اکانت یا پکیج یافت نشد.", show_alert=True)
            
            pkg_name, phone = package.name, account.phone_number
            account.photo_package_id = package.id
            await session.commit()
    except Exception as e:
        logger.error(f"DB Error in assign_package_to_account: {e}")
        return await report_db_error(callback, e)

    safe_name = html.escape(pkg_name)
    await callback.answer(f"✅ پکیج «{pkg_name}» متصل شد.", show_alert=True)
    
    applied = False
    try:
        from workers.session_manager import apply_photo_package_now
        applied = await apply_photo_package_now(acc_id)
    except Exception as e:
        logger.warning(f"Immediate photo rotation failed for account {acc_id}: {e}")

    builder = InlineKeyboardBuilder()
    builder.row(types.InlineKeyboardButton(text="↩️ لیست اکانت‌ها", callback_data="photo_pkg_assign/"))
    builder.row(types.InlineKeyboardButton(text="🖼 پنل پکیج‌ها", callback_data="photo_pkg_panel/"))
    
    status_note = (
        "✅ <b>عکس‌های پکیج همین حالا روی اکانت اعمال شد.</b>" 
        if applied else 
        "ℹ️ عکس‌ها هنگام <b>استارت بعدی ورکر</b> و با روشن بودن <b>Auto Set Photo</b> اعمال می‌شوند."
    )
    
    await safe_edit_or_answer(
        callback.message,
        f"✅ پکیج «{safe_name}» به <code>{phone}</code> متصل شد.\n\n{status_note}",
        reply_markup=builder.as_markup(),
    )


@router.callback_query(F.data.startswith("photo_pkg_unset:"), IsAdmin())
async def unassign_package_from_account(callback: types.CallbackQuery) -> None:
    acc_id = int(callback.data.split(":")[1])
    
    try:
        async with async_session_maker() as session:
            account = await session.get(Account, acc_id)
            if not account or not account.photo_package_id:
                return await callback.answer("⚠️ این اکانت پکیجی ندارد.", show_alert=True)
            account.photo_package_id = None
            await session.commit()
    except Exception as e:
        logger.error(f"DB Error in unassign_package_from_account: {e}")
        return await report_db_error(callback, e)
        
    await callback.answer("✅ تخصیص حذف شد.", show_alert=True)
    await _send_accounts_page(callback.message, page=0)


# ==========================================
# ⚡️ AUTO-ASSIGN (کم‌استفاده‌ترین پکیج)
# ==========================================
@router.callback_query(F.data == "photo_pkg_auto/", IsAdmin())
async def auto_assign_packages(callback: types.CallbackQuery) -> None:
    await callback.answer()

    try:
        async with async_session_maker() as session:
            packages = (
                await session.execute(
                    select(ProfilePhotoPackage).options(selectinload(ProfilePhotoPackage.photos))
                )
            ).scalars().all()
            complete = [p for p in packages if len(p.photos) == PACKAGE_PHOTO_COUNT]

            if not complete:
                return await safe_edit_or_answer(
                    callback.message,
                    f"⚠️ هیچ پکیج کاملی ({PACKAGE_PHOTO_COUNT} عکسی) برای تخصیص وجود ندارد.",
                    reply_markup=get_photo_panel_keyboard(),
                )

            counts = {p.id: 0 for p in complete}
            rows = (
                await session.execute(
                    select(Account.photo_package_id, func.count(Account.id))
                    .where(Account.photo_package_id.isnot(None))
                    .group_by(Account.photo_package_id)
                )
            ).all()
            for pid, cnt in rows:
                if pid in counts:
                    counts[pid] = cnt

            free_accounts = (
                await session.execute(
                    select(Account).where(
                        Account.photo_package_id.is_(None),
                        Account.is_banned == False,
                    )
                )
            ).scalars().all()

            for acc in free_accounts:
                best = min(complete, key=lambda p: counts[p.id])
                acc.photo_package_id = best.id
                counts[best.id] += 1

            name_by_id = {p.id: p.name for p in complete}
            assigned_count = len(free_accounts)
            await session.commit()
            
    except Exception as e:
        logger.error(f"DB Error in auto_assign_packages: {e}")
        return await report_db_error(callback, e)

    if assigned_count == 0:
        return await safe_edit_or_answer(
            callback.message,
            "ℹ️ همه‌ی اکانت‌ها (غیربن) پکیج دارند؛ چیزی برای تخصیص نبود.",
            reply_markup=get_photo_panel_keyboard(),
        )

    dist = "\n".join(f"• «{html.escape(name_by_id[pid])}»: {cnt} اکانت" for pid, cnt in counts.items())
    await safe_edit_or_answer(
        callback.message,
        f"⚡️ <b>تخصیص خودکار انجام شد</b>\n\n"
        f"🔗 {assigned_count} اکانتِ بدون پکیج، به کم‌استفاده‌ترین پکیج‌ها متصل شدند.\n\n"
        f"<b>توزیع نهایی (کل اتصال‌ها):</b>\n{dist}",
        reply_markup=get_photo_panel_keyboard(),
    )



# ==========================================
# 🏠 باگ ۷ — هندلر دکمه متنی منوی اصلی
# مسیر: (اضافه شود به بخش PANEL یا انتهای فایل)
# ==========================================
@router.message(F.text.in_({"🖼 پکیج پروفایل 🖼", "🖼 پروفایل‌ها"}), IsAdmin())
async def photo_pkg_text_entry(message: types.Message, state: FSMContext) -> None:
    if await state.get_state() is not None:
        try:
            await cleanup_fsm_temp_files(state)
        except Exception:
            pass
        await state.clear()
        
    await _drop_incomplete_flow(state)
    
    auto_set_photo = False
    try:
        async with async_session_maker() as session:
            settings = await session.scalar(select(GlobalSettings).limit(1))
            if settings:
                auto_set_photo = settings.auto_set_photo
    except Exception as e:
        logger.error(f"Error reading settings: {e}")
        
    status_indicator = (
        "✅ <b>Auto Set Photo فعال</b> — پکیج‌ها به‌طور خودکار اعمال می‌شوند." 
        if auto_set_photo else 
        "⚠️ <b>توجه: تنظیم Auto Set Photo در تنظیمات خاموش است.</b> پکیج‌ها ساخته می‌شوند اما روی اکانت‌ها اعمال نخواهند شد. برای فعال‌سازی به ⚙️ تنظیمات بروید."
    )

    await message.answer(
        f"🖼 <b>مدیریت پکیج‌های عکس پروفایل</b>\n\n"
        f"{status_indicator}\n\n"
        "هر پکیج دقیقاً ۳ عکس دارد که هنگام استارت ورکر، جایگزین عکس‌های قبلی اکانت می‌شوند.",
        reply_markup=get_photo_panel_keyboard(),
    )

@router.message(F.text.in_({"🖼 پکیج پروفایل 🖼", "🖼 پروفایل‌ها"}))
async def photo_pkg_text_entry_forbidden(message: types.Message) -> None:
    await message.answer("⛔️ شما دسترسی ندارید.")

@router.callback_query(F.data == "photo_pkg_apply_all/", IsAdmin())
async def apply_all_assigned_photos(callback: types.CallbackQuery) -> None:
    await callback.answer("⏳ در حال اعمال عکس‌ها روی ورکرهای آنلاین...")
    from workers.session_manager import worker_pool, apply_photo_package_now
    
    success, offline, failed = 0, 0, 0
    for account_id in list(worker_pool.keys()):
        try:
            applied = await apply_photo_package_now(account_id)
            if applied:
                success += 1
            else:
                failed += 1
        except Exception:
            offline += 1
            
    await safe_edit_or_answer(
        callback.message,
        f"🚀 <b>گزارش اعمال فوری عکس‌ها</b>\n\n"
        f"✅ موفقیت‌آمیز: {success}\n"
        f"⚠️ ناموفق (بدون پکیج/خطا): {failed}\n"
        f"💤 ورکرهای آفلاین: {offline}\n\n"
        f"توجه: این عملیات فقط روی اکانت‌های آنلاین و دارای پکیج متصل اعمال می‌شود.",
        reply_markup=get_photo_panel_keyboard()
    )

# ==========================================
# ✏️ EDIT PACKAGE (Rename & Replace Photo)
# ==========================================
@router.callback_query(F.data.startswith("photo_pkg_edit:"), IsAdmin())
async def edit_package_prompt(callback: types.CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    await _drop_incomplete_flow(state)
    pkg_id = int(callback.data.split(":")[1])

    try:
        async with async_session_maker() as session:
            pkg = await session.get(ProfilePhotoPackage, pkg_id)
    except Exception as e:
        logger.error(f"DB Error in edit_package_prompt: {e}")
        return await report_db_error(callback, e)

    if not pkg:
        return await callback.answer("⚠️ پکیج یافت نشد.", show_alert=True)

    safe_name = html.escape(pkg.name)
    builder = InlineKeyboardBuilder()
    
    for i in range(1, PACKAGE_PHOTO_COUNT + 1):
        builder.row(types.InlineKeyboardButton(
            text=f"📷 جایگزینی عکس {i}",
            callback_data=f"photo_pkg_replace:{pkg_id}:{i}"
        ))
        
    builder.row(types.InlineKeyboardButton(text="✏️ تغییر نام پکیج", callback_data=f"photo_pkg_rename:{pkg_id}"))
    builder.row(types.InlineKeyboardButton(text="🔙 بازگشت به لیست", callback_data="photo_pkg_list/"))

    await safe_edit_or_answer(
        callback.message,
        f"✏️ <b>ویرایش پکیج «{safe_name}»</b>\n\nلطفاً عملیات مورد نظر را انتخاب کنید:",
        reply_markup=builder.as_markup(),
    )

@router.callback_query(F.data.startswith("photo_pkg_rename:"), IsAdmin())
async def rename_package_prompt(callback: types.CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    pkg_id = int(callback.data.split(":")[1])
    await state.set_state(PhotoPackageStates.waiting_for_rename)
    await state.update_data(edit_pkg_id=pkg_id)
    await safe_edit_or_answer(
        callback.message,
        with_cancel_hint("✏️ <b>تغییر نام پکیج</b>\n\nلطفاً نام جدید پکیج را ارسال کنید:"),
        reply_markup=get_cancel_keyboard()
    )

@router.message(PhotoPackageStates.waiting_for_rename, F.text & ~F.text.startswith("/"))
async def process_rename_package(message: types.Message, state: FSMContext) -> None:
    data = await state.get_data()
    pkg_id = data.get("edit_pkg_id")
    name = message.text.strip()
    
    if not name or len(name) > 100:
        return await message.answer(
            with_cancel_hint("⚠️ نام نامعتبر است. دوباره بفرستید:"), 
            reply_markup=get_cancel_keyboard()
        )
        
    try:
        async with async_session_maker() as session:
            duplicate = await session.scalar(
                select(ProfilePhotoPackage).where(ProfilePhotoPackage.name == name)
            )
            if duplicate and duplicate.id != pkg_id:
                return await message.answer(
                    with_cancel_hint("⚠️ این نام از قبل وجود دارد. نام دیگری بفرستید:"), 
                    reply_markup=get_cancel_keyboard()
                )
            
            pkg = await session.get(ProfilePhotoPackage, pkg_id)
            if pkg:
                pkg.name = name
                await session.commit()
    except Exception as e:
        return await report_db_error(message, e)

    await state.clear()
    await message.answer(
        f"✅ نام پکیج با موفقیت به «{html.escape(name)}» تغییر یافت.", 
        reply_markup=get_photo_finish_keyboard()
    )

@router.callback_query(F.data.startswith("photo_pkg_replace:"), IsAdmin())
async def replace_photo_prompt(callback: types.CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    _, pkg_id_str, pos_str = callback.data.split(":")
    await state.set_state(PhotoPackageStates.waiting_for_replace_photo)
    await state.update_data(edit_pkg_id=int(pkg_id_str), edit_photo_pos=int(pos_str))
    
    await safe_edit_or_answer(
        callback.message,
        with_cancel_hint(f"📷 <b>جایگزینی عکس {pos_str}</b>\n\nلطفاً عکس جدید را ارسال کنید:"),
        reply_markup=get_cancel_keyboard()
    )

@router.message(PhotoPackageStates.waiting_for_replace_photo, F.photo)
async def process_replace_photo(message: types.Message, state: FSMContext) -> None:
    data = await state.get_data()
    pkg_id = data.get("edit_pkg_id")
    pos = data.get("edit_photo_pos")
    
    if not pkg_id or not pos:
        await state.clear()
        return await message.answer("⚠️ اطلاعات از دست رفت. لطفاً دوباره تلاش کنید.")
        
    final_dir = os.path.join(PHOTO_ROOT, str(pkg_id))
    os.makedirs(final_dir, exist_ok=True)
    file_path = os.path.join(final_dir, f"{pos}.jpg")
    
    if os.path.exists(file_path):
        try:
            os.remove(file_path)
        except OSError:
            logger.warning(f"Could not remove old photo: {file_path}")
            
    try:
        await message.bot.download(message.photo[-1], destination=file_path)
    except Exception as e:
        logger.error(f"Error downloading replacement photo: {e}")
        return await message.answer(
            with_cancel_hint("⚠️ دانلود عکس ناموفق بود. دوباره ارسال کنید:"), 
            reply_markup=get_cancel_keyboard()
        )
        
    try:
        async with async_session_maker() as session:
            photo_rec = await session.scalar(
                select(ProfilePhoto).where(ProfilePhoto.package_id == pkg_id, ProfilePhoto.position == pos)
            )
            if not photo_rec:
                session.add(ProfilePhoto(package_id=pkg_id, file_path=file_path, position=pos))
            await session.commit()
    except Exception as e:
        return await report_db_error(message, e)
        
    await state.clear()
    await message.answer(f"✅ عکس {pos} با موفقیت جایگزین شد.", reply_markup=get_photo_finish_keyboard())
    