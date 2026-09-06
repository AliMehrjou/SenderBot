"""
🧩 زیرساخت مشترک صفحه‌بندی (Pagination) — فاز ۱

استاندارد پروژه برای «تمام» لیست‌ها:

    📄   PAGINATION_SIZE → ۱۰ آیتم در هر صفحه
    ⬅️➡️ دکمه‌های «صفحه قبل / صفحه بعد» + نمایشگر شماره صفحه
    🔄   دکمه «بروزرسانی» → رندر مجدد همان صفحه (همان callback صفحه فعلی)
    🏛   دکمه «منوی اصلی»

الگوی استفاده در هندلر لیست:

    total_count = await session.scalar(select(func.count(Entity.id))) or 0
    total_pages = calculate_total_pages(total_count)
    page = clamp_page(page, total_pages)

    items = (
        await session.scalars(
            select(Entity)
            .order_by(Entity.id.asc())
            .offset(get_page_offset(page))
            .limit(PAGINATION_SIZE)
        )
    ).all()

    builder = InlineKeyboardBuilder()
    for item in items:
        builder.button(...)  # دکمه‌های آیتم

    builder.adjust(2)  # چیدمان دکمه‌های آیتم
    add_pagination_nav_row(builder, page, total_pages, callback_prefix="list_<entity>_")
    add_list_footer(builder, refresh_callback=f"list_<entity>_page_{page}/")

⚠️  ثبت روتر: pagination_router (هندلر دکمه‌های نمایشی شماره صفحه) باید
    در dispatcher ثبت شود — همان‌جایی که سایر routerها ثبت می‌شوند.
"""

from contextlib import suppress

from aiogram import Router, types, F
from aiogram.exceptions import TelegramBadRequest
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder

# ──────────────────────────────────────────────
# ثابت‌ها
# ──────────────────────────────────────────────

#: تعداد آیتم در هر صفحه — استاندارد همهٔ لیست‌های پروژه
PAGINATION_SIZE = 10

#: کال‌بک دکمهٔ نمایشی «شماره صفحه» (هیچ عملی انجام نمی‌دهد)
PAGE_INFO_CALLBACK = "pagination_info_ignore/"

#: کال‌بک نمایشی الگوی اولیهٔ پروژه — برای سازگاری با دکمه‌های قدیمی
LEGACY_PAGE_INFO_CALLBACK = "page_info_ignore/"

#: مارکر شماره صفحه در callback_data (مثال: list_admins_page_3/)
PAGE_MARKER = "page_"


# ──────────────────────────────────────────────
# توابع محاسباتی
# ──────────────────────────────────────────────

def calculate_total_pages(total_items: int, page_size: int = PAGINATION_SIZE) -> int:
    """
    محاسبهٔ تعداد کل صفحات از روی تعداد کل آیتم‌ها.
    همیشه حداقل ۱ برمی‌گرداند (حتی برای لیست خالی).
    """
    if total_items is None or total_items < 0:
        total_items = 0
    return max(1, (total_items + page_size - 1) // page_size)


def clamp_page(page: int, total_pages: int) -> int:
    """
    محدود کردن شمارهٔ صفحه به بازهٔ معتبر [1, total_pages].
    کاربرد: بعد از حذف آخرین آیتمِ صفحهٔ آخر، شمارهٔ صفحه از بازه خارج می‌شود.
    """
    try:
        page = int(page)
    except (TypeError, ValueError):
        return 1
    if page < 1:
        return 1
    if page > total_pages:
        return total_pages
    return page


def get_page_offset(page: int, page_size: int = PAGINATION_SIZE) -> int:
    """محاسبهٔ offset کوئری (LIMIT/OFFSET) برای صفحهٔ مشخص."""
    return (page - 1) * page_size


def parse_page_from_callback(callback_data: str) -> int:
    """
    استخراج شمارهٔ صفحه از callback_data (مستقل از prefix لیست).

    مثال:
        parse_page_from_callback("list_admins_page_3/")  → 3
        parse_page_from_callback("api_page_12/")         → 12
        parse_page_from_callback("list_admins_page_xx/") → 1 (مقدار امن)
    """
    marker_pos = callback_data.rfind(PAGE_MARKER)
    if marker_pos == -1:
        return 1
    tail = callback_data[marker_pos + len(PAGE_MARKER):].replace("/", "").strip()
    return int(tail) if tail.isdigit() else 1


# ──────────────────────────────────────────────
# ساخت کیبورد صفحه‌بندی
# ──────────────────────────────────────────────

def build_pagination_nav_row(
    current_page: int,
    total_pages: int,
    callback_prefix: str,
) -> list[InlineKeyboardButton]:
    """
    ساخت ردیف دکمه‌های ناوبری صفحه‌بندی:

        [⬅️ صفحه قبل] [📄 صفحه X از Y] [صفحه بعد ➡️]

    - callback_prefix باید به «_» ختم شود:
        callback_prefix="list_admins_"  →  callback_data: "list_admins_page_2/"
    - دکمهٔ «صفحه قبل» فقط از صفحهٔ ۲ به بعد و «صفحه بعد» فقط تا
      یکی‌مانده‌به‌آخرین صفحه نمایش داده می‌شود.
    - اگر کل یک صفحه وجود دارد، ردیف خالی برمی‌گردد
      (نمایش «۱ از ۱» به‌صورت دکمه، فقط نویز است — اطلاعات صفحه در متن پیام هست).
    """
    if total_pages <= 1:
        return []

    buttons: list[InlineKeyboardButton] = []

    if current_page > 1:
        buttons.append(
            InlineKeyboardButton(
                text="⬅️ صفحه قبل",
                callback_data=f"{callback_prefix}page_{current_page - 1}/",
            )
        )

    buttons.append(
        InlineKeyboardButton(
            text=f"📄 صفحه {current_page} از {total_pages}",
            callback_data=PAGE_INFO_CALLBACK,
        )
    )

    if current_page < total_pages:
        buttons.append(
            InlineKeyboardButton(
                text="صفحه بعد ➡️",
                callback_data=f"{callback_prefix}page_{current_page + 1}/",
            )
        )

    return buttons


def add_pagination_nav_row(
    builder: InlineKeyboardBuilder,
    current_page: int,
    total_pages: int,
    callback_prefix: str,
) -> None:
    """
    افزودن ردیف ناوبری صفحه‌بندی به کیبورد موجود (در صورت نیاز).

    نکته: این تابع باید «بعد از» adjust دکمه‌های آیتم و «قبل از»
    دکمه‌های پایانی (بروزرسانی/منوی اصلی) صدا زده شود.
    """
    buttons = build_pagination_nav_row(current_page, total_pages, callback_prefix)
    if buttons:
        builder.row(*buttons)


def add_list_footer(
    builder: InlineKeyboardBuilder,
    refresh_callback: str,
    home_callback: str = "menu_home/",
) -> None:
    """
    افزودن دکمه‌های استاندارد پایین هر لیست:

        🔄 بروزرسانی   → رندر مجدد همان صفحه (refresh_callback)
        🏛 منوی اصلی   → بازگشت به منوی اصلی

    الگوی متداول: refresh_callback=f"list_<entity>_page_{page}/"
    """
    builder.row(InlineKeyboardButton(text="🔄 بروزرسانی", callback_data=refresh_callback))
    builder.row(InlineKeyboardButton(text="🏛 منوی اصلی", callback_data=home_callback))


def build_pagination_keyboard(
    current_page: int,
    total_pages: int,
    callback_prefix: str,
) -> InlineKeyboardMarkup:
    """
    ساخت کیبورد صفحه‌بندی مستقل (طبق الگوی استاندارد پروژه) —
    برای حالت‌هایی که لیست فقط کیبورد ناوبری دارد.
    """
    builder = InlineKeyboardBuilder()
    add_pagination_nav_row(builder, current_page, total_pages, callback_prefix)
    return builder.as_markup()


# ──────────────────────────────────────────────
# روتر دکمه‌های نمایشی
# ──────────────────────────────────────────────

pagination_router = Router(name="pagination_router")


@pagination_router.callback_query(
    F.data.in_((PAGE_INFO_CALLBACK, LEGACY_PAGE_INFO_CALLBACK))
)
async def page_info_button_handler(callback: types.CallbackQuery) -> None:
    """
    دکمهٔ «📄 صفحه X از Y» صرفاً نمایشگر است؛ با یک toast کوتاه پاسخ داده
    می‌شود تا اسپینر روی دکمه باقی نماند.

    🎁 بونوس: فایل‌های موجود (stats/api/order handlers) از قبل کال‌بک
    «pagination_info_ignore/» emit می‌کنند؛ این هندلر اسپینر معلق
    آن دکمه‌ها را هم یک‌جا رفع می‌کند.
    """
    with suppress(TelegramBadRequest):
        await callback.answer("📄 این دکمه فقط شمارهٔ صفحه را نمایش می‌دهد.")