import logging
from datetime import datetime, timezone

from aiogram import Router, types, F
from aiogram.utils.keyboard import InlineKeyboardBuilder
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from database.models import Account, Proxy, Order, OrderLog, OrderStatus

logger = logging.getLogger(__name__)

router = Router(name="stats_handlers_router")

def get_back_keyboard():
    """کیبورد بازگشت به منوی اصلی"""
    builder = InlineKeyboardBuilder()
    builder.button(text="🔙 بازگشت به منو", callback_data="menu_home/")
    return builder.as_markup()

# ==========================================
# REPORTS: SYSTEM & NETWORK STATS
# ==========================================
@router.callback_query(F.data == "menu_stats/")
async def show_system_stats(callback: types.CallbackQuery, session: AsyncSession) -> None:
    await callback.answer()
    now = datetime.now(timezone.utc)

    # 1. Accounts Analytics
    active_acc_stmt = select(func.count(Account.id)).where(Account.is_banned == False)
    banned_acc_stmt = select(func.count(Account.id)).where(Account.is_banned == True)
    flood_acc_stmt = select(func.count(Account.id)).where(Account.flood_wait_until > now)

    active_acc = (await session.execute(active_acc_stmt)).scalar() or 0
    banned_acc = (await session.execute(banned_acc_stmt)).scalar() or 0
    flood_acc = (await session.execute(flood_acc_stmt)).scalar() or 0

    # 2. Proxy Analytics
    active_proxy_stmt = select(func.count(Proxy.id)).where(Proxy.is_active == True)
    failed_proxy_stmt = select(func.count(Proxy.id)).where(Proxy.is_active == False)

    active_proxy = (await session.execute(active_proxy_stmt)).scalar() or 0
    failed_proxy = (await session.execute(failed_proxy_stmt)).scalar() or 0

    report_text = (
        "📊 <b>System & Network Statistics</b>\n\n"
        "👥 <b>Worker Accounts:</b>\n"
        f"├ Active & Ready: <code>{active_acc - flood_acc}</code>\n"
        f"├ In FloodWait: <code>{flood_acc}</code>\n"
        f"└ Banned/Dead: <code>{banned_acc}</code>\n\n"
        "🌐 <b>Proxy Pool:</b>\n"
        f"├ Healthy Proxies: <code>{active_proxy}</code>\n"
        f"└ Burned Proxies: <code>{failed_proxy}</code>"
    )

    await callback.message.edit_text(report_text, reply_markup=get_back_keyboard())


# ==========================================
# REPORTS: ORDER & DELIVERY ANALYTICS
# ==========================================
@router.callback_query(F.data == "menu_analysis/")
async def show_order_analysis(callback: types.CallbackQuery, session: AsyncSession) -> None:
    await callback.answer()

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

    total_success = (await session.execute(success_stmt)).scalar() or 0
    total_error = (await session.execute(error_stmt)).scalar() or 0
    total_logs = total_success + total_error
    
    delivery_rate = round((total_success / total_logs * 100), 2) if total_logs > 0 else 0.0

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

    error_report = ""
    if top_errors:
        error_report = "\n\n⚠️ <b>Top Failure Causes:</b>\n"
        for err, count in top_errors:
            # Clean up long flood wait messages for a cleaner report
            clean_err = err.split(":")[0] if err else "Unknown"
            error_report += f"├ <i>{clean_err}</i> : <code>{count}</code>\n"

    report_text = (
        "📈 <b>Order & Delivery Analytics</b>\n\n"
        "📋 <b>Queue Status:</b>\n"
        f"├ Running: <code>{running_orders}</code>\n"
        f"├ Pending: <code>{pending_orders}</code>\n"
        f"└ Completed: <code>{completed_orders}</code>\n\n"
        "🚀 <b>Delivery Performance:</b>\n"
        f"├ Delivered: <code>{total_success}</code>\n"
        f"├ Failed: <code>{total_error}</code>\n"
        f"└ Success Rate: <b>{delivery_rate}%</b>"
        f"{error_report}"
    )

    await callback.message.edit_text(report_text, reply_markup=get_back_keyboard())

# ==========================================
# HOME ROUTING
# ==========================================
@router.callback_query(F.data == "menu_home/")
async def return_to_home(callback: types.CallbackQuery) -> None:
    from bot.keyboards.main_menu import get_main_menu_keyboard
    await callback.message.edit_text(
        "🎛 <b>Master Control Panel</b>\n\nSelect an option below:",
        reply_markup=get_main_menu_keyboard()
    )