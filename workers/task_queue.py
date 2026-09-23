import asyncio
import html   # 🛡 فاز ۸ (BUG-25): escape لینک‌ها در پیام اطلاع‌رسانی ادمین
import json
import logging
import os
import random
import re     # 🛡 فاز ۸ (BUG-25): پارس/اعتبارسنجی چند لینک در target_data
import shutil
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional
import aiofiles
from typing import Dict, List, Optional, Union   # 🔄 Union اضافه شد
import aiofiles
import aiofiles.os
from workers.extractor import (
    extract_active_users,
    extract_members_for_sending,
    extract_members_parallel,     # 🔄 فاز استخراج موازی
    _PARALLEL_SAFE_FILTERS,       # 🟢 اضافه شده برای جلوگیری از قفل اضافی در استراتژی‌های نامعتبر
)

from aiogram import Bot
from aiogram.types import FSInputFile
from pyrogram import Client
from pyrogram.errors import FloodWait, UserNotParticipant, ChannelPrivate  # 🚪 فاز ۶ (R3-ب)
from sqlalchemy import func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from utils.speed_profile import get_speed_profile, SpeedProfile
from config import config
from database.models import (
    Account,
    Admin,  # Phase 5 progress opt-out
    Banner,
    GlobalSettings,
    Order,
    OrderJoin,
    OrderLog,
    OrderStatus,
    order_category_assoc,
)
from database.models import Proxy
from aiogram.utils.keyboard import InlineKeyboardBuilder
# 🧹 B10: پاک کردن ایمپورت تکراری extract_active_users و extract_members_for_sending
from workers.sender import (
    _get_redis,  # 🛡 فاز ۲ (BUG-04): کلاینت Redis مشترک (رجیستری busy) — hoisted from mid-file
    _daily_key,
    daily_cap_reached,
    effective_daily_limit,
    _hourly_key,
    execute_bulk_send,
    is_global_slowdown,
    is_in_cooldown,
    mark_chunk_cooldown,
)
from utils.progress_reporter import ProgressReporter
from utils.admin_broadcast import broadcast_to_admins, broadcast_to_admins_with_keyboard

logger = logging.getLogger(__name__)

# ثابت PENDING_APPROVAL_RETRY_LIMIT حذف و به config منتقل شد.

# 🎨 چرخش بنر: مجموعه‌ی سفارش‌هایی که به‌خاطر «مخزن بنرِ خالی» هشدار داده‌اند
# (جلوگیری از اسپم پیام هشدار به ادمین در هر سیکل دیسپچ؛ فقط یک‌بار در طول عمر پروسه)
_banner_warned_orders: set = set()

# ==========================================
# 🟢 فاز ۴: کش عضویت ورکر در کانال‌های مبدا — برای اولویت‌دهی در dispatch
# کلید: src_member:{account_id}:{source_channel_id} = "1" با TTL 24 ساعت
# وقتی یک ورکر با موفقیت از یک کانال مبدا فوروارد می‌کند، این کلید set می‌شود
# تا در dispatchهای بعدی همان سفارش (یا سفارش‌های دیگر روی همان کانال) اولویت پیدا کند.
# ==========================================


async def _send_hold_message(session: AsyncSession, bot: Bot, order_id: int) -> None:
    """ارسال پیام هولد به کاربر جهت تصمیم‌گیری در صورت پایان پروکسی‌های سالم."""
    if bot is None:
        return
        
    order = await session.scalar(select(Order).where(Order.id == order_id))
    if not order:
        return
        
    builder = InlineKeyboardBuilder()
    builder.button(text="◀️ ادامه باقی سفارش با IP سرور (بدون پروکسی)", callback_data=f"hold_continue_ip_{order_id}/")
    builder.button(text="⏳ منتظر پروکسی جدید می‌مانم", callback_data=f"hold_wait_proxy_{order_id}/")
    builder.adjust(1)
    
    text = (
        f"⚠️ <b>سفارش متوقف شد</b>\n\n"
        f"کد پیگیری: <code>{order.tracking_code or order.id}</code>\n"
        f"پروکسی‌های سالم به پایان رسیده‌اند و سفارش شما موقتاً در وضعیت هولد قرار گرفت.\n"
        f"لطفاً از ادمین بخواهید پروکسی جدید اضافه کند، یا یکی از گزینه‌های زیر را انتخاب کنید:"
    )
    
    if order.user_id:
        try:
            await bot.send_message(chat_id=order.user_id, text=text, reply_markup=builder.as_markup())
        except Exception as e:
            logger.warning(f"Could not send hold message for Order #{order_id}: {e}")
    else:
        try:
            await broadcast_to_admins_with_keyboard(bot=bot, text=text, keyboard=builder.as_markup())
        except Exception as e:
            logger.warning(f"Could not broadcast hold message for Order #{order_id}: {e}")


async def _mark_source_membership(account_id: int, source_channel_id: int) -> None:
    """ثبت اینکه این ورکر به این کانال مبدا دسترسی دارد."""
    try:
        redis = _get_redis()
        await redis.set(
            f"src_member:{account_id}:{source_channel_id}",
            "1",
            ex=86400,  # 24 ساعت
        )
    except Exception as e:
        logger.debug(f"Could not mark source membership for user_{account_id}/: {e}")

async def _is_known_source_member(account_id: int, source_channel_id: int) -> bool:
    """بررسی اینکه آیا این ورکر قبلاً به این کانال مبدا دسترسی داشته یا نه."""
    try:
        redis = _get_redis()
        val = await redis.get(f"src_member:{account_id}:{source_channel_id}")
        return bool(val)
    except Exception:
        return False

async def _find_source_member_workers(
    account_ids: List[int],
    source_channel_id: int,
) -> List[int]:
    """برگرداندن زیرمجموعه‌ای از account_idهایی که عضو کانال مبدا شناخته می‌شوند."""
    if not source_channel_id or not account_ids:
        return []
    known: List[int] = []
    for aid in account_ids:
        if await _is_known_source_member(aid, source_channel_id):
            known.append(aid)
    return known


# ==========================================
# 🔥 فاز ۶ (R7): گیت گرم‌شدن اکانت تازه-لاگین
# ==========================================
def _warmup_hours() -> int:
    """
    🔥 فاز ۶ (R7): طول دوره‌ی گرم‌شدن (ساعت) — کلید config MIN_WARMUP_HOURS با
    پیش‌فرض ۲۴. (دوقلوی workers/session_manager.py::warmup_hours.)
    تمایز ۴۸h برای «اکانت تازه» نیازمند فیلد سن/نوع اکانت در مدل Account است که
    وجود ندارد → یک مقدار واحد برای همه (نقطه‌ی الحاق آینده).
    """
    try:
        return max(0, int(config.MIN_WARMUP_HOURS))
    except (AttributeError, TypeError, ValueError):
        return 24

async def _is_warmed_up(session: AsyncSession, acc: Account, now: datetime) -> bool:
    """
    🔥 فاز ۶ (R7) + سوییچ دستی بای‌پَس
    """
    # ==============================================================
    # 🟢 سوییچ دستی: با True کردن این مقدار، تمام اکانت‌ها (حتی جدید) بلافاصله کار می‌کنند.
    # برای فعال شدن مجدد "دوره گرم‌شدن" (مثلاً ۲۴ ساعته)، فقط کافیست آن را False کنید.
    # ==============================================================
    BYPASS_WARMUP = config.BYPASS_WARMUP

    warmed = acc.warmed_up_at
    if warmed is None:
        try:
            # زمان پایان دوره گرم‌شدن همچنان در دیتابیس ثبت می‌شود تا دیتای شما تمیز بماند
            await session.execute(
                update(Account)
                .where(Account.id == acc.id, Account.warmed_up_at.is_(None))
                .values(warmed_up_at=now + timedelta(hours=_warmup_hours()))
                .execution_options(synchronize_session=False)
            )
        except Exception as e:
            logger.warning(f"Warmup lazy-init failed for account {acc.id}: {e}")
        
        if BYPASS_WARMUP:
            return True
        return False

    if warmed.tzinfo is None:
        warmed = warmed.replace(tzinfo=timezone.utc)
        
    return BYPASS_WARMUP or (now >= warmed)

# ==========================================
# 🚪 فاز ۶ (R3-ب): عضویت‌های «به‌خاطر سفارش» + leave بعد از اتمام
# ==========================================
async def _record_order_join(
    session: AsyncSession,
    order_id: int,
    account_db_id: int,
    group_link: str,
    chat_id: Optional[int],
    joined_now: bool,
    status_code: str,
) -> None:
    """
    🚪 فاز ۶ (R3-ب): bookkeeping جدول order_joins.

    - joined_now=True → ردیف جدید (join واقعی همین الان؛ حتی اگر استخراج بعداً خطا
      خورد — مثل error_not_admin — leave بعدی انجام می‌شود)
    - status_code == "pending_approval" → ردیف بدون chat_id (درخواست عضویت ارسال
      شد؛ اگر بعداً تأیید شود، چرخه‌ی بعد chat_id را روی همین ردیف تکمیل می‌کند)
    - success + joined_now=False → فقط اگر ردیف قبلی باشد chat_id تکمیل می‌شود
      (تأییدِ درخواست قبلی)؛ بدون ردیف = اکانت «از قبل» عضو بوده → هیچ ردیفی
      ساخته نمی‌شود و leave هرگز رخ نمی‌دهد.
    """
    try:
        row = await session.scalar(
            select(OrderJoin).where(
                OrderJoin.order_id == order_id,
                OrderJoin.account_id == account_db_id,
            )
        )
    except Exception as e:
        logger.warning(f"order_joins lookup failed for Order #{order_id}: {e}")
        return

    if row is not None:
        if chat_id is not None and row.chat_id is None:
            row.chat_id = chat_id
        return

    if joined_now or status_code in ("pending_approval", "already_member", "success"):
        session.add(OrderJoin(
            order_id=order_id,
            account_id=account_db_id,
            group_link=group_link,
            chat_id=chat_id,
        ))


# 🚪 فاز ۶ (R3-ب): حلقه‌ی leave — throttle و ضد-همزمانی
_LEAVE_SWEEP_INTERVAL_SECONDS = 60.0
_LEAVE_SWEEP_BATCH = 20
_leave_sweep_running: bool = False
_last_leave_sweep_at: float = 0.0

_leave_sweep_fails: Dict[int, int] = {}
_stuck_leave_ids: set = set()

async def _notify_owner(bot: Bot, session_maker: async_sessionmaker[AsyncSession], order_id: int, text: str):
    """ارسال نوتیفیکیشن Push به مالک سفارش (در صورت وجود)."""
    try:
        async with session_maker() as session:
            order = await session.scalar(select(Order).where(Order.id == order_id))
            if order and order.user_id:
                
                # --- فیکس: جلوگیری از ارسال پیام تکراری اگر مشتری همان ادمین باشد ---
                admin_ids = []
                if getattr(config, "ADMIN_ID", None):
                    admin_ids.append(int(config.ADMIN_ID))
                try:
                    sub_admins = (await session.scalars(select(Admin.telegram_id))).all()
                    admin_ids.extend(int(aid) for aid in sub_admins)
                except Exception:
                    pass
                
                # اگر آیدی ثبت‌کننده سفارش در لیست ادمین‌ها بود، پیام مشتری را نفرست
                if int(order.user_id) in admin_ids:
                    return
                # ----------------------------------------------------------------------

                await bot.send_message(
                    chat_id=order.user_id,
                    text=text,
                    parse_mode="HTML",
                    disable_web_page_preview=True
                )
    except Exception as e:
        logger.warning(f"Failed to push notify owner for Order #{order_id}: {e}")


def _maybe_schedule_leave_sweep(
    session_maker: async_sessionmaker[AsyncSession],
    worker_pool: Dict[int, Client],
    bot: Bot,
) -> None:
    """زمان‌بندی sweep غیرمسدودکننده — حداکثر هر ۶۰ ثانیه؛ leaveها در create_task جدا اجرا می‌شوند تا polling هرگز بلاک نشود."""
    global _leave_sweep_running, _last_leave_sweep_at
    if _leave_sweep_running:
        return
    if time.monotonic() - _last_leave_sweep_at < _LEAVE_SWEEP_INTERVAL_SECONDS:
        return

    async def _runner() -> None:
        global _leave_sweep_running
        try:
            await leave_sweep_terminal_orders(session_maker, worker_pool, bot)
        except Exception as e:
            logger.error(f"Leave-sweep crashed: {e}", exc_info=True)
        finally:
            _leave_sweep_running = False

    _leave_sweep_running = True
    _last_leave_sweep_at = time.monotonic()
    _spawn_background_task(_runner())


async def leave_sweep_terminal_orders(
    session_maker: async_sessionmaker[AsyncSession],
    worker_pool: Dict[int, Client],
    bot: Bot,
) -> None:
    """
    🚪 فاز ۶ (R3-ب): سفارش‌های terminal (completed/error — شامل Kill Switch ادمین) →
    اکانت‌هایی که «به‌خاطر آن سفارش» join شده‌اند (order_joins با leave_done=False)
    گروه را leave می‌کنند. جایگزین فراخوانی مستقیم در فاینالایز است و همه‌ی مسیرهای
    terminal را پوشش می‌دهد. idempotent + retry-safe: ردیف ناموفق (ورکر آفلاین/
    FloodWait/خطای گذرا) برای sweep بعدی باز می‌ماند.
    """
    try:
        async with session_maker() as session:
            query = select(OrderJoin).join(Order, OrderJoin.order_id == Order.id).where(
                OrderJoin.leave_done == False,  # noqa: E712
                Order.status.in_([OrderStatus.completed, OrderStatus.error]),
            )
            if _stuck_leave_ids:
                query = query.where(OrderJoin.id.notin_(list(_stuck_leave_ids)))
                
            rows = (await session.scalars(
                query.order_by(OrderJoin.id.asc()).limit(_LEAVE_SWEEP_BATCH)
            )).all()
    except Exception as e:
        logger.warning(f"Leave-sweep: DB query failed: {e}")
        return

    if not rows:
        return

    logger.info(f"Leave-sweep: cleaning up {len(rows)} joined account(s) of terminal orders.")
    for order_join in rows:
        await _leave_one_order_join(order_join, session_maker, worker_pool)
        # مکث انسانی بین leaveهای پیاپی
        await asyncio.sleep(random.uniform(1.0, 3.0))


async def _leave_one_order_join(
    order_join: OrderJoin,
    session_maker: async_sessionmaker[AsyncSession],
    worker_pool: Dict[int, Client],
) -> None:
    # عضویت هرگز رخ نداده (درخواست بدون تأیید / chat_id resolve نشده) → فقط بستن ردیف
    if order_join.chat_id is None:
        await _mark_join_done(session_maker, order_join.id)
        return

    client = worker_pool.get(order_join.account_id)

    # 🚪 اگر همین اکانت برای سفارشِ «هنوز فعال» دیگری عضوِ همین چت است، leave
    # الان به آن سفارش آسیب می‌زند → این ردیف برای sweep بعدی می‌ماند
    try:
        async with session_maker() as s:
            active_same_chat = await s.scalar(
                select(func.count(OrderJoin.id))
                .join(Order, OrderJoin.order_id == Order.id)
                .where(
                    OrderJoin.account_id == order_join.account_id,
                    OrderJoin.chat_id == order_join.chat_id,
                    OrderJoin.id != order_join.id,
                    OrderJoin.leave_done == False,  # noqa: E712
                    Order.status.in_([OrderStatus.pending, OrderStatus.running]),
                )
            ) or 0
    except Exception as e:
        logger.warning(f"Leave-sweep: active-order check failed for account {order_join.account_id}: {e}")
        active_same_chat = 0
    if active_same_chat:
        logger.info(
            f"Leave-sweep: user_{order_join.account_id}/ still joined for another ACTIVE order "
            f"in chat {order_join.chat_id}; deferring leave (Order #{order_join.order_id})."
        )
        return

    # 🚪 فاز ۹: بررسی Leave Dwell Time قبل از خروج
    try:
        async with session_maker() as s:
            order = await s.scalar(select(Order).where(Order.id == order_join.order_id))
            if order and order.status in [OrderStatus.completed, OrderStatus.error]:
                # +++ فاز اختیاری: استراتژی ورکر ساکن +++
                settings = await s.scalar(select(GlobalSettings).limit(1))
                worker_residency = settings.worker_residency if settings else "ephemeral"
                time_since_done = (datetime.now(timezone.utc) - (order.updated_at.replace(tzinfo=timezone.utc) if getattr(order, 'updated_at', None) else order.created_at.replace(tzinfo=timezone.utc))).total_seconds()
                
                if worker_residency == "resident" and time_since_done < 48 * 3600:
                    seven_days_ago = datetime.now(timezone.utc) - timedelta(days=7)
                    recent_orders_count = await s.scalar(
                        select(func.count(func.distinct(OrderJoin.order_id)))
                        .where(
                            OrderJoin.group_link == order_join.group_link,
                            OrderJoin.created_at >= seven_days_ago
                        )
                    )
                    if recent_orders_count >= 2:
                        active_joins = await s.scalar(
                            select(func.count(OrderJoin.id))
                            .where(OrderJoin.account_id == order_join.account_id, OrderJoin.leave_done == False)
                        )
                        if active_joins < 150:
                            logger.debug(f"Leave-sweep deferred for Order #{order.id} (Resident mode: frequent group, active joins={active_joins}).")
                            return
                # +++++++++++++++++++++++++++++++++++++++

                from utils.speed_profile import get_speed_profile
                profile = await get_speed_profile(order.speed_mode)
                dwell_min, dwell_max = profile.leave_dwell_hours
                if dwell_max > 0:
                    dwell_seconds = random.uniform(dwell_min, dwell_max) * 3600
                    time_since_done = (datetime.now(timezone.utc) - (getattr(order, 'updated_at', None).replace(tzinfo=timezone.utc) if getattr(order, 'updated_at', None) else order.created_at.replace(tzinfo=timezone.utc))).total_seconds()
                    if time_since_done < dwell_seconds:
                        logger.debug(f"Leave-sweep deferred for Order #{order.id} (Dwell rule active: elapsed {int(time_since_done)}s < target {int(dwell_seconds)}s).")
                        return # برای Sweep بعدی می‌ماند
    except Exception as e:
        logger.warning(f"Leave-sweep dwell time check failed for Order #{order_join.order_id}: {e}")

    if client is None or not getattr(client, "is_connected", False):
        # ورکر آفلاین — اگر اکانت بن شده باشد ردیف بسته می‌شود؛ وگرنه retry در sweep بعدی
        is_banned: Optional[bool] = None
        try:
            async with session_maker() as s:
                is_banned = await s.scalar(
                    select(Account.is_banned).where(Account.id == order_join.account_id)
                )
        except Exception as e:
            logger.warning(f"Leave-sweep: ban-check failed for account {order_join.account_id}: {e}")
        if is_banned:
            logger.warning(
                f"Leave-sweep: account {order_join.account_id} is banned - closing join row "
                f"(Order #{order_join.order_id}) without leave."
            )
            await _mark_join_done(session_maker, order_join.id)
            _leave_sweep_fails.pop(order_join.id, None)
            _stuck_leave_ids.discard(order_join.id)
        else:
            logger.warning(
                f"Leave-sweep: worker user_{order_join.account_id}/ offline; join row stays "
                f"for next sweep (Order #{order_join.order_id})."
            )
            _leave_sweep_fails[order_join.id] = _leave_sweep_fails.get(order_join.id, 0) + 1
            if _leave_sweep_fails[order_join.id] >= 10:
                _stuck_leave_ids.add(order_join.id)
                logger.warning(f"Leave-sweep: Join row {order_join.id} is stuck offline. Skipping for future sweeps.")
        return

    left = False
    try:
        await client.leave_chat(order_join.chat_id)
        left = True
        logger.info(
            f"Leave-sweep: worker user_{order_join.account_id}/ left chat "
            f"{order_join.chat_id} (Order #{order_join.order_id})."
        )
    except FloodWait as e:
        logger.warning(
            f"Leave-sweep: FloodWait {e.value}s on leave for user_{order_join.account_id}/; "
            f"row stays for next sweep."
        )
        _leave_sweep_fails[order_join.id] = _leave_sweep_fails.get(order_join.id, 0) + 1
        if _leave_sweep_fails[order_join.id] >= 10:
            _stuck_leave_ids.add(order_join.id)
        return
    except (UserNotParticipant, ChannelPrivate):
        # قبلاً خارج/کیک شده یا چت دیگر در دسترس نیست — نتیجه همان است: بستن ردیف
        left = True
        logger.info(
            f"Leave-sweep: user_{order_join.account_id}/ was not a member of "
            f"{order_join.chat_id} anymore (Order #{order_join.order_id})."
        )
    except Exception as e:
        logger.warning(
            f"Leave-sweep: leave_chat failed for user_{order_join.account_id}/ on "
            f"{order_join.chat_id}: {e} — row stays for next sweep."
        )
        _leave_sweep_fails[order_join.id] = _leave_sweep_fails.get(order_join.id, 0) + 1
        if _leave_sweep_fails[order_join.id] >= 10:
            _stuck_leave_ids.add(order_join.id)
        return

    if left:
        await _mark_join_done(session_maker, order_join.id)
        _leave_sweep_fails.pop(order_join.id, None)
        _stuck_leave_ids.discard(order_join.id)


async def _mark_join_done(session_maker: async_sessionmaker[AsyncSession], join_id: int) -> None:
    try:
        async with session_maker() as session:
            async with session.begin():
                await session.execute(
                    update(OrderJoin)
                    .where(OrderJoin.id == join_id)
                    .values(leave_done=True)
                    .execution_options(synchronize_session=False)
                )
    except Exception as e:
        logger.warning(f"Leave-sweep: failed to mark order_join #{join_id} as done: {e}")



# ==========================================
# 🛡 فاز ۲ (BUG-04): رجیستری busy — Distributed Lock با Redis
# ==========================================

async def _acquire_busy(account_db_id: int) -> bool:
    """رزرو اتمیک اکانت در Redis (اسکیل‌پذیر برای چند پروسه)"""
    redis = _get_redis()
    key = f"busy_worker:{account_db_id}"
    
    # استفاده از SET NX (فقط در صورتی که وجود نداشته باشد ست می‌شود)
    # انقضای ۳۰ دقیقه‌ای (1800 ثانیه) به عنوان سپر ایمنی برای جلوگیری از قفل ماندن ابدی در صورت کرش شدید پروسه
    try:
        is_acquired = await redis.set(key, "1", nx=True, ex=1800)
        return bool(is_acquired)
    except Exception as e:
        logger.error(f"Redis lock acquisition failed for account {account_db_id}: {e}")
        return False


async def _release_busy(account_db_id: int) -> None:
    """آزادسازی قفل از Redis — همیشه در بلوک finally صدا زده می‌شود."""
    redis = _get_redis()
    key = f"busy_worker:{account_db_id}"
    try:
        await redis.delete(key)
    except Exception as e:
        logger.error(f"Redis lock release failed for account {account_db_id}: {e}")

async def _salvage_unsent_after_crash(
    session: AsyncSession,
    order_id: int,
    account_db_id: int,
    targets: List[str],
) -> List[str]:
    """
    🔴 مکمل فاز ۲ و ۶: بازیابی امن پس از کرش با پشتیبانی از Chunkهای عظیم
    """
    try:
        await session.commit()
    except Exception as e:
        logger.error(f"Salvage: commit of in-flight logs failed for Order #{order_id}: {e}")
        try:
            await session.rollback()
        except Exception:
            pass
        return []
        
    log_rows = []
    chunk_size = 500  # 🟢 فاز ۶: قطعه‌بندی برای جلوگیری از شکستن محدودیت پارامترهای SQL
    try:
        for i in range(0, len(targets), chunk_size):
            batch = targets[i:i + chunk_size]
            rows = (await session.execute(
                select(OrderLog.target, OrderLog.status, OrderLog.error_message)
                .where(
                    OrderLog.order_id == order_id,
                    OrderLog.account_id == account_db_id,
                    OrderLog.target.in_(batch),
                )
                .order_by(OrderLog.id.asc())
            )).all()
            log_rows.extend(rows)
    except Exception as e:
        logger.error(f"Salvage: log query failed for Order #{order_id}: {e}")
        return []

    last_log_by_target: Dict[str, tuple] = {}
    succeeded_targets: set = set()
    for tgt, status, err in log_rows:
        last_log_by_target[tgt] = (status, err)
        if status == "success":
            succeeded_targets.add(tgt)

    unsent: List[str] = []
    for t in targets:
        if t in succeeded_targets:
            continue
        log = last_log_by_target.get(t)
        if log is None:
            unsent.append(t)
            continue
        _, err = log
        if (
            "FloodWait" in (err or "")
            or "TransientConnection" in (err or "")
            or err == "UserRestricted"
        ):
            unsent.append(t)
            
    return unsent


def _build_chunk_order(order: Order, banner: Banner) -> Order:
    """
    🎨 چرخش بنر: ساخت نمونه‌ی Order جدا و transient برای هر chunk.
    در منطق جدید، بنر همیشه به عنوان پیام دوم (message_2_text) در نظر گرفته می‌شود 
    تا در هر دو حالت (هوشمند و عادی) پیام اول شما دست‌نخورده باقی بماند.
    """
    return Order(
        id=order.id,
        order_type=order.order_type,
        message_text=order.message_text,       # حفظ پیام اول کاربر (یخ‌شکن)
        media_path=order.media_path,
        media_type=order.media_type,
        button_text=order.button_text,
        button_url=order.button_url,
        message_2_text=banner.text,            # تزریق بنر همیشه به جایگاه پیام دوم
        media_2_path=banner.media_path,
        media_2_type=banner.media_type,
        message_3_text=order.message_3_text,   # حفظ پیام سوم کاربر
        media_3_path=order.media_3_path,
        media_3_type=order.media_3_type,
        smart_flow=order.smart_flow,
        use_banner_pool=order.use_banner_pool,
        source_channel_id=order.source_channel_id,
        source_message_ids=order.source_message_ids,
    )

async def extractor_task_wrapper(
    clients: Union[Client, List[Client]],     # 🔄 List[Client] (تک‌کلاینت هم پذیرفته می‌شود)
    account_db_ids: Union[int, List[int]],    # 🔄 لیست موازیِ اکانت‌های درگیر
    order: Order,
    group_link: str,
    session_maker: async_sessionmaker[AsyncSession],
    bot: Bot,
    total_estimate: Optional[int] = None,     # 🔄 تخمینِ گرفته‌شده توسط دیسپچر
) -> tuple[List[str], Optional[str]]:
    """
    🔄 بازنویسی برای «استخراج موازی»:

    - بیش از یک کلاینت ← extract_members_parallel اجرا و نتایجِ ادغام‌شده
      دقیقاً از همان جریان قبلی عبور می‌کند؛ کاملاً سازگار با لاگ/ارسال بقیه‌ی سیستم.
    - ثبت order_joins و limit_handler برای «تمام» اکانت‌های درگیر.
    - 🛡 خروجی ساختاریافته (تارگت‌های ناموفق، دلیل توقف) برای پشتیبانی از Failover.
    """
    # ============================================================
    # 🔄 نرمال‌سازی ورودی — «قبل» از هر کدی که ممکن است خطا دهد
    # ============================================================
    if isinstance(clients, Client):
        clients = [clients]
    if isinstance(account_db_ids, int):
        account_db_ids = [account_db_ids]
    clients = list(clients or [])
    account_db_ids = list(account_db_ids or [])
    
    if len(clients) != len(account_db_ids):
        logger.error(
            f"extractor_task_wrapper: clients/accounts mismatch "
            f"({len(clients)} vs {len(account_db_ids)}) — Order #{order.id}"
        )
        pair_n = min(len(clients), len(account_db_ids))
        clients, account_db_ids = clients[:pair_n], account_db_ids[:pair_n]
        
    if not clients:
        return [group_link], "error"

    primary_id = account_db_ids[0] if account_db_ids else 0
    is_parallel = len(clients) > 1
    worker_label = f"{len(clients)}×ورکر موازی" if is_parallel else f"user_{primary_id}/"

    order_speed_mode = getattr(order, "speed_mode", "safe") or "safe"
    profile = await get_speed_profile(order_speed_mode)

    reporter = await _get_or_start_reporter(
        bot=bot,
        session_maker=session_maker,
        order_id=order.id,
        task_type="extract",
        target_hint=group_link,
    )

    # 🔄 تخمین تعداد اعضا
    if total_estimate is None:
        try:
            total_estimate = await _estimate_member_count(clients[0], group_link)
        except Exception:
            total_estimate = None

    async def _progress_cb(count: int, scanned: int = 0) -> bool:
        nonlocal total_estimate
        fallback_active = False
        # 🟢 سیگنال توقف: چک کردن اینکه آیا ادمین دکمه توقف را زده است یا خیر
        try:
            from workers.sender import is_order_killed
            if await is_order_killed(order.id):
                return False  # ارسال سیگنال توقف به موتور استخراج
                
            async with session_maker() as prog_session:
                async with prog_session.begin():
                    current_order = await prog_session.scalar(select(Order).where(Order.id == order.id))
                    if current_order and current_order.reject_reason == "fallback_messages":
                        fallback_active = True

                    await prog_session.execute(
                        update(Order).where(Order.id == order.id)
                        .values(extracted_count=int(count))
                    )
        except Exception:
            pass

        # 🟢 Fallback تخمین برای گروه‌های خصوصی که get_chat قبل از join ناموفق بود
        if total_estimate is None and order.filter_type != "messages":
            try:
                total_estimate = await _estimate_member_count(clients[0], group_link)
            except Exception:
                pass
            
        if reporter is not None:
            if order.filter_type == "messages" or fallback_active:
                await reporter.update(
                    done=scanned,
                    total=5000,
                    status=f"در حال اسکن پیام‌ها (کاربران منحصربه‌فرد یافته‌شده: {count})…",
                    account=worker_label,
                )
            else:
                await reporter.update(
                    done=int(count),
                    total=total_estimate,
                    status="در حال استخراج اعضای گروه…",
                    account=worker_label,
                )
        return True # ادامه عملیات
    progress_cb = _progress_cb

    status_code: str = "error"
    file_path: Optional[str] = None
    join_records: List[dict] = []    # [{"index","join_chat_id","joined_now","status"}]
    limit_records: List[dict] = []   # [{"index","limit_type","wait_seconds"}]

    # ============================================================
    # 🔄 استخراج chat_id_hint در صورت وجود برای پرش سریع
    # ============================================================
    chat_id_hint = None
    try:
        async with session_maker() as temp_session:
            row = await temp_session.scalar(
                select(OrderJoin.chat_id)
                .where(
                    OrderJoin.group_link == group_link, 
                    OrderJoin.account_id == account_db_ids[0],
                    OrderJoin.chat_id.isnot(None)
                )
                .order_by(OrderJoin.id.desc())
            )
            if row: chat_id_hint = row
    except Exception: pass

    # ============================================================
    # 🔄 اجرای استخراج — موازی یا تک‌کلاینت (ارسال order_id برای R4)
    # ============================================================
    slice_stats = ""
    no_username_total = 0
    if is_parallel:
        parallel_result = await extract_members_parallel(
            clients,
            group_link,
            filter_type=order.filter_type,
            progress_cb=progress_cb,
            total_estimate=total_estimate,
            chat_id_hint=chat_id_hint,
            order_id=order.id,
            speed_profile=profile
        )
        status_code = parallel_result.get("status_code") or "error"
        file_path = parallel_result.get("file_path")
        join_records = list(parallel_result.get("join_records") or [])
        limit_records = list(parallel_result.get("limit_records") or [])
        no_username_total = parallel_result.get("no_username_count", 0)
        
        # استخراج دیتای شفاف برای ریپورت (B4)
        total_slices = parallel_result.get("total_slices", 0)
        failed_slices = parallel_result.get("failed_slices", 0)
        if total_slices > 0:
            coverage = int((total_slices - failed_slices) / total_slices * 100)
            slice_stats = f"\nپوشش استخراج: {coverage}٪ ({total_slices - failed_slices} ورکر موفق از {total_slices})"
    else:
        status_code, file_path, join_chat_id, joined_now, no_username_total = await extract_active_users(
            clients[0], group_link, filter_type=order.filter_type, progress_cb=progress_cb,
            chat_id_hint=chat_id_hint, 
            order_id=order.id,
            speed_profile=profile
        )
        join_records = [{
            "index": 0,
            "join_chat_id": join_chat_id,
            "joined_now": joined_now,
            "status": status_code,
        }]
        if status_code.startswith("limit:"):
            parts = status_code.split(":")
            limit_records = [{
                "index": 0,
                "limit_type": parts[1] if len(parts) > 1 else "unknown",
                "wait_seconds": int(parts[2]) if len(parts) > 2 else 0,
            }]

    # ============================================================
    # 🔄 ثبت محدودیت برای «تمام» اکانت‌های درگیر (نه فقط primary).
    # ============================================================
    if limit_records:
        from utils.limit_handler import register_account_limit
        for rec in limit_records:
            idx = int(rec.get("index", -1))
            if not (0 <= idx < len(account_db_ids)):
                continue
            try:
                async with session_maker() as session:
                    await register_account_limit(
                        session,
                        account_db_ids[idx],
                        clients[idx] if 0 <= idx < len(clients) else None,
                        str(rec.get("limit_type", "unknown")),
                        int(rec.get("wait_seconds", 0) or 0),
                    )
            except Exception as e:
                logger.error(
                    f"Failed to register account limit for {account_db_ids[idx]} "
                    f"(Order #{order.id}): {e}"
                )

    # ---------- محدودیت ← توقف ورکر ----------
    if status_code.startswith("limit:"):
        parts = status_code.split(":")
        limit_type = parts[1] if len(parts) > 1 else "unknown"
        wait_seconds = int(parts[2]) if len(parts) > 2 else 60

        try:
            async with session_maker() as session:
                next_check = datetime.now(timezone.utc) + timedelta(seconds=wait_seconds)
                new_retry_count = (order.retry_count or 0) + 1
                await session.execute(
                    update(Order).where(Order.id == order.id).values(
                        status=OrderStatus.pending,
                        scheduled_for=next_check,
                        retry_count=new_retry_count
                    )
                )
                await session.commit()
        except Exception as e:
            logger.error(f"Failed to reschedule limit-hit Order #{order.id}: {e}")

        if reporter is not None:
            await reporter.update(
                status=f"⏳ ورکر با محدودیت {limit_type} مواجه شد. توقف موقت به مدت {wait_seconds} ثانیه..."
            )
            
        return [group_link], "limit"

    if status_code == "members_hidden":
        if config.EXTRACT_AUTO_FALLBACK_TO_MESSAGES:
            logger.info(f"Order #{order.id}: Members hidden, auto-fallback to messages.")
            async with session_maker() as session:
                await session.execute(
                    update(Order).where(Order.id == order.id).values(
                        filter_type="messages"
                    )
                )
                session.add(OrderLog(
                    order_id=order.id,
                    account_id=primary_id,
                    target=group_link,
                    status="info",
                    error_message="استراتژی به‌خاطر مخفی‌بودن اعضا از users به messages تغییر کرد."
                ))
                await session.commit()
                order.filter_type = "messages"
            
            try:
                notify_msg = (
                    f"⚠️ <b>تغییر استراتژی استخراج</b>\n\n"
                    f"سفارش: <code>{order.tracking_code or order.id}</code>\n"
                    f"<i>به‌خاطر مخفی‌بودن اعضای گروه، ربات به‌طور خودکار استراتژی را به «فرستندگان پیام» تغییر داد.</i>"
                )
                await broadcast_to_admins(bot, text=notify_msg)
                
                # 🟢 ارسال پیام مستقیم (نوتیفیکیشن) به کاربری که سفارش را ثبت کرده است
                await _notify_owner(bot, session_maker, order.id, notify_msg)
                
            except Exception as e:
                logger.error(f"Failed to broadcast/notify strategy change: {e}")
                
            if reporter is not None:
                await reporter.update(
                    status="⚠️ لیست مخفی بود. تغییر استراتژی به «فرستندگان پیام»…",
                )
                
            # 🟢 اجرای مجدد با فیلتر messages 
            # (استخراج پیام‌ها فقط در حالت تک-ورکر پشتیبانی می‌شود، پس همیشه از کلاینت اصلی استفاده می‌کنیم)
            status_code, file_path, join_chat_id, joined_now, no_username_total = await extract_active_users(
                clients[0], group_link, filter_type="messages", progress_cb=progress_cb, speed_profile=profile
            )
            
            new_joins = [{
                "index": 0, "join_chat_id": join_chat_id, "joined_now": joined_now, "status": status_code,
            }]
            
            limit_records = []
            if status_code.startswith("limit:"):
                parts = status_code.split(":")
                limit_records = [{
                    "index": 0, "limit_type": parts[1] if len(parts) > 1 else "unknown", "wait_seconds": int(parts[2]) if len(parts) > 2 else 0,
                }]
            
            # 🟢 ادغام رکوردهای جوین اجرای اول و دوم برای جلوگیری از پاک شدن joinهای واقعی
            join_dict = {r.get("index", -1): r for r in join_records}
            for nr in new_joins:
                idx = nr.get("index", -1)
                if idx in join_dict:
                    if join_dict[idx].get("joined_now"):
                        nr["joined_now"] = True
                    if not nr.get("join_chat_id") and join_dict[idx].get("join_chat_id"):
                        nr["join_chat_id"] = join_dict[idx]["join_chat_id"]
                join_dict[idx] = nr
            join_records = list(join_dict.values())
            
            # ثبت مجدد محدودیت‌ها در صورت نیاز
            if limit_records:
                from utils.limit_handler import register_account_limit
                for rec in limit_records:
                    idx = int(rec.get("index", -1))
                    if not (0 <= idx < len(account_db_ids)): continue
                    try:
                        async with session_maker() as session:
                            await register_account_limit(
                                session, account_db_ids[idx], clients[idx] if 0 <= idx < len(clients) else None,
                                str(rec.get("limit_type", "unknown")), int(rec.get("wait_seconds", 0) or 0),
                            )
                    except Exception as e:
                        logger.error(f"Failed to register account limit during fallback: {e}")
            
            if status_code.startswith("limit:"):
                parts = status_code.split(":")
                limit_type = parts[1] if len(parts) > 1 else "unknown"
                if reporter is not None:
                    await reporter.fail(f"⛔️ <b>توقف ورکر</b>\n\nورکر با محدودیت {limit_type} مواجه شد.")
                    _pop_progress_reporter("extract", order.id)
                return [group_link], "limit"
                
        else:
            # رفتار دستی قبلی در صورت غیرفعال بودن فلگ
            async with session_maker() as session:
                await session.execute(
                    update(Order).where(Order.id == order.id).values(
                        status=OrderStatus.pending,
                        is_approved=False,
                        reject_reason="members_hidden"
                    )
                )
                session.add(OrderLog(
                    order_id=order.id,
                    account_id=primary_id,
                    target=group_link,
                    status="error",
                    error_message="لیست اعضا مخفی بود و فال‌بک خودکار غیرفعال است. منتظر تصمیم ادمین."
                ))
                await session.commit()
    
            fallback_text = (
                "⚠️ <b>لیست اعضای این گروه مخفی شده است.</b>\n\n"
                "ادمین گروه دسترسی مشاهده‌ی اعضا را محدود کرده. اما می‌توانید از استراتژی «💬 فرستندگان پیام» استفاده کنید که از تاریخچه‌ی پیام‌های اخیر، کاربران فعال را استخراج می‌کند.\n\n"
                "برای ثبت سفارش جدید با استراتژی messages، روی دکمه زیر کلیک کنید."
            )
            
            from aiogram.utils.keyboard import InlineKeyboardBuilder
            builder = InlineKeyboardBuilder()
            builder.button(text="🔄 ثبت سفارش جدید با messages", callback_data=f"new_extract_messages_{order.id}/")
            
            try:
                from utils.admin_broadcast import broadcast_to_admins_with_keyboard
                await broadcast_to_admins_with_keyboard(
                    bot,
                    text=f"سفارش استخراج <code>{order.tracking_code or order.id}</code> متوقف شد.\n\n{fallback_text}",
                    keyboard=builder.as_markup()
                )
            except Exception as e:
                logger.error(f"Failed to broadcast members_hidden warning: {e}")
            
            if reporter is not None:
                await reporter.fail(fallback_text)
                _pop_progress_reporter("extract", order.id)
            
            return [], "members_hidden"

    async with session_maker() as session:
        # ============================================================
        # 🔄 ثبت order_joins برای «همه‌ی» اکانت‌های مشارکت‌کننده
        # ============================================================
        try:
            for rec in join_records:
                idx = int(rec.get("index", -1))
                if not (0 <= idx < len(account_db_ids)):
                    continue
                    
                await _record_order_join(
                    session, order.id, account_db_ids[idx], group_link,
                    rec.get("join_chat_id"),
                    bool(rec.get("joined_now")),
                    str(rec.get("status", status_code)),
                )
            await session.commit()
        except Exception as e:
            await session.rollback()
            logger.warning(f"Failed to record order_join(s) for Order #{order.id}: {e}")

        if status_code == "pending_approval":
            new_retry_count = (order.retry_count or 0) + 1

            if new_retry_count >= profile.approval_retry_limit:
                stmt = update(Order).where(Order.id == order.id).values(
                    status=OrderStatus.error,
                    scheduled_for=None,
                    retry_count=new_retry_count,
                )
                await session.execute(stmt)

                session.add(OrderLog(
                    order_id=order.id,
                    account_id=primary_id,   # 🔄 (قبلاً account_db_id)
                    target=group_link,
                    status="error",
                    error_message=(
                        f"Pending approval retry limit reached "
                        f"({profile.approval_retry_limit} cycles)."
                    ),
                ))
                await session.commit()

                await broadcast_to_admins(
                    bot,
                    text=(
                        f"⛔️ <b>توقف عملیات استخراج</b>\n\n"
                        f"سفارش: <code>{order.tracking_code}</code>\n"
                        f"گروه: <b>{group_link}</b>\n\n"
                        f"متأسفانه زمان مجاز سپری شد و با درخواست عضویت ربات موافقت نشد.\n"
                        f"<i>عملیات استخراج کاملاً متوقف و پرونده این سفارش بسته شد.</i>"
                    )
                )

                if reporter is not None:
                    await reporter.fail(
                        "⛔️ <b>سفارش استخراج متوقف شد</b>\n\n"
                        f"درخواست عضویت گروه خصوصی پس از {profile.approval_retry_limit} چرخه هنوز تأیید نشد."
                    )
                    _pop_progress_reporter("extract", order.id)

                return [], "approval_limit"

            next_check = datetime.now(timezone.utc) + timedelta(seconds=profile.approval_cycle_seconds)
            stmt = update(Order).where(Order.id == order.id).values(
                scheduled_for=next_check,
                retry_count=new_retry_count,
            )
            await session.execute(stmt)
            await session.commit()
            
            if new_retry_count == 1 or new_retry_count % 5 == 0:
                worker_label_text = f"user_{primary_id}/" if not is_parallel else f"{len(clients)} ورکر"
                notify_text = (
                    f"⏳ <b>درخواست عضویت ورکر ارسال شد!</b>\n\n"
                    f"سفارش: <code>{order.tracking_code}</code>\n"
                    f"گروه <b>{group_link}</b> خصوصی (ریکوئستی) است.\n"
                    f"ورکر: <b>{worker_label_text}</b>\n\n"
                    f"<i>ربات منتظر تایید ادمینِ گروه می‌ماند. لطفاً در گروه تأیید کنید.</i>\n\n"
                    f"🕐 تلاش فعلی: <b>{new_retry_count} از {profile.approval_retry_limit}</b>"
                )
                await broadcast_to_admins(bot, text=notify_text)
                await _notify_owner(bot, session_maker, order.id, notify_text)

            if reporter is not None:
                await reporter.update(
                    status="در انتظار تأیید درخواست عضویت توسط ادمین گروه…",
                )
                
            return [group_link], "pending_approval"

        # --- 🟢 ارسال پیام تأیید (با جلوگیری قطعی از ارسال تکراری با nx=True) ---
        if (order.retry_count or 0) > 0 and status_code in ("success", "partial_success", "success_fallback"):
            try:
                from workers.sender import _get_redis
                redis = _get_redis()
                if await redis.set(f"join_approved_notified:{order.id}", "1", nx=True, ex=30*86400):
                    app_txt = f"✅ <b>درخواست عضویت تأیید شد!</b>\nسفارش: <code>{order.tracking_code or order.id}</code>\nاستخراج هم‌اکنون آغاز می‌شود..."
                    await broadcast_to_admins(bot, text=app_txt)
                    await _notify_owner(bot, session_maker, order.id, app_txt)
                    if reporter is not None:
                        await reporter.update(status="✅ درخواست عضویت تأیید شد! در حال استخراج...")
            except Exception as e:
                logger.error(f"Failed to send silent approval note: {e}")
        # --------------------------------------------------------------------

        log_entry = OrderLog(order_id=order.id, account_id=primary_id, target=group_link)
        
        # 🟢 اضافه شدن stopped به لیست وضعیت‌های موفق که فایل در آنها حفظ می‌شود
        if status_code in ("success", "partial_success", "success_fallback", "stopped") and file_path and os.path.exists(file_path):
            log_entry.status = status_code 

            permanent_path = f"exports/extract_order_{order.id}.txt"
            shutil.copy(file_path, permanent_path)
            
            extracted_count = 0
            try:
                with open(permanent_path, "r", encoding="utf-8") as f:
                    extracted_count = sum(1 for line in f if line.strip())
            except Exception as cnt_err:
                logger.warning(f"Order #{order.id}: could not count export lines ({cnt_err})")
                
            try:
                await session.execute(update(Order).where(Order.id == order.id).values(
                    media_path=permanent_path, 
                    extracted_count=extracted_count
                ))
            except Exception as db_err:
                logger.error(f"Failed to save permanent_path for extract order #{order.id}: {db_err}")

            admin_ids = []
            if getattr(config, "ADMIN_ID", None):
                admin_ids.append(int(config.ADMIN_ID))
            try:
                sub_admins = (await session.scalars(select(Admin.telegram_id))).all()
                admin_ids.extend(int(aid) for aid in sub_admins)
                admin_ids = list(set(admin_ids))
            except Exception as e:
                logger.error(f"Failed to fetch sub-admins for extract broadcast: {e}")

            if reporter is not None:
                admin_ids = [aid for aid in admin_ids if aid != reporter.chat_id]

            engine_note = (
                f"⚙️ حالت اجرا: <b>استخراج موازی ({len(clients)} ورکر)</b>\n"
                if is_parallel else "⚙️ حالت اجرا: <b>تک‌ورکر</b>\n"
            )

            sends_failed = False
                
            for admin_id in admin_ids:
                try:
                    document = FSInputFile(file_path)
                    
                    no_user_note = f"⚠️ {no_username_total} کاربر بدون یوزرنیم نادیده گرفته شد.\n" if no_username_total > 0 else ""
                    caption_text = (
                        f"✅ <b>عملیات استخراج تکمیل شد</b>\n\n"
                        f"سفارش: <code>{order.tracking_code}</code>\n"
                        f"تارگت: {group_link}\n"
                        f"{engine_note}{slice_stats}\n"
                        f"{no_user_note}"
                    )
                    if status_code == "partial_success":
                        warning_text = (
                            "⚠️ نتیجه ناقص است: بخشی از اسکن با خطا/FloodWait متوقف شد "
                            "اما داده‌های استخراج‌شده معتبرند.\n"
                        )
                        if extracted_count >= config.EXTRACT_MEMBERS_API_LIMIT:
                            warning_text += (
                                "⚠️ سقف تلگرام (۱۰,۰۰۰ عضو) پر شد — لیست کامل فقط با "
                                "ادمین‌بودن اکانت قابل استخراج است.\n"
                            )
                        caption_text = warning_text + caption_text
                    # 🟢 پیام ادمین برای زمانی که وسط کار لغو شده
                    elif status_code == "stopped":
                        caption_text = "🛑 عملیات استخراج توسط ادمین در میانه راه لغو شد، اما فایل استخراج‌شده تا این لحظه معتبر است.\n" + caption_text
                        
                    elif status_code == "success_fallback":
                        caption_text = "⚠️ <i>لیست اعضا مخفی بود؛ استخراج به‌صورت خودکار از تاریخچه پیام‌ها انجام شد.</i>\n\n" + caption_text

                    await bot.send_document(
                        chat_id=admin_id,
                        document=document,
                        caption=caption_text
                    )
                except Exception as e:
                    logger.error(f"Failed to send extracted file to admin {admin_id} via Bot: {e}")
                    sends_failed = True

            if not sends_failed:
                try:
                    os.remove(file_path)
                    logger.info(f"Garbage Collection: Deleted extracted temp file {file_path}")
                except Exception:
                    pass

            if reporter is not None:
                # 🟢 مدیریت گرافیکِ گزارش زنده کاربر برای حالت لغو
                if status_code == "stopped":
                    outcome_label = "متوقف شده"
                    partial_note = "\n🛑 اسکن در میانه راه توسط شما لغو شد."
                    title_msg = "🛑 <b>عملیات استخراج متوقف شد</b>"
                elif status_code == "partial_success":
                    outcome_label = "ناقص"
                    partial_note = "\n⚠️ بخشی از اسکن با خطا/FloodWait متوقف شد اما داده‌ها معتبرند."
                    title_msg = "✅ <b>عملیات استخراج تکمیل شد</b>"
                elif status_code == "success_fallback":
                    outcome_label = "کامل (فال‌بک پیام‌ها)"
                    partial_note = "\n⚠️ لیست اعضا مخفی بود؛ استخراج به‌صورت خودکار از میان پیام‌دهندگان انجام شد."
                    title_msg = "✅ <b>عملیات استخراج تکمیل شد</b>"
                else:
                    outcome_label = "کامل"
                    partial_note = ""
                    title_msg = "✅ <b>عملیات استخراج تکمیل شد</b>"

                engine_note_owner = (
                    f"⚙️ حالت اجرا: استخراج موازی ({len(clients)} ورکر)\n"
                    if is_parallel else "⚙️ حالت اجرا: تک‌ورکر\n"
                )
                no_user_note_reporter = f"⚠️ {no_username_total} کاربر بدون یوزرنیم نادیده گرفته شد.\n" if no_username_total > 0 else ""
                
                await reporter.finish(
                    f"{title_msg}\n\n"
                    f"👤 تعداد عضو استخراج‌شده: <b>{extracted_count}</b>\n"
                    f"🏷 نوع نتیجه: <b>{outcome_label}</b>{partial_note}\n"
                    f"{engine_note_owner}{slice_stats}\n"
                    f"{no_user_note_reporter}"
                    f"📄 فایل نتیجه در پیام بعدی برای شما ارسال شد.",
                    document_path=permanent_path,
                )
                _pop_progress_reporter("extract", order.id)
                
            stop_reason = None
            unsent_targets = []

        else:
            log_entry.status = "error"
            if status_code == "error_not_admin":
                log_entry.error_message = "عدم دسترسی: ورکر ادمین کانال نیست."
                await broadcast_to_admins(
                    bot,
                    text=(
                        f"⚠️ <b>خطای دسترسی در استخراج</b>\n\n"
                        f"سفارش: <code>{order.tracking_code}</code>\n"
                        f"تارگت: <b>{group_link}</b>\n\n"
                        f"<i>این تارگت یک کانال است. برای استخراج آیدی از کانال، اکانت ورکر باید حتماً ادمینِ کانال باشد. عملیات متوقف شد.</i>"
                    )
                )
                stop_reason = "error_not_admin"
            elif status_code == "rejected":
                log_entry.error_message = "درخواست عضویت رد شد یا ربات اخراج شده است."
                try:
                    from workers.sender import _get_redis
                    redis = _get_redis()
                    if await redis.set(f"join_rejected_notified:{order.id}", "1", nx=True, ex=30*86400):
                        rej_txt = (
                            f"❌ <b>درخواست عضویت رد شد</b>\n\n"
                            f"سفارش: <code>{order.tracking_code or order.id}</code>\n"
                            f"تارگت: <b>{group_link}</b>\n\n"
                            f"<i>ادمین گروه درخواست ورود ربات را رد کرد (یا ربات مسدود شده است). عملیات متوقف شد.</i>"
                        )
                        await broadcast_to_admins(bot, text=rej_txt)
                        await _notify_owner(bot, session_maker, order.id, rej_txt)
                except Exception as e:
                    logger.error(f"Failed to send silent reject note: {e}")
                stop_reason = "rejected"
            elif status_code == "fallback_empty":
                log_entry.status = "success"
                log_entry.error_message = _get_resolver_error_message("empty_fallback_result")
                try:
                    async with session_maker() as session:
                        await session.execute(
                            update(Order).where(Order.id == order.id).values(
                                status=OrderStatus.completed,
                                reject_reason="fallback_empty",
                                extracted_count=0
                            )
                        )
                        await session.commit()
                except Exception as e:
                    logger.error(f"Failed to set fallback_empty state for Order #{order.id}: {e}")
                
                await broadcast_to_admins(
                    bot,
                    text=(
                        f"⚠️ <b>فال‌بک بدون نتیجه</b>\n\n"
                        f"سفارش: <code>{order.tracking_code}</code>\n"
                        f"تارگت: <b>{group_link}</b>\n\n"
                        f"<i>{_get_resolver_error_message('empty_fallback_result')}</i>"
                    )
                )
                if reporter is not None:
                    await reporter.finish(
                        f"⚠️ <b>استخراج به پایان رسید (بدون نتیجه)</b>\n\n"
                        f"{_get_resolver_error_message('empty_fallback_result')}"
                    )
                    _pop_progress_reporter("extract", order.id)
                stop_reason = "fallback_empty"
            elif status_code == "proxy_connection_error":
                log_entry.error_message = "خطای اتصال پروکسی یا شبکه."
                stop_reason = "proxy_connection_error"
            else:
                worker_note = ""
                if is_parallel and join_records:
                    failed_ids = [
                        account_db_ids[int(r.get("index", -1))]
                        for r in join_records
                        if r.get("status") not in ("ok",)
                        and 0 <= int(r.get("index", -1)) < len(account_db_ids)
                    ]
                    if failed_ids:
                        worker_note = f" (failed workers: {failed_ids})"
                log_entry.error_message = f"Extraction failed or access denied.{worker_note}"
                stop_reason = "error"

            unsent_targets = [group_link]

        if reporter is not None and log_entry.status == "error":
            fail_reason = (
                "این تارگت یک کانال است و ورکر ادمین آن نیست؛ استخراج آیدی از کانال ممکن نیست."
                if status_code == "error_not_admin"
                else ("درخواست ورود به گروه رد شد (یا ربات بلاک شده است)." if status_code == "rejected" 
                else ("ارتباط با پروکسی یا شبکه قطع شد." if status_code == "proxy_connection_error" 
                else ("عملیات قبل از یافتن کاربری توسط شما لغو شد." if status_code == "stopped" else "استخراج اعضا ناموفق بود یا دسترسی وجود ندارد.")))
            )
            await reporter.fail(
                f"⛔️ <b>استخراج ناموفق بود</b>\n\n{fail_reason}"
            )
            _pop_progress_reporter("extract", order.id)

        session.add(log_entry)
        
        try:
            await session.commit()
        except Exception as db_err:
            await session.rollback()
            logger.error(f"DB Error saving extract log: {db_err}")
            
    return unsent_targets, stop_reason

# ==========================================
# 🛡 فاز ۸ (BUG-25): پارس و اعتبارسنجی چند لینک گروه در یک سفارش
# (تفکیک با خط جدید / فاصله / کاما / سمی‌کالن)
# ==========================================
# لینک عمومی: (https?://)t.me/<username> یا telegram.me/<username> (با/بدون اسلش انتهایی)
_RE_PUBLIC_LINK = re.compile(
    r"^(?:https?://)?(?:t|telegram)\.me/[A-Za-z0-9_]{4,64}/?$", re.IGNORECASE
)
# لینک دعوت: (https?://)t.me/+<hash> یا t.me/joinchat/<hash>
_RE_INVITE_LINK = re.compile(
    r"^(?:https?://)?(?:t|telegram)\.me/(?:\+|joinchat/)[A-Za-z0-9_\-]{8,}/?$", re.IGNORECASE
)
# یوزرنیم با/بدون @ (به لینک t.me نرمال می‌شود)
_RE_USERNAME = re.compile(r"^@?[A-Za-z0-9_]{4,64}$")


def _parse_target_links(raw: Optional[str]) -> List[str]:
    """
    🛡 فاز ۸ (BUG-25): تبدیل target_data سفارش لینکی به لیست لینک‌های معتبر.

    - تفکیک توکن‌ها با خط جدید / فاصله / کاما / سمی‌کالن
    - اعتبارسنجی هر توکن (لینک عمومی t.me، لینک دعوت +/joinchat، یا @username)
    - نرمال‌سازی @username / یوزرنیم خالی به لینک t.me
    - حذف تکراری‌ها (حفظ ترتیب ورودی)
    خروجی: لیست لینک‌های معتبر — خالی یعنی هیچ لینک قابل استفاده‌ای وجود ندارد.
    """
    tokens = [t.strip() for t in re.split(r"[\s,;\n]+", raw or "") if t.strip()]
    links: List[str] = []
    seen: set = set()
    for token in tokens:
        if _RE_USERNAME.fullmatch(token):
            token = f"https://t.me/{token.lstrip('@')}"
        if not (_RE_PUBLIC_LINK.fullmatch(token) or _RE_INVITE_LINK.fullmatch(token)):
            continue
        if token not in seen:
            seen.add(token)
            links.append(token)
    return links

# 🟢 این تابع جدید دقیقاً اینجا اضافه می‌شود
def _get_resolver_error_message(status_code: str) -> str:
    if status_code == "empty_filter_result":
        return "⚠️ فیلتر انتخابی شما نتیجه‌ای نداشت (یا اعضای گروه یوزرنیم نداشتند). پیشنهاد می‌شود با استراتژی «فرستندگان پیام» سفارش جدید ثبت کنید."
    elif status_code == "empty_fallback_result":
        return "⚠️ لیست اعضا مخفی بود و ربات خودکار به پیام‌ها سوییچ کرد، اما هیچ فرستنده فعالی (دارای یوزرنیم) در تاریخچه اخیر گروه یافت نشد."
    elif status_code == "members_hidden":
        return "⚠️ لیست اعضای این گروه مخفی است. می‌توانید از استراتژی «فرستندگان پیام» برای استخراج کاربران استفاده کنید."
    elif status_code == "error_not_admin":
        return "این تارگت یک کانال است و ورکر ادمین آن نیست؛ استخراج اعضا ممکن نیست."
    # +++ ترجمه خطاهای جدید اضافه شد +++
    elif status_code == "rejected":
        return "❌ درخواست عضویت ربات توسط ادمین گروه رد شد (یا ربات از گروه اخراج/بن شده است)."
    elif status_code == "invalid_link":
        return "🔗 لینک وارد شده نامعتبر یا منقضی شده است. لطفاً لینک گروه را بررسی کنید."
    # ++++++++++++++++++++++++++++++++++
    else:
        return "استخراج اعضای گروه ناموفق بود یا دسترسی وجود ندارد."
    

async def link_send_resolver_wrapper(
    client: Client,
    account_db_id: int,
    order_id: int,
    group_link: str,
    filter_type: Optional[str],
    session_maker: async_sessionmaker[AsyncSession],
    bot: Bot
) -> None:
    admin_notify_text: Optional[str] = None
    media_paths_to_clean: List[str] = []

    # 🟢 دریافت پروفایل سرعت از دیتابیس برای تزریق به موتور رزولور
    speed_profile = None
    try:
        async with session_maker() as session:
            order = await session.scalar(select(Order).where(Order.id == order_id))
            speed_mode = order.speed_mode if order else "safe"
        from utils.speed_profile import get_speed_profile
        speed_profile = await get_speed_profile(speed_mode)
    except Exception:
        pass
    if not speed_profile:
        from utils.speed_profile import SAFE_PROFILE
        speed_profile = SAFE_PROFILE

    # Phase 5 — shared live-progress reporter (send-order link-resolution phase).
    reporter = await _get_or_start_reporter(
        bot=bot,
        session_maker=session_maker,
        order_id=order_id,
        task_type="order",
        target_hint=group_link or "لینک‌های گروه",
    )
    # Deferred owner-report action: ("update", persian_status) | ("fail", html)
    reporter_action: Optional[tuple] = None

    if reporter is not None:
        await reporter.update(
            status="در حال استخراج اعضای گروه‌ها برای ارسال…",
            account=f"user_{account_db_id}/",
        )

    try:
        links = _parse_target_links(group_link)

        aggregated_members: List[str] = []
        seen_members: set = set()
        failed_links: List[str] = []
        pending_link: Optional[str] = None
        pending_limit: Optional[str] = None  # 🟢 متغیر جدید برای ذخیره موقت محدودیت‌ها
        used_messages_fallback = False

        status_code, members, join_chat_id, joined_now = "error", None, None, False

        for link_index, link in enumerate(links, start=1):
            if reporter is not None:
                await reporter.update(
                    status=(
                        f"در حال استخراج اعضای گروه برای ارسال "
                        f"({link_index} از {len(links)})…"
                    ),
                    account=f"user_{account_db_id}/",
                )

            link_progress_cb = None
            if reporter is not None:
                async def _link_cb(count: int, scanned: int = 0) -> bool:
                    try:
                        from workers.sender import is_order_killed
                        if await is_order_killed(order_id):
                            return False
                    except Exception:
                        pass
                    await reporter.update(
                        done=int(count),
                        status=(
                            f"در حال استخراج اعضای گروه برای ارسال "
                            f"({link_index} از {len(links)})…"
                        ),
                        account=f"user_{account_db_id}/",
                    )
                    return True
                link_progress_cb = _link_cb

            # 🟢 اضافه‌شدن حلقه سوییچ اتمیک پروکسی فقط برای همین لینک
            link_retries = 0
            max_link_retries = 2
            
            while link_retries <= max_link_retries:
                try:
                    chat_id_hint = None
                    try:
                        async with session_maker() as temp_session:
                            row = await temp_session.scalar(select(OrderJoin.chat_id).where(OrderJoin.order_id == order_id, OrderJoin.account_id == account_db_id, OrderJoin.group_link == link))
                            if row: chat_id_hint = row
                    except Exception: pass

                    # 🟢 تزریق speed_profile در فراخوانی استخراج اعضا
                    status_code, members, join_chat_id, joined_now = await extract_members_for_sending(
                        client, link, filter_type=filter_type, progress_cb=link_progress_cb,
                        chat_id_hint=chat_id_hint, 
                        order_id=order_id,
                        speed_profile=speed_profile
                    )
                    
                    if status_code == "members_hidden" and config.EXTRACT_AUTO_FALLBACK_TO_MESSAGES:
                        logger.info(f"Order #{order_id}: Members hidden, auto-fallback to messages.")
                        if reporter is not None:
                            await reporter.update(
                                status=f"⚠️ لیست مخفی بود. تغییر استراتژی به «فرستندگان پیام» ({link_index} از {len(links)})…",
                                account=f"user_{account_db_id}/"
                            )
                        # 🟢 تزریق speed_profile در مسیر fallback پیام‌ها
                        fb_status, fb_file, _, _, _ = await extract_active_users(
                            client, link, filter_type="messages", progress_cb=None, speed_profile=speed_profile
                        )
                        
                        # 🟢 رفع باگ قبلی: پاس دادن ارور پروکسی در مسیر Fallback جهت فعال‌شدن سوییچ
                        if fb_status == "proxy_connection_error":
                            status_code = "proxy_connection_error"
                        elif fb_status in ["success", "partial_success"] and fb_file and os.path.exists(fb_file):
                            try:
                                async with aiofiles.open(fb_file, mode="r", encoding="utf-8") as f:
                                    lines = await f.readlines()
                                members = [l.strip() for l in lines if l.strip()]
                                if members:
                                    status_code = "success"
                                    used_messages_fallback = True
                                else:
                                    status_code = "empty_fallback_result"
                            except Exception as e:
                                logger.error(f"Error reading fallback file: {e}")
                            finally:
                                os.remove(fb_file)
                        else:
                            status_code = "empty_fallback_result"

                    # 🟢 مسیر سوییچ پروکسی (Failover در فاز Resolve)
                    if status_code == "proxy_connection_error":
                        logger.warning(f"Order #{order_id}: Proxy error on resolve for {link}. Switching proxy...")
                        
                        async with session_maker() as session:
                            from workers.session_manager import switch_worker_proxy, worker_pool
                            switch_ok = await switch_worker_proxy(
                                account_db_id, session, "Reactive: Proxy DEAD (Resolve)", ignore_cooldown=True
                            )
                        
                        if switch_ok:
                            link_retries += 1
                            client = worker_pool[account_db_id]  # آپدیت رفرنس کلاینت
                            if reporter is not None:
                                await reporter.update(
                                    status=f"پروکسی قطع شد؛ سوییچ موفقیت‌آمیز بود. تلاش مجدد ({link_retries}/{max_link_retries})…",
                                    account=f"user_{account_db_id}/"
                                )
                            continue
                        else:
                            status_code = "on_hold_proxy"
                            break  # خروج از حلقه Retry یک لینک
                            
                    break  # خروج از حلقه در صورت موفقیت یا خطای منطقی

                except Exception as e:
                    logger.error(f"Link resolution crashed for {link}: {e}", exc_info=True)
                    status_code, members, join_chat_id, joined_now = "error", None, None, False
                    break

            # 🟢 اتمام پروکسی‌های سالم <- خروج از حلقه کل لینک‌ها
            if status_code == "on_hold_proxy":
                break

            try:
                async with session_maker() as j_session:
                    async with j_session.begin():
                        await _record_order_join(
                            j_session, order_id, account_db_id, link,
                            join_chat_id, joined_now, status_code,
                        )
            except Exception as e:
                logger.warning(f"Failed to record order_join for Order #{order_id}: {e}")

            if status_code == "pending_approval":
                pending_link = link
                break
                
            if status_code.startswith("limit:"):
                pending_limit = status_code
                break

            if status_code == "success":
                if members:
                    for m in members:
                        if m not in seen_members:
                            seen_members.add(m)
                            aggregated_members.append(m)
                else:
                    logger.info(f"Order #{order_id}: link {link} resolved successfully but yielded 0 members with filter={filter_type}.")
            else:
                failed_links.append(link)
                logger.warning(f"Order #{order_id}: link {link} resolved with status={status_code}; link skipped.")

        # 🟢 هندلینگ دقیق On-Hold، کاملاً مطابق معماری دیسپچر شما
        if status_code == "on_hold_proxy":
            from workers.session_manager import direct_ip_fallback_enabled, direct_budget_ok
            fallback_successful = False
            async with session_maker() as session:
                if direct_ip_fallback_enabled(account_db_id) and await direct_budget_ok(session):
                    logger.warning(
                        f"Worker {account_db_id} ran out of proxy during link resolve. Fallback allowed. "
                        "Switching to direct connection."
                    )
                    try:
                        stmt = (
                            update(Account)
                            .where(Account.id == account_db_id)
                            .values(proxy_string=None, proxy_status="NO_PROXY")
                            .execution_options(synchronize_session=False)
                        )
                        await session.execute(stmt)
                        await session.commit()
                        
                        from workers.session_manager import build_worker_client, worker_pool
                        acc = await session.get(Account, account_db_id)
                        new_client = await build_worker_client(acc, session, None)
                        if new_client:
                            await asyncio.wait_for(new_client.start(), timeout=45)
                            worker_pool[account_db_id] = new_client
                            fallback_successful = True
                    except Exception as e:
                        logger.error(f"Fallback failed during link resolve for worker {account_db_id}: {e}")
                        await session.rollback()

            if fallback_successful:
                if reporter is not None:
                    await reporter.update(
                        status="پروکسی‌ها به پایان رسید؛ سیستم به IP سرور سوییچ کرد. در حال تلاش مجدد...",
                        account=f"user_{account_db_id}/"
                    )
                # ریست کردن وضعیت و تلاش مجدد برای لینک ناموفق در فاز resolve
                status_code = "error"
                # توجه: بهتر است این تابع از ابتدا برای این لینک با کلاینت جدید فراخوانی شود،
                # اما از آنجا که طراحی تابعی شما مبتنی بر for است، این بازگشت سریع به منظور 
                # عدم هولد شدن و اجازه به Order Dispatcher برای پخش مجدد تسک انجام می‌شود.
                async with session_maker() as session:
                    db_order = await session.scalar(select(Order).where(Order.id == order_id))
                    if db_order:
                        db_order.status = OrderStatus.pending
                        await session.commit()
                return

            async with session_maker() as session:
                db_order = await session.scalar(select(Order).where(Order.id == order_id))
                if db_order and not db_order.server_ip_consent:
                    db_order.status = OrderStatus.on_hold_proxy
                    db_order.hold_reason = "تمام شدن پروکسی‌های سالم در مرحله استخراج لینک و عدم دسترسی به فال‌بک مستقیم"
                    await session.commit()
                    
                    # فراخوانی مستقیم تابع درون فایلی برای آگاه‌سازی مشتری
                    await _send_hold_message(session, bot, order_id)
                    
            if reporter is not None:
                await reporter.update(
                    status="⏳ سفارش متوقف شد: پروکسی‌های سالم به پایان رسیده‌اند (در انتظار پروکسی جدید)..."
                )
            return

        if pending_link is not None:
            status_code = "pending_approval"
            members = None
        elif pending_limit is not None:
            status_code = pending_limit
            members = None
        elif aggregated_members:
            status_code = "success"
            members = aggregated_members

        async with session_maker() as session:
            async with session.begin():
                order_stmt = select(Order).where(Order.id == order_id)
                order = (await session.execute(order_stmt)).scalar_one_or_none()

                if not order:
                    logger.warning(f"Order #{order_id} was deleted during link resolution! Skipping.")
                    return

                if order.status != OrderStatus.running:
                    logger.warning(f"Order #{order_id} is no longer running (status={order.status}). Aborting link resolution.")
                    return

                display_code = order.tracking_code if order.tracking_code else f"ID-{order.id}"

                if status_code == "pending_approval":
                    order.retry_count = (order.retry_count or 0) + 1

                    if order.retry_count >= speed_profile.approval_retry_limit:
                        order.status = OrderStatus.error
                        order.scheduled_for = None
                        # ⚠️ target_data پاک نمی‌شود
                        media_paths_to_clean = [
                            p for p in (order.media_path, order.media_2_path, order.media_3_path)
                            if p and isinstance(p, str)
                        ]
                        order.media_path = None
                        order.media_2_path = None
                        order.media_3_path = None

                        session.add(OrderLog(
                            order_id=order.id,
                            account_id=account_db_id,
                            target=group_link,
                            status="error",
                            error_message=f"Pending approval retry limit reached ({speed_profile.approval_retry_limit} cycles).",
                        ))

                        admin_notify_text = (
                            f"⛔️ <b>توقف عملیات ارسال انبوه</b>\n\n"
                            f"سفارش: <code>{display_code}</code>\n"
                            f"گروه: <b>{html.escape(group_link)}</b>\n\n"
                            f"متأسفانه زمان مجاز سپری شد و با درخواست عضویت ربات موافقت نشد.\n"
                            f"<i>عملیات ارسال کاملاً متوقف و پرونده این سفارش بسته شد.</i>"
                        )
                        
                        reporter_action = (
                            "fail",
                            f"⛔️ <b>سفارش متوقف شد</b>\n\nدرخواست عضویت گروه خصوصی پس از "
                            f"{speed_profile.approval_retry_limit} چرخه هنوز تأیید نشد.",
                        )
                    else:
                        next_check = datetime.now(timezone.utc) + timedelta(seconds=speed_profile.approval_cycle_seconds)
                        order.scheduled_for = next_check
                        order.status = OrderStatus.pending
                        if order.retry_count == 1 or order.retry_count % 5 == 0:
                            worker_label_text = f"user_{account_db_id}/"
                            admin_notify_text = (
                                f"⏳ <b>درخواست عضویت ورکر ارسال شد! (ارسال انبوه)</b>\n\n"
                                f"سفارش: <code>{display_code}</code>\n"
                                f"گروه <b>{html.escape(group_link)}</b> خصوصی (ریکوئستی) است.\n"
                                f"ورکر: <b>{worker_label_text}</b>\n\n"
                                f"<i>ربات منتظر تایید ادمینِ گروه می‌ماند. لطفاً در گروه تأیید کنید.</i>\n\n"
                                f"🕐 تلاش فعلی: <b>{order.retry_count} از {speed_profile.approval_retry_limit}</b>"
                            )
                            await _notify_owner(bot, session_maker, order.id, admin_notify_text)
                        else:
                            admin_notify_text = None
                        
                        reporter_action = (
                            "update",
                            "در انتظار تأیید درخواست عضویت توسط ادمین گروه…",
                        )

                elif status_code.startswith("limit:"):
                    parts = status_code.split(":")
                    limit_type = parts[1] if len(parts) > 1 else "unknown"
                    wait_seconds = int(parts[2]) if len(parts) > 2 else 60

                    order.status = OrderStatus.pending
                    order.scheduled_for = datetime.now(timezone.utc) + timedelta(seconds=wait_seconds)
                    order.retry_count = (order.retry_count or 0) + 1
                    
                    try:
                        from utils.limit_handler import register_account_limit
                        await register_account_limit(session, account_db_id, client, limit_type, wait_seconds)
                    except Exception as e:
                        logger.error(f"Failed to register limit {limit_type} in link resolver: {e}")

                    reporter_action = (
                        "update",
                        f"⏳ در حالت استراحت/محافظت دفاعی ({limit_type}). از سرگیری عملیات تا {wait_seconds} ثانیه دیگر...",
                    )

                elif status_code != "success" or not members:
                    if not links:
                        reason = "هیچ لینک معتبری (t.me یا @username) در متن سفارش یافت نشد."
                    elif status_code == "success":
                        reason = "هیچ عضو قابل ارسالی با این فیلتر یافت نشد."
                    else:
                        reason = _get_resolver_error_message(status_code)

                    order.status = OrderStatus.error
                    # 🚫 دستور مخرب order.target_data = "" حذف شد تا لینک‌های ورودی حفظ شوند.
                    media_paths_to_clean = [
                        p for p in (order.media_path, order.media_2_path, order.media_3_path)
                        if p and isinstance(p, str)
                    ]
                    order.media_path = None
                    order.media_2_path = None
                    order.media_3_path = None

                    session.add(OrderLog(
                        order_id=order.id,
                        account_id=account_db_id,
                        target=group_link,
                        status="error",
                        error_message=f"Link resolution failed ({status_code}): {reason}",
                    ))

                    admin_notify_text = (
                        f"⚠️ <b>خطا در آماده‌سازی سفارش ارسال لینکی</b>\n\n"
                        f"سفارش: <code>{display_code}</code>\n"
                        f"تارگت: <b>{html.escape(group_link)}</b>\n\n"
                        f"<i>{reason} سفارش متوقف شد.</i>"
                    )
                    reporter_action = (
                        "fail",
                        f"⛔️ <b>آماده‌سازی سفارش ناموفق بود</b>\n\n{reason}",
                    )

                else:
                    cap = order.target_count or 0
                    if cap > 0 and len(members) > cap:
                        logger.info(f"Order #{order_id}: applying target_count cap ({len(members)} -> {cap} targets).")
                        members = members[:cap]

                    # 🟢 فقط در صورت استخراج موفقیت‌آمیز اعضا، target_data رونویسی می‌شود
                    order.target_data = "\n".join(members)
                    order.filter_type = None
                    order.status = OrderStatus.pending
                    order.scheduled_for = None
                    order.retry_count = 0  # 👈 اضافه شدن این خط برای ریست کردن تلاش‌ها و آزادسازی اکانت

                    logger.info(f"Order #{order_id}: link resolved to {len(members)} sendable targets. Re-queued for dispatch.")

                    failed_note = ""
                    if failed_links:
                        failed_note = (
                            f"\n⚠️ لینک‌های ناموفق ({len(failed_links)}): "
                            f"{html.escape(', '.join(failed_links))}\n"
                        )

                    fallback_msg = "\n⚠️ لیست اعضای این گروه مخفی بود. ربات به‌طور خودکار به استراتژی «فرستندگان پیام» سوییچ کرد.\n" if used_messages_fallback else ""

                    admin_notify_text = (
                        f"✅ <b>اعضای گروه استخراج شدند</b>\n\n"
                        f"سفارش: <code>{display_code}</code>\n"
                        f"گروه(ها): <b>{html.escape(group_link or '')}</b>\n"
                        f"تعداد تارگت‌های آماده ارسال: <b>{len(members)}</b>\n"
                        f"{failed_note}"
                        f"{fallback_msg}"
                        f"<i>سفارش به صف ارسال بازگشت؛ ارسال انبوه به‌زودی آغاز می‌شود (داشبورد زنده فعال شد).</i>"
                    )
                    reporter_action = (
                        "update",
                        f"اعضای گروه استخراج شدند ({len(members)} تارگت) — در انتظار شروع ارسال انبوه…",
                    )

        if reporter is not None and reporter_action is not None:
            action, payload = reporter_action
            if action == "fail":
                await reporter.fail(payload)
                _pop_progress_reporter("order", order_id)
            else:
                await reporter.update(status=payload)

        if admin_notify_text:
            await broadcast_to_admins(bot, admin_notify_text)

        for path in media_paths_to_clean:
            if os.path.exists(path):
                try:
                    os.remove(path)
                    logger.info(f"Garbage Collection: Deleted media {path} for failed link Order #{order_id}.")
                except Exception:
                    pass

    except Exception as e:
        logger.error(f"Fatal error in link_send_resolver_wrapper (Order #{order_id}): {e}", exc_info=True)
        try:
            async with session_maker() as session:
                async with session.begin():
                    await session.execute(
                        update(Order)
                        .where(Order.id == order_id, Order.status == OrderStatus.running)
                        .values(status=OrderStatus.error)  # 🚫 دستور target_data="" حذف شد
                    )
            if reporter is not None:
                await reporter.fail(
                    "⛔️ <b>خطای بحرانی در آماده‌سازی سفارش ارسال لینکی</b>\n\n"
                    "سفارش متوقف شد. جزئیات در لاگ سرور."
                )
                _pop_progress_reporter("order", order_id)
            await broadcast_to_admins(
                bot,
                text=(
                    f"⛔️ <b>خطای بحرانی در آماده‌سازی سفارش ارسال لینکی</b>\n\n"
                    f"سفارش: <code>#{order_id}</code>\nتارگت: <b>{html.escape(group_link)}</b>\n\n"
                    f"<i>سفارش متوقف شد. جزئیات در لاگ سرور.</i>"
                )
            )
        except Exception as db_err:
            logger.error(f"Failed to mark link Order #{order_id} as error after fatal failure: {db_err}")
    finally:
        await _release_busy(account_db_id)


async def worker_task_wrapper(
    client: Client, 
    account_db_id: int, 
    order: Order, 
    targets: list[str], 
    session_maker: async_sessionmaker[AsyncSession],
    bot: Bot
) -> tuple[List[str], Optional[str]]:
    """اجرای ایزوله تسک ارسال انبوه با بازگردانی تارگت‌های ناموفق و علت توقف."""
    reporter = await _get_or_start_reporter(
        bot=bot,
        session_maker=session_maker,
        order_id=order.id,
        task_type="order",
        target_hint=f"{order.target_count or len(targets)} تارگت",
    )
    if reporter is not None:
        await reporter.update(
            status=f"شروع ارسال دسته‌ای از {len(targets)} تارگت…",
            total=len(targets),
            account=f"user_{account_db_id}/",
        )

    async with session_maker() as session:
        effective_order = order
        if order.use_banner_pool:
            try:
                active_banner_ids = (await session.scalars(
                    select(Banner.id).where(Banner.is_active == True)
                )).all()

                banner = None
                if active_banner_ids:
                    chosen_id = random.choice(active_banner_ids)
                    banner = await session.scalar(select(Banner).where(Banner.id == chosen_id))

                if banner is not None:
                    await session.execute(
                        update(Banner).where(Banner.id == banner.id).values(usage_count=Banner.usage_count + 1)
                    )
                    await session.commit()
                    effective_order = _build_chunk_order(order, banner)
                else:
                    if order.id not in _banner_warned_orders:
                        _banner_warned_orders.add(order.id)
                        await broadcast_to_admins(bot, text=f"⚠️ <b>مخزن بنر خالی است!</b>\nسفارش <code>{order.tracking_code or order.id}</code> با متن خودش ارسال خواهد شد.")
            except Exception as e:
                await session.rollback()
                logger.error(f"Banner pool selection failed for Order #{order.id}. ({e})")

        try:
            unsent_targets, stop_reason = await execute_bulk_send(
                client=client,
                account_db_id=account_db_id,
                order=effective_order,
                targets=targets,
                session=session,
                progress_reporter=reporter,
            )
        except Exception as e:
            logger.error(f"Worker user_{account_db_id}/ crashed mid-chunk for Order #{order.id}: {e}", exc_info=True)
            if reporter is not None:
                await reporter.update(status=f"خطای بحرانی در ورکر user_{account_db_id}/ — بازیابی تارگت‌ها…")
            unsent_targets = await _salvage_unsent_after_crash(session, order.id, account_db_id, targets)
            stop_reason = "crash"

        finally:
            # اعمال کول‌داون فقط در صورتی که حداقل یک پیام ارسال شده باشد
            if unsent_targets is None or len(unsent_targets) < len(targets):
                cooldown_hours = 24 
                try:
                    settings_row = await session.scalar(select(GlobalSettings).limit(1))
                    if settings_row and settings_row.cooldown_hours:
                        cooldown_hours = settings_row.cooldown_hours
                except Exception: pass
                
                await mark_chunk_cooldown(account_db_id, cooldown_hours)
                if reporter is not None:
                    await reporter.update(
                        status=f"اکانت user_{account_db_id}/ وارد استراحت دوره‌ای شد ({cooldown_hours} ساعت)."
                    )

        return unsent_targets, stop_reason

async def background_order_execution(
    client: Union[Client, List[Client]],
    account_db_id: Union[int, List[int]],
    order_id: int,
    targets: List[str],
    session_maker: async_sessionmaker[AsyncSession],
    bot: Bot,
    total_estimate: Optional[int] = None,
) -> None:
    """اجرای پس‌زمینه با پشتیبانی یکپارچه سوییچ خودکار (Reactive) و چرخش پیشگیرانه (Proactive)."""
    if isinstance(client, Client): clients = [client]
    else: clients = list(client or [])
    
    if isinstance(account_db_id, int): account_ids = [account_db_id]
    else: account_ids = list(account_db_id or [])
    
    primary_id = account_ids[0] if account_ids else 0
    from workers.session_manager import switch_worker_proxy, worker_pool

    try:
        unsent_targets: Optional[List[str]] = None
        stop_reason: Optional[str] = None
        proxy_retries = 0
        max_proxy_retries = 3
        
        while True:
            async with session_maker() as session:
                order = await session.scalar(select(Order).where(Order.id == order_id))

            if order is None:
                logger.warning(f"Background execution: Order #{order_id} deleted.")
                break
                
            if _is_extract_order(order):
                group_link = targets[0] if targets else ""
                wrapper_res = await extractor_task_wrapper(
                    clients, account_ids, order, group_link, session_maker, bot, total_estimate=total_estimate,
                )
                # 🟢 پردازش خروجی Tuple که در آپدیت جدید اضافه شد
                if isinstance(wrapper_res, tuple):
                    unsent_targets, stop_reason = wrapper_res
                else:
                    unsent_targets, stop_reason = wrapper_res, None
            else:
                unsent_targets, stop_reason = await worker_task_wrapper(
                    clients[0], primary_id, order, targets, session_maker, bot
                )
                unsent_targets = unsent_targets or []

            # 🔴 سوییچ واکنشی (Reactive Failover - فاز ۴ اصلاح‌شده)
            if stop_reason == "proxy_connection_error" and unsent_targets:
                proxy_retries += 1
                if proxy_retries > max_proxy_retries:
                    logger.error(f"Order #{order_id}: Max proxy retries ({max_proxy_retries}) reached. Halting.")
                    async with session_maker() as session:
                        db_order = await session.get(Order, order_id)
                        if db_order:
                            db_order.status = OrderStatus.error
                            db_order.reject_reason = "پروکسی سالم یافت نشد"
                            db_order.scheduled_for = None
                            session.add(OrderLog(order_id=order_id, target="System", status="error", error_message="Max proxy retries reached."))
                            await session.commit()
                    task_type = "extract" if order and _is_extract_order(order) else "order"
                    if reporter := _progress_reporters.get(f"{task_type}:{order_id}"):
                        await reporter.fail("⛔️ <b>توقف سفارش</b>\n\nپروکسی سالم یافت نشد و سقف تلاش مجدد به پایان رسید.")
                        _pop_progress_reporter(task_type, order_id)
                    break

                async with session_maker() as session:
                    # 🛡 رفع باگ: بررسی اینکه آیا اکانت واقعاً در حال استفاده از پروکسی بوده یا دایرکت است
                    acc = await session.get(Account, primary_id)
                    if not acc or not acc.proxy_string:
                        # اتصال دایرکت بوده و خطای شبکه خورده است، نباید پروکسی اختصاص دهیم!
                        switch_ok = False
                        stop_reason = "direct_network_error"
                    else:
                        # سوییچ روی همان ورکر، نادیده‌گرفتن Cooldown برای خروج از وضعیت اضطراری
                        switch_ok = await switch_worker_proxy(primary_id, session, "Reactive: Proxy DEAD (Failover)", ignore_cooldown=True)
                
                if switch_ok:
                    targets = unsent_targets
                    # 🟢 رفع باگ ریپورتر: تشخیص نوع تسک برای نمایش آپدیت زنده
                    task_type = "extract" if _is_extract_order(order) else "order"
                    reporter = _progress_reporters.get(f"{task_type}:{order_id}")
                    if reporter:
                        await reporter.update(status="پروکسی قطع شد؛ سوییچ موفقیت‌آمیز بود و عملیات ادامه دارد...", account=f"user_{primary_id}/")
                    
                    clients[0] = worker_pool[primary_id]
                    continue  # اجرای مجدد چانک باقی‌مانده با همان ورکر و پروکسی جدید
                else:
                    # تلاش برای فال‌بک به سرور آی‌پی پیش از هولد کردن
                    from workers.session_manager import direct_ip_fallback_enabled, direct_budget_ok
                    fallback_successful = False
                    async with session_maker() as session:
                        if direct_ip_fallback_enabled(primary_id) and await direct_budget_ok(session):
                            logger.warning(
                                f"Worker {primary_id}: Proxy depleted but fallback is allowed. "
                                "Switching to direct connection."
                            )
                            try:
                                stmt = (
                                    update(Account)
                                    .where(Account.id == primary_id)
                                    .values(proxy_string=None, proxy_status="NO_PROXY")
                                    .execution_options(synchronize_session=False)
                                )
                                await session.execute(stmt)
                                await session.commit()
                                
                                # بازسازی کلاینت بدون پروکسی
                                from workers.session_manager import build_worker_client, worker_pool
                                acc = await session.get(Account, primary_id)
                                new_client = await build_worker_client(acc, session, None)
                                if new_client:
                                    await asyncio.wait_for(new_client.start(), timeout=45)
                                    worker_pool[primary_id] = new_client
                                    fallback_successful = True
                            except Exception as e:
                                logger.error(f"Failed to reset proxy state and fallback for worker {primary_id}: {e}")
                                await session.rollback()

                    if fallback_successful:
                        targets = unsent_targets
                        task_type = "extract" if _is_extract_order(order) else "order"
                        reporter = _progress_reporters.get(f"{task_type}:{order_id}")
                        if reporter:
                            await reporter.update(
                                status="پروکسی‌ها به پایان رسید؛ سوییچ به IP سرور موفقیت‌آمیز بود و عملیات ادامه دارد...",
                                account=f"user_{primary_id}/"
                            )
                        clients[0] = worker_pool[primary_id]
                        continue
                    else:
                        # هولد: هیچ پروکسی سالمی باقی نمانده و فال‌بک مجاز نیست/ظرفیت ندارد
                        async with session_maker() as session:
                            db_order = await session.scalar(select(Order).where(Order.id == order_id))
                            if db_order and not db_order.server_ip_consent:
                                db_order.status = OrderStatus.on_hold_proxy
                                db_order.hold_reason = "تمام شدن پروکسی‌های سالم و عدم دسترسی به فال‌بک مستقیم"
                                await session.commit()
                                
                                from workers.task_queue import _send_hold_message
                                await _send_hold_message(session, bot, order_id)
                                
                                order.status = OrderStatus.on_hold_proxy
                        break

            # 🔴 سوییچ فوری روی لیمیت ورکر (Immediate Limit Failover)
            limit_reasons = (
                "flood_wait", "blocked", "banned", "consecutive_errors", 
                "chunk_limit", "hourly_cap", "daily_cap", "error", 
                "proxy_connection_error", "account_limited", 
                "account_banned", "account_inactive"
            )
            if not _is_extract_order(order) and stop_reason in limit_reasons and unsent_targets:
                new_worker_found = False
                async with session_maker() as session:
                    now_naive = datetime.now(timezone.utc).replace(tzinfo=None)
                    now_utc = datetime.now(timezone.utc)
                    order_cat_ids = list((await session.scalars(select(order_category_assoc.c.category_id).where(order_category_assoc.c.order_id == order_id))).all())
                    
                    from workers.session_manager import worker_pool
                    connected_ids = [aid for aid, c in list(worker_pool.items()) if getattr(c, "is_connected", False)]
                    if connected_ids:
                        from database.models import AccountStatus
                        acc_filters = [
                            Account.id.in_(connected_ids),
                            Account.status == AccountStatus.active,
                            Account.is_banned == False,
                            or_(Account.flood_wait_until.is_(None), Account.flood_wait_until <= now_naive),
                            or_(Account.restricted_until.is_(None), Account.restricted_until <= now_naive)
                        ]
                        if order_cat_ids:
                            acc_filters.append(Account.category_id.in_(order_cat_ids))
                            
                        candidate_accs = (await session.scalars(select(Account).where(*acc_filters))).all()
                        redis = _get_redis()
                        
                        for acc in candidate_accs:
                            if acc.id == primary_id: continue
                            if not await _is_warmed_up(session, acc, now_utc): continue
                            if await redis.exists(f"busy_worker:{acc.id}") or await redis.exists(f"chunk_cooldown:{acc.id}"): continue
                            if int(await redis.get(_daily_key(acc.id)) or 0) >= effective_daily_limit(acc.created_at): continue
                            
                            if await _acquire_busy(acc.id):
                                await redis.set(f"last_dispatched:{acc.id}", time.time(), ex=172800)
                                await _release_busy(primary_id)
                                primary_id = acc.id
                                account_ids[0] = acc.id
                                clients[0] = worker_pool[acc.id]
                                targets = unsent_targets
                                new_worker_found = True
                                
                                task_type = "order"
                                reporter = _progress_reporters.get(f"{task_type}:{order_id}")
                                if reporter:
                                    await reporter.update(status="سوییچ فوری به ورکر جدید پس از برخورد به لیمیت...", account=f"user_{primary_id}/")
                                break
                
                if new_worker_found:
                    continue
                else:
                    # 🟢 بازنویسی منطق: پایان فوری سفارش در صورت اتمام ورکرهای آماده و ارسال فایل خروجی برای هر دلیلی
                    async with session_maker() as session:
                        db_order = await session.get(Order, order_id)
                        if db_order:
                            success_count = await session.scalar(
                                select(func.count())
                                .select_from(OrderLog)
                                .where(OrderLog.order_id == order_id, OrderLog.status == "success")
                            ) or 0
                            
                            finish_msg = f"سفارش تا این مرحله انجام شد (ارسال موفق: {success_count}). به دلیل عدم وجود ورکر آماده جایگزین (لیمیت/استراحت سایر اکانت‌ها)، سفارش پایان یافت."
                            
                            final_status = OrderStatus.completed if success_count > 0 else OrderStatus.error
                            db_order.status = final_status
                            db_order.scheduled_for = None
                            db_order.reject_reason = "اتمام ورکرهای آماده و در دسترس"
                            
                            session.add(OrderLog(order_id=order_id, target="System", status=final_status, error_message=finish_msg))
                            await session.commit()
                            
                            # ارسال اتوماتیک فایل خروجی برای کارفرما
                            if not _is_extract_order(db_order):
                                _spawn_background_task(_send_export_file(session_maker, bot, order_id, finish_msg))
                                
                            # بروزرسانی داشبورد لایو کاربر
                            task_type = "extract" if _is_extract_order(db_order) else "order"
                            if reporter := _progress_reporters.get(f"{task_type}:{order_id}"):
                                if final_status == OrderStatus.completed:
                                    await reporter.finish(f"✅ <b>پایان سفارش</b>\n\n{finish_msg}")
                                else:
                                    await reporter.fail(f"⛔️ <b>لغو سفارش</b>\n\n{finish_msg}")
                                _pop_progress_reporter(task_type, order_id)
                                
                            # اطلاع‌رسانی به ادمین سیستم
                            from utils.admin_broadcast import broadcast_to_admins
                            await broadcast_to_admins(bot, text=f"🏁 <b>پایان سفارش (کمبود ورکر)</b>\n\nسفارش: <code>{order_id}</code>\n{finish_msg}")
                    break

            elif stop_reason == "media_missing":
                async with session_maker() as session:
                    db_order = await session.get(Order, order_id)
                    if db_order:
                        db_order.status = OrderStatus.error
                        db_order.scheduled_for = None
                        session.add(OrderLog(
                            order_id=db_order.id, 
                            target="System", 
                            status="error",
                            error_message="فایل مدیای سفارش یافت نشد (احتمالاً پاک شده) — سفارش قابل اجرا نیست"
                        ))
                        await session.commit()
                        unsent_targets = []
                        
                reporter = _progress_reporters.get(f"order:{order_id}")
                if reporter:
                    await reporter.fail("⛔️ <b>توقف سفارش</b>\n\nفایل مدیای سفارش یافت نشد (احتمالاً پاک شده است). سفارش قابل اجرا نیست.")
                
                try:
                    from utils.admin_broadcast import broadcast_to_admins
                    await broadcast_to_admins(bot, text=f"⛔️ <b>توقف سفارش</b>\n\nسفارش <code>{order_id}</code> به دلیل یافت نشدن فایل مدیا در دیسک متوقف شد.")
                except Exception:
                    pass
                break

            # 🔴 خطاهای سخت استخراج (مثل ادمین نبودن در کانال یا ریجکت شدن ریکوئست)
            elif _is_extract_order(order) and stop_reason in ("error", "error_not_admin", "rejected"):
                async with session_maker() as session:
                    db_order = await session.get(Order, order_id)
                    if db_order:
                        remaining_in_db = [t for t in (db_order.target_data or "").split("\n") if t.strip()]
                        # اگر تارگت دیگری در صف نیست و هیچ دیتایی تا الان استخراج نشده
                        if not remaining_in_db and not db_order.extracted_count:
                            db_order.status = OrderStatus.error
                            db_order.reject_reason = stop_reason
                            db_order.scheduled_for = None
                            await session.commit()
                            unsent_targets = []  # جلوگیری از بازگشت به صف برای این لینک
                break 

            else:
                # 🟢 چرخش پیشگیرانه (Proactive Rotation - فاز ۵) بین چانک‌ها
                if stop_reason not in ("proxy_connection_error", "crash", "blocked", "banned") and not _is_extract_order(order):
                    try:
                        redis = _get_redis()
                        chunks_count = await redis.incr(f"worker_chunks:{primary_id}")
                        if chunks_count == 1:
                            await redis.expire(f"worker_chunks:{primary_id}", 86400)
                        last_rot_str = await redis.get(f"worker_last_rot:{primary_id}")
                        if not last_rot_str:
                            await redis.set(f"worker_last_rot:{primary_id}", time.time(), ex=86400)
                        last_rot = float(last_rot_str) if last_rot_str else time.time()
                        mins_elapsed = (time.time() - last_rot) / 60.0
                        
                        rot_reason = None
                        if chunks_count >= int(getattr(config, "ROTATE_AFTER_CHUNKS", 5)):
                            rot_reason = f"reached {chunks_count} chunks"
                        elif mins_elapsed >= int(getattr(config, "ROTATE_AFTER_MINUTES", 60)):
                            rot_reason = f"reached {int(mins_elapsed)} mins"
                        else:
                            async with session_maker() as session:
                                db_acc = await session.get(Account, primary_id)
                                errors = db_acc.consecutive_errors if db_acc else 0
                                if errors >= int(getattr(config, "ROTATE_AFTER_CONSECUTIVE_ERRORS", 3)):
                                    rot_reason = f"reached {errors} consecutive errors"

                        if rot_reason:
                            async with session_maker() as session:
                                await switch_worker_proxy(primary_id, session, f"Proactive: {rot_reason}", ignore_cooldown=False)
                    except Exception as e:
                        logger.error(f"Proactive rotation check failed for user_{primary_id}/: {e}")
                
                break # خروج از حلقه

        remaining_inflight = await _decrement_inflight_chunks(order_id)
        
        await _finalize_chunk(
            order_id=order_id,
            unsent_targets=unsent_targets or [],
            remaining_inflight=remaining_inflight,
            session_maker=session_maker,
            bot=bot,
            chunk_targets=targets,
        )
    except Exception as e:
        logger.error(f"Critical error in background_order_execution for Order #{order_id}: {e}", exc_info=True)
        try:
            async with session_maker() as session:
                await session.execute(
                    update(Order).where(Order.id == order_id).values(
                        status=OrderStatus.error, 
                        scheduled_for=None, 
                        reject_reason="خطای داخلی اجرا"
                    )
                )
                session.add(OrderLog(
                    order_id=order_id, 
                    target="System", 
                    status="error",
                    error_message=f"Internal execution error: {e}"
                ))
                await session.commit()
            task_type = "extract" if order and _is_extract_order(order) else "order"
            if reporter := _progress_reporters.get(f"{task_type}:{order_id}"):
                await reporter.fail(f"⛔️ <b>توقف سفارش</b>\n\nخطای داخلی در اجرای پردازش رخ داد.")
                _pop_progress_reporter(task_type, order_id)
        except Exception as fallback_err:
            logger.error(f"Failed to set error status for Order #{order_id} in exception handler: {fallback_err}")
    finally:
        for acc_id in account_ids:
            await _release_busy(acc_id)
    

# ==========================================
# 🛡 فاز ۲ (BUG-04): شمارنده‌ی chunkهای در جریان به‌ازای هر سفارش
# ==========================================
# تعیین «آخرین chunk» برای فاینالایز (completed/pending) بدون فیلد اضافه در DB.
# معماری تک-پروسه است (همان رجیستری busy)؛ سفارش runningِ جامانده از کرش در
# بوتِ بعدی توسط reset_zombie_orders (main.py) به pending برمی‌گردد.
_order_inflight_chunks: Dict[int, int] = {}
_order_inflight_lock = asyncio.Lock()


async def _register_inflight_chunks(order_id: int, count: int) -> None:
    """ثبت تعداد chunkهایی که برای این سفارش در حال اجرا گذاشته می‌شوند."""
    async with _order_inflight_lock:
        _order_inflight_chunks[order_id] = _order_inflight_chunks.get(order_id, 0) + count


async def _decrement_inflight_chunks(order_id: int) -> int:
    """
    کاهش شمارنده — باید «دقیقاً یک‌بار» در هر مسیر خروجِ chunk صدا زده شود.
    خروجی: تعداد chunkهایی که هنوز در جریان‌اند (>= 0).
    """
    async with _order_inflight_lock:
        current = _order_inflight_chunks.get(order_id, 1)
        remaining = current - 1
        if remaining > 0:
            _order_inflight_chunks[order_id] = remaining
        else:
            _order_inflight_chunks.pop(order_id, None)
        return max(0, remaining)


# 🛡 قفل per-order برای فاینالایز: جلوگیری از race هم‌زمانِ requeue دو chunk
# (خواندن/نوشتن هم‌زمان order.target_data — آخرین‌نویسنده برنده می‌شد)
_order_finalize_locks: Dict[int, asyncio.Lock] = {}
_order_finalize_locks_guard = asyncio.Lock()


class DistributedFinalizeLock:
    def __init__(self, order_id: int):
        self.order_id = order_id
        self.lock_key = f"finalize_lock:{order_id}"
        self.acquired = False

    async def __aenter__(self):
        redis = _get_redis()
        # تلاش برای دریافت قفل (Spinlock با سقف ۶۰ ثانیه صبر)
        for _ in range(120):
            # قفل با انقضای ۳۰ ثانیه‌ای (Self-healing در صورت کرش ناگهانی سرور)
            self.acquired = await redis.set(self.lock_key, "1", nx=True, ex=30)
            if self.acquired:
                break
            await asyncio.sleep(0.5)
        
        if not self.acquired:
            logger.warning(f"Timeout acquiring Redis finalize lock for Order #{self.order_id}. Proceeding anyway...")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # آزادسازی هوشمند قفل پس از خروج از بلوک
        if self.acquired:
            try:
                redis = _get_redis()
                await redis.delete(self.lock_key)
            except Exception as e:
                logger.error(f"Failed to release Redis lock {self.lock_key}: {e}")
    
def _is_extract_order(order: Order) -> bool:
    """
    تشخیص سفارش استخراج — سازگار با هر دو شکل مقدار order_type در مدل
    (literal رشته‌ای «extract» یا Enum با value مشابه).
    """
    return "extract" in str(getattr(order, "order_type", "") or "").lower()


# ⏱ فاصله‌ی polling دیسپچر — تنظیم‌شده از کانفیگ
DISPATCH_INTERVAL_SECONDS = max(1.0, float(getattr(config, "DISPATCH_INTERVAL_SECONDS", 2.0)))

# 🛡 نگه‌داری مرجع تسک‌های پس‌زمینه (الگوی مستند asyncio: نتیجه‌ی create_task
# باید مرجع قوی داشته باشد تا تسک در حین اجرا GC نشود)
_running_background_tasks: set = set()


def _spawn_background_task(coro) -> asyncio.Task:
    """create_task امن: مرجع در مجموعه نگه داشته می‌شود و بعد از done آزاد می‌گردد."""
    task = asyncio.create_task(coro)
    _running_background_tasks.add(task)
    task.add_done_callback(_running_background_tasks.discard)
    return task


# ==========================================
# 📊 Phase 5 — live progress reporting for the order owner
# ==========================================
# One ProgressReporter per (task_type, order): concurrent chunks share the same
# instance (and therefore the same Telegram message); a restarted process
# re-attaches to the existing message via Redis (progress:msg:{type}:{id}).
_progress_reporters: Dict[str, ProgressReporter] = {}
_progress_reporters_lock = asyncio.Lock()


async def _resolve_progress_owner_chat_id(
    session_maker: async_sessionmaker[AsyncSession],
    order_id: int,
) -> Union[int, List[int], None]:
    """
    Phase 5/New — Resolve owner chat id from Order.user_id.
    Fallback to all admins for old orders (or if user_id is empty).
    """
    owner_tg_id = None
    try:
        async with session_maker() as session:
            order = await session.scalar(select(Order).where(Order.id == order_id))
            if order and order.user_id:
                owner_tg_id = order.user_id
    except Exception as e:
        logger.warning(f"Failed to fetch order owner for #{order_id}: {e}")

    # Fallback to all admins
    if not owner_tg_id:
        admin_ids = []
        if getattr(config, "ADMIN_ID", None) and int(config.ADMIN_ID) != 0:
            admin_ids.append(int(config.ADMIN_ID))
        
        try:
            async with session_maker() as session:
                sub_admins = (await session.scalars(select(Admin.telegram_id))).all()
                admin_ids.extend(int(aid) for aid in sub_admins)
        except Exception as e:
            logger.warning(f"ProgressReporter: sub-admin lookup failed: {e}")
            
        return list(set(admin_ids)) if admin_ids else None
        
    return owner_tg_id



async def _get_or_start_reporter(
    bot: Bot,
    session_maker: async_sessionmaker[AsyncSession],
    order_id: int,
    task_type: str,
    target_hint: str,
) -> Optional[ProgressReporter]:
    """
    Phase 5 — get (or create + start) the shared live-progress reporter for a
    job. Title = tracking code + target hint. Fully guarded fire-and-forget;
    returns None when the feature flag or the owner opt-out says "no".
    """
    if not getattr(config, "PROGRESS_NOTIFY_ENABLED", True):
        return None

    key = f"{task_type}:{order_id}"
    try:
        async with _progress_reporters_lock:
            existing = _progress_reporters.get(key)
            if existing is not None:
                return existing

            owner_chat_id = await _resolve_progress_owner_chat_id(session_maker, order_id)
            if owner_chat_id is None:
                return None

            # 🟣 بررسی فلگ progress_notify کاربر صاحب سفارش جهت خفه‌کردن (Mute) آپدیت‌های زنده
            notify_enabled = True
            try:
                async with session_maker() as session:
                    if isinstance(owner_chat_id, int):
                        admin_row = await session.scalar(select(Admin).where(Admin.telegram_id == owner_chat_id))
                        if admin_row is not None and not admin_row.progress_notify:
                            notify_enabled = False
            except Exception as e:
                logger.warning(f"Failed to check progress_notify for {owner_chat_id}: {e}")

            tracking_code = None
            try:
                async with session_maker() as session:
                    tracking_code = await session.scalar(
                        select(Order.tracking_code).where(Order.id == order_id)
                    )
            except Exception as e:
                logger.warning(
                    f"ProgressReporter: tracking-code lookup failed for Order #{order_id}: {e}"
                )
            code = tracking_code if tracking_code else f"ID-{order_id}"

            reporter = ProgressReporter(
                bot=bot,
                chat_id=owner_chat_id,
                task_type=task_type,
                task_id=order_id,
                title=f"کد پیگیری {code} | {target_hint}",
            )
            
            if not notify_enabled:
                # 🟣 خفه‌کردن متدهای زنده؛ ارسال مستقل پیام/فایل نهایی بدون نیاز به Message ID قبلی
                async def silent_noop(*args, **kwargs): pass
                
                async def silent_finish(text: str, document_path: str = None, **kwargs):
                    try:
                        if document_path and os.path.exists(document_path):
                            from aiogram.types import FSInputFile
                            await bot.send_document(chat_id=owner_chat_id, document=FSInputFile(document_path), caption=text)
                        else:
                            await bot.send_message(chat_id=owner_chat_id, text=text)
                    except Exception as e:
                        logger.warning(f"Silent finish failed for Order #{order_id}: {e}")
                        
                async def silent_fail(text: str, **kwargs):
                    try:
                        await bot.send_message(chat_id=owner_chat_id, text=text)
                    except Exception as e:
                        logger.warning(f"Silent fail failed for Order #{order_id}: {e}")
                        
                reporter.start = silent_noop
                reporter.update = silent_noop
                reporter.finish = silent_finish
                reporter.fail = silent_fail

            await reporter.start()  # guarded internally — never raises
            _progress_reporters[key] = reporter
            return reporter
    except Exception as e:
        logger.warning(f"ProgressReporter: reporter creation failed for {key}: {e}")
        return None


def _pop_progress_reporter(task_type: str, task_id: int) -> Optional[ProgressReporter]:
    """Phase 5 — drop a finished reporter from the in-process registry."""
    return _progress_reporters.pop(f"{task_type}:{task_id}", None)


async def _estimate_member_count(client: Client, group_link: str) -> Optional[int]:
    """
    Phase 5 — best-effort member-count estimate for the progress bar.
    Purely cosmetic: any failure (private link, FloodWait, network) returns
    None and the reporter simply omits the percentage.
    """
    try:
        chat = await client.get_chat(group_link)
        if chat is None or chat.id is None:
            return None
        count = await client.get_chat_member_count(chat.id)
        return int(count) if count else None
    except Exception as e:
        logger.debug(f"ProgressReporter: member-count estimate failed for {group_link}: {e}")
        return None


async def _report_order_terminal(
    order_id: int,
    session_maker: async_sessionmaker[AsyncSession],
    fallback_success_count: int = 0,
) -> None:
    """
    Phase 5 — after the last chunk of a SEND order finalizes, report the
    terminal state (completed / partial stats / error / requeued) on the
    owner's live message. Runs as a background task and polls briefly so the
    finalize transaction commits first. Fully guarded fire-and-forget —
    never affects job semantics.
    """
    reporter = _progress_reporters.get(f"order:{order_id}")
    if reporter is None:
        return
    try:
        final_status = None
        success_count = fallback_success_count
        error_count = 0
        for _attempt in range(6):  # up to ~30s
            await asyncio.sleep(5.0)
            try:
                async with session_maker() as session:
                    order = await session.scalar(
                        select(Order).where(Order.id == order_id)
                    )
                    if order is None:
                        _pop_progress_reporter("order", order_id)
                        return
                    final_status = order.status
                    success_count = int(
                        await session.scalar(
                            select(func.count())
                            .select_from(OrderLog)
                            .where(OrderLog.order_id == order_id, OrderLog.status == "success")
                        )
                        or fallback_success_count
                    )
                    error_count = int(
                        await session.scalar(
                            select(func.count())
                            .select_from(OrderLog)
                            .where(OrderLog.order_id == order_id, OrderLog.status == "error")
                        )
                        or 0
                    )
            except Exception as e:
                logger.warning(f"ProgressReporter: terminal status read failed (Order #{order_id}): {e}")
                return
            if final_status != OrderStatus.running:
                break

        if final_status == OrderStatus.completed:
            # "Partial" is covered by showing successful vs failed counts.
            await reporter.finish(
                "✅ <b>ارسال انبوه تکمیل شد</b>\n\n"
                f"👤 پیام‌های ارسال‌شده با موفقیت: <b>{success_count}</b>\n"
                f"⚠️ تلاش‌های ناموفق: <b>{error_count}</b>"
            )
            _pop_progress_reporter("order", order_id)
        elif final_status == OrderStatus.error:
            await reporter.fail(
                "⛔️ <b>سفارش ارسال متوقف شد</b>\n\n"
                f"👤 پیام‌های ارسال‌شده با موفقیت تا این لحظه: <b>{success_count}</b>\n"
                f"⚠️ تلاش‌های ناموفق: <b>{error_count}</b>"
            )
            _pop_progress_reporter("order", order_id)
        else:
            # Still running (re-dispatched) or requeued: the live message stays
            # alive for the next chunk wave — only bump the status line.
            await reporter.update(
                status="در انتظار ادامهٔ ارسال (تارگت‌های باقی‌مانده در صف)…",
            )
    except Exception as e:
        logger.warning(f"ProgressReporter: terminal report failed for Order #{order_id}: {e}")

async def _send_export_file(session_maker: async_sessionmaker[AsyncSession], bot: Bot, order_id: int, reason_text: str):
    try:
        async with session_maker() as session:
            order = await session.scalar(select(Order).where(Order.id == order_id))
            if not order or _is_extract_order(order): return
            
            stmt = select(OrderLog.target).where(OrderLog.order_id == order_id, OrderLog.status == "success")
            logs = (await session.execute(stmt)).scalars().all()
            if not logs: return
            
            os.makedirs("exports", exist_ok=True)
            file_path = f"exports/order_{order_id}_results_{int(time.time())}.txt"
            async with aiofiles.open(file_path, "w", encoding="utf-8") as f:
                for t in logs:
                    await f.write(f"{t}\n")
                    
            document = FSInputFile(file_path)
            caption = f"📥 خروجی ارسال‌های موفق سفارش <code>{order.tracking_code or order_id}</code>\n{reason_text}"
            
            owner_notified = False
            if order.user_id:
                try:
                    await bot.send_document(chat_id=order.user_id, document=document, caption=caption)
                    owner_notified = True
                except Exception: pass
                    
            admin_ids = []
            if getattr(config, "ADMIN_ID", None): admin_ids.append(int(config.ADMIN_ID))
            sub_admins = (await session.scalars(select(Admin.telegram_id))).all()
            admin_ids.extend(int(aid) for aid in sub_admins)
            
            for admin_id in set(admin_ids):
                if owner_notified and admin_id == order.user_id: continue
                try:
                    await bot.send_document(chat_id=admin_id, document=document, caption=caption)
                except Exception: pass
                    
            os.remove(file_path)
    except Exception as e:
        logger.error(f"Failed to auto-send export file for order {order_id}: {e}")


async def _finalize_chunk(
    order_id: int,
    unsent_targets: List[str],
    remaining_inflight: int,
    session_maker: async_sessionmaker[AsyncSession],
    bot: Bot,
    chunk_targets: Optional[List[str]] = None,
) -> None:
    admin_notify_text: Optional[str] = None
    media_paths_to_clean: List[str] = []

    # 🟢 فاز ۵: استفاده مستقیم از کانتکست منیجر کلاس قفل توزیع‌شده (Stateless)
    try:
        async with DistributedFinalizeLock(order_id):
            try:
                async with session_maker() as session:
                    async with session.begin():
                        order = await session.scalar(
                            select(Order).where(Order.id == order_id)
                        )
                        if order is None:
                            logger.warning(
                                f"Finalize: Order #{order_id} was deleted - nothing to requeue."
                            )
                            return

                        # بازگرداندن تارگت‌های ناموفق/ارسال‌نشده به صف انتظار حتی در حالت هولد (Idempotent Deduplication)
                        if unsent_targets and order.status in (
                            OrderStatus.running, OrderStatus.pending, OrderStatus.on_hold_proxy
                        ):
                            current = [
                                t.strip() for t in (order.target_data or "").split("\n") if t.strip()
                            ]
                            
                            seen = set(current)
                            for t in unsent_targets:
                                clean_t = t.strip()
                                if clean_t and clean_t not in seen:
                                    current.append(clean_t)
                                    seen.add(clean_t)
                                        
                            order.target_data = "\n".join(current)

                        if order.status not in (OrderStatus.running, OrderStatus.on_hold_proxy):
                            return

                        # 🛡 فاز ۲ (B3): پاک کردن تارگت‌های پردازش‌شده‌ی این chunk از دفترکل در-جریان
                        if order.inflight_data and chunk_targets:
                            try:
                                inflight = json.loads(order.inflight_data)
                                chunk_set = set(chunk_targets)
                                new_inflight = [t for t in inflight if t not in chunk_set]
                                order.inflight_data = json.dumps(new_inflight) if new_inflight else None
                            except Exception as e:
                                logger.warning(
                                    f"Finalize: corrupt inflight ledger for Order #{order_id} cleared ({e})"
                                )
                                order.inflight_data = None

                        # اگر هنوز chunkهای دیگری برای این سفارش باز هستند، خارج می‌شویم
                        if remaining_inflight > 0:
                            return

                        leftover = [
                            t for t in (order.target_data or "").split("\n") if t.strip()
                        ]

                        # 🟢 فاز ۴: توقف فرآیند فاینالایز در صورتی که سفارش هولد شده است
                        if order.status == OrderStatus.on_hold_proxy:
                            return

                        # استخراج تعداد ارسال‌های موفق تا این لحظه جهت بررسی پیشرفت سفارش
                        success_count = 0
                        try:
                            success_count = (
                                await session.scalar(
                                    select(func.count())
                                    .select_from(OrderLog)
                                    .where(
                                        OrderLog.order_id == order_id,
                                        OrderLog.status == "success",
                                    )
                                )
                                or 0
                            )
                        except Exception as e:
                            logger.warning(f"Finalize: success-count query failed for Order #{order_id}: {e}")

                        _spawn_background_task(
                            _report_order_terminal(order_id, session_maker, success_count)
                        )

                        # --- سناریوی اول: سفارش همچنان تارگت دارد ---
                        if leftover:
                            order.status = OrderStatus.pending
                            sched = order.scheduled_for
                            
                            if sched is not None and sched.tzinfo is None:
                                sched = sched.replace(tzinfo=timezone.utc)
                                
                            # اگر تعویق قبلی (مانند عضویت گروه) نداشتیم یا گذشته بود:
                            if not (sched and sched > datetime.now(timezone.utc)):
                                order.scheduled_for = None
                                
                                # 🛡 فاز ۲ (B9): استراتژی Backoff برای سفارشات بدون پیشرفت
                                if success_count == 0:
                                    order.fail_streak = (order.fail_streak or 0) + 1
                                    
                                    if order.fail_streak >= 8:
                                        # توقف کامل پس از ۸ چرخه شکست متوالی
                                        order.status = OrderStatus.error
                                        order.scheduled_for = None
                                        # ⚠️ تغییر: target_data پاک نمی‌شود تا دیتا از دست نرود
                                        order.inflight_data = None
                                        
                                        session.add(OrderLog(
                                            order_id=order.id, 
                                            target="System", 
                                            status="error",
                                            error_message="Zero-progress retry limit reached (8 cycles)"
                                        ))
                                        display_code = (
                                            order.tracking_code if order.tracking_code else f"ID-{order.id}"
                                        )
                                        admin_notify_text = (
                                            f"⛔️ <b>توقف سفارش به دلیل شکست‌های پیاپی</b>\n\n"
                                            f"سفارش: <code>{display_code}</code>\n"
                                            f"پس از ۸ چرخه تلاش هیچ ارسالی موفق نبود (احتمالاً تمام تارگت‌ها مسدود/ربات هستند یا فایل مدیا نامعتبر است).\n"
                                            f"<i>سفارش متوقف شد.</i>"
                                        )
                                    else:
                                        # تعویق نمایی (۵، ۱۰، ۲۰، ۴۰ دقیقه ... تا حداکثر ۶ ساعت)
                                        backoff_minutes = min(5 * (2 ** (order.fail_streak - 1)), 360)
                                        order.scheduled_for = datetime.now(timezone.utc) + timedelta(minutes=backoff_minutes)
                            return

                        # --- سناریوی دوم: سفارش به پایان رسیده است ---
                        order.status = OrderStatus.completed
                        order.scheduled_for = None
                        order.inflight_data = None
                        order.fail_streak = 0

                        error_count = 0
                        try:
                            error_count = (
                                await session.scalar(
                                    select(func.count())
                                    .select_from(OrderLog)
                                    .where(
                                        OrderLog.order_id == order_id,
                                        OrderLog.status == "error",
                                    )
                                )
                                or 0
                            )
                        except Exception as stat_err:
                            logger.warning(
                                f"Finalize: stats query for error_count failed for Order #{order_id}: {stat_err}"
                            )

                        display_code = (
                            order.tracking_code if order.tracking_code else f"ID-{order.id}"
                        )
                        
                        if not _is_extract_order(order):
                            admin_notify_text = (
                                "✅ <b>سفارش با موفقیت تکمیل شد!</b>\n\n"
                                f"سفارش: <code>{display_code}</code>\n\n"
                                "📊 <b>گزارش نهایی:</b>\n"
                                f"▫️ عملیات موفق: <code>{success_count}</code>\n"
                                f"▫️ عملیات ناموفق: <code>{error_count}</code>\n\n"
                                "<i>همه‌ی تارگت‌های صف پردازش شدند.</i>"
                            )
                            _spawn_background_task(_send_export_file(session_maker, bot, order_id, "سفارش با موفقیت به پایان رسید."))
                        else:
                            admin_notify_text = None

                        media_paths_to_clean = [
                            p for p in (order.media_path, order.media_2_path, order.media_3_path)
                            if p and isinstance(p, str) and not p.startswith("exports/")
                        ]
                        
                        if not _is_extract_order(order):
                            order.media_path = None
                            order.media_2_path = None
                            order.media_3_path = None
                        else:
                            media_paths_to_clean.clear()

            except Exception as e:
                logger.error(f"Finalize failed for Order #{order_id}: {e}", exc_info=True)
                try:
                    async with session_maker() as fallback_session:
                        async with fallback_session.begin():
                            await fallback_session.execute(
                                update(Order)
                                .where(Order.id == order_id)
                                .values(status=OrderStatus.pending)
                            )
                    await broadcast_to_admins(
                        bot,
                        text=(
                            f"⚠️ <b>خطا در تکمیل سفارش</b>\n\n"
                            f"سفارش <code>#{order_id}</code> به دلیل خطای سیستمی بسته نشد و "
                            f"به حالت Pending بازگشت.\n<i>جزئیات در لاگ سرور ثبت شد.</i>"
                        )
                    )
                except Exception as fb_err:
                    logger.error(f"Best-effort fallback failed for Order #{order_id}: {fb_err}")
                return

        if admin_notify_text:
            await broadcast_to_admins(bot, admin_notify_text)
                
        for path in media_paths_to_clean:
            if os.path.exists(path):
                try:
                    os.remove(path)
                    logger.info(
                        f"Garbage Collection: Deleted media {path} "
                        f"for completed Order #{order_id}."
                    )
                except Exception:
                    pass
                    
        _banner_warned_orders.discard(order_id)

    except Exception as lock_err:
        logger.error(f"Failed to process _finalize_chunk for Order #{order_id}: {lock_err}", exc_info=True)

async def order_dispatcher_loop(
    session_maker: async_sessionmaker[AsyncSession], 
    worker_pool: Dict[int, Client],
    bot: Bot
) -> None:
    try:
        extract_parallel_max_workers = 1
    except (TypeError, ValueError):
        extract_parallel_max_workers = 3
    # 🔄 مهلتِ حداکثریِ تخمین تعداد اعضا (ثانیه) — حلقه‌ی دیسپچ گیر نمی‌کند
    extract_estimate_timeout = 20.0

    logger.info("Order Dispatcher Loop started.")
    
    _logged_pending_orders: set = set()
    _warmup_notified_orders: set = set()

    while True:
        global_profile = await get_speed_profile()
        dispatch_batch = global_profile.dispatch_batch
        # 🚪 فاز ۶ (R3-ب): sweep دوره‌ای leave
        _maybe_schedule_leave_sweep(session_maker, worker_pool, bot)

        # ====== چک کردن از سرگیری خودکار (Auto-Resume) فاز ۴ ======
        try:
            async with session_maker() as session:
                held_orders = (await session.scalars(
                    select(Order).where(Order.status == OrderStatus.on_hold_proxy).order_by(Order.id.asc())
                )).all()
                
                if held_orders:
                    # بررسی وجود حداقل یک پروکسی سالم (یا ضعیف در صورت روشن بودن Fallback) که ظرفیت خالی دارد
                    allow_weak_fallback = getattr(config, "PROXY_ALLOW_WEAK_FALLBACK", True)
                    health_states = ["HEALTHY", "WEAK"] if allow_weak_fallback else ["HEALTHY"]
                    
                    has_capable_proxy = await session.scalar(
                        select(Proxy.id).where(
                            Proxy.is_active == True, 
                            Proxy.health_state.in_(health_states), 
                            Proxy.in_use < max(1, getattr(config, 'MAX_ACCOUNTS_PER_PROXY', 100))
                        ).limit(1)
                    )
                    if has_capable_proxy:
                        for ho in held_orders:
                            ho.status = OrderStatus.pending
                            ho.hold_reason = None
                            try:
                                await _notify_owner(
                                    bot, session_maker, ho.id, 
                                    f"✅ پروکسی جدید به شبکه اضافه شد.\nسفارش <code>{ho.tracking_code or ho.id}</code> از سر گرفته شد."
                                )
                            except Exception: pass
                        await session.commit()
                        logger.info(f"Auto-resumed {len(held_orders)} on_hold_proxy orders due to healthy proxy recovery.")
        except Exception as e:
            logger.error(f"Error in auto-resume check: {e}")
        # ============================================================

        # --- کار ۳: بررسی بلادرنگ فلگ‌های تأیید/رد درخواست عضویت ---
        try:
            async with session_maker() as session:
                now_utc = datetime.now(timezone.utc)
                pending_joins = (await session.scalars(
                    select(Order).where(
                        Order.status == OrderStatus.pending,
                        Order.retry_count > 0
                    )
                )).all()
                
                redis = _get_redis()
                flag_processed = False
                from pyrogram.enums import ChatMemberStatus
                for p_order in pending_joins:
                    approved_marker_key = f"join_approved_notified:{p_order.id}"
                    rejected_marker_key = f"join_rejected_notified:{p_order.id}"
                    
                    flag_key = f"join_request:{p_order.id}:result"
                    flag = await redis.get(flag_key)
                    event_source = "unknown"
                    
                    if flag:
                        flag = flag.decode('utf-8') if isinstance(flag, bytes) else flag
                        await redis.delete(flag_key)
                        event_source = "listener"
                    else:
                        if await redis.exists(approved_marker_key) or await redis.exists(rejected_marker_key):
                            continue
                            
                        # 🛡 فاز ۹: پول فعال وضعیت عضویت در هر چرخه‌ی انتظار
                        try:
                            first_join = (await session.scalars(select(OrderJoin).where(OrderJoin.order_id == p_order.id).limit(1))).first()
                            if first_join and first_join.chat_id and first_join.account_id in worker_pool:
                                acc_id = first_join.account_id
                                poll_key = f"join_poll:{acc_id}"
                                if not await redis.get(poll_key):
                                    await redis.set(poll_key, "1", ex=10)
                                    client = worker_pool[acc_id]
                                    if getattr(client, "is_connected", False):
                                        try:
                                            member = await client.get_chat_member(first_join.chat_id, "me")
                                            if member.status in [ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR]:
                                                flag = "approved"
                                                event_source = "poll"
                                        except UserNotParticipant:
                                            pass
                                        except Exception as e:
                                            logger.debug(f"Poll check failed for order #{p_order.id}: {e}")
                        except Exception as e:
                            logger.error(f"Error during active join poll: {e}")
                        
                    if flag:
                        is_duplicate = False
                        if flag == "approved":
                            if not await redis.set(approved_marker_key, "1", nx=True, ex=30*86400):
                                is_duplicate = True
                        elif flag == "rejected":
                            if not await redis.set(rejected_marker_key, "1", nx=True, ex=30*86400):
                                is_duplicate = True

                        if is_duplicate:
                            continue
                        
                        if flag == "approved":
                            await session.execute(update(Order).where(Order.id == p_order.id).values(scheduled_for=now_utc))
                            approved_text = f"✅ <b>درخواست عضویت تأیید شد!</b>\nسفارش: <code>{p_order.tracking_code or p_order.id}</code>\nاستخراج/ارسال هم‌اکنون آغاز می‌شود..."
                            
                            await broadcast_to_admins(bot, text=approved_text)
                            await _notify_owner(bot, session_maker, p_order.id, approved_text)
                            
                            task_type = "extract" if _is_extract_order(p_order) else "order"
                            reporter = await _get_or_start_reporter(
                                bot=bot, session_maker=session_maker, order_id=p_order.id, 
                                task_type=task_type, target_hint="تأیید درخواست عضویت"
                            )
                            if reporter:
                                try:
                                    await reporter.update(
                                        status="✅ درخواست عضویت تأیید شد! در حال آماده‌سازی و استخراج..."
                                    )
                                except Exception as e:
                                    logger.error(f"Failed to update reporter for approved join: {e}")

                        elif flag == "rejected":
                            await session.execute(update(Order).where(Order.id == p_order.id).values(
                                status=OrderStatus.error, 
                                scheduled_for=None,
                                target_data=""
                            ))
                            first_join = (await session.scalars(select(OrderJoin).where(OrderJoin.order_id == p_order.id).limit(1))).first()
                            acc_id = first_join.account_id if first_join else 0
                            
                            session.add(OrderLog(
                                order_id=p_order.id, 
                                account_id=acc_id,
                                target="Join Request", 
                                status="error", 
                                error_message="❌ درخواست عضویت رد شد. لطفاً با ادمین گروه تماس بگیرید یا گروه دیگری انتخاب کنید."
                            ))
                            rejected_text = f"❌ <b>درخواست عضویت رد شد.</b>\nسفارش: <code>{p_order.tracking_code or p_order.id}</code>\nلطفاً با ادمین گروه تماس بگیرید یا گروه دیگری انتخاب کنید."
                            await broadcast_to_admins(bot, text=rejected_text)
                            await _notify_owner(bot, session_maker, p_order.id, rejected_text)
                            
                            task_type = "extract" if _is_extract_order(p_order) else "order"
                            reporter = await _get_or_start_reporter(
                                bot=bot, session_maker=session_maker, order_id=p_order.id, 
                                task_type=task_type, target_hint="رد درخواست عضویت"
                            )
                            if reporter:
                                await reporter.fail(
                                    "⛔️ <b>درخواست عضویت رد شد</b>\n\n"
                                    "ادمین گروه با درخواست ورود ربات مخالفت کرد (یا ربات بلاک شد). عملیات کاملاً متوقف شد."
                                )
                                _pop_progress_reporter("order", p_order.id)
                                _pop_progress_reporter("extract", p_order.id)
                        flag_processed = True
                
                if flag_processed:
                    await session.commit()
        except Exception as e:
            logger.error(f"Error checking real-time join request flags: {e}")

        # 🐌 ترمز جهانی (Global Slowdown)
        if await is_global_slowdown():
            logger.debug("Dispatcher: global slowdown active - skipping dispatch cycle.")
            await asyncio.sleep(max(1.0, float(getattr(config, "DISPATCH_INTERVAL_SECONDS", 2.0))))
            continue

        _batch_seen_order_ids: set = set()
        active_pending_orders_this_cycle = set()
        
        # 🚀 فاز ۴: مخزن تسک‌های موازی
        tasks_to_launch = []
        busy_account_ids: List[int] = []
        
        # خواندن وضعیت ردیس
        redis_prechecks = {}
        try:
            async with session_maker() as session:
                now_utc = datetime.now(timezone.utc)
                now_naive = now_utc.replace(tzinfo=None)
                connected_ids = [acc_id for acc_id, w_client in list(worker_pool.items()) if getattr(w_client, "is_connected", False)]
                
                if connected_ids:
                    stmt_accs = select(Account).where(
                        Account.id.in_(connected_ids),
                        Account.is_banned == False,
                        or_(Account.flood_wait_until.is_(None), Account.flood_wait_until <= now_naive),
                        or_(Account.restricted_until.is_(None), Account.restricted_until <= now_naive)
                    )
                    candidate_accounts = (await session.scalars(stmt_accs)).all()
                    
                    warmed_up_accs = []
                    for acc in candidate_accounts:
                        if await _is_warmed_up(session, acc, now_utc):
                            warmed_up_accs.append(acc)
                            
                    if warmed_up_accs:
                        redis = _get_redis()
                        pipe = redis.pipeline()
                        
                        for acc in warmed_up_accs:
                            pipe.exists(f"busy_worker:{acc.id}")
                            pipe.exists(f"chunk_cooldown:{acc.id}")
                            pipe.get(_daily_key(acc.id)) 
                            pipe.get(f"last_dispatched:{acc.id}")
                            pipe.get(_hourly_key(acc.id))
                        results = await pipe.execute()
                        
                        idx = 0
                        for acc in warmed_up_accs:
                            redis_prechecks[acc.id] = {
                                "is_busy": bool(results[idx]),
                                "is_cooldown": bool(results[idx+1]),
                                "daily_sent": int(results[idx+2] or 0),
                                "last_dispatched": float(results[idx+3] or 0.0),
                                "hourly_sent": int(results[idx+4] or 0)
                            }
                            idx += 5
        except Exception as e:
            logger.error(f"Dispatcher Redis pre-check failed: {e}")

        for _cycle in range(dispatch_batch):
            send_jobs = []       
            extract_jobs = []    
            resolver_jobs = []
            
            order_id_db = None
            dispatched_targets_count = 0
            background_scheduled = False
            link_resolver_scheduled = False
            empty_target_notify_code: Optional[str] = None
            
            try:
                async with session_maker() as session:
                    now_utc = datetime.now(timezone.utc)
                    now_naive = datetime.now(timezone.utc).replace(tzinfo=None) 

                    pending_filters = [
                        Order.status == OrderStatus.pending,
                        Order.is_approved == True, 
                        or_(
                            Order.scheduled_for.is_(None),
                            Order.scheduled_for <= now_utc,
                        ),
                    ]
                    if _batch_seen_order_ids:
                        pending_filters.append(Order.id.notin_(_batch_seen_order_ids))
                    
                    from sqlalchemy import case
                    pending_stmt = (
                        select(Order)
                        .where(*pending_filters)
                        .order_by(
                            case((Order.order_type == 'extract', 0), else_=1),
                            Order.id.asc()
                        )
                        .limit(1)
                    )
                    order = (await session.scalars(pending_stmt)).first()
                    if order is None:
                        break

                    order_id_db = order.id
                    _batch_seen_order_ids.add(order.id)
                    active_pending_orders_this_cycle.add(order.id)
                    order_tracking_code = (
                        order.tracking_code if order.tracking_code else f"ID-{order.id}"
                    )

                    order_profile = await get_speed_profile(order.speed_mode or global_profile.name)
                    if not order.speed_mode:
                        order.speed_mode = order_profile.name

                    settings_row = (await session.scalars(select(GlobalSettings).limit(1))).first()
                    send_limit_per_run = (settings_row.send_limit_per_run if settings_row else None) or 0

                    order_cat_ids: List[int] = list(
                        (await session.scalars(
                            select(order_category_assoc.c.category_id)
                            .where(order_category_assoc.c.order_id == order.id)
                        )).all()
                    )

                    connected_ids = [
                        acc_id for acc_id, w_client in list(worker_pool.items())
                        if getattr(w_client, "is_connected", False)
                    ]
                    
                    eligible_accounts: List[Account] = []
                    total_connected = len(connected_ids)
                    accounts_passed_category = 0
                    accounts_delayed_by_warmup = 0
                    max_warmup_end_time = None

                    if connected_ids:
                        from database.models import AccountStatus
                        acc_filters = [
                            Account.id.in_(connected_ids),
                            Account.status == AccountStatus.active,
                            Account.is_banned == False,
                            or_(Account.flood_wait_until.is_(None), Account.flood_wait_until <= now_naive),
                            or_(Account.restricted_until.is_(None), Account.restricted_until <= now_naive)
                        ]
                        
                        if order_cat_ids:
                            acc_filters.append(Account.category_id.in_(order_cat_ids))
                            
                        stmt_accs = select(Account).where(*acc_filters)
                        acc_rows = (await session.scalars(stmt_accs)).all()
                        accounts_passed_category = len(acc_rows)
                        
                        for acc in acc_rows:
                            if await _is_warmed_up(session, acc, now_utc):
                                eligible_accounts.append(acc)
                            else:
                                accounts_delayed_by_warmup += 1
                                w_time = acc.warmed_up_at
                                if w_time:
                                    if w_time.tzinfo is None:
                                        w_time = w_time.replace(tzinfo=timezone.utc)
                                    if not max_warmup_end_time or w_time > max_warmup_end_time:
                                        max_warmup_end_time = w_time

                    reserved_acc_ids = []
                    try:
                        reserved_acc_ids = list((await session.scalars(
                            select(OrderJoin.account_id)
                            .join(Order, OrderJoin.order_id == Order.id)
                            .where(
                                Order.status == OrderStatus.pending, 
                                Order.retry_count > 0,
                                Order.id != order.id  # 👈 اضافه شدن این شرط برای جلوگیری از قفل شدن اکانت توسط سفارش جاری (Self-blocking)
                            )
                        )).all())
                    except Exception: pass

                    if order.retry_count and order.retry_count > 0:
                        try:
                            joined_acc_ids = (await session.scalars(
                                select(OrderJoin.account_id)
                                .where(OrderJoin.order_id == order.id)
                            )).all()
                            
                            if joined_acc_ids:
                                filtered_accounts = [acc for acc in eligible_accounts if acc.id in joined_acc_ids]
                                if not filtered_accounts and eligible_accounts:
                                    logger.info(f"Order #{order.id}: Previous join-requested accounts unavailable, allowing new accounts to apply.")
                                else:
                                    eligible_accounts = filtered_accounts
                        except Exception as e:
                            logger.error(f"Failed to filter joined accounts for pending_approval Order #{order.id}: {e}")

                    if eligible_accounts:
                        source_channel_id_for_priority = None
                        if getattr(order, "source_channel_id", None) or getattr(order, "source_message_ids", None):
                            source_channel_id_for_priority = getattr(order, "source_channel_id", None)

                        if source_channel_id_for_priority:
                            try:
                                known_member_ids = await _find_source_member_workers(
                                    [acc.id for acc in eligible_accounts],
                                    source_channel_id_for_priority,
                                )
                            except Exception:
                                known_member_ids = []
                            
                            eligible_accounts.sort(key=lambda acc: (
                                acc.id not in (known_member_ids or []),
                                redis_prechecks.get(acc.id, {}).get("hourly_sent", 0),
                                redis_prechecks.get(acc.id, {}).get("last_dispatched", 0.0),
                            ))
                        else:
                            eligible_accounts.sort(key=lambda acc: (
                                redis_prechecks.get(acc.id, {}).get("hourly_sent", 0),
                                redis_prechecks.get(acc.id, {}).get("last_dispatched", 0.0)
                            ))

                    if not eligible_accounts:
                        # 🔴 فاز ۴: تشخیص هولد به‌جای توقف بی‌دلیل
                        if not order.server_ip_consent:
                            healthy_proxies = await session.scalar(
                                select(func.count(Proxy.id)).where(Proxy.is_active == True, Proxy.health_state == "HEALTHY")
                            ) or 0
                            
                            if healthy_proxies == 0:
                                # پیش از هولد، بررسی فال‌بک
                                from workers.session_manager import direct_ip_fallback_enabled, direct_budget_ok
                                # چون اکانتی انتخاب نشده (eligible_accounts خالی است)، چک می‌کنیم آیا سیاست فال‌بک عمومی برقرار است
                                fallback_allowed = direct_ip_fallback_enabled(0) and await direct_budget_ok(session)
                                
                                if fallback_allowed:
                                    logger.warning(f"Order #{order.id}: No proxies available but fallback is enabled globally. Keeping order pending for direct connection.")
                                else:
                                    await session.execute(
                                        update(Order)
                                        .where(Order.id == order.id)
                                        .values(status=OrderStatus.on_hold_proxy, hold_reason="تمام پروکسی‌های سالم DEAD شده‌اند و فال‌بک دایرکت مسدود است")
                                    )
                                    await session.commit()
                                    # وارد کردن هوک برای پیام هولد
                                    from workers.task_queue import _send_hold_message
                                    await _send_hold_message(session, bot, order.id)
                                    continue

                        if order.id not in _logged_pending_orders:
                            if not connected_ids:
                                logger.info(f"Order #{order.id}: no connected workers available; staying pending.")
                            elif order_cat_ids and accounts_passed_category == 0:
                                logger.info(f"Order #{order.id}: no connected worker matches categories {order_cat_ids}; staying pending.")
                            elif accounts_delayed_by_warmup > 0:
                                logger.info(f"Order #{order.id}: {accounts_delayed_by_warmup} worker(s) matched criteria but are delayed by warmup period; staying pending.")
                            else:
                                logger.info(f"Order #{order.id}: no eligible worker available; staying pending.")
                                
                            _logged_pending_orders.add(order.id)
                        
                        if (accounts_delayed_by_warmup > 0 and 
                            accounts_passed_category == accounts_delayed_by_warmup and
                            order.id not in _warmup_notified_orders):
                            
                            _warmup_notified_orders.add(order.id)
                            warmup_time_str = max_warmup_end_time.strftime("%H:%M (UTC)") if max_warmup_end_time else "نامشخص"
                            
                            notify_text = (
                                f"⏳ <b>تأخیر به‌دلیل دوره‌ی گرم‌شدن اکانت</b>\n\n"
                                f"سفارش: <code>{order_tracking_code}</code>\n\n"
                                f"<i>اکانت(های) متصل و مناسب یافت شدند، اما هنوز در دوره‌ی گرم‌شدن اولیه هستند. "
                                f"این سفارش در صف انتظار باقی می‌ماند (تقریباً تا ساعت {warmup_time_str}).</i>"
                            )
                            await broadcast_to_admins(bot, text=notify_text)
                                
                    else:
                        _logged_pending_orders.discard(order.id)
                        _warmup_notified_orders.discard(order.id)

                    if _is_extract_order(order):
                        extract_links = _parse_target_links(order.target_data)
                        if not extract_links:
                            empty_target_notify_code = order_tracking_code
                            await session.execute(
                                update(Order)
                                .where(Order.id == order.id)
                                .values(status=OrderStatus.error, target_data="")
                                .execution_options(synchronize_session=False)
                            )
                        else:
                            group_link = extract_links[0]

                            picked_client = None
                            picked_acc_id = None
                            
                            prioritized_acc_ids = []
                            try:
                                async with session_maker() as tmp_s:
                                    joined_accs = (await tmp_s.scalars(
                                        select(OrderJoin.account_id)
                                        .where(OrderJoin.group_link == group_link, OrderJoin.leave_done == False)
                                    )).all()
                                    prioritized_acc_ids = list(joined_accs)
                            except Exception:
                                pass
                                
                            if prioritized_acc_ids:
                                eligible_accounts.sort(key=lambda acc: (
                                    acc.id not in prioritized_acc_ids, 
                                    redis_prechecks.get(acc.id, {}).get("last_dispatched", 0.0)
                                ))
                            
                            for acc in eligible_accounts:
                                p = redis_prechecks.get(acc.id, {})
                                if p.get("is_busy") or p.get("is_cooldown") or acc.id in busy_account_ids: continue
                                acc_daily_limit = effective_daily_limit(acc.created_at)  
                                if p.get("daily_sent", 0) >= acc_daily_limit: continue

                                if await _acquire_busy(acc.id):
                                    await _get_redis().set(f"last_dispatched:{acc.id}", time.time(), ex=172800)
                                    picked_acc_id = acc.id
                                    picked_client = worker_pool[acc.id]
                                    break

                            if picked_acc_id is not None:
                                busy_account_ids.append(picked_acc_id)

                                parallel_clients: List[Client] = [picked_client]
                                parallel_acc_ids: List[int] = [picked_acc_id]
                                total_estimate: Optional[int] = None

                                if order_profile.estimate_on_critical_path and extract_parallel_max_workers > 1:
                                    try:
                                        total_estimate = await asyncio.wait_for(
                                            _estimate_member_count(picked_client, group_link),
                                            timeout=extract_estimate_timeout,
                                        )
                                    except Exception as est_err:
                                        logger.warning(f"Order #{order.id}: estimate failed → single-worker extraction.")
                                        total_estimate = None

                                try:
                                    go_parallel = False
                                    is_valid_parallel = (order.filter_type in _PARALLEL_SAFE_FILTERS) or (isinstance(order.filter_type, str) and order.filter_type.startswith("{"))
                                    if is_valid_parallel:
                                        if order_profile.extract_parallel_min_members == 0:
                                            go_parallel = True
                                        elif total_estimate and int(total_estimate) > order_profile.extract_parallel_min_members:
                                            go_parallel = True
                                except (TypeError, ValueError):
                                    go_parallel = False
                                
                                # 🔴 کلید قطع/وصل استخراج موازی:
                                # برای فعال‌سازی مجدد استخراج موازی، کافیست خط زیر را پاک یا کامنت (با #) کنید.
                                go_parallel = False

                                if go_parallel:
                                    for acc in eligible_accounts:
                                        if len(parallel_acc_ids) >= extract_parallel_max_workers:
                                            break
                                        if acc.id in parallel_acc_ids:
                                            continue
                                        p = redis_prechecks.get(acc.id, {})
                                        if p.get("is_busy") or p.get("is_cooldown") or acc.id in busy_account_ids: continue
                                        acc_daily_limit = effective_daily_limit(acc.created_at)
                                        if p.get("daily_sent", 0) >= acc_daily_limit: continue

                                        if await _acquire_busy(acc.id):
                                            await _get_redis().set(f"last_dispatched:{acc.id}", time.time(), ex=172800)
                                            parallel_acc_ids.append(acc.id)
                                            parallel_clients.append(worker_pool[acc.id])
                                            busy_account_ids.append(acc.id)

                                if len(parallel_acc_ids) > 1:
                                    logger.info(
                                        f"🔄 Order #{order.id}: PARALLEL extraction with "
                                        f"{len(parallel_acc_ids)} worker(s) "
                                        f"(est. members={total_estimate})."
                                    )

                                current_inflight = []
                                if order.inflight_data:
                                    try:
                                        current_inflight = json.loads(order.inflight_data)
                                    except Exception:
                                        pass
                                if group_link not in current_inflight:
                                    current_inflight.append(group_link)

                                result = await session.execute(
                                    update(Order)
                                    .where(Order.id == order.id, Order.status == OrderStatus.pending)
                                    .values(
                                        status=OrderStatus.running,
                                        target_data="\n".join(extract_links[1:]),
                                        inflight_data=json.dumps(current_inflight)
                                    )
                                    .execution_options(synchronize_session=False)
                                )
                                if result.rowcount == 0:
                                    logger.info(f"Dispatcher race condition: Order #{order.id} is no longer pending. Skipping.")
                                    for _aid in parallel_acc_ids:
                                        await _release_busy(_aid)
                                        if _aid in busy_account_ids:
                                            busy_account_ids.remove(_aid)
                                    continue
                                
                                await _register_inflight_chunks(order.id, 1)
                                extract_jobs.append((parallel_clients, parallel_acc_ids, group_link, total_estimate))

                    elif order.filter_type:
                        raw_link_data = (order.target_data or "").strip()
                        if not raw_link_data:
                            empty_target_notify_code = order_tracking_code
                            await session.execute(
                                update(Order)
                                .where(Order.id == order.id)
                                .values(status=OrderStatus.error, target_data="")
                                .execution_options(synchronize_session=False)
                            )
                        else:
                            extract_links = _parse_target_links(raw_link_data)
                            prioritized_acc_ids = []
                            if extract_links:
                                try:
                                    async with session_maker() as tmp_s:
                                        joined_accs = (await tmp_s.scalars(
                                            select(OrderJoin.account_id)
                                            .where(OrderJoin.group_link.in_(extract_links), OrderJoin.leave_done == False)
                                        )).all()
                                        prioritized_acc_ids = list(joined_accs)
                                except Exception:
                                    pass

                            if prioritized_acc_ids:
                                eligible_accounts.sort(key=lambda acc: (
                                    acc.id not in prioritized_acc_ids, 
                                    redis_prechecks.get(acc.id, {}).get("last_dispatched", 0.0)
                                ))

                            picked_client = None
                            picked_acc_id = None
                            for acc in eligible_accounts:
                                p = redis_prechecks.get(acc.id, {})
                                if p.get("is_busy") or p.get("is_cooldown") or acc.id in busy_account_ids: continue
                                acc_daily_limit = effective_daily_limit(acc.created_at)  
                                if p.get("daily_sent", 0) >= acc_daily_limit: continue

                                if await _acquire_busy(acc.id):
                                    await _get_redis().set(f"last_dispatched:{acc.id}", time.time(), ex=172800)
                                    picked_acc_id = acc.id
                                    picked_client = worker_pool[acc.id]
                                    break
                            if picked_acc_id is not None:
                                busy_account_ids.append(picked_acc_id)
                                result = await session.execute(
                                    update(Order)
                                    .where(Order.id == order.id, Order.status == OrderStatus.pending)
                                    .values(status=OrderStatus.running)
                                    .execution_options(synchronize_session=False)
                                )
                                if result.rowcount == 0:
                                    logger.info(f"Dispatcher race condition: Order #{order.id} is no longer pending. Skipping.")
                                    await _release_busy(picked_acc_id)
                                    busy_account_ids.remove(picked_acc_id)
                                    continue
                                
                                resolver_jobs.append(
                                    (picked_client, picked_acc_id, raw_link_data, order.filter_type)
                                )

                    else:
                        all_targets = [
                            t.strip() for t in (order.target_data or "").split("\n") if t.strip()
                        ]
                        if not all_targets:
                            empty_target_notify_code = order_tracking_code
                            await session.execute(
                                update(Order)
                                .where(Order.id == order.id)
                                .values(status=OrderStatus.error, target_data="")
                                .execution_options(synchronize_session=False)
                            )
                        else:
                            remaining_targets = list(all_targets)
                            
                            try:
                                max_concurrent = max(1, min(3, int(getattr(config, "ORDER_MAX_CONCURRENT_SENDERS", 1))))
                            except (TypeError, ValueError):
                                max_concurrent = 1
                            assigned_senders = 0
                            
                            for acc in eligible_accounts:
                                if not remaining_targets or assigned_senders >= max_concurrent:
                                    break
                                
                                if acc.id in reserved_acc_ids:
                                    continue
                                    
                                p = redis_prechecks.get(acc.id, {})
                                if p.get("is_busy") or acc.id in busy_account_ids:
                                    continue
                                if p.get("is_cooldown"):
                                    continue
                                    
                                acc_daily_limit = effective_daily_limit(acc.created_at)  
                                if p.get("daily_sent", 0) >= acc_daily_limit:  
                                    continue
                                    
                                if not await _acquire_busy(acc.id):
                                    continue
                                    
                                await _get_redis().set(f"last_dispatched:{acc.id}", time.time(), ex=172800)
                                
                                chunk_limit = acc_daily_limit
                                if send_limit_per_run and send_limit_per_run > 0:
                                    chunk_limit = min(chunk_limit, send_limit_per_run)

                                if not chunk_limit or chunk_limit <= 0:
                                    await _release_busy(acc.id)
                                    continue
                                chunk = remaining_targets[:chunk_limit]
                                remaining_targets = remaining_targets[chunk_limit:]
                                busy_account_ids.append(acc.id)
                                send_jobs.append((worker_pool[acc.id], acc.id, chunk))
                                dispatched_targets_count += len(chunk)
                                assigned_senders += 1

                            if send_jobs:
                                current_inflight = []
                                if order.inflight_data:
                                    try:
                                        current_inflight = json.loads(order.inflight_data)
                                    except Exception:
                                        pass
                                seen = set(current_inflight)
                                for _, _, chunk in send_jobs:
                                    for t in chunk:
                                        if t not in seen:
                                            seen.add(t)
                                            current_inflight.append(t)

                                result = await session.execute(
                                    update(Order)
                                    .where(Order.id == order.id, Order.status == OrderStatus.pending)
                                    .values(
                                        status=OrderStatus.running,
                                        target_data="\n".join(remaining_targets),
                                        inflight_data=json.dumps(current_inflight)
                                    )
                                    .execution_options(synchronize_session=False)
                                )
                                if result.rowcount == 0:
                                    logger.info(f"Dispatcher race condition: Order #{order.id} is no longer pending. Skipping.")
                                    for _, acc_id, _ in send_jobs:
                                        await _release_busy(acc_id)
                                        busy_account_ids.remove(acc_id)
                                    send_jobs.clear()
                                    continue
                                
                                await _register_inflight_chunks(order.id, len(send_jobs))

                    await session.commit()
                    
                dispatched_anything = bool(send_jobs or extract_jobs or resolver_jobs)
                
                if not dispatched_anything and not empty_target_notify_code:
                    # --- بازنویسی منطق توقف سریع (Fail-Fast) دیسپچر ---
                    # در این حالت هیچ تارگتی در این چرخه به ورکرها داده نشده است.
                    
                    fresh_prechecks = {}
                    if eligible_accounts:
                        try:
                            redis = _get_redis()
                            pipe = redis.pipeline()
                            for acc in eligible_accounts:
                                pipe.exists(f"busy_worker:{acc.id}")
                            results = await pipe.execute()
                            for idx, acc in enumerate(eligible_accounts):
                                fresh_prechecks[acc.id] = {"is_busy": bool(results[idx])}
                        except Exception as e:
                            logger.error(f"Failed to fetch fresh redis states for cancellation decision: {e}")
                            for acc in eligible_accounts:
                                fresh_prechecks[acc.id] = {"is_busy": True}

                    # آیا ورکری در حال ارسال (busy) هست؟ اگر بله، سیستم باید منتظر بماند.
                    is_any_worker_busy = any(fresh_prechecks.get(acc.id, {}).get("is_busy", False) for acc in eligible_accounts)
                    is_waiting_for_warmup = (accounts_delayed_by_warmup > 0)
                    
                    # اگر هیچ اکانتی نیست، یا اکانت‌ها هستند ولی هیچ‌کدام busy یا در حال warmup نیستند 
                    # (یعنی همگی در لیمیت، استراحت یا سقف روزانه گیر کرده‌اند) -> سفارش بلافاصله متوقف می‌شود
                    if not eligible_accounts or (not is_any_worker_busy and not is_waiting_for_warmup):
                        # توقف فوری سفارش و ارسال خروجی
                        success_count = (
                            await session.scalar(
                                select(func.count())
                                .select_from(OrderLog)
                                .where(OrderLog.order_id == order.id, OrderLog.status == "success")
                            )
                            or 0
                        )
                        
                        finish_msg = f"سفارش تا این مرحله انجام شد (ارسال موفق: {success_count}). به دلیل عدم وجود ورکر آماده (لیمیت/استراحت سایر اکانت‌ها)، سفارش لغو و پایان یافت."
                        final_status = OrderStatus.completed if success_count > 0 else OrderStatus.error
                        
                        await session.execute(
                            update(Order)
                            .where(Order.id == order.id)
                            .values(
                                status=final_status, 
                                scheduled_for=None, 
                                reject_reason="اتمام ورکرهای آماده و در دسترس"
                            )
                            .execution_options(synchronize_session=False)
                        )
                        session.add(OrderLog(order_id=order.id, target="System", status=final_status, error_message=finish_msg))
                        await session.commit()
                        
                        if not _is_extract_order(order):
                            _spawn_background_task(_send_export_file(session_maker, bot, order.id, finish_msg))
                            
                        task_type = "extract" if _is_extract_order(order) else "order"
                        if reporter := _progress_reporters.get(f"{task_type}:{order.id}"):
                            if final_status == OrderStatus.completed:
                                await reporter.finish(f"✅ <b>پایان سفارش</b>\n\n{finish_msg}")
                            else:
                                await reporter.fail(f"⛔️ <b>لغو سفارش</b>\n\n{finish_msg}")
                            _pop_progress_reporter(task_type, order.id)
                            
                        await broadcast_to_admins(
                            bot,
                            text=f"🏁 <b>پایان سفارش به دلیل اتمام منابع</b>\n\nسفارش: <code>{order_tracking_code}</code>\n{finish_msg}"
                        )
                        continue

                for _client, _acc_id, _chunk in send_jobs:
                    tasks_to_launch.append(
                        background_order_execution(_client, _acc_id, order_id_db, _chunk, session_maker, bot)
                    )
                    background_scheduled = True

                for _clients, _acc_ids, _group_link, _est in extract_jobs:
                    tasks_to_launch.append(
                        background_order_execution(
                            _clients,
                            _acc_ids,
                            order_id_db,
                            [_group_link],
                            session_maker,
                            bot,
                            total_estimate=_est,
                        )
                    )
                    background_scheduled = True

                for _client, _acc_id, _raw_links, _filter_type in resolver_jobs:
                    tasks_to_launch.append(
                        link_send_resolver_wrapper(_client, _acc_id, order_id_db, _raw_links, _filter_type, session_maker, bot)
                    )
                    link_resolver_scheduled = True

                if background_scheduled or link_resolver_scheduled:
                    logger.info(
                        f"Dispatcher: Order #{order_id_db} ({order_tracking_code}) → "
                        f"Jobs queued for parallel launch "
                        f"({dispatched_targets_count} target(s) dispatched)."
                    )

                if empty_target_notify_code:
                    await broadcast_to_admins(
                        bot,
                        text=(
                            f"⚠️ <b>سفارش بدون تارگت معتبر</b>\n\n"
                            f"سفارش <code>{empty_target_notify_code}</code> هیچ تارگت قابل "
                            f"پردازشی ندارد و به حالت خطا منتقل شد.\n"
                            f"<i>لطفاً محتوای سفارش را بررسی کنید.</i>"
                        )
                    )

            except Exception as e:
                logger.error(
                    f"Dispatcher cycle failed"
                    + (f" for Order #{order_id_db}" if order_id_db else "")
                    + f": {e}",
                    exc_info=True,
                )
                for acc_id in list(busy_account_ids):
                    await _release_busy(acc_id)
                if order_id_db is not None and not (background_scheduled or link_resolver_scheduled):
                    async with _order_inflight_lock:
                        _order_inflight_chunks.pop(order_id_db, None)

        if tasks_to_launch:
            async def _launch_batch(coroutines):
                tasks = [asyncio.create_task(coro) for coro in coroutines]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for res in results:
                    if isinstance(res, Exception):
                        logger.error(f"Unhandled exception in background task: {res}", exc_info=res)
            
            _spawn_background_task(_launch_batch(tasks_to_launch))

        _logged_pending_orders.intersection_update(active_pending_orders_this_cycle)
        _warmup_notified_orders.intersection_update(active_pending_orders_this_cycle)

        await asyncio.sleep(max(1.0, float(getattr(config, "DISPATCH_INTERVAL_SECONDS", 2.0))))

from pathlib import Path

# ==========================================
# 🧹 فاز ۳: زباله‌روب خودکار فایل‌های موقت (Garbage Collector)
# ==========================================

# workers/task_queue.py
# بازنویسی کامل تابع
async def temp_file_gc_loop(cleanup_interval_hours: int = 12, max_age_hours: int = 24):
    """
    🧹 فاز ۳: زباله‌روب خودکار با محافظت از فایل‌های ارجاع‌داده‌شده (تارگت‌های منتظر یا خروجی استخراج)
    """
    directories_to_clean = ["downloads", "exports"]
    max_age_seconds = max_age_hours * 3600

    logger.info(f"Temp File Garbage Collector started. Running every {cleanup_interval_hours} hours.")

    from database.engine import async_session
    from database.models import Order

    while True:
        try:
            protected_files = set()
            try:
                async with async_session() as session:
                    # استخراج فایل‌های متصل به سفارش‌های فعال (نیاز به مدیا) یا سفارش‌های extract (خروجی دانلود)
                    stmt = select(Order.media_path, Order.media_2_path, Order.media_3_path).where(
                        or_(
                            Order.status.notin_(["completed", "error"]),
                            Order.order_type == "extract"
                        )
                    )
                    rows = (await session.execute(stmt)).all()
                    for r in rows:
                        if r[0]: protected_files.add(os.path.abspath(r[0]))
                        if r[1]: protected_files.add(os.path.abspath(r[1]))
                        if r[2]: protected_files.add(os.path.abspath(r[2]))
            except Exception as e:
                logger.error(f"GC: Failed to fetch protected files from DB: {e}")
                await asyncio.sleep(600)
                continue

            now = time.time()
            deleted_count = 0

            for dir_name in directories_to_clean:
                dir_path = Path(dir_name)
                if not dir_path.exists() or not dir_path.is_dir():
                    continue

                for file_path in dir_path.iterdir():
                    if not file_path.is_file() or file_path.name == ".gitkeep":
                        continue

                    abs_path = os.path.abspath(file_path)
                    if abs_path in protected_files:
                        continue  # 🛡 محافظت از قربانی شدن فایل ارجاع‌دار

                    file_mtime = file_path.stat().st_mtime
                    if (now - file_mtime) > max_age_seconds:
                        try:
                            await aiofiles.os.remove(file_path)
                            deleted_count += 1
                        except Exception as e:
                            logger.warning(f"GC failed to remove {file_path}: {e}")

            if deleted_count > 0:
                logger.info(f"Garbage Collector: Removed {deleted_count} stale temp files.")

        except Exception as e:
            logger.error(f"Error in Temp File GC Loop: {e}", exc_info=True)

        await asyncio.sleep(cleanup_interval_hours * 3600)

