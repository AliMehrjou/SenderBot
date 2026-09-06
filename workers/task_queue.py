import asyncio
import html   # 🛡 فاز ۸ (BUG-25): escape لینک‌ها در پیام اطلاع‌رسانی ادمین
import logging
import os
import random  
import re     # 🛡 فاز ۸ (BUG-25): پارس/اعتبارسنجی چند لینک در target_data
import time   
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional
import html
from sqlalchemy import or_
from aiogram import Bot
from aiogram.types import FSInputFile
from pyrogram import Client
from pyrogram.errors import FloodWait, UserNotParticipant, ChannelPrivate  # 🚪 فاز ۶ (R3-ب)
from sqlalchemy import select, or_, update, func
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
import json
from datetime import datetime, timezone, timedelta
from typing import List, Optional
from sqlalchemy import select, update, func
from aiogram import Bot
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from config import config
from database.models import Order, OrderStatus, Account, GlobalSettings, OrderLog, order_category_assoc, Banner, OrderJoin  # OrderJoin: 🚪 فاز ۶ (R3-ب)
from workers.sender import execute_bulk_send, daily_cap_reached, is_in_cooldown, mark_chunk_cooldown, effective_daily_limit, is_global_slowdown
from workers.extractor import extract_active_users, extract_members_for_sending

logger = logging.getLogger(__name__)

# 🕐 بخش ب — سقف تلاش برای لینک‌های ریکوئستی: هر چرخه‌ی تعویق ۱ ساعته به دلیل
# pending_approval شمارنده‌ی retry_count سفارش را یکی افزایش می‌دهد؛ اگر به ۲۴
# برسد، سفارش به‌جای تکرار بی‌نهایت error می‌شود و ادمین مطلع می‌گردد.
PENDING_APPROVAL_RETRY_LIMIT = 24

# 🎨 چرخش بنر: مجموعه‌ی سفارش‌هایی که به‌خاطر «مخزن بنرِ خالی» هشدار داده‌اند
# (جلوگیری از اسپم پیام هشدار به ادمین در هر سیکل دیسپچ؛ فقط یک‌بار در طول عمر پروسه)
_banner_warned_orders: set = set()

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
    BYPASS_WARMUP = True

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

    if joined_now or status_code == "pending_approval":
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
            rows = (await session.scalars(
                select(OrderJoin)
                .join(Order, OrderJoin.order_id == Order.id)
                .where(
                    OrderJoin.leave_done == False,  # noqa: E712
                    Order.status.in_([OrderStatus.completed, OrderStatus.error]),
                )
                .order_by(OrderJoin.id.asc())
                .limit(_LEAVE_SWEEP_BATCH)
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
        else:
            logger.warning(
                f"Leave-sweep: worker user_{order_join.account_id}/ offline; join row stays "
                f"for next sweep (Order #{order_join.order_id})."
            )
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
        return

    if left:
        await _mark_join_done(session_maker, order_join.id)


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



from workers.sender import _get_redis # وارد کردن کلاینت Redis از sender.py

# ==========================================
# 🛡 فاز ۲ (BUG-04): رجیستری busy — Distributed Lock با Redis
# ==========================================

async def _acquire_busy(account_db_id: int) -> bool:
    """رزرو اتمیک اکانت در Redis (اسکیل‌پذیر برای چند پروسه)"""
    redis = _get_redis()
    key = f"busy_worker:{account_db_id}"
    
    # استفاده از SET NX (فقط در صورتی که وجود نداشته باشد ست می‌شود)
    # انقضای ۲ ساعته (7200 ثانیه) به عنوان سپر ایمنی برای جلوگیری از قفل ماندن ابدی در صورت کرش شدید پروسه
    try:
        is_acquired = await redis.set(key, "1", nx=True, ex=7200)
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
    client: Client,
    account_db_id: int,
    order: Order,
    group_link: str,
    session_maker: async_sessionmaker[AsyncSession],
    bot: Bot
) -> list[str]:
    
    status_code, file_path, join_chat_id, joined_now = await extract_active_users(
        client, group_link, filter_type=order.filter_type
    )

    async with session_maker() as session:
        try:
            await _record_order_join(
                session, order.id, account_db_id, group_link,
                join_chat_id, joined_now, status_code,
            )
            await session.commit()
        except Exception as e:
            await session.rollback()
            logger.warning(f"Failed to record order_join for Order #{order.id}: {e}")

        if status_code == "pending_approval":
            new_retry_count = (order.retry_count or 0) + 1

            if new_retry_count >= PENDING_APPROVAL_RETRY_LIMIT:
                stmt = update(Order).where(Order.id == order.id).values(
                    status=OrderStatus.error,
                    scheduled_for=None,
                    retry_count=new_retry_count,
                )
                await session.execute(stmt)

                session.add(OrderLog(
                    order_id=order.id,
                    account_id=account_db_id,
                    target=group_link,
                    status="error",
                    error_message=(
                        f"Pending approval retry limit reached "
                        f"({PENDING_APPROVAL_RETRY_LIMIT} cycles)."
                    ),
                ))
                await session.commit()

                try:
                    await bot.send_message(
                        chat_id=config.ADMIN_ID,
                        text=(
                            f"⛔️ <b>سقف تلاش عضویت ریکوئستی پر شد!</b>\n\n"
                            f"سفارش: <code>{order.tracking_code}</code>\n"
                            f"گروه: <b>{group_link}</b>\n\n"
                            f"درخواست عضویت بعد از ۲۴ ساعت/چرخه هنوز تأیید نشده است.\n"
                            f"<i>سفارش استخراج متوقف شد. لطفاً لینک را بررسی کنید و در صورت نیاز سفارش جدید ثبت کنید.</i>"
                        )
                    )
                except Exception as e:
                    logger.error(f"Failed to notify admin about pending-approval retry limit: {e}")

                return []

            next_check = datetime.now(timezone.utc) + timedelta(hours=1)
            stmt = update(Order).where(Order.id == order.id).values(
                scheduled_for=next_check,
                retry_count=new_retry_count,
            )
            await session.execute(stmt)
            await session.commit()
            
            try:
                await bot.send_message(
                    chat_id=config.ADMIN_ID,
                    text=(
                        f"⏳ <b>درخواست عضویت ورکر ارسال شد!</b>\n\n"
                        f"سفارش: <code>{order.tracking_code}</code>\n"
                        f"گروه <b>{group_link}</b> خصوصی است و نیاز به تایید ادمینِ آن دارد.\n\n"
                        f"<i>سیستم این سفارش را به تعویق انداخت و ۱ ساعت دیگر مجدداً بررسی خواهد کرد. ورکر آزاد شد.</i>\n\n"
                        f"🕐 چرخه‌ی انتظار: <b>{new_retry_count} از {PENDING_APPROVAL_RETRY_LIMIT}</b>"
                    )
                )
            except Exception as e:
                logger.error(f"Failed to notify admin about pending approval: {e}")
                
            return [group_link]

        log_entry = OrderLog(order_id=order.id, account_id=account_db_id, target=group_link)
        
        if status_code == "success" and file_path and os.path.exists(file_path):
            log_entry.status = "success"

            # --- FIX M11 (b): Keep a stored reference ---
            permanent_path = f"exports/extract_order_{order.id}.txt"
            import shutil
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
            # --------------------------------------------

            # --- FIX M11 (a): Broadcast to all admins (Creator ID NOT-FOUND) ---
            from database.models import Admin
            admin_ids = []
            if getattr(config, "ADMIN_ID", None):
                admin_ids.append(int(config.ADMIN_ID))
            try:
                sub_admins = (await session.scalars(select(Admin.telegram_id))).all()
                admin_ids.extend(int(aid) for aid in sub_admins)
                admin_ids = list(set(admin_ids))
            except Exception as e:
                logger.error(f"Failed to fetch sub-admins for extract broadcast: {e}")

            sends_failed = False
            for admin_id in admin_ids:
                try:
                    document = FSInputFile(file_path)
                    await bot.send_document(
                        chat_id=admin_id,
                        document=document,
                        caption=(
                            f"✅ <b>عملیات استخراج تکمیل شد</b>\n\n"
                            f"سفارش: <code>{order.tracking_code}</code>\n"
                            f"تارگت: {group_link}"
                        )
                    )
                except Exception as e:
                    logger.error(f"Failed to send extracted file to admin {admin_id} via Bot: {e}")
                    sends_failed = True
            # -------------------------------------------------------------------

            # --- FIX M11 (c): Delete-after-send only after ALL sends succeeded ---
            if not sends_failed:
                try:
                    os.remove(file_path)
                    logger.info(f"Garbage Collection: Deleted extracted temp file {file_path}")
                except Exception:
                    pass
            # ---------------------------------------------------------------------

        else:
            log_entry.status = "error"
            if status_code == "error_not_admin":
                log_entry.error_message = "عدم دسترسی: ورکر ادمین کانال نیست."
                try:
                    await bot.send_message(
                        chat_id=config.ADMIN_ID,
                        text=(
                            f"⚠️ <b>خطای دسترسی در استخراج</b>\n\n"
                            f"سفارش: <code>{order.tracking_code}</code>\n"
                            f"تارگت: <b>{group_link}</b>\n\n"
                            f"<i>این تارگت یک کانال است. برای استخراج آیدی از کانال، اکانت ورکر باید حتماً ادمینِ کانال باشد. عملیات متوقف شد.</i>"
                        )
                    )
                except Exception as e:
                    logger.error(f"Failed to notify admin about channel access error: {e}")
            else:
                log_entry.error_message = "Extraction failed or access denied."
            
        session.add(log_entry)
        
        try:
            await session.commit()
        except Exception as db_err:
            await session.rollback()
            logger.error(f"DB Error saving extract log: {db_err}")
            
    return []


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

    try:
        links = _parse_target_links(group_link)

        aggregated_members: List[str] = []
        seen_members: set = set()
        failed_links: List[str] = []
        pending_link: Optional[str] = None

        status_code, members, join_chat_id, joined_now = "error", None, None, False

        for link in links:
            try:
                status_code, members, join_chat_id, joined_now = await extract_members_for_sending(
                    client, link, filter_type=filter_type
                )
            except Exception as e:
                logger.error(f"Link resolution crashed for {link}: {e}", exc_info=True)
                status_code, members, join_chat_id, joined_now = "error", None, None, False

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

            if status_code == "success":
                if members:
                    for m in members:
                        if m not in seen_members:
                            seen_members.add(m)
                            aggregated_members.append(m)
                else:
                    logger.info(
                        f"Order #{order_id}: link {link} resolved successfully but "
                        f"yielded 0 members with filter={filter_type}."
                    )
            else:
                failed_links.append(link)
                logger.warning(
                    f"Order #{order_id}: link {link} resolved with status={status_code}; "
                    f"link skipped."
                )

        if pending_link is not None:
            status_code = "pending_approval"
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
                    logger.warning(
                        f"Order #{order_id} is no longer running (status={order.status}). "
                        f"Aborting link resolution."
                    )
                    return

                display_code = order.tracking_code if order.tracking_code else f"ID-{order.id}"

                if status_code == "pending_approval":
                    order.retry_count = (order.retry_count or 0) + 1

                    if order.retry_count >= PENDING_APPROVAL_RETRY_LIMIT:
                        order.status = OrderStatus.error
                        order.scheduled_for = None
                        order.target_data = ""
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
                            error_message=(
                                f"Pending approval retry limit reached "
                                f"({PENDING_APPROVAL_RETRY_LIMIT} cycles)."
                            ),
                        ))

                        admin_notify_text = (
                            f"⛔️ <b>سقف تلاش عضویت ریکوئستی پر شد! (سفارش ارسال لینکی)</b>\n\n"
                            f"سفارش: <code>{display_code}</code>\n"
                            f"گروه: <b>{html.escape(group_link)}</b>\n\n"
                            f"درخواست عضویت بعد از ۲۴ ساعت/چرخه هنوز تأیید نشده است.\n"
                            f"<i>سفارش متوقف شد. لطفاً لینک را بررسی کنید و در صورت نیاز سفارش جدید ثبت کنید.</i>"
                        )
                    else:
                        next_check = datetime.now(timezone.utc) + timedelta(hours=1)
                        order.scheduled_for = next_check
                        order.status = OrderStatus.pending
                        admin_notify_text = (
                            f"⏳ <b>درخواست عضویت ورکر ارسال شد! (سفارش ارسال لینکی)</b>\n\n"
                            f"سفارش: <code>{display_code}</code>\n"
                            f"گروه <b>{html.escape(group_link)}</b> خصوصی است و نیاز به تایید ادمینِ آن دارد.\n\n"
                            f"<i>سیستم این سفارش را به تعویق انداخت و ۱ ساعت دیگر مجدداً بررسی خواهد کرد. ورکر آزاد شد.</i>\n\n"
                            f"🕐 چرخه‌ی انتظار: <b>{order.retry_count} از {PENDING_APPROVAL_RETRY_LIMIT}</b>"
                        )

                elif status_code != "success" or not members:
                    if not links:
                        reason = "هیچ لینک معتبری (t.me یا @username) در متن سفارش یافت نشد."
                    elif status_code == "success":
                        reason = "هیچ عضو قابل ارسالی با این فیلتر یافت نشد."
                    elif status_code == "error_not_admin":
                        reason = "این تارگت یک کانال است و ورکر ادمین آن نیست؛ استخراج اعضا ممکن نیست."
                    else:
                        reason = "استخراج اعضای گروه ناموفق بود یا دسترسی وجود ندارد."

                    order.status = OrderStatus.error
                    order.target_data = ""
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

                else:
                    cap = order.target_count or 0
                    if cap > 0 and len(members) > cap:
                        logger.info(
                            f"Order #{order_id}: applying target_count cap "
                            f"({len(members)} -> {cap} targets)."
                        )
                        members = members[:cap]

                    order.target_data = "\n".join(members)
                    order.filter_type = None
                    order.status = OrderStatus.pending
                    order.scheduled_for = None

                    logger.info(
                        f"Order #{order_id}: link resolved to {len(members)} sendable targets. "
                        f"Re-queued for dispatch."
                    )

                    failed_note = ""
                    if failed_links:
                        failed_note = (
                            f"\n⚠️ لینک‌های ناموفق ({len(failed_links)}): "
                            f"{html.escape(', '.join(failed_links))}\n"
                        )

                    admin_notify_text = (
                        f"✅ <b>اعضای گروه استخراج شدند</b>\n\n"
                        f"سفارش: <code>{display_code}</code>\n"
                        f"گروه(ها): <b>{html.escape(group_link or '')}</b>\n"
                        f"تعداد تارگت‌های آماده ارسال: <b>{len(members)}</b>\n"
                        f"{failed_note}"
                        f"<i>سفارش به صف ارسال بازگشت؛ ارسال انبوه به‌زودی آغاز می‌شود.</i>"
                    )

        if admin_notify_text:
            try:
                await bot.send_message(chat_id=config.ADMIN_ID, text=admin_notify_text)
            except Exception as e:
                logger.error(f"Failed to notify admin about link resolution (Order #{order_id}): {e}")

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
                        .values(status=OrderStatus.error, target_data="")
                    )
            try:
                await bot.send_message(
                    chat_id=config.ADMIN_ID,
                    text=(
                        f"⛔️ <b>خطای بحرانی در آماده‌سازی سفارش ارسال لینکی</b>\n\n"
                        f"سفارش: <code>#{order_id}</code>\nتارگت: <b>{html.escape(group_link)}</b>\n\n"
                        f"<i>سفارش متوقف شد. جزئیات در لاگ سرور.</i>"
                    )
                )
            except Exception:
                pass
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
) -> List[str]:
    """
    اجرای ایزوله تسک ارسال انبوه.
    این تابع لیست تارگت‌های ارسال‌نشده (Unsent Targets) را برمی‌گرداند.

    (🎨 آپدیت چرخش بنر: اگر order.use_banner_pool فعال باشد، برای «این chunk» یک بنر
    تصادفی از مخزن انتخاب شده (ORDER BY RAND() LIMIT 1) و شمارنده‌ی مصرف آن یک واحد
    زیاد می‌شود؛ سپس متن/مدیای chunk با آن بنر جایگزین می‌گردد. ردیف دیتابیس سفارش و
    نمونه‌ی مشترک order دست‌نخورده می‌مانند. اگر بنر فعالی نباشد، سفارش با متن خودش
    ادامه می‌دهد و فقط یک‌بار به ادمین هشدار داده می‌شود.)
    """
    async with session_maker() as session:
        effective_order = order

        # ==========================================
        # 🎨 چرخش بنر: انتخاب بنر برای این chunk (بخش ب)
        # ==========================================
        if order.use_banner_pool:
            try:
                # بهینه‌سازی: واکشی IDها و انتخاب رندوم در حافظه به جای ORDER BY RAND() در دیتابیس
                active_banner_ids = (await session.scalars(
                    select(Banner.id)
                    .where(Banner.is_active == True)  # noqa: E712
                )).all()

                banner = None
                if active_banner_ids:
                    chosen_id = random.choice(active_banner_ids)
                    banner = await session.scalar(
                        select(Banner).where(Banner.id == chosen_id)
                    )

                if banner is not None:
                    # افزایش «اتمی» شمارنده‌ی مصرف بنر (ضد رقابت بین ورکرهای هم‌زمان)
                    await session.execute(
                        update(Banner)
                        .where(Banner.id == banner.id)
                        .values(usage_count=Banner.usage_count + 1)
                    )
                    await session.commit()

                    logger.info(
                        f"Order #{order.id}: banner #{banner.id} assigned to chunk of "
                        f"worker user_{account_db_id}/ (usage={banner.usage_count + 1})."
                    )
                    effective_order = _build_chunk_order(order, banner)

                else:
                    # هیچ بنر فعالی وجود ندارد → سفارش با متن خودش ادامه می‌دهد + هشدار ادمین
                    if order.id not in _banner_warned_orders:
                        _banner_warned_orders.add(order.id)
                        try:
                            await bot.send_message(
                                chat_id=config.ADMIN_ID,
                                text=(
                                    f"⚠️ <b>مخزن بنر خالی است!</b>\n\n"
                                    f"سفارش <code>{order.tracking_code if order.tracking_code else order.id}</code> "
                                    f"برای استفاده از مخزن بنر تنظیم شده، اما هیچ بنر فعالی وجود ندارد.\n"
                                    f"<i>این سفارش با متن خودش ارسال خواهد شد. "
                                    f"از «🎨 مدیریت بنرها» یک بنر ثبت/فعال کنید.</i>"
                                )
                            )
                        except Exception as e:
                            logger.error(f"Failed to notify admin about empty banner pool: {e}")

            except Exception as e:
                # سپر: هر خطایی در مخزن بنر نباید جلوی ارسال را بگیرد → fallback به متن سفارش
                await session.rollback()
                logger.error(
                    f"Banner pool selection failed for Order #{order.id}; "
                    f"fall back to order text. ({e})", exc_info=True
                )

        # 🛡 فاز ۲ (BUG-03/ج): اعمال cooldown_hours — «پایان این chunk» ثبت می‌شود
        # (کلید chunk_cooldown:{account_id} با TTL=cooldown_hours). دیسپچر تا انقضای
        # TTL به این اکانت chunk جدید نمی‌دهد؛ بنابراین requeue فوریِ باقی‌مانده‌ی
        # تارگت‌ها دیگر به «ارسال پیوسته‌ی همان اکانت» منجر نمی‌شود.
        # مقدار از GlobalSettings خوانده می‌شود تا ویرایش ادمین در settings_handlers
        # واقعاً روی اجرا اثر کند (همان نام کلید؛ اثر از chunk بعدیِ این اکانت).
        # حتی اگر execute_bulk_send کرش کند cooldown اعمال می‌شود (محافظه‌کارانه).
        try:
            unsent_targets = await execute_bulk_send(
                client=client,
                account_db_id=account_db_id,
                order=effective_order,
                targets=targets,
                session=session
            )
        except Exception as e:

            logger.error(
                f"Worker user_{account_db_id}/ crashed mid-chunk for Order #{order.id}: "
                f"{e.__class__.__name__}: {e}", exc_info=True
            )
            unsent_targets = await _salvage_unsent_after_crash(
                session, order.id, account_db_id, targets
            )
        finally:

            cooldown_hours = 24  # پیش‌فرض محافظه‌کارانه (همان default مدل GlobalSettings)
            try:
                settings_row = await session.scalar(
                    select(GlobalSettings).limit(1)
                )
                if settings_row is not None and settings_row.cooldown_hours is not None:
                    cooldown_hours = settings_row.cooldown_hours
            except Exception as e:
                logger.warning(
                    f"Could not load cooldown_hours for worker user_{account_db_id}/ "
                    f"({e.__class__.__name__}: {e}); using default 24h."
                )
            await mark_chunk_cooldown(account_db_id, cooldown_hours)

        return unsent_targets


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


# ⏱ فاصله‌ی polling دیسپچر — همان «سیکل ۱۰ثانیه‌ای» DEP-3
DISPATCH_INTERVAL_SECONDS = 10

# 🛡 نگه‌داری مرجع تسک‌های پس‌زمینه (الگوی مستند asyncio: نتیجه‌ی create_task
# باید مرجع قوی داشته باشد تا تسک در حین اجرا GC نشود)
_running_background_tasks: set = set()


def _spawn_background_task(coro) -> asyncio.Task:
    """create_task امن: مرجع در مجموعه نگه داشته می‌شود و بعد از done آزاد می‌گردد."""
    task = asyncio.create_task(coro)
    _running_background_tasks.add(task)
    task.add_done_callback(_running_background_tasks.discard)
    return task
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

                    # بازگرداندن تارگت‌های ناموفق/ارسال‌نشده به صف انتظار
                    if unsent_targets and order.status in (
                        OrderStatus.running, OrderStatus.pending
                    ):
                        current = [
                            t for t in (order.target_data or "").split("\n") if t.strip()
                        ]
                        merged = current + [t for t in unsent_targets if t.strip()]
                        order.target_data = "\n".join(merged)

                    if order.status != OrderStatus.running:
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
                    except Exception as stat_err:
                        logger.warning(
                            f"Finalize: stats query for success_count failed for Order #{order_id}: {stat_err}"
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
                                    order.target_data = ""
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
                    admin_notify_text = (
                        "✅ <b>سفارش با موفقیت تکمیل شد!</b>\n\n"
                        f"سفارش: <code>{display_code}</code>\n\n"
                        "📊 <b>گزارش نهایی:</b>\n"
                        f"▫️ عملیات موفق: <code>{success_count}</code>\n"
                        f"▫️ عملیات ناموفق: <code>{error_count}</code>\n\n"
                        "<i>همه‌ی تارگت‌های صف پردازش شدند.</i>"
                    )

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
                await bot.send_message(
                    chat_id=config.ADMIN_ID,
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
        try:
            await bot.send_message(chat_id=config.ADMIN_ID, text=admin_notify_text)
        except Exception as e:
            logger.error(
                f"Failed to notify admin about completion of Order #{order_id}: {e}"
            )

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


async def background_order_execution(
    client: Client,
    account_db_id: int,
    order_id: int,
    targets: List[str],
    session_maker: async_sessionmaker[AsyncSession],
    bot: Bot,
) -> None:

    try:
        unsent_targets: Optional[List[str]] = None
        try:
            # --- واکشی تازه‌ی سفارش (سشن کوتاه؛ بدون I/O شبکه داخل آن) ---
            async with session_maker() as session:
                order = await session.scalar(select(Order).where(Order.id == order_id))

            if order is None:
                logger.warning(
                    f"Background execution: Order #{order_id} was deleted - "
                    f"worker user_{account_db_id}/ released without requeue."
                )
            elif _is_extract_order(order):
                # سفارش استخراج: targets[0] لینک گروه است
                group_link = targets[0] if targets else ""
                unsent_targets = await extractor_task_wrapper(
                    client, account_db_id, order, group_link, session_maker, bot
                ) or []
            else:
                unsent_targets = await worker_task_wrapper(
                    client, account_db_id, order, targets, session_maker, bot
                ) or []

        except Exception as e:
            logger.error(
                f"Background execution crashed for Order #{order_id} / worker "
                f"user_{account_db_id}/: {e}",
                exc_info=True,
            )
            # 🔴 مکمل فاز ۲ (یافته ۵): بازیابی پس از کرش — فقط تارگت‌های «بدون لاگ»
            # یا با آخرین لاگ retryable برمی‌گردند (جلوگیری از ارسال تکراری)
            try:
                async with session_maker() as salvage_session:
                    unsent_targets = await _salvage_unsent_after_crash(
                        salvage_session, order_id, account_db_id, targets
                    )
            except Exception as salvage_err:
                logger.error(
                    f"Salvage failed for Order #{order_id} "
                    f"(worker user_{account_db_id}/): {salvage_err}"
                )
                unsent_targets = []  # مسیر امن: بدون requeue

        # کاهش شمارنده‌ی inflight — دقیقاً یک‌بار در هر مسیر خروج (حتی سفارش حذف‌شده)
        remaining_inflight = await _decrement_inflight_chunks(order_id)

        if unsent_targets is not None:
            try:
                await _finalize_chunk(
                    order_id, unsent_targets, remaining_inflight, session_maker, bot, chunk_targets=targets
                )
            except Exception as fin_err:
                logger.error(
                    f"Finalize raised for Order #{order_id}: {fin_err}", exc_info=True
                )
    finally:
        # 🛡 فاز ۲ (BUG-04): آزادسازی حتمی رزرو busy — همیشه اجرا می‌شود
        await _release_busy(account_db_id)

async def order_dispatcher_loop(
    session_maker: async_sessionmaker[AsyncSession], 
    worker_pool: Dict[int, Client],
    bot: Bot
) -> None:
    # 🚀 فاز ۹ (DEP-3): حداکثر تعداد سفارشی که در هر سیکل ۱۰ثانیه‌ای «شروع» می‌شود.
    # مقدار از config (env: DISPATCH_BATCH_SIZE، پیش‌فرض ۳). مقادیر > ۵ توصیه نمی‌شود.
    try:
        dispatch_batch = max(1, int(getattr(config, "DISPATCH_BATCH_SIZE", 3)))
    except (TypeError, ValueError):
        dispatch_batch = 3
    logger.info(
        f"Order Dispatcher Loop started. Polling for pending orders "
        f"(batch={dispatch_batch} order(s)/cycle)."
    )
    
    # برای جلوگیری از تکرار لاگ‌ها و نوتیف ادمین، شناسه‌ی سفارش‌هایی که درباره‌شان
    # در وضعیت خاصی لاگ/نوتیف زده شده ثبت می‌شود.
    _logged_pending_orders: set = set()
    _warmup_notified_orders: set = set()

    while True:
        # 🚪 فاز ۶ (R3-ب): sweep دوره‌ای leave
        _maybe_schedule_leave_sweep(session_maker, worker_pool, bot)

        # 🐌 ترمز جهانی (Global Slowdown)
        if await is_global_slowdown():
            logger.debug("Dispatcher: global slowdown active - skipping dispatch cycle.")
            await asyncio.sleep(DISPATCH_INTERVAL_SECONDS)
            continue

        _batch_seen_order_ids: set = set()
        active_pending_orders_this_cycle = set()

        for _cycle in range(dispatch_batch):
            tasks_to_run = []
            link_resolution_tasks = []

            send_jobs = []       
            extract_jobs = []    
            resolver_jobs = []
            
            order_id_db = None
            dispatched_targets_count = 0
            busy_account_ids: List[int] = []
            background_scheduled = False
            link_resolver_scheduled = False
            empty_target_notify_code: Optional[str] = None
            
            try:
                # ==========================================
                # فاز ۱: واکشی هوشمند سفارش و اختصاص تارگت‌ها (نسخه ضد-قفل)
                # ==========================================
                async with session_maker() as session:
                    now_utc = datetime.now(timezone.utc)
                    now_naive = datetime.now(timezone.utc).replace(tzinfo=None)  # 🛡 برای چک سقف FloodWait

                    pending_filters = [
                        Order.status == OrderStatus.pending,
                        Order.is_approved == True,  # 🛡 فقط سفارش‌های تایید شده توسط ادمین
                        or_(
                            Order.scheduled_for.is_(None),
                            Order.scheduled_for <= now_utc,
                        ),
                    ]
                    if _batch_seen_order_ids:
                        pending_filters.append(Order.id.notin_(_batch_seen_order_ids))
                    pending_stmt = (
                        select(Order)
                        .where(*pending_filters)
                        .order_by(Order.id.asc())
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

                    settings_row = (await session.scalars(select(GlobalSettings).limit(1))).first()
                    send_limit_per_run = (settings_row.send_limit_per_run if settings_row else None) or 0

                    order_cat_ids: List[int] = list(
                        (await session.scalars(
                            select(order_category_assoc.c.category_id)
                            .where(order_category_assoc.c.order_id == order.id)
                        )).all()
                    )

                    # --- گزینش ورکرهای واجد شرایط (متصل + سالم + گرم‌شده) ---
                    connected_ids = [
                        acc_id for acc_id, w_client in list(worker_pool.items())
                        if getattr(w_client, "is_connected", False)
                    ]
                    
                    eligible_accounts: List[Account] = []
                    
                    # متغیرهای تفکیک علل عدم اجرای سفارش
                    total_connected = len(connected_ids)
                    accounts_passed_category = 0
                    accounts_delayed_by_warmup = 0
                    max_warmup_end_time = None

                    if connected_ids:
                        acc_filters = [
                            Account.id.in_(connected_ids),
                            Account.is_banned == False,
                            or_(Account.flood_wait_until.is_(None), Account.flood_wait_until <= now_naive)
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
                                # محاسبه طولانی‌ترین زمان انتظار (برای پیام ادمین)
                                w_time = acc.warmed_up_at
                                if w_time:
                                    if w_time.tzinfo is None:
                                        w_time = w_time.replace(tzinfo=timezone.utc)
                                    if not max_warmup_end_time or w_time > max_warmup_end_time:
                                        max_warmup_end_time = w_time

                    # ==========================================
                    # منطق لاگ‌زنی شفاف و ارسال נוتیف ادمین (یک‌بار)
                    # ==========================================
                    if not eligible_accounts:
                        # سفارش در این سیکل نمی‌تواند اجرا شود.
                        # لاگ فقط زمانی زده می‌شود که سفارش قبلاً در وضعیت pending (و لاگ‌شده) نبوده است
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
                        
                        # ارسال נוتیف به ادمین: تنها زمانی که تنها دلیل اجرا نشدن، "گرم‌شدن" باشد
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
                            try:
                                await bot.send_message(chat_id=config.ADMIN_ID, text=notify_text)
                            except Exception as notify_err:
                                logger.error(f"Failed to notify admin about warmup delay (Order #{order.id}): {notify_err}")
                                
                    else:
                        # اگر سفارش در این چرخه قابلیت اجرا پیدا کرده است، از لیست‌های نگه‌داری لاگ حذف شود
                        _logged_pending_orders.discard(order.id)
                        _warmup_notified_orders.discard(order.id)


                    # ==========================================
                    # مسیر ۱: سفارش استخراج — یک ورکر + اولین لینک معتبر
                    # ==========================================
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
                            for acc in eligible_accounts:
                                if await _acquire_busy(acc.id):
                                    picked_acc_id = acc.id
                                    picked_client = worker_pool[acc.id]
                                    break
                            if picked_acc_id is not None:
                                busy_account_ids.append(picked_acc_id)
                                
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
                                    await _release_busy(picked_acc_id)
                                    busy_account_ids.remove(picked_acc_id)
                                    continue
                                
                                await _register_inflight_chunks(order.id, 1)
                                extract_jobs.append((picked_client, picked_acc_id, group_link))

                    # ==========================================
                    # مسیر ۲: سفارش ارسالِ resolveنشده — رزولور لینک
                    # ==========================================
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
                            picked_client = None
                            picked_acc_id = None
                            for acc in eligible_accounts:
                                if await _acquire_busy(acc.id):
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

                    # ==========================================
                    # مسیر ۳: ارسال معمولی — chunk کردن تارگت‌ها بین ورکرها
                    # ==========================================
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
                            for acc in eligible_accounts:
                                if not remaining_targets:
                                    break
                                if await is_in_cooldown(acc.id):
                                    continue
                                acc_daily_limit = effective_daily_limit(acc.created_at)  
                                if await daily_cap_reached(acc.id, acc_daily_limit):  
                                    continue
                                if not await _acquire_busy(acc.id):
                                    continue
                                
                                chunk_limit = acc_daily_limit
                                if send_limit_per_run and send_limit_per_run > 0:
                                    chunk_limit = min(chunk_limit, send_limit_per_run)
                                    
                                logger.debug(f"Order #{order.id}: chunk_limit={chunk_limit} (daily={acc_daily_limit}, per_run={send_limit_per_run})")

                                if not chunk_limit or chunk_limit <= 0:
                                    await _release_busy(acc.id)
                                    continue
                                chunk = remaining_targets[:chunk_limit]
                                remaining_targets = remaining_targets[chunk_limit:]
                                busy_account_ids.append(acc.id)
                                send_jobs.append((worker_pool[acc.id], acc.id, chunk))
                                dispatched_targets_count += len(chunk)

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

                # ==========================================
                # فاز ۲: ساخت تسک‌های پس‌زمینه — فقط «بعد از» commit
                # ==========================================
                for _client, _acc_id, _chunk in send_jobs:
                    task = _spawn_background_task(
                        background_order_execution(
                            _client, _acc_id, order_id_db, _chunk, session_maker, bot
                        )
                    )
                    tasks_to_run.append(task)
                    background_scheduled = True
                    if _acc_id in busy_account_ids:
                        busy_account_ids.remove(_acc_id)

                for _client, _acc_id, _group_link in extract_jobs:
                    task = _spawn_background_task(
                        background_order_execution(
                            _client, _acc_id, order_id_db, [_group_link],
                            session_maker, bot
                        )
                    )
                    tasks_to_run.append(task)
                    background_scheduled = True
                    if _acc_id in busy_account_ids:
                        busy_account_ids.remove(_acc_id)

                for _client, _acc_id, _raw_links, _filter_type in resolver_jobs:
                    task = _spawn_background_task(
                        link_send_resolver_wrapper(
                            _client, _acc_id, order_id_db, _raw_links, _filter_type,
                            session_maker, bot
                        )
                    )
                    link_resolution_tasks.append(task)
                    link_resolver_scheduled = True
                    if _acc_id in busy_account_ids:
                        busy_account_ids.remove(_acc_id)

                if tasks_to_run or link_resolution_tasks:
                    logger.info(
                        f"Dispatcher: Order #{order_id_db} ({order_tracking_code}) → "
                        f"{len(tasks_to_run)} chunk/extract task(s) + "
                        f"{len(link_resolution_tasks)} link resolver(s) "
                        f"({dispatched_targets_count} target(s) dispatched)."
                    )

                if empty_target_notify_code:
                    try:
                        await bot.send_message(
                            chat_id=config.ADMIN_ID,
                            text=(
                                f"⚠️ <b>سفارش بدون تارگت معتبر</b>\n\n"
                                f"سفارش <code>{empty_target_notify_code}</code> هیچ تارگت قابل "
                                f"پردازشی ندارد و به حالت خطا منتقل شد.\n"
                                f"<i>لطفاً محتوای سفارش را بررسی کنید.</i>"
                            )
                        )
                    except Exception as notify_err:
                        logger.error(
                            f"Failed to notify admin about empty-target order "
                            f"#{order_id_db}: {notify_err}"
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
                if order_id_db is not None and not (
                    background_scheduled or link_resolver_scheduled
                ):
                    async with _order_inflight_lock:
                        _order_inflight_chunks.pop(order_id_db, None)

        # پاک‌سازی سفارش‌هایی که دیگر pending نیستند از لاگ‌های تکراری
        _logged_pending_orders.intersection_update(active_pending_orders_this_cycle)
        _warmup_notified_orders.intersection_update(active_pending_orders_this_cycle)

        # ⏱ خواب ۱۰ ثانیه‌ای تا سیکل بعدی
        await asyncio.sleep(DISPATCH_INTERVAL_SECONDS)

import aiofiles.os
from pathlib import Path

# ==========================================
# 🧹 فاز ۳: زباله‌روب خودکار فایل‌های موقت (Garbage Collector)
# ==========================================

async def temp_file_gc_loop(cleanup_interval_hours: int = 12, max_age_hours: int = 24):
    """
    تسک پس‌زمینه برای پاکسازی فایل‌های قدیمی در پوشه‌های downloads و exports
    این تسک به صورت مستقل در کنار دیسپچر اجرا می‌شود.
    """
    directories_to_clean = ["downloads", "exports"]
    max_age_seconds = max_age_hours * 3600

    logger.info(f"Temp File Garbage Collector started. Running every {cleanup_interval_hours} hours.")

    while True:
        try:
            now = time.time()
            deleted_count = 0

            for dir_name in directories_to_clean:
                dir_path = Path(dir_name)
                if not dir_path.exists() or not dir_path.is_dir():
                    continue

                for file_path in dir_path.iterdir():
                    if not file_path.is_file() or file_path.name == ".gitkeep":
                        continue

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

        # خواب تا سیکل بعدی پاکسازی
        await asyncio.sleep(cleanup_interval_hours * 3600)
