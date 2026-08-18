import asyncio
import logging
import os
from datetime import datetime, timezone
from typing import Dict, List

from pyrogram import Client
from sqlalchemy import select, or_
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from database.models import Order, OrderStatus, Account, GlobalSettings, OrderLog
from workers.sender import execute_bulk_send
from workers.extractor import extract_active_users
from config import config

logger = logging.getLogger(__name__)


async def extractor_task_wrapper(
    client: Client,
    account_db_id: int,
    order: Order,
    group_link: str,
    session_maker: async_sessionmaker[AsyncSession]
) -> List[str]:
    file_path = await extract_active_users(client, group_link)
    
    async with session_maker() as session:
        log_entry = OrderLog(order_id=order.id, account_id=account_db_id, target=group_link)
        
        if file_path and os.path.exists(file_path):
            log_entry.status = "success"
            try:
                await client.send_document(
                    chat_id=config.ADMIN_ID,
                    document=file_path,
                    caption=f"✅ <b>عملیات استخراج تکمیل شد</b>\n\nسفارش: <code>#{order.id}</code>\nتارگت: {group_link}"
                )
            except Exception as e:
                logger.error(f"Failed to send extracted file to admin: {e}")
            finally:
                # 🔴 رفع باگ نشت حافظه: پاکسازی فایل گلدن لیست از روی هارد سرور
                try:
                    os.remove(file_path)
                    logger.info(f"Garbage Collection: Deleted extracted file {file_path}")
                except Exception as e:
                    logger.warning(f"Could not delete {file_path}: {e}")
        else:
            log_entry.status = "error"
            log_entry.error_message = "Extraction failed or access denied."
            
        session.add(log_entry)
        
        try:
            await session.commit()
        except Exception as db_err:
            await session.rollback()
            logger.error(f"DB Error saving extract log: {db_err}")
            
    return []

async def worker_task_wrapper(
    client: Client, 
    account_db_id: int, 
    order: Order, 
    targets: list[str], 
    session_maker: async_sessionmaker[AsyncSession]
) -> List[str]:
    """
    اجرای ایزوله تسک ارسال انبوه.
    این تابع لیست تارگت‌های ارسال‌نشده (Unsent Targets) را برمی‌گرداند.
    """
    async with session_maker() as session:
        unsent_targets = await execute_bulk_send(
            client=client,
            account_db_id=account_db_id,
            order=order,
            targets=targets,
            session=session
        )
        return unsent_targets

async def order_dispatcher_loop(
    session_maker: async_sessionmaker[AsyncSession], 
    worker_pool: Dict[int, Client]
) -> None:
    logger.info("Order Dispatcher Loop started. Polling for pending orders...")
    
    while True:
        tasks_to_run = []
        order_id_db = None
        dispatched_targets_count = 0
        
        try:
            # ==========================================
            # فاز ۱: واکشی هوشمند سفارش و اختصاص تارگت‌ها (نسخه ضد-قفل)
            # ==========================================
            async with session_maker() as session:
                async with session.begin():
                    # واکشی تا ۱۰ سفارش قدیمی‌تر در حالت انتظار
                    order_stmt = select(Order).where(
                        Order.status == OrderStatus.pending,
                        or_(
                            Order.scheduled_for.is_(None),
                            Order.scheduled_for <= datetime.now(timezone.utc)
                        )
                    ).order_by(Order.id.asc()).limit(10)
                    
                    order_result = await session.execute(order_stmt)
                    pending_orders = order_result.scalars().all()

                    order = None
                    active_workers = []

                    # جستجو میان سفارش‌های معلق برای یافتن اولین سفارشی که ورکر آزاد دارد
                    for potential_order in pending_orders:
                        now = datetime.now(timezone.utc)
                        acc_stmt = select(Account).where(
                            Account.category_id == potential_order.category_id,
                            Account.is_banned == False,
                            or_(
                                Account.flood_wait_until.is_(None),
                                Account.flood_wait_until <= now
                            )
                        )
                        acc_result = await session.execute(acc_stmt)
                        available_accounts = acc_result.scalars().all()
                        
                        workers_for_this_order = [
                            (acc.id, worker_pool[acc.id]) 
                            for acc in available_accounts if acc.id in worker_pool
                        ]

                        if workers_for_this_order:
                            order = potential_order
                            active_workers = workers_for_this_order
                            break # پیدا شدن سفارش قابل اجرا و خروج از حلقه جستجو

                    if order:
                        order_id_db = order.id
                        logger.info(f"Picked up pending Order #{order_id_db}")

                        # ==========================================
                        # مسیر اول: سفارش استخراج (Extractor)
                        # ==========================================
                        if order.order_type == "extract":
                            acc_id, client = active_workers[0]
                            group_link = order.target_data.strip()
                            
                            logger.info(f"Assigning EXTRACTION of {group_link} to Worker {client.name}")
                            
                            tasks_to_run.append(
                                extractor_task_wrapper(
                                    client=client,
                                    account_db_id=acc_id,
                                    order=order,
                                    group_link=group_link,
                                    session_maker=session_maker
                                )
                            )
                            order.target_data = ""
                            order.status = OrderStatus.running
                            await session.flush()
                            
                        # ==========================================
                        # مسیر دوم: سفارش ارسال انبوه (Sender)
                        # ==========================================
                        else:
                            settings_stmt = select(GlobalSettings).where(GlobalSettings.id == 1)
                            settings_result = await session.execute(settings_stmt)
                            settings = settings_result.scalar_one_or_none()
                            send_limit = settings.send_limit_per_run if settings else 40
                            
                            all_targets = [t.strip() for t in order.target_data.split('\n') if t.strip()]
                            
                            for acc_id, client in active_workers:
                                chunk = all_targets[dispatched_targets_count : dispatched_targets_count + send_limit]
                                if not chunk:
                                    break 
                                    
                                dispatched_targets_count += len(chunk)
                                logger.info(f"Assigning {len(chunk)} targets to Worker {client.name}")
                                
                                tasks_to_run.append(
                                    worker_task_wrapper(
                                        client=client, 
                                        account_db_id=acc_id, 
                                        order=order, 
                                        targets=chunk, 
                                        session_maker=session_maker
                                    )
                                )
                            
                            remaining_unassigned = all_targets[dispatched_targets_count:]
                            order.target_data = '\n'.join(remaining_unassigned)
                            order.status = OrderStatus.running
                            await session.flush()
                    else:
                        if pending_orders:
                            logger.warning("سفارشاتی در صف وجود دارند اما هیچ ورکری برای دسته‌بندی آن‌ها آزاد نیست. در انتظار آزادسازی ورکرها...")
            
            # ==========================================
            # فاز ۲: اجرای همزمان ورکرها خارج از تراکنش دیتابیس
            # ==========================================
            returned_unsent_targets = []
            if tasks_to_run:
                results = await asyncio.gather(*tasks_to_run, return_exceptions=True)
                
                for res in results:
                    if isinstance(res, Exception):
                        logger.error(f"Worker Exception during dispatch: {res}")
                    elif isinstance(res, list):
                        returned_unsent_targets.extend(res)
                        
            # ==========================================
            # فاز ۳: جمع‌بندی وضعیت سفارش و پاکسازی امن فایل‌ها
            # ==========================================
            if order_id_db and tasks_to_run:
                async with session_maker() as session:
                    async with session.begin():
                        order_stmt = select(Order).where(Order.id == order_id_db)
                        order_result = await session.execute(order_stmt)
                        
                        # رفع باگ کرش دیسپچر: استفاده از هندلینگ امن
                        order = order_result.scalar_one_or_none()
                        
                        if not order:
                            logger.warning(f"Order #{order_id_db} was deleted from database during execution! Skipping cleanup.")
                            continue
                        
                        # 🔴 منطق Kill Switch
                        if order.status == OrderStatus.error:
                            logger.warning(f"Order #{order_id_db} was KILLED by admin. Discarding remaining targets.")
                            if order.media_path and os.path.exists(order.media_path):
                                try:
                                    os.remove(order.media_path)
                                    logger.info(f"Garbage Collection: Deleted media file {order.media_path} for killed Order #{order_id_db}.")
                                except Exception:
                                    pass
                            continue 
                        
                        # منطق عادی برای سفارشات فعال
                        current_targets = [t.strip() for t in order.target_data.split('\n') if t.strip()]
                        all_remaining_targets = current_targets + returned_unsent_targets
                        
                        if all_remaining_targets:
                            order.target_data = '\n'.join(all_remaining_targets)
                            order.status = OrderStatus.pending
                            logger.info(f"Order #{order_id_db}: Re-queued {len(all_remaining_targets)} remaining targets.")
                        else:
                            order.target_data = ""
                            order.status = OrderStatus.completed
                            logger.info(f"Order #{order_id_db}: All targets processed. Marking as completed.")
                            
                            if order.media_path and os.path.exists(order.media_path):
                                try:
                                    os.remove(order.media_path)
                                    logger.info(f"Garbage Collection: Deleted media file {order.media_path} for Order #{order_id_db}.")
                                except Exception as e:
                                    logger.error(f"Failed to delete media file {order.media_path}: {e}")

        # --- این دو بخش که پاک شده بودند باید دقیقاً در این سطح از تورفتگی برگردند ---
        except Exception as e:
            logger.error(f"Critical error in dispatcher loop: {e}", exc_info=True)
            
        await asyncio.sleep(10)
        await asyncio.sleep(10)