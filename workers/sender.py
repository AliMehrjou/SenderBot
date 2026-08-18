import asyncio
import logging
import random
from typing import List
from database.models import OrderLog, Order, OrderStatus
from sqlalchemy import select
from pyrogram import Client
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import (
    FloodWait, 
    UserIsBlocked, 
    PeerIdInvalid, 
    UsernameInvalid,
    UsernameNotOccupied,
    UserIsBot,
    UserRestricted
)
from sqlalchemy.ext.asyncio import AsyncSession

from database.models import OrderLog, Order
from utils.anti_ban import parse_spintax, apply_adaptive_flood_wait
from utils.advanced_anti_ban import appeal_to_spambot

logger = logging.getLogger(__name__)

async def execute_bulk_send(
    client: Client, 
    account_db_id: int, 
    order: Order, 
    targets: List[str], 
    session: AsyncSession
) -> List[str]:
    """
    موتور اصلی ارسال پیام 
    (آپدیت فاز ۴: افزودن تشخیص توقف اضطراری برای جلوگیری از فانتوم ورکرها)
    """
    logger.info(f"Worker user_{account_db_id}/ starting chunk for Order #{order.id}.")
    
    success_count = 0
    unsent_targets = []
    
    raw_message = order.message_text or ""
    
    # تبدیل دکمه شیشه‌ای به لینک (سازگار با یوزربات)
    if order.button_text and order.button_url:
        raw_message += f"\n\n🔗 <a href='{order.button_url}'>{order.button_text}</a>"

    safe_message = raw_message.replace("{first_name}", "[[FIRST_NAME]]").replace("{username}", "[[USERNAME]]")

    for i, target in enumerate(targets):
        target = target.strip()
        if not target:
            continue

        # 🔴 سیستم تشخیص فوری توقف اضطراری (Kill Switch)
        # چک می‌کنیم که آیا ادمین سفارش را در حین اجرای این دسته لغو کرده است یا خیر
        current_status = await session.scalar(select(Order.status).where(Order.id == order.id))
        if current_status == OrderStatus.error:
            logger.warning(f"Kill Switch activated! Worker user_{account_db_id}/ aborting chunk.")
            unsent_targets.extend(targets[i:])
            break # خروج فوری از حلقه ارسال

        spintaxed_text = parse_spintax(safe_message)
        final_text = spintaxed_text
        
        if "[[FIRST_NAME]]" in spintaxed_text or "[[USERNAME]]" in spintaxed_text:
            try:
                user_info = await client.get_users(target)
                first_name = user_info.first_name or "دوست عزیز"
                username = f"@{user_info.username}" if user_info.username else str(target)
                final_text = spintaxed_text.replace("[[FIRST_NAME]]", first_name).replace("[[USERNAME]]", username)
            except Exception:
                final_text = spintaxed_text.replace("[[FIRST_NAME]]", "دوست عزیز").replace("[[USERNAME]]", str(target))

        log_entry = OrderLog(order_id=order.id, account_id=account_db_id, target=target)
        
        try:
            if order.media_path and order.media_type:
                if order.media_type == "photo":
                    await client.send_photo(chat_id=target, photo=order.media_path, caption=final_text)
                elif order.media_type == "video":
                    await client.send_video(chat_id=target, video=order.media_path, caption=final_text)
                elif order.media_type == "document":
                    await client.send_document(chat_id=target, document=order.media_path, caption=final_text)
            else:
                await client.send_message(chat_id=target, text=final_text)
            
            log_entry.status = "success"
            session.add(log_entry)
            success_count += 1
            
            await asyncio.sleep(random.uniform(5, 12))
            
        except FloodWait as e:
            wait_seconds = e.value
            logger.warning(f"Worker user_{account_db_id}/ triggered FloodWait ({wait_seconds}s).")
            
            log_entry.status = "error"
            log_entry.error_message = f"FloodWait: {wait_seconds}s"
            session.add(log_entry)
            
            await apply_adaptive_flood_wait(session=session, account_id=account_db_id, wait_seconds=wait_seconds)
            unsent_targets.extend(targets[i:])
            break 
            
        except UserRestricted as e:
            logger.warning(f"Worker user_{account_db_id}/ is RESTRICTED (Spam limit). Triggering SpamBot appeal.")
            
            log_entry.status = "error"
            log_entry.error_message = "UserRestricted"
            session.add(log_entry)
            
            asyncio.create_task(appeal_to_spambot(client, account_db_id))
            unsent_targets.extend(targets[i:])
            break 
            
        except (UserIsBlocked, PeerIdInvalid, UsernameInvalid, UsernameNotOccupied, UserIsBot) as e:
            logger.info(f"Worker user_{account_db_id}/ skipped {target}: {e.__class__.__name__}")
            log_entry.status = "error"
            log_entry.error_message = e.__class__.__name__
            session.add(log_entry)
            continue
            
        except Exception as e:
            logger.error(f"Worker user_{account_db_id}/ unexpected error on {target}: {e}")
            log_entry.status = "error"
            log_entry.error_message = str(e)
            session.add(log_entry)
            continue

    try:
        await session.commit()
    except Exception as db_err:
        await session.rollback()
        logger.error(f"Database commit failed for worker user_{account_db_id}/ chunk: {db_err}")

    logger.info(f"Worker user_{account_db_id}/ finished chunk for Order #{order.id}. Sent: {success_count}/{len(targets)}.")
    
    return unsent_targets