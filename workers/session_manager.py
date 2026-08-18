import asyncio
import logging
from typing import Dict, Optional
from urllib.parse import urlparse

from pyrogram import Client
from pyrogram.errors import (
    AuthKeyUnregistered,
    UserDeactivated,
    UserDeactivatedBan,
    Unauthorized
)
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.expression import func

from database.models import Account, Proxy
from config import config
from utils.advanced_anti_ban import randomize_profile
from pyrogram.handlers import MessageHandler
from pyrogram import filters
from utils.crm_catcher import incoming_message_handler
from utils.crypto import decrypt_session
logger = logging.getLogger(__name__)

# ==========================================
# GLOBAL WORKER POOL
# ==========================================
worker_pool: Dict[int, Client] = {}


# ==========================================
# UTILITY: PROXY PARSER
# ==========================================
def parse_proxy_string(proxy_string: str) -> Optional[dict]:
    if not proxy_string:
        return None
        
    try:
        parsed = urlparse(proxy_string)
        if not parsed.hostname or not parsed.port:
            logger.error("Missing hostname or port in proxy string.")
            return None
            
        scheme = parsed.scheme.lower()
        if scheme not in ["socks4", "socks5"]:
            logger.error(f"Unsupported proxy scheme '{scheme}'. Pyrogram requires socks4 or socks5.")
            return None
            
        return {
            "scheme": scheme,
            "hostname": parsed.hostname,
            "port": parsed.port,
            "username": parsed.username,
            "password": parsed.password
        }
    except Exception as e:
        logger.error(f"Failed to parse proxy string '{proxy_string}': {e}")
        return None


# ==========================================
# PROXY ROTATION LOGIC
# ==========================================
async def get_random_active_proxy(session: AsyncSession) -> Optional[str]:
    """Fetches a random, active proxy from the database."""
    stmt = select(Proxy).where(Proxy.is_active == True).order_by(func.rand()).limit(1)
    result = await session.execute(stmt)
    proxy_obj = result.scalar_one_or_none()
    return proxy_obj.proxy_string if proxy_obj else None

async def mark_proxy_failed(session: AsyncSession, proxy_string: str) -> None:
    """Increments fail count for a proxy and deactivates it if it fails too often."""
    stmt = select(Proxy).where(Proxy.proxy_string == proxy_string)
    result = await session.execute(stmt)
    proxy_obj = result.scalar_one_or_none()
    
    if proxy_obj:
        proxy_obj.fail_count += 1
        if proxy_obj.fail_count >= 5:
            proxy_obj.is_active = False
            logger.warning(f"Proxy {proxy_string} marked as inactive due to high failure rate.")
        await session.commit()


# ==========================================
# WORKER INITIALIZATION
# ==========================================
async def initialize_workers(session: AsyncSession) -> None:
    """Instantiates Pyrogram Clients for all active accounts."""
    stmt = select(Account).where(Account.is_banned == False)
    result = await session.execute(stmt)
    accounts = result.scalars().all()

    for account in accounts:
        if not account.session_string:
            continue

        decrypted_session = decrypt_session(account.session_string)
        proxy_dict = parse_proxy_string(account.proxy_string) if account.proxy_string else None

        client = Client(
            name=f"worker_acc_{account.id}",
            session_string=decrypted_session,
            api_id=config.API_ID,      
            api_hash=config.API_HASH,  
            proxy=proxy_dict,
            in_memory=True
        )

        # ----------------------------------------------------
        # اضافه کردن سیستم CRM به کلاینت (فقط پیام‌های شخصی)
        # ----------------------------------------------------
        client.add_handler(
            MessageHandler(
                incoming_message_handler, 
                filters.private & ~filters.me
            )
        )

        worker_pool[account.id] = client
        
    logger.info(f"Initialized {len(worker_pool)} worker(s) in the pool with CRM attached.")


# ==========================================
# LIFECYCLE MANAGEMENT
# ==========================================

async def start_all_workers(session: AsyncSession) -> None:
    """Starts all clients with retry and proxy rotation mechanisms."""
    logger.info("Starting all initialized workers with Proxy Rotation...")
    MAX_RETRIES = 3
    
    for account_id, client in list(worker_pool.items()):
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                await client.start()
                logger.info(f"Worker {account_id} connected successfully.")
                asyncio.create_task(randomize_profile(client, account_id))
                break 
                
            except (AuthKeyUnregistered, UserDeactivated, UserDeactivatedBan, Unauthorized) as e:
                logger.error(f"Worker {account_id} session revoked or banned: {e}")
                worker_pool.pop(account_id, None)
                try:
                    stmt = update(Account).where(Account.id == account_id).values(is_banned=True)
                    await session.execute(stmt)
                    await session.commit()
                    logger.info(f"Account {account_id} flagged as banned in the database.")
                except Exception as db_err:
                    await session.rollback()
                    logger.error(f"Failed to flag account {account_id} as banned: {db_err}")
                break 
                
            except Exception as e:
                logger.warning(f"Worker {account_id} connection failed (Attempt {attempt}/{MAX_RETRIES}): {e}")
                
                if client.proxy:
                    scheme = client.proxy.get('scheme')
                    host = client.proxy.get('hostname')
                    port = client.proxy.get('port')
                    user = client.proxy.get('username')
                    pwd = client.proxy.get('password')
                    
                    if user and pwd:
                        failed_proxy_str = f"{scheme}://{user}:{pwd}@{host}:{port}"
                    else:
                        failed_proxy_str = f"{scheme}://{host}:{port}"
                        
                    await mark_proxy_failed(session, failed_proxy_str)
                
                new_proxy_str = await get_random_active_proxy(session)
                if new_proxy_str:
                    logger.info(f"Rotating proxy for worker {account_id}...")
                    client.proxy = parse_proxy_string(new_proxy_str)
                    try:
                        stmt = update(Account).where(Account.id == account_id).values(proxy_string=new_proxy_str)
                        await session.execute(stmt)
                        await session.commit()
                    except Exception:
                        await session.rollback()
                else:
                    # FIX: توقف حلقه بی‌نهایت در صورت اتمام پراکسی‌ها
                    logger.critical(f"CRITICAL: No active proxies left for Worker {account_id}! Disconnecting.")
                    worker_pool.pop(account_id, None)
                    break

                if attempt < MAX_RETRIES:
                    await asyncio.sleep(5)
                else:
                    logger.error(f"Worker {account_id} completely failed. Removing from pool.")
                    worker_pool.pop(account_id, None)



async def stop_all_workers() -> None:
    """Cleanly disconnects all active Pyrogram clients."""
    logger.info("Stopping all workers...")
    for account_id, client in list(worker_pool.items()):
        try:
            if client.is_connected:
                await client.stop()
            logger.info(f"Worker {account_id} disconnected safely.")
        except Exception as e:
            logger.error(f"Error disconnecting worker {account_id}: {e}")
        finally:
            worker_pool.pop(account_id, None)
            
    logger.info("All workers have been stopped and removed from the pool.")


async def start_single_worker(account: Account, session: AsyncSession) -> bool:
    """
    روشن کردن و اضافه کردن پویای یک اکانت جدید به استخر ورکرها بدون نیاز به ری‌استارت سرور.
    """
    from utils.crypto import decrypt_session # ایمپورت در داخل اسکوپ برای جلوگیری از تداخل
    
    if not account.session_string:
        return False

    decrypted_session = decrypt_session(account.session_string)
    proxy_dict = parse_proxy_string(account.proxy_string) if account.proxy_string else None

    client = Client(
        name=f"worker_acc_{account.id}",
        session_string=decrypted_session,
        api_id=config.API_ID,      
        api_hash=config.API_HASH,  
        proxy=proxy_dict,
        in_memory=True
    )

    # اتصال CRM
    client.add_handler(
        MessageHandler(
            incoming_message_handler, 
            filters.private & ~filters.me
        )
    )

    try:
        await client.start()
        worker_pool[account.id] = client
        logger.info(f"Dynamically started new Worker {account.id} and added to pool.")
        
        # اجرای Anti-ban اولیه
        asyncio.create_task(randomize_profile(client, account.id))
        return True
    except Exception as e:
        logger.error(f"Failed to start new Worker {account.id} dynamically: {e}")
        return False