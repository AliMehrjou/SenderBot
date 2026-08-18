import asyncio
import logging
import os
from contextlib import suppress

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.fsm.storage.redis import RedisStorage
from redis.asyncio import Redis

from bot.handlers.settings_handlers import router as settings_router
from utils.advanced_anti_ban import perform_warmup_cycle
import random
from bot.handlers.emergency import router as emergency_router
from bot.handlers.admin_panel import router as admin_router
from bot.middlewares.force_join import ForceJoinMiddleware
from bot.handlers.general_handlers import router as general_router
from bot.handlers.login_handlers import router as login_router
from bot.handlers.order_handlers import router as order_router
from bot.handlers.stats_handlers import router as stats_router
from bot.handlers.extractor_handlers import router as extractor_router
from database.engine import async_session, init_db
from bot.handlers.tools_handlers import router as tools_router
from bot.middlewares.admin_auth import AdminMiddleware
from bot.middlewares.database import DatabaseMiddleware
from utils.health_checker import auto_health_check_loop
from workers.session_manager import initialize_workers, start_all_workers, worker_pool, stop_all_workers
from workers.task_queue import order_dispatcher_loop

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# متغیرهای سراسری برای ذخیره رفرنس تسک‌های پس‌زمینه
background_tasks = []

def dispatcher_crash_handler(task: asyncio.Task) -> None:
    """اگر حلقه‌های پس‌زمینه کرش کنند، این هندلر لاگ‌های حیاتی را ثبت می‌کند"""
    try:
        task.result()
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.critical(f"FATAL: Background task crashed unexpectedly: {e}", exc_info=True)

from sqlalchemy import update
from database.models import Order, OrderStatus

async def reset_zombie_orders() -> None:
    """بازگردانی سفارشاتِ در حال اجرا به حالت انتظار در زمان ری‌استارت سرور"""
    logger.info("Checking for zombie orders...")
    async with async_session() as session:
        stmt = (
            update(Order)
            .where(Order.status == OrderStatus.running)
            .values(status=OrderStatus.pending)
        )
        result = await session.execute(stmt)
        await session.commit()
        if result.rowcount > 0:
            logger.warning(f"Reset {result.rowcount} zombie order(s) back to pending.")


async def on_startup(bot: Bot, dispatcher: Dispatcher) -> None:
    logger.info("Connecting to database...")
    
    # مکانیسم تلاش مجدد برای اتصال به دیتابیس
    max_retries = 5
    for attempt in range(1, max_retries + 1):
        try:
            await init_db()
            logger.info("Database connected and tables initialized successfully.")
            break
        except Exception as e:
            if attempt == max_retries:
                logger.critical(f"FATAL: Could not connect to database after {max_retries} attempts.")
                raise e
            logger.warning(f"Database not ready yet (Attempt {attempt}/{max_retries}). Retrying in 5 seconds...")
            await asyncio.sleep(5)
            
    # پاکسازی سفارشات زامبی (اضافه شده در فاز ۱)
    await reset_zombie_orders()
    
    logger.info("Initializing worker engine...")
    async with async_session() as session:
        await initialize_workers(session)
        await start_all_workers(session)
    
    dispatcher_task = asyncio.create_task(order_dispatcher_loop(async_session, worker_pool))
    dispatcher_task.add_done_callback(dispatcher_crash_handler)
    background_tasks.append(dispatcher_task)
    
    warmup_task = asyncio.create_task(account_warmup_loop(worker_pool))
    warmup_task.add_done_callback(dispatcher_crash_handler)
    background_tasks.append(warmup_task)

    health_task = asyncio.create_task(auto_health_check_loop(worker_pool, bot))
    health_task.add_done_callback(dispatcher_crash_handler)
    background_tasks.append(health_task)
    
    logger.info("All background loops are running safely.")


async def on_shutdown(bot: Bot, dispatcher: Dispatcher) -> None:
    logger.info("Received shutdown signal. Stopping background tasks safely...")
    
    # لغو تمام تسک‌های پس‌زمینه
    for task in background_tasks:
        if not task.done():
            task.cancel()
            
    # صبر کردن تا تمام تسک‌ها فرآیند توقف خود را کامل کنند
    if background_tasks:
        await asyncio.gather(*background_tasks, return_exceptions=True)
        
    logger.info("Background tasks stopped successfully.")
    
    # خاموش کردن ورکرهای Pyrogram
    await stop_all_workers()


async def account_warmup_loop(worker_pool: dict) -> None:
    """تسک پس‌زمینه برای گرم نگه‌داشتن اکانت‌های متصل"""
    logger.info("Account Warm-up Loop started.")
    while True:
        try:
            # اجرای Warm-up هر ۳ تا ۶ ساعت یک‌بار
            sleep_hours = random.uniform(3, 6)
            await asyncio.sleep(sleep_hours * 3600)
            
            logger.info("Initiating scheduled warm-up cycle for all active workers...")
            active_workers = list(worker_pool.items())
            
            for account_id, client in active_workers:
                if client.is_connected:
                    await perform_warmup_cycle(client, account_id)
                    # تاخیر بین گرم کردن هر اکانت تا به سرور فشار نیاید
                    await asyncio.sleep(random.uniform(10, 30))
                    
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Error in warm-up loop: {e}")


async def main() -> None:
    BOT_TOKEN = os.getenv("BOT_TOKEN", "YOUR_TELEGRAM_BOT_TOKEN")
    REDIS_HOST = os.getenv("REDIS_HOST", "127.0.0.1")
    REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

    redis_client = Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    storage = RedisStorage(redis=redis_client)

    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher(storage=storage)
    
    # -----------------------------------------------------------------
    # اضافه کردن میدلورها
    # -----------------------------------------------------------------
    # ۱. میدلور دیتابیس (باید قبل از بقیه رجیستر بشه تا سشن در دسترس همه قرار بگیره)
    db_middleware = DatabaseMiddleware(session_maker=async_session)
    dp.message.middleware(db_middleware)
    dp.callback_query.middleware(db_middleware)

    admin_mw = AdminMiddleware()
    dp.message.middleware(admin_mw)
    dp.callback_query.middleware(admin_mw)

    # ۲. میدلور فورس جوین
    force_join_mw = ForceJoinMiddleware()
    dp.message.middleware(force_join_mw)
    dp.callback_query.middleware(force_join_mw)
    # -----------------------------------------------------------------

    dp.include_router(emergency_router) 
    dp.include_router(admin_router)     

    dp.include_router(login_router)
    dp.include_router(order_router)

    dp.include_router(general_router)

    dp.include_router(extractor_router)
    dp.include_router(stats_router)
    dp.include_router(settings_router)
    dp.include_router(tools_router)
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)



    logger.info("Starting Master Control Panel...")
    await bot.delete_webhook(drop_pending_updates=True)
    
    try:
        await dp.start_polling(bot)
    finally:
        await bot.session.close()
        await redis_client.aclose()
        logger.info("Master Control Panel shut down safely.")

if __name__ == "__main__":
    with suppress(KeyboardInterrupt):
        asyncio.run(main())