import asyncio
import logging
import os
from contextlib import suppress
from utils.error_aggregator import report_admin_error
from aiogram import types
from dotenv import load_dotenv
load_dotenv()
from bot.handlers.cleanup_handlers import router as cleanup_router
from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.fsm.storage.redis import RedisStorage
from redis.asyncio import Redis
from bot.handlers.api_handlers import router as api_router
from bot.handlers.settings_handlers import router as settings_router
from utils.advanced_anti_ban import perform_warmup_cycle
from utils.pagination import pagination_router
import random
from bot.handlers import cancel_handlers
from bot.handlers.emergency import router as emergency_router
from bot.handlers.admin_panel import router as admin_router
from bot.middlewares.force_join import ForceJoinMiddleware
from bot.handlers.general_handlers import router as general_router
from bot.handlers.login_handlers import router as login_router
from bot.handlers.order_handlers import router as order_router
from bot.handlers.stats_handlers import router as stats_router
from bot.handlers.extractor_handlers import router as extractor_router
from config import config  # 🔐 فاز ۹ (SEC-7): منبع واحد BOT_TOKEN
import utils.pyro_patches  # noqa: F401  (C-01 fix; must import before any Client)
from database.engine import async_session, init_db
from database.migrations import run_startup_migrations
from utils.crm_catcher import close_crm_redis  
from bot.handlers.tools_handlers import router as tools_router
from bot.middlewares.admin_auth import AdminMiddleware
from bot.middlewares.database import DatabaseMiddleware
from utils.health_checker import auto_health_check_loop, auto_reconnect_loop, proxy_health_monitor_task
from workers.session_manager import initialize_workers, start_all_workers, worker_pool, stop_all_workers
from workers.task_queue import order_dispatcher_loop
from workers.sender import close_sender_redis  
from bot.handlers.photo_handlers import router as photo_router
from bot.handlers.admin_manage import router as admin_manage_router
from bot.handlers.banner_handlers import router as banner_router  
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

import json
from sqlalchemy import update, select
from database.models import Order, OrderStatus

async def reset_zombie_orders() -> None:
    """بازگردانی سفارشات در حال اجرا به حالت انتظار در زمان ری‌استارت سرور
    + بازیابی تارگت‌های در-flight از دفترکل (B3)."""
    logger.info("Checking for zombie orders...")
    async with async_session() as session:
        zombies = (await session.scalars(
            select(Order).where(Order.status == OrderStatus.running)
        )).all()
        for order in zombies:
            inflight = []
            if order.inflight_data:
                try:
                    inflight = [t for t in json.loads(order.inflight_data) if t.strip()]
                except Exception as e:
                    logger.warning(f"Order #{order.id}: corrupt inflight ledger cleared ({e})")
            current = [t for t in (order.target_data or "").split("\n") if t.strip()]
            merged = current + [t for t in inflight if t not in current]
            if inflight:
                logger.warning(
                    f"Recovered {len(inflight)} in-flight target(s) for Order #{order.id}."
                )
            order.target_data = "\n".join(merged)
            order.inflight_data = None
            order.fail_streak = 0
            order.status = OrderStatus.pending
        await session.commit()
        if zombies:
            logger.warning(f"Reset {len(zombies)} zombie order(s) back to pending.")

def supervise(name: str, coro_factory):
    """Wrapper برای ری‌استارت کردن تسک‌های پس‌زمینه‌ای که کرش می‌کنند"""
    async def _runner():
        while True:
            task = asyncio.create_task(coro_factory())
            try:
                await task
                break  # خروج طبیعی (مثل CancelledError)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.critical(f"Loop {name} crashed; restarting in 60s", exc_info=True)
                await asyncio.sleep(60)
    return _runner()
from workers.task_queue import temp_file_gc_loop
from utils.error_aggregator import error_aggregator_loop

async def on_startup(bot: Bot, dispatcher: Dispatcher) -> None:
    logger.info("Connecting to database...")
    
    max_retries = 5
    for attempt in range(1, max_retries + 1):
        try:
            await init_db()
            await run_startup_migrations()
            logger.info("Database connected and tables initialized successfully.")
            break
        except Exception as e:
            if attempt == max_retries:
                logger.critical(f"FATAL: Could not connect to database after {max_retries} attempts.")
                raise e
            logger.warning(f"Database not ready yet (Attempt {attempt}/{max_retries}). Retrying in 5 seconds...", exc_info=True)
            await asyncio.sleep(5)
            
    await reset_zombie_orders()
    
    logger.info("Initializing worker engine...")
    async with async_session() as session:
        await initialize_workers(session, bot)
        await start_all_workers(session, bot)
    
    # استفاده از supervise برای جلوگیری از مرگ خاموش تسک‌ها
    dispatcher_task = asyncio.create_task(supervise("dispatcher", lambda: order_dispatcher_loop(async_session, worker_pool, bot)))
    dispatcher_task.add_done_callback(dispatcher_crash_handler)
    background_tasks.append(dispatcher_task)
    
    warmup_task = asyncio.create_task(supervise("warmup", lambda: account_warmup_loop(worker_pool)))
    warmup_task.add_done_callback(dispatcher_crash_handler)
    background_tasks.append(warmup_task)

    health_task = asyncio.create_task(supervise("health", lambda: auto_health_check_loop(worker_pool, bot)))
    health_task.add_done_callback(dispatcher_crash_handler)
    background_tasks.append(health_task)

    reconnect_task = asyncio.create_task(supervise("reconnect", lambda: auto_reconnect_loop(worker_pool, bot)))
    reconnect_task.add_done_callback(dispatcher_crash_handler)
    background_tasks.append(reconnect_task)
    
    # ۳. تسک پایش سلامت مداوم پروکسی‌ها بدون ایجاد اختلال در عملکرد دیسپچر
    proxy_monitor_task = asyncio.create_task(supervise("proxy_health", lambda: proxy_health_monitor_task(bot)))
    proxy_monitor_task.add_done_callback(dispatcher_crash_handler)
    background_tasks.append(proxy_monitor_task)
    
    # ۱. اجرای تسک زباله‌روب فایل‌های موقت (محافظت هارد سرور)
    gc_task = asyncio.create_task(supervise("temp_gc", lambda: temp_file_gc_loop()))
    gc_task.add_done_callback(dispatcher_crash_handler)
    background_tasks.append(gc_task)

    # ۲. اجرای تسک تجمیع‌کننده خطاها (محافظت در برابر اسپم ادمین)
    aggregator_task = asyncio.create_task(supervise("error_aggregator", lambda: error_aggregator_loop(bot)))
    aggregator_task.add_done_callback(dispatcher_crash_handler)
    background_tasks.append(aggregator_task)
    
    logger.info("All background loops (including GC & Error Aggregator) are running safely.")


async def on_shutdown(bot: Bot, dispatcher: Dispatcher) -> None:
    logger.info("Received shutdown signal. Stopping background tasks safely...")
    
    for task in background_tasks:
        if not task.done():
            task.cancel()
            
    if background_tasks:
        await asyncio.gather(*background_tasks, return_exceptions=True)
        
    # فرصت برای finalize شدن chunkهای در جریان
    logger.info("Draining in-flight tasks for 5 seconds...")
    await asyncio.sleep(5)
        
    logger.info("Background tasks stopped successfully.")
    
    await stop_all_workers()
    await close_sender_redis()
    await close_crm_redis()


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
    BOT_TOKEN = config.BOT_TOKEN
    if not BOT_TOKEN:
        logger.critical("FATAL: BOT_TOKEN is not set in env/.env — Master Control Panel cannot start.")
        raise RuntimeError("BOT_TOKEN is missing")

    if config.ADMIN_ID == 0:
        logger.warning("DEP-5: ADMIN_ID=0 → هشدارهای Realtime امنیتی فقط لاگ می‌شوند؛ در .env مقداردهی کنید.")
    if config.DB_PASS == "password":
        logger.warning("DEP-5: DB_PASS روی پیش‌فرض ناامن «password» است — برای production تغییر دهید.")

    # اصلاح باگ ۱: ساخت کلاینت ردیس با استفاده از آبجکت config و اختصاص دیتابیس
    redis_client = Redis(
        host=config.REDIS_HOST,
        port=config.REDIS_PORT,
        db=config.REDIS_DB,
        password=config.REDIS_PASS or None,
        decode_responses=True,
    )
    storage = RedisStorage(redis=redis_client)

    max_redis_retries = 5
    for attempt in range(1, max_redis_retries + 1):
        try:
            await redis_client.ping()
            logger.info("Redis connected successfully.")
            break
        except Exception as e:
            if attempt == max_redis_retries:
                logger.critical(f"FATAL: Could not connect to Redis after {max_redis_retries} attempts.")
                with suppress(Exception):
                    await redis_client.aclose()
                raise
            logger.warning(f"Redis not ready yet (Attempt {attempt}/{max_redis_retries}). Retrying in 5 seconds...", exc_info=True)
            await asyncio.sleep(5)

    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher(storage=storage)
    
# اصلاح باگ ۳: ثبت هندلر خطای سراسری جهت جلوگیری از بی‌پاسخ ماندن و کرش ربات
    

    @dp.errors()
    async def global_error_handler(event: types.ErrorEvent):
        logger.error(f"Unhandled handler error: {event.exception}", exc_info=event.exception)
        
        # ارسال بی‌درنگ و امنِ خطای بحرانی به بافر ردیس تا اسپم نشود
        error_summary = f"💥 Unhandled Exception: {type(event.exception).__name__}\nDetail: {str(event.exception)[:150]}"
        asyncio.create_task(report_admin_error(error_summary))
        
        try:
            if event.update.callback_query:
                await event.update.callback_query.answer("⚠️ خطای داخلی رخ داد. لطفاً دوباره تلاش کنید.", show_alert=True)
            elif event.update.message:
                await event.update.message.answer("⚠️ خطای داخلی رخ داد. لطفاً دوباره تلاش کنید.")
        except Exception as e:
            # 🛡 رفع باگ: جلوگیری از قورت‌دادن استثنا و لاگ گرفتن از خطای تلگرام
            logger.warning(f"Failed to send global error message to user: {e}", exc_info=True)
        return True

    # ثبت میدلورها
    db_middleware = DatabaseMiddleware(session_maker=async_session)
    dp.message.middleware(db_middleware)
    dp.callback_query.middleware(db_middleware)
    dp.include_router(cancel_handlers.router)
    dp.include_router(api_router)
    
    admin_mw = AdminMiddleware()
    dp.message.middleware(admin_mw)
    dp.callback_query.middleware(admin_mw)

    force_join_mw = ForceJoinMiddleware()
    dp.message.middleware(force_join_mw)
    dp.callback_query.middleware(force_join_mw)
    
    # ثبت روترها
    dp.include_router(pagination_router)
    dp.include_router(emergency_router) 
    dp.include_router(admin_router)     
    dp.include_router(login_router)
    dp.include_router(order_router)
    dp.include_router(general_router)
    dp.include_router(cleanup_router)
    dp.include_router(admin_manage_router)
    dp.include_router(banner_router)   
    dp.include_router(extractor_router)
    dp.include_router(stats_router)
    dp.include_router(settings_router)
    dp.include_router(tools_router)
    dp.include_router(photo_router)
    
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)
    
    logger.info("Starting Master Control Panel...")
    
    # اصلاح باگ ۴: عدم دور ریختن دستورات ادمین‌ها حین downtime
    await bot.delete_webhook(drop_pending_updates=False)
    
    try:
        await dp.start_polling(bot)
    finally:
        await bot.session.close()
        await redis_client.aclose()
        logger.info("Master Control Panel shut down safely.")


if __name__ == "__main__":
    with suppress(KeyboardInterrupt):
        asyncio.run(main())