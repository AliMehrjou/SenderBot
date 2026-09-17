import asyncio
import logging
import python_socks
from python_socks.async_.asyncio import Proxy as AsyncProxy
from workers.session_manager import _no_proxy_alert_throttled
from aiogram import Bot
from sqlalchemy import select, update, func
from config import config
from database.engine import async_session
from database.models import Account, GlobalSettings
from utils.crypto import mask_phone
from datetime import datetime, timezone
import time
from urllib.parse import urlparse
import asyncio
import time
import asyncio
from datetime import datetime, timezone
from urllib.parse import urlparse
from sqlalchemy import select
from database.models import Proxy
from database.engine import async_session
from workers.session_manager import (
    build_worker_client,
    claim_proxy_for_account,
    direct_ip_fallback_enabled,
    direct_budget_ok,
    is_account_quarantined,
    notify_admins,
    parse_proxy_string,
    start_worker_with_rotation,
    get_use_proxy_for_sending,
)

logger = logging.getLogger(__name__)


async def auto_health_check_loop(worker_pool: dict, bot: Bot) -> None:
    """تسک پس‌زمینه برای بررسی سلامت روزانه پراکسی‌ها و کلاینت‌ها"""
    logger.info("Auto Health Checker Loop started. Waiting 60 seconds for workers to boot...")
    
    # فاز ۲: تاخیر اولیه یک دقیقه‌ای برای بالا آمدن ورکرها
    await asyncio.sleep(180)
    
    while True:
        try:
            total_workers = len(worker_pool)
            
            # --- فیکس فاز ۴: استخراج آیدی ورکرهای قطعی ---
            # 🛡 فاز ۴ (BUG-26): iterate روی snapshot (list(...)) — حلقه‌ی Reconnect
            # همزمان ممکن است pop/insert کند ← "dictionary changed size during iteration"
            disconnected_ids = [acc_id for acc_id, client in list(worker_pool.items()) if not client.is_connected]
            disconnected_count = len(disconnected_ids)
            connected_workers = total_workers - disconnected_count
            
            disconnected_details = ""
            if disconnected_ids:
                try:
                    # استفاده از سشن برای استخراج شماره تلفن‌ها از دیتابیس
                    async with async_session() as db_session:
                        stmt = select(Account.id, Account.phone_number).where(Account.id.in_(disconnected_ids))
                        result = await db_session.execute(stmt)
                        accounts = result.all()
                        
                        # --- FIX M8: Mask phone numbers in the daily report ---
                        details_list = [f"▫️ آیدی {acc.id} (<code>{mask_phone(acc.phone_number)}</code>)" for acc in accounts]
                        # ------------------------------------------------------
                        
                        # گارد محدودیت طول پیام برای قطعی‌های گسترده
                        if len(details_list) > 30:
                            details_list = details_list[:30]
                            details_list.append("▫️ ... و موارد دیگر")
                            
                        if details_list:
                            disconnected_details = "\n📋 <b>لیست ورکرهای قطعی:</b>\n" + "\n".join(details_list) + "\n"
                except Exception as db_err:
                    logger.error(f"Failed to fetch disconnected accounts details: {db_err}")
                    disconnected_details = f"\n📋 <b>لیست آیدی‌های قطعی:</b> {', '.join(map(str, disconnected_ids))}\n"
            # -----------------------------------------------------
            
            report_text = (
                "🩺 <b>گزارش روزانه سلامت موتور سندر</b>\n\n"
                f"🟢 <b>ورکرهای آنلاین و سالم:</b> <code>{connected_workers}</code>\n"
                f"🔴 <b>ورکرهای قطع یا بن شده:</b> <code>{disconnected_count}</code>\n"
                f"🌐 <b>کل اکانت‌های در استخر:</b> <code>{total_workers}</code>\n"
                f"{disconnected_details}\n"
                "<i>💡 برای جزئیات بیشتر می‌توانید از منوی اصلی وارد بخش «📈 آمار» شوید.</i>"
            )
            
            # فاز ۳: ارسال گزارش به ادمین‌ها (ADMIN_ID از config + ساب‌ادمین‌های
            # جدول Admin) از طریق تابع مشترک notify_admins
            sent_count = await notify_admins(bot, report_text)
            if sent_count:
                logger.info(f"Daily Health Check report sent to {sent_count} admin(s).")
            
            # فاز ۲: خواب ۲۴ ساعته در انتهای حلقه
            await asyncio.sleep(24 * 3600)
            
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Error in Health Check loop: {e}")
            await asyncio.sleep(60)

# ==========================================
# 🔁 RECONNECT خودکار ورکرها (هر ۵ دقیقه)
# ==========================================
RECONNECT_INTERVAL_SECONDS = 5 * 60  # هر ۵ دقیقه
ALERT_THRESHOLD = 10 # آستانه هشدار: ارسال پیام در صورت قطعی یا تغییر ۱۰ ورکر یا بیشتر


# بازنویسی کامل: auto_reconnect_loop — utils/health_checker.py

async def auto_reconnect_loop(worker_pool: dict, bot: Bot) -> None:
    """
    حلقه‌ی پس‌زمینه‌ی Reconnect خودکار (هر ۵ دقیقه):
      ۱) بازگردانی اکانت‌های خارج شده از Cooldown.
      ۲) restart ورکرهای قطعیِ موجود در استخر.
      ۳) اضافه کردن اکانت‌های سالم (فقط ASSIGNED و NO_PROXY) به استخر.
      ۴) چرخش پروکسی‌های WEAK بیکار به HEALTHY (بهبود M6).
    """
    logger.info("Auto Reconnect Loop started (every 5 minutes). Waiting 60 seconds for workers to boot...")
    await asyncio.sleep(60)

    while True:
        try:
            reconnected_ids = []
            added_ids = []

            # ---------------------------------------------------------
            # بخش ۰: بازگردانی اتوماتیک اکانت‌هایی که زمان Cooldown آنها تمام شده
            # ---------------------------------------------------------
            async with async_session() as db_session:
                from database.models import Account, AccountStatus, WorkerEvent
                now_utc = datetime.now(timezone.utc)
                cooldown_accs = (await db_session.scalars(
                    select(Account).where(
                        Account.status == AccountStatus.cooldown,
                        Account.expected_return_time <= now_utc
                    )
                )).all()
                
                for acc in cooldown_accs:
                    acc.status = AccountStatus.active
                    acc.status_reason = "Auto cooldown finished"
                    acc.consecutive_errors = 0
                    db_session.add(WorkerEvent(
                        account_id=acc.id, 
                        old_status=AccountStatus.cooldown.value, 
                        new_status=AccountStatus.active.value, 
                        reason="Cooldown expired"
                    ))
                
                if cooldown_accs:
                    await db_session.commit()
                    logger.info(f"HealthChecker: Automatically reactivated {len(cooldown_accs)} account(s) from cooldown.")

            # ---------------------------------------------------------
            # بخش ۱: restart ورکرهای قطعیِ موجود در استخر
            # ---------------------------------------------------------
            disconnected_ids = [
                acc_id for acc_id, client in list(worker_pool.items())
                if not client.is_connected
            ]
            for account_id in disconnected_ids:
                client = worker_pool.get(account_id)
                if client is None or client.is_connected:
                    continue
                try:
                    async with async_session() as db_session:
                        stmt_settings = select(GlobalSettings).limit(1)
                        global_settings = await db_session.scalar(stmt_settings)
                        
                        ok = await start_worker_with_rotation(
                            account_id, client, db_session, global_settings, bot=bot
                        )
                        if ok:
                            reconnected_ids.append(account_id)
                except Exception as e:
                    logger.error(f"Reconnect loop: failed to restart worker {account_id}: {e}")
                
                await asyncio.sleep(2)

            # ---------------------------------------------------------
            # بخش ۲: اضافه کردن اکانت‌های سالمِ دیتابیس
            # ---------------------------------------------------------
            async with async_session() as db_session:
                from database.models import AccountStatus
                stmt_accounts = select(Account.id).where(
                    Account.status == AccountStatus.active, 
                    Account.is_banned == False,
                    Account.session_string.isnot(None),
                    Account.proxy_status.in_(["ASSIGNED", "NO_PROXY"])
                )
                missing_db_ids = (await db_session.scalars(stmt_accounts)).all()

            missing_account_ids = [acc_id for acc_id in missing_db_ids if acc_id not in worker_pool]
            still_unproxied = 0
            
            for account_id in missing_account_ids:
                if account_id in worker_pool or await is_account_quarantined(account_id):
                    continue

                try:
                    async with async_session() as db_session:
                        account = await db_session.scalar(
                            select(Account)
                            .where(Account.id == account_id)
                            .execution_options(populate_existing=True)
                        )
                        
                        if account is None or account.is_banned or not account.session_string:
                            continue

                        global_settings = await db_session.scalar(select(GlobalSettings).limit(1))
                        use_proxy_for_sending = await get_use_proxy_for_sending(db_session)
                        
                        proxy_dict = None

                        if use_proxy_for_sending:
                            proxy_dict = parse_proxy_string(account.proxy_string) if account.proxy_string else None
                            is_proxy_healthy = False
                            if account.proxy_string:
                                proxy_obj = await db_session.scalar(select(Proxy).where(Proxy.proxy_string == account.proxy_string))
                                if proxy_obj and proxy_obj.is_active and proxy_obj.health_state != "DEAD":
                                    is_proxy_healthy = True

                            if getattr(config, "PROXY_REASSIGN_ENABLED", True) and (not proxy_dict or not is_proxy_healthy):
                                candidate = await claim_proxy_for_account(db_session, account.id)
                                
                                if candidate:
                                    candidate_dict = parse_proxy_string(candidate)
                                    if candidate_dict:
                                        try:
                                            await db_session.commit()
                                            proxy_dict = candidate_dict
                                            logger.info(f"Reconnect loop: claimed a new proxy for account {account.id}.")
                                        except Exception as db_err:
                                            await db_session.rollback()
                                            logger.error(f"Reconnect loop: failed to commit proxy for {account.id}: {db_err}")
                                            candidate = None
                                    else:
                                        try:
                                            await db_session.rollback()
                                        except Exception:
                                            pass
                                        candidate = None
                                
                                if not candidate:
                                    from sqlalchemy import func
                                    await db_session.execute(
                                        update(Account)
                                        .where(Account.id == account.id)
                                        .values(
                                            proxy_status="WAITING_PROXY", 
                                            proxy_string=None, 
                                            proxy_queue_joined_at=func.now()
                                        )
                                    )
                                    from workers.session_manager import log_proxy_event
                                    await log_proxy_event(db_session, account.id, "ENQUEUE", None, "Lost active proxy, waiting for new one in queue")
                                    await db_session.commit()
                                    continue 

                            if not proxy_dict:
                                if not direct_ip_fallback_enabled(account.id):
                                    still_unproxied += 1
                                    continue
                                if not await direct_budget_ok(db_session):
                                    still_unproxied += 1
                                    continue
                        else:
                            if not await direct_budget_ok(db_session):
                                still_unproxied += 1
                                continue
                            proxy_dict = None 
                            
                            if account.proxy_status != "NO_PROXY":
                                await db_session.execute(
                                    update(Account)
                                    .where(Account.id == account.id)
                                    .values(proxy_status="NO_PROXY")
                                )
                                await db_session.commit()

                        account = await db_session.scalar(
                            select(Account)
                            .where(Account.id == account.id)
                            .execution_options(populate_existing=True)
                        )
                        
                        if account is None or account.is_banned or not account.session_string:
                            continue

                        client = await build_worker_client(account, db_session, proxy_dict)
                        if client is None:
                            continue

                        ok = await start_worker_with_rotation(
                            account.id, client, db_session, global_settings, bot=bot
                        )
                        if ok:
                            added_ids.append(account.id)
                            
                except Exception as e:
                    logger.error(f"Reconnect loop: failed to add worker {account_id}: {e}")

                await asyncio.sleep(2)

            if still_unproxied:
                logger.debug(f"Reconnect loop: {still_unproxied} account(s) skipped (IP Leak Guard / Direct Budget Guard).")

            # ---------------------------------------------------------
            # بخش ۳: ارتقاء پروکسی‌های WEAK بیکار به HEALTHY (بهبود M6)
            # ---------------------------------------------------------
            try:
                from workers.sender import _get_redis
                redis = _get_redis()
                allow_weak_fallback = getattr(config, "PROXY_ALLOW_WEAK_FALLBACK", True)
                
                if allow_weak_fallback and worker_pool:
                    async with async_session() as db_session:
                        # پیدا کردن اکانت‌های متصل که پروکسی WEAK دارند
                        stmt_weak = select(Account).where(
                            Account.id.in_(list(worker_pool.keys())),
                            Account.proxy_string.isnot(None)
                        )
                        connected_accs = (await db_session.scalars(stmt_weak)).all()
                        rotated_count = 0
                        
                        for acc in connected_accs:
                            # در زمان Low-load: اگر ورکر مشغول سفارش نیست
                            if not await redis.exists(f"busy_worker:{acc.id}"):
                                proxy = await db_session.scalar(select(Proxy).where(Proxy.proxy_string == acc.proxy_string))
                                
                                if proxy and proxy.health_state == "WEAK":
                                    # بررسی وجود ظرفیت روی پراکسی‌های HEALTHY
                                    healthy_candidate = await db_session.scalar(
                                        select(Proxy.id).where(
                                            Proxy.is_active == True,
                                            Proxy.health_state == "HEALTHY",
                                            Proxy.in_use < max(1, getattr(config, 'MAX_ACCOUNTS_PER_PROXY', 100))
                                        ).limit(1)
                                    )
                                    if healthy_candidate:
                                        logger.info(f"HealthChecker (M6): Upgrading idle worker {acc.id} from WEAK to HEALTHY proxy.")
                                        from workers.session_manager import switch_worker_proxy
                                        # چرخش بدون نادیده‌گرفتن Cooldown تا سیاست‌های Sticky Binding حفظ شود
                                        await switch_worker_proxy(acc.id, db_session, "Rotate idle WEAK to HEALTHY", ignore_cooldown=False)
                                        rotated_count += 1
                                        await asyncio.sleep(2) 
                                        
                        if rotated_count > 0:
                            logger.info(f"HealthChecker: Successfully upgraded {rotated_count} idle WEAK proxies to HEALTHY.")
            except Exception as e:
                logger.error(f"Error in WEAK to HEALTHY proxy rotation: {e}")

            # ---------------------------------------------------------
            # 📨 بخش ۴: پیام جمع‌بندی چرخه (هوشمند)
            # ---------------------------------------------------------
            total_changes = len(reconnected_ids) + len(added_ids)
            
            if total_changes >= ALERT_THRESHOLD or len(disconnected_ids) >= ALERT_THRESHOLD:
                connected_now = sum(1 for c in list(worker_pool.values()) if c.is_connected)
                parts = ["⚠️ <b>هشدار: گزارش چرخه‌ی Reconnect (تغییرات عمده)</b>"]
                
                if reconnected_ids:
                    ids_text = ", ".join(f"<code>{i}</code>" for i in reconnected_ids[:20])
                    if len(reconnected_ids) > 20:
                        ids_text += " و ..."
                    parts.append(
                        f"♻️ <b>ورکرهای Reconnectشده:</b> <code>{len(reconnected_ids)}</code>\n▫️ {ids_text}"
                    )
                if added_ids:
                    ids_text = ", ".join(f"<code>{i}</code>" for i in added_ids[:20])
                    if len(added_ids) > 20:
                        ids_text += " و ..."
                    parts.append(
                        f"➕ <b>ورکرهای اضافه‌شده به استخر:</b> <code>{len(added_ids)}</code>\n▫️ {ids_text}"
                    )
                parts.append(
                    f"\n🌐 <b>وضعیت نهایی استخر:</b> <code>{connected_now}</code> متصل از "
                    f"<code>{len(worker_pool)}</code> ورکر"
                )
                await notify_admins(bot, "\n\n".join(parts))
                
            elif total_changes > 0:
                logger.info(f"Background Reconnect: {len(reconnected_ids)} reconnected, {len(added_ids)} added. Telegram notification suppressed.")

            await asyncio.sleep(RECONNECT_INTERVAL_SECONDS)

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Error in Reconnect loop: {e}")
            await asyncio.sleep(60)
            

async def check_proxy_health(proxy_string: str, timeout: float = 3.0) -> tuple[bool, int | None]:
    """
    بررسی سلامت دومرحله‌ای (تست واقعی):
    مرحله ۱: اتصال TCP به پروکسی
    مرحله ۲: تونل‌زنی SOCKS5/4 و اتصال به IP یکی از دیتاسنترهای تلگرام (DC4) برای گرفتن Latency واقعی
    """
    if not proxy_string:
        return False, None

    start_time = time.perf_counter()
    try:
        # ساخت کلاینت پراکسی
        proxy = AsyncProxy.from_url(proxy_string)
        
        # IP یکی از سرورهای تلگرام (DC 4 - 149.154.167.50:443)
        sock = await asyncio.wait_for(
            proxy.connect("149.154.167.50", 443),
            timeout=timeout
        )
        ping_ms = int((time.perf_counter() - start_time) * 1000)
        sock.close()
        return True, ping_ms
        
    except (asyncio.TimeoutError, ConnectionRefusedError, OSError, ValueError, python_socks.ProxyError):
        return False, None
    except Exception as e:
        logger.debug(f"Unexpected health check error for {proxy_string}: {e}")
        return False, None

# بازنویسی کامل: report_proxy_result — utils/health_checker.py
async def report_proxy_result(proxy_string: str, is_success: bool, latency_ms: int = None) -> None:
    """
    مسیر واحد برای مدیریت Hysteresis، تغییر وضعیت پروکسی، و سینک با is_healthy.
    این متد در هر دور از چک‌ها یا خطاهای زنده سندر فراخوانی می‌شود.
    """
    if not proxy_string:
        return
        
    async with async_session() as session:
        try:
            proxy = await session.scalar(select(Proxy).where(Proxy.proxy_string == proxy_string))
            if not proxy:
                return

            now_utc = datetime.now(timezone.utc)
            old_state = proxy.health_state
            
            # پرچم برای اسپاون تسک بعد از کامیت
            needs_queue_process = False

            if is_success:
                proxy.consecutive_failures = 0
                proxy.consecutive_successes += 1
                
                # تعیین وضعیت براساس Latency
                target_state = "WEAK" if (latency_ms and latency_ms >= config.PROXY_WEAK_LATENCY_MS) else "HEALTHY"

                if old_state == "DEAD":
                    # بازگشت از DEAD نیازمند M موفقیت پیاپی است
                    if proxy.consecutive_successes >= config.PROXY_RECOVER_CONSECUTIVE_OKS:
                        proxy.health_state = target_state
                        proxy.is_active = True
                else:
                    proxy.health_state = target_state
                    proxy.is_active = True

                if proxy.health_state == "HEALTHY" and old_state != "HEALTHY":
                    needs_queue_process = True

            else:
                proxy.consecutive_successes = 0
                proxy.consecutive_failures += 1
                
                # سقوط به DEAD نیازمند N شکست پیاپی است
                if proxy.consecutive_failures >= config.PROXY_DEAD_CONSECUTIVE_FAILS:
                    proxy.health_state = "DEAD"
                    proxy.is_active = False

            # اگر وضعیت تغییر کرد، لاگ می‌زنیم و is_healthy را سینک می‌کنیم
            if proxy.health_state != old_state:
                logger.info(f"Proxy State Changed [{proxy.id}]: {old_state} -> {proxy.health_state} (Latency: {latency_ms}ms, Result: {is_success})")
                proxy.last_state_changed_at = now_utc
                proxy.is_healthy = (proxy.health_state != "DEAD")

            proxy.last_checked_at = now_utc
            if latency_ms is not None:
                proxy.ping_ms = latency_ms

            await session.commit()
            
            # باگ ۳ (M4): Spawn تسک بعد از commit موفق انجام می‌شود تا دیتای DB قابل رویت باشد
            if needs_queue_process:
                try:
                    from workers.session_manager import background_process_proxy_queue
                    asyncio.create_task(background_process_proxy_queue())
                except Exception as e:
                    logger.error(f"Failed to spawn background_process_proxy_queue: {e}")
                    
        except Exception as e:
            logger.error(f"Error reporting proxy result for {proxy_string}: {e}")


async def get_pool_status() -> dict:
    """
    آمار تجمیعی استخر پروکسی و هشدار به ادمین در صورت خالی شدن پروکسی‌های سالم دارای ظرفیت.
    قابل فراخوانی در فازهای بعدی.
    """
    async with async_session() as session:
        cap = max(1, config.MAX_ACCOUNTS_PER_PROXY)
        
        healthy_free = await session.scalar(
            select(func.count(Proxy.id)).where(
                Proxy.is_active == True,
                Proxy.health_state == 'HEALTHY',
                Proxy.usage_type.in_(("sender", "both")),
                Proxy.in_use < cap
            )
        ) or 0
        
        weak_count = await session.scalar(select(func.count(Proxy.id)).where(Proxy.is_active == True, Proxy.health_state == 'WEAK')) or 0
        dead_count = await session.scalar(select(func.count(Proxy.id)).where(Proxy.is_active == True, Proxy.health_state == 'DEAD')) or 0
        total_active = await session.scalar(select(func.count(Proxy.id)).where(Proxy.is_active == True)) or 0
        
        stats = {
            "healthy_free": healthy_free,
            "weak": weak_count,
            "dead": dead_count,
            "total_active": total_active
        }
                    
        return stats


async def check_all_proxies(bot: Bot = None) -> None:
    """تست سلامت تمام پروکسی‌های فعال در دیتابیس (بدون بلاک کردن UI)"""
    async with async_session() as session:
        try:
            from sqlalchemy import or_
            proxies = (await session.execute(select(Proxy.proxy_string).where(
                or_(Proxy.is_active == True, Proxy.health_state == 'DEAD')
            ))).scalars().all()
        except Exception as e:
            logger.error(f"Failed to fetch proxies for check: {e}")
            return
            
    for proxy_str in proxies:
        is_healthy, ping = await check_proxy_health(proxy_str, timeout=3.0)
        await report_proxy_result(proxy_str, is_success=is_healthy, latency_ms=ping)
        
    # چک آمار و ارسال هشدار اتمام
    stats = await get_pool_status()
    if stats["healthy_free"] == 0 and stats["total_active"] > 0 and bot:
        if not _no_proxy_alert_throttled(0):
            await notify_admins(
                bot,
                "⚠️ <b>پروکسی‌های سالم و آزاد به پایان رسیده‌اند!</b>\n\n"
                f"🟢 سالمِ آزاد: <b>0</b>\n🟡 ضعیف: <b>{stats['weak']}</b>\n🔴 مرده: <b>{stats['dead']}</b>\n"
                "<i>لطفاً جهت اتصال اکانت‌های جدید پروکسی اضافه کنید.</i>"
            )


async def proxy_health_monitor_task(bot: Bot) -> None:
    """Background Task برای تست دوره‌ای پروکسی‌ها (پویایی بر اساس تنظیمات)"""
    interval = config.PROXY_HEALTH_CHECK_INTERVAL
    logger.info(f"Proxy health monitor task started (every {interval} seconds).")
    await asyncio.sleep(30)
    
    while True:
        try:
            await check_all_proxies(bot)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Error in proxy_health_monitor_task: {e}")
            
        await asyncio.sleep(config.PROXY_HEALTH_CHECK_INTERVAL)