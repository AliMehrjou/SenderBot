import asyncio
import logging

from aiogram import Bot
from sqlalchemy import select, update

from database.engine import async_session
from database.models import Account, GlobalSettings
from utils.crypto import mask_phone
from workers.session_manager import (
    build_worker_client,
    claim_proxy_for_account,
    direct_ip_fallback_enabled,
    is_account_quarantined,
    notify_admins,
    parse_proxy_string,
    start_worker_with_rotation,
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

async def auto_reconnect_loop(worker_pool: dict, bot: Bot) -> None:
    """
    حلقه‌ی پس‌زمینه‌ی Reconnect خودکار (هر ۵ دقیقه):
      ۱) ورکرهای موجود در worker_pool که is_connected نیستند را با همان منطق
         retry/چرخش پراکسی (start_worker_with_rotation) دوباره استارت می‌کند.
      ۲) اکانت‌های سالمِ دیتابیس که ورکر ندارند را — با احترام کامل به گارد
         پراکسی (IP Leak Guard) — به استخر اضافه می‌کند.
      ۳) گزارش تلگرامی فقط زمانی ارسال می‌شود که تعداد قطعی‌ها/تغییرات بالا باشد.
    """
    logger.info("Auto Reconnect Loop started (every 5 minutes). Waiting 60 seconds for workers to boot...")
    await asyncio.sleep(60)

    while True:
        try:
            reconnected_ids = []
            added_ids = []

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
                stmt_accounts = select(Account.id).where(
                    Account.is_banned == False,
                    Account.session_string.isnot(None),
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
                        proxy_dict = parse_proxy_string(account.proxy_string) if account.proxy_string else None

                        if not proxy_dict:
                            candidate = await claim_proxy_for_account(db_session, account.id)
                            if candidate:
                                candidate_dict = parse_proxy_string(candidate)
                                if candidate_dict:
                                    try:
                                        await db_session.commit()
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
                            if candidate:
                                proxy_dict = candidate_dict
                                logger.info(f"Reconnect loop: claimed a new proxy for account {account.id}.")
                                account = await db_session.scalar(
                                    select(Account)
                                    .where(Account.id == account.id)
                                    .execution_options(populate_existing=True)
                                )
                                if account is None or account.is_banned or not account.session_string:
                                    continue

                        if not proxy_dict and not direct_ip_fallback_enabled(account.id):
                            still_unproxied += 1
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
                logger.debug(f"Reconnect loop: {still_unproxied} account(s) skipped (IP Leak Guard).")

            # ---------------------------------------------------------
            # 📨 بخش ۳: پیام جمع‌بندی چرخه (هوشمند)
            # ---------------------------------------------------------
            total_changes = len(reconnected_ids) + len(added_ids)
            
            # اگر تعداد تغییرات یا قطعی‌ها از عدد آستانه (۱۰) بیشتر بود، پیام را ارسال کن
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
                # اگر تغییرات جزئی بود (مثلاً ۱ یا ۲ ورکر)، به جای تلگرام، فقط در لاگ سرور ثبت کن
                logger.info(f"Background Reconnect: {len(reconnected_ids)} reconnected, {len(added_ids)} added. Telegram notification suppressed.")

            # ⏱ خواب ۵ دقیقه‌ای تا چرخه‌ی بعدی
            await asyncio.sleep(RECONNECT_INTERVAL_SECONDS)

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Error in Reconnect loop: {e}")
            await asyncio.sleep(60)