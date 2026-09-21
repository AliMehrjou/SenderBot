"""
🧱 فاز ۱۲ (DEP-1) — مهاجرت idempotent استارتاپی برای دیتابیس موجود.

init_db از create_all استفاده می‌کند؛ create_all جدولِ غایب را می‌سازد ولی
هرگز ALTER TABLE نمی‌زند ← ستون‌هایی که فازهای ۲/۵/۶ به مدل‌ها اضافه شدند
(orders.source_channel_id / source_message_ids / retry_count / use_banner_pool،
accounts.warmed_up_at / photo_package_id / proxy_string، proxies.in_use و…)
روی دیتابیس موجود ساخته نمی‌شوند و اولین کوئری بعد از deploy کرش می‌کند.

این ماژول باید «بعد از» init_db صدا زده شود:
  ۱) ستون‌ها و FKهای موجود از INFORMATION_SCHEMA خوانده می‌شوند؛
  ۲) فقط برای ستون‌های «غایب» ALTER TABLE ... ADD COLUMN اجرا می‌شود؛
  ۳) برای ستون‌های index=True ایندکس ساخته و FKهای غایب اضافه می‌شوند؛
  ۴) data-fixهای مستندشده (مثل backfill warmed_up_at) اجرا می‌شوند.

اجرای مکرر کاملاً بی‌خطر است (idempotent) — دفعات بعد هیچ ALTERی زده نمی‌شود.
"""

import logging
from typing import Dict, Set, Tuple

from sqlalchemy import text
from sqlalchemy.dialects import mysql

from database.engine import async_session
from database.models import Base

logger = logging.getLogger(__name__)

# دیالکت MySQL برای کامپایل تایپ ستون‌ها (درایور فعال پروژه: asyncmy)
_MYSQL_DIALECT = mysql.dialect()

# 🗄 data-fixهای «فقط بعد از افزودن ستون» — اگر ستون از قبل موجود باشد، اجرا نمی‌شوند.
_POST_ADD_FIXES: Dict[Tuple[str, str], str] = {
    # فاز ۶ (R7) — دقیقاً مطابق دستور مستند در docstring مدل Account:
    # اکانت‌های قدیمی (created_at بیشتر از ۲۴ ساعت پیش) بلافاصله «گرم» تلقی می‌شوند.
    ("accounts", "warmed_up_at"): (
        "UPDATE accounts SET warmed_up_at = IF("
        " created_at IS NULL"
        " OR created_at <= UTC_TIMESTAMP() - INTERVAL 24 HOUR,"
        " UTC_TIMESTAMP(),"
        " DATE_ADD(created_at, INTERVAL 24 HOUR)"
        ") WHERE warmed_up_at IS NULL"
    ),
    ("orders", "extracted_count"): (
        "UPDATE orders SET extracted_count = ("
        "  SELECT COUNT(*) FROM order_logs ol"
        "  WHERE ol.order_id = orders.id AND ol.status = 'success'"
        ") WHERE order_type = 'extract' AND extracted_count IS NULL"
    ),
    ("orders", "user_id"): "SELECT 1",
    ("orders", "speed_mode"): "UPDATE orders SET speed_mode = 'safe' WHERE speed_mode IS NULL",
    # 🧱 فاز ۳: تبدیل اکانت‌های قبلی به وضعیت‌های استاندارد قرارداد جدید
    ("accounts", "proxy_status"): (
        "UPDATE accounts SET proxy_status = IF(proxy_string IS NOT NULL, 'ASSIGNED', 'WAITING_PROXY') "
        "WHERE proxy_status IS NULL OR proxy_status = ''"
    ),
}


def _sql_literal(value) -> str:
    """تبدیل امن مقدار scalar پایتونی به literal SQL (فقط برای DEFAULT ستون جدید)."""
    if value is None:
        return "NULL"
    if value is True:
        return "1"
    if value is False:
        return "0"
    if isinstance(value, (int, float)):
        return str(value)
    return "'" + str(value).replace("\\", "\\\\").replace("'", "''") + "'"


def _add_column_ddl(table_name: str, column) -> str:
    """ساخت «ALTER TABLE ... ADD COLUMN ...» منطبق با تعریف ستون در مدل."""
    type_sql = column.type.compile(_MYSQL_DIALECT)
    column_def = f"`{column.name}` {type_sql}"
    if not column.nullable:
        column_def += " NOT NULL"
    # default کلاینت‌سمتِ ORM → برای ردیف‌های موجود به‌صورت DEFAULT نوشته می‌شود
    if column.default is not None and column.default.is_scalar:
        column_def += f" DEFAULT {_sql_literal(column.default.arg)}"
    return f"ALTER TABLE `{table_name}` ADD COLUMN {column_def}"


def _add_index_ddl(table_name: str, column) -> str:
    return (f"ALTER TABLE `{table_name}` ADD INDEX "
            f"`ix_{table_name}_{column.name}` (`{column.name}`)")


def _add_fk_ddl(table_name: str, column, fk) -> str:
    on_delete = f" ON DELETE {fk.ondelete}" if fk.ondelete else ""
    # 🧹 فاز ۴ (رفع آنتی‌پترن): عبارت قبلی «table.name if False else table_name»
    # شاخه‌ی if False هرگز اجرا نمی‌شد و در صورت اجرا با NameError شکست می‌خورد
    # (متغیر table در این scope تعریف نشده است). فرم تمیز و منطقاً معادل:
    return (f"ALTER TABLE `{table_name}` ADD CONSTRAINT "
            f"`fk_{table_name}_{column.name}` "
            f"FOREIGN KEY (`{column.name}`) REFERENCES `{fk.column.table.name}` "
            f"(`{fk.column.name}`){on_delete}")


# database/migrations.py

async def run_startup_migrations() -> None:
    """باید «بعد از» init_db (create_all) صدا زده شود تا جدول‌های جدید موجود باشند."""
    async with async_session() as session:
        # ۱) عکسِ وضعیت فعلی schema از INFORMATION_SCHEMA
        col_rows = (await session.execute(text(
            "SELECT TABLE_NAME, COLUMN_NAME FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA = DATABASE()"
        ))).fetchall()

        existing_columns: Dict[str, Set[str]] = {}
        for table_name, column_name in col_rows:
            existing_columns.setdefault(table_name, set()).add(column_name)

        fk_rows = (await session.execute(text(
            "SELECT TABLE_NAME, COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE "
            "WHERE TABLE_SCHEMA = DATABASE() AND REFERENCED_TABLE_NAME IS NOT NULL"
        ))).fetchall()
        existing_fks: Set[Tuple[str, str]] = {(t, c) for t, c in fk_rows}

        for table in Base.metadata.sorted_tables:
            if table.name not in existing_columns:
                # جدول جدید — create_all آن را کامل ساخته است
                continue

            db_columns = existing_columns[table.name]
            present_columns = set(db_columns)

            # ۲) ستون‌های غایب → ALTER
            #    ⚠️ خطا عمداً propagate می‌شود تا مکانیزم retry استارتاپ فعال شود.
            for column in table.columns:
                if column.name in db_columns:
                    continue
                if column.primary_key:
                    logger.error(
                        "DEP-1: ستون PK «%s.%s» غایب است — schema drift جدی؛ "
                        "مهاجرت خودکار رد شد (بررسی دستی لازم).",
                        table.name, column.name,
                    )
                    continue

                ddl = _add_column_ddl(table.name, column)
                logger.warning("DEP-1: افزودن ستون غایب → %s", ddl)
                await session.execute(text(ddl))
                await session.commit()
                present_columns.add(column.name)

                # ایندکس (غیربحراتی — شکست نباید استارتاپ را متوقف کند)
                if column.index:
                    try:
                        idx_ddl = _add_index_ddl(table.name, column)
                        await session.execute(text(idx_ddl))
                        await session.commit()
                    except Exception as exc:
                        logger.warning("DEP-1: ساخت ایندکس ناموفق (%s): %s", idx_ddl, exc)

                # data-fix مستندشده (مثل backfill warmed_up_at)
                fix_sql = _POST_ADD_FIXES.get((table.name, column.name))
                if fix_sql:
                    fix_res = await session.execute(text(fix_sql))
                    await session.commit()
                    logger.warning(
                        "DEP-1: data-fix اجرا شد (%s.%s — %s ردیف).",
                        table.name, column.name, fix_res.rowcount,
                    )

            # ۳) FKهای غایب روی ستون‌های موجود (غیربحراتی — behavioral parity مثل
            #    ON DELETE SET NULL برای photo_package_id)
            for fk in table.foreign_keys:
                local_col = fk.parent
                if local_col.name not in present_columns:
                    continue
                if (table.name, local_col.name) in existing_fks:
                    continue
                try:
                    fk_ddl = _add_fk_ddl(table.name, local_col, fk)
                    await session.execute(text(fk_ddl))
                    await session.commit()
                    logger.warning("DEP-1: افزودن FK غایب → %s", fk_ddl)
                except Exception as exc:
                    logger.warning(
                        "DEP-1: افزودن FK ناموفق (%s.%s — غیربحرانی): %s",
                        table.name, local_col.name, exc,
                    )

        # ۴) رفع باگ فاز ۳: بررسی و ایجاد ایندکس یکتا برای api_keys.api_id
        idx_check_sql = """
        SELECT COUNT(1) 
        FROM information_schema.STATISTICS 
        WHERE TABLE_SCHEMA = DATABASE() 
          AND TABLE_NAME = 'api_keys' 
          AND INDEX_NAME = 'uq_api_keys_api_id'
        """
        has_idx = (await session.execute(text(idx_check_sql))).scalar()

        if not has_idx:
            logger.warning("DEP-1: در حال حذف APIهای تکراری و ایجاد ایندکس یکتا برای api_id...")
            # حذف ردیف‌های تکراری و نگه‌داشتن کوچک‌ترین id برای هر api_id
            dedup_sql = """
            DELETE t1 FROM api_keys t1
            INNER JOIN api_keys t2 
            WHERE t1.id > t2.id AND t1.api_id = t2.api_id
            """
            await session.execute(text(dedup_sql))
            
            # افزودن ایندکس یکتا
            add_idx_sql = "ALTER TABLE api_keys ADD UNIQUE INDEX uq_api_keys_api_id (api_id)"
            await session.execute(text(add_idx_sql))
            await session.commit()
        try:
            fk_check_sql = """
            SELECT DELETE_RULE 
            FROM information_schema.REFERENTIAL_CONSTRAINTS 
            WHERE CONSTRAINT_SCHEMA = DATABASE() 
              AND TABLE_NAME = 'accounts' 
              AND CONSTRAINT_NAME = 'fk_accounts_category_id'
            """
            delete_rule = (await session.execute(text(fk_check_sql))).scalar()

            if delete_rule and delete_rule.upper() != "SET NULL":
                logger.warning("DEP-1: در حال اصلاح رفتار FK دسته‌بندی‌ها به SET NULL (حفظ اکانت‌ها)...")
                drop_fk_sql = "ALTER TABLE accounts DROP FOREIGN KEY fk_accounts_category_id"
                await session.execute(text(drop_fk_sql))
                
                add_fk_sql = """
                ALTER TABLE accounts 
                ADD CONSTRAINT fk_accounts_category_id 
                FOREIGN KEY (category_id) REFERENCES categories(id) ON DELETE SET NULL
                """
                await session.execute(text(add_fk_sql))
                await session.commit()
                logger.info("DEP-1: اصلاح FK دسته‌بندی‌ها با موفقیت انجام شد.")
        except Exception as exc:
            logger.warning("DEP-1: اصلاح FK دسته‌بندی‌ها (SET NULL) ناموفق بود (احتمالاً دیتابیس تازه است): %s", exc)
            await session.rollback()

        # +++ اضافه شدن بلوک اصلاح سایز ستون‌های رمزنگاری شده برای رفع خطای Data too long +++
        try:
            logger.info("DEP-1: در حال اصلاح سایز ستون‌های رمزنگاری شده...")
            await session.execute(text("ALTER TABLE accounts MODIFY COLUMN last_login_code VARCHAR(255)"))
            await session.execute(text("ALTER TABLE accounts MODIFY COLUMN two_step_password VARCHAR(512)"))
            await session.commit()
            logger.info("DEP-1: سایز ستون‌ها با موفقیت به‌روزرسانی شد.")
        except Exception as exc:
            logger.warning("DEP-1: تغییر سایز ستون‌ها ناموفق بود: %s", exc)
            await session.rollback()
        # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

        # +++ ارتقای ستون‌های Orders به LONGTEXT برای جلوگیری از خطای Data too long (رانش طرح) +++
        try:
            col_type_sql = """
            SELECT COLUMN_NAME, DATA_TYPE 
            FROM information_schema.COLUMNS 
            WHERE TABLE_SCHEMA = DATABASE() 
              AND TABLE_NAME = 'orders' 
              AND COLUMN_NAME IN ('target_data', 'inflight_data')
            """
            col_types = (await session.execute(text(col_type_sql))).fetchall()
            for c_name, d_type in col_types:
                if d_type.upper() == 'TEXT':
                    logger.info(f"DEP-2: ارتقای ستون {c_name} به LONGTEXT...")
                    await session.execute(text(f"ALTER TABLE orders MODIFY COLUMN {c_name} LONGTEXT"))
            await session.commit()
        except Exception as exc:
            logger.warning("DEP-2: ارتقای سایز ستون‌های Order به LONGTEXT ناموفق بود: %s", exc)
            await session.rollback()
        # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

        # +++ فاز اختیاری: استراتژی ورکر ساکن +++
        try:
            exists = await session.scalar(
                text(
                    "SELECT COUNT(*) FROM information_schema.columns "
                    "WHERE table_schema = DATABASE() "
                    "AND table_name = 'global_settings' AND column_name = 'worker_residency'"
                )
            )
            if not exists:
                await session.execute(text("ALTER TABLE global_settings ADD COLUMN worker_residency VARCHAR(15) NOT NULL DEFAULT 'ephemeral'"))
                await session.commit()
                logger.info("Migration: global_settings.worker_residency column added.")
        except Exception as exc:
            logger.warning("Migration for worker_residency failed: %s", exc)
            await session.rollback()
        # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
        
        # +++ فاز ۳: مهاجرت idempotent برای تنظیمات تهاجمی‌تر +++
        try:
            legacy_settings_sql = """
            UPDATE global_settings
            SET send_limit_per_run = 80, cooldown_hours = 1, spam_penalty_days = 1
            WHERE send_limit_per_run = 40 AND cooldown_hours = 24 AND spam_penalty_days = 3;
            """
            res = await session.execute(text(legacy_settings_sql))
            await session.commit()
            if res.rowcount > 0:
                logger.info("Phase 3: Migrated legacy GlobalSettings to aggressive speed limits.")
        except Exception as exc:
            logger.warning("Phase 3 Settings Migration failed: %s", exc)
            await session.rollback()
        # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
        
        # +++ فاز ۴: اصلاح پیش‌فرض auto_set_photo در سطح دیتابیس +++
        try:
            await session.execute(text("ALTER TABLE global_settings ALTER COLUMN auto_set_photo SET DEFAULT 1"))
            await session.commit()
            logger.info("DEP-1: Default value for auto_set_photo updated to 1 in database schema.")
        except Exception as exc:
            logger.warning("DEP-1: Migration for auto_set_photo DEFAULT failed: %s", exc)
        # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

        # +++ فاز ۷: یکدست‌سازی مبنای زمانی دیتابیس به UTC (Historical Data Shift) +++
        try:
            tz_mig_exists = await session.scalar(
                text(
                    "SELECT COUNT(*) FROM information_schema.columns "
                    "WHERE table_schema = DATABASE() "
                    "AND table_name = 'global_settings' AND column_name = 'tz_utc_migrated'"
                )
            )
            if not tz_mig_exists:
                # Sanity Check: بررسی می‌کنیم آیا دیتابیس از قبل تهران‌محور بوده است یا خیر.
                # فرض بر این است که اگر رکوردی داشته باشیم که created_at آن در آینده (نسبت به UTC) باشد، یعنی با تایم‌زون محلی ذخیره شده است.
                future_dates = await session.scalar(
                    text("SELECT COUNT(*) FROM orders WHERE created_at > UTC_TIMESTAMP() + INTERVAL 30 MINUTE")
                )
                
                if future_dates and future_dates > 0:
                    logger.info("DEP-7: دیتابیس تهران‌محور تشخیص داده شد. شیفت به UTC (-3:30)...")
                    for table_name in ['accounts', 'orders', 'order_logs', 'worker_events']:
                        await session.execute(text(f"UPDATE {table_name} SET created_at = DATE_SUB(created_at, INTERVAL 210 MINUTE) WHERE created_at IS NOT NULL"))
                    
                    try:
                        await session.execute(text("UPDATE order_joins SET created_at = DATE_SUB(created_at, INTERVAL 210 MINUTE) WHERE created_at IS NOT NULL"))
                    except Exception:
                        pass
                    
                    logger.info("DEP-7: Historical timestamps shifted to UTC successfully.")
                else:
                    logger.warning("DEP-7: داده‌ها تهران‌محور نیستند (خالی یا از قبل UTC). شیفت زمانی لغو شد.")
                
                # پرچم idempotent: ایجاد فیلد برای جلوگیری از تکرار مجدد بررسی
                await session.execute(text("ALTER TABLE global_settings ADD COLUMN tz_utc_migrated TINYINT(1) NOT NULL DEFAULT 1"))
                await session.commit()
        except Exception as exc:
            logger.warning("DEP-7: Timezone migration failed: %s", exc)
            await session.rollback()
        # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

        idx_ol_check = """
        SELECT COUNT(1) FROM information_schema.STATISTICS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = 'order_logs'
          AND INDEX_NAME = 'ix_order_logs_order_status'
        """
        if not (await session.execute(text(idx_ol_check))).scalar():
            await session.execute(text(
                "ALTER TABLE order_logs ADD INDEX ix_order_logs_order_status (order_id, status)"
            ))
            await session.commit()
            logger.warning("DEP-1: composite index ix_order_logs_order_status created.")

    logger.info("DEP-1: بررسی مهاجرت idempotent schema تکمیل شد.")

async def migrate_admins_progress_notify(engine) -> None:
    """
    Phase 5 — ensures admins.progress_notify (TINYINT(1) NOT NULL DEFAULT 1).

    Idempotent: safe to run on every startup and concurrently
    (information_schema existence check + duplicate-column swallow).
    """
    from sqlalchemy import text  # local import keeps module-level deps untouched
    import logging
    log = logging.getLogger(__name__)

    try:
        async with engine.connect() as conn:
            exists = await conn.scalar(
                text(
                    "SELECT COUNT(*) FROM information_schema.columns "
                    "WHERE table_schema = DATABASE() "
                    "AND table_name = 'admins' AND column_name = 'progress_notify'"
                )
            )
            if exists:
                return
            await conn.execute(
                text(
                    "ALTER TABLE admins "
                    "ADD COLUMN progress_notify TINYINT(1) NOT NULL DEFAULT 1"
                )
            )
            await conn.commit()
            log.info("Migration: admins.progress_notify column ensured.")
    except Exception as e:
        if "duplicate column" in str(e).lower():
            return  # a concurrent migration already added it
        log.error(f"Migration migrate_admins_progress_notify failed: {e}")

async def migrate_account_limits_columns(engine) -> None:
    """
    Phase 12-B — ensures limit and spambot columns exist in accounts table.
    Idempotent: safe to run on every startup and concurrently.
    """
    from sqlalchemy import text
    import logging
    log = logging.getLogger(__name__)

    columns = [
        ("last_limit_type", "VARCHAR(50) NULL"),
        ("spambot_report", "TEXT NULL"),
        ("spambot_checked_at", "DATETIME NULL"),
        ("restricted_until", "DATETIME NULL")
    ]

    try:
        async with engine.connect() as conn:
            for col_name, col_type in columns:
                exists = await conn.scalar(
                    text(
                        "SELECT COUNT(*) FROM information_schema.columns "
                        "WHERE table_schema = DATABASE() "
                        "AND table_name = 'accounts' AND column_name = :cname"
                    ),
                    {"cname": col_name}
                )
                if not exists:
                    await conn.execute(
                        text(f"ALTER TABLE accounts ADD COLUMN {col_name} {col_type}")
                    )
            await conn.commit()
            log.info("Migration: accounts limit columns ensured.")
    except Exception as e:
        if "duplicate column" in str(e).lower():
            return
        log.error(f"Migration migrate_account_limits_columns failed: {e}")

async def migrate_proxy_health_and_settings(engine) -> None:
    """
    Phase New — ensures proxy health columns and global sending setting exist.
    Idempotent: safe to run on every startup and concurrently.
    """
    from sqlalchemy import text
    import logging
    log = logging.getLogger(__name__)

    proxy_columns = [
        ("usage_type", "VARCHAR(20) NOT NULL DEFAULT 'both'"),
        ("is_healthy", "TINYINT(1) NOT NULL DEFAULT 1"),
        ("health_state", "VARCHAR(20) NOT NULL DEFAULT 'HEALTHY'"),
        ("consecutive_successes", "INT NOT NULL DEFAULT 0"),
        ("consecutive_failures", "INT NOT NULL DEFAULT 0"),
        ("last_state_changed_at", "DATETIME NULL"),
        ("ping_ms", "INT NULL"),
        ("last_checked_at", "DATETIME NULL")
    ]

    try:
        async with engine.begin() as conn: # استفاده از تراکنش خودکار
            # 1. Update Global Settings
            settings_exists = await conn.scalar(
                text(
                    "SELECT COUNT(*) FROM information_schema.columns "
                    "WHERE table_schema = DATABASE() "
                    "AND table_name = 'global_settings' AND column_name = 'use_proxy_for_sending'"
                )
            )
            if not settings_exists:
                await conn.execute(
                    text("ALTER TABLE global_settings ADD COLUMN use_proxy_for_sending TINYINT(1) NOT NULL DEFAULT 1")
                )
                
            
            # 2. Update Proxies Table
            for col_name, col_type in proxy_columns:
                exists = await conn.scalar(
                    text(
                        "SELECT COUNT(*) FROM information_schema.columns "
                        "WHERE table_schema = DATABASE() "
                        "AND table_name = 'proxies' AND column_name = :cname"
                    ),
                    {"cname": col_name}
                )
                if not exists:
                    try:
                        await conn.execute(
                            text(f"ALTER TABLE proxies ADD COLUMN {col_name} {col_type}")
                        )
                    except Exception as col_err:
                        # جلوگیری از کرش در صورت تداخل همزمان (Race Condition)
                        if "duplicate column" not in str(col_err).lower():
                            raise col_err
                            
            log.info("Migration: Proxy health and global settings columns ensured safely.")
    except Exception as e:
        log.error(f"Migration migrate_proxy_health_and_settings failed but caught safely: {e}")

    