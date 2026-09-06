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