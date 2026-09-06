#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
🔐 فاز ۱۰ (SEC-2 / BUG-07) — مهاجرت یک‌بارهٔ رمزنگاری مقادیر حساس موجود

two_step_password و last_login_code همهٔ اکانت‌ها را از خام → Fernet-encrypted
تبدیل می‌کند (session_string از قبل رمز است و دست نمی‌خورد).

ترتیب اجرای صحیح:
  ۱) بکاپ:
     docker compose exec db sh -c 'mysqldump -u root -p"$MYSQL_ROOT_PASSWORD" --single-transaction "$MYSQL_DATABASE"' > backup_$(date +%F).sql
  ۲) بزرگ‌کردن ستون‌ها (لازم — توکن Fernet طولانی‌تر از VARCHAR فعلی است):
     docker compose exec db sh -c 'mysql -u root -p"$MYSQL_ROOT_PASSWORD" "$MYSQL_DATABASE" -e "ALTER TABLE accounts MODIFY COLUMN last_login_code VARCHAR(255) NULL; ALTER TABLE accounts MODIFY COLUMN two_step_password VARCHAR(512) NULL;"'
  ۳) dry-run:  docker compose exec bot python scripts/migrate_encrypt_secrets.py
  ۴) اجرا:     docker compose exec bot python scripts/migrate_encrypt_secrets.py --apply
  ۵) راستی‌آزمایی: مرحلهٔ ۳ دوباره — همه باید «از قبل رمز» گزارش شوند

خصوصیت‌ها:
  - idempotent: مقدار قابل-decrypt با Fernet → skip می‌شود
  - پیش‌فرض dry-run؛ فقط با --apply نوشتن انجام می‌شود
  - هیچ مقدار حساسی لاگ نمی‌شود (فقط id ردیف + نام ستون + وضعیت)
  - گارد طول: توکن بلندتر از ستون → ردیف رد می‌شود، بدون کرش

⚠️ در حین اجرا لاگین/ویرایش اکانت انجام نشود (یا bot را stop کنید و از
   `docker compose run --rm bot python scripts/migrate_encrypt_secrets.py ...` استفاده کنید).
"""
import argparse
import asyncio
import importlib.util
import logging
import os
import sys
from urllib.parse import quote_plus

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

# اجرا از ریشهٔ پروژه (داخل کانتینر: WORKDIR /app) → import utils.crypto

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from utils.crypto import encrypt_session, decrypt_session  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("migrate_encrypt_secrets")

COLUMNS = ("last_login_code", "two_step_password")
MAX_LEN = {"last_login_code": 255, "two_step_password": 512}  # بعد از ALTER


def _is_encrypted(value: str) -> bool:
    """decrypt موفق = از قبل رمز شده."""
    try:
        return decrypt_session(value) is not None
    except Exception:
        return False


def _engine():
    name, user, pwd = os.getenv("DB_NAME"), os.getenv("DB_USER"), os.getenv("DB_PASS")
    if not all((name, user, pwd)):
        sys.exit("❌ DB_NAME/DB_USER/DB_PASS از env خوانده نشد — داخل کانتینر bot اجرا کنید.")
    driver = next((m for m in ("aiomysql", "asyncmy") if importlib.util.find_spec(m)), None)
    if not driver:
        sys.exit("❌ درایور async MySQL (aiomysql/asyncmy) در image یافت نشد.")
    host, port = os.getenv("DB_HOST", "db"), os.getenv("DB_PORT", "3306")
    return create_async_engine(
        f"mysql+{driver}://{quote_plus(user)}:{quote_plus(pwd)}@{host}:{port}/{name}"
    )


async def run(apply_changes: bool) -> None:
    engine = _engine()
    encrypted = already = rejected = 0
    try:
        async with engine.connect() as conn:
            rows = (await conn.execute(text(
                "SELECT id, last_login_code, two_step_password FROM accounts "
                "WHERE (last_login_code IS NOT NULL AND last_login_code <> '') "
                "   OR (two_step_password IS NOT NULL AND two_step_password <> '')"
            ))).mappings().all()
        logger.info(f"اکانت‌های دارای مقادیر حساس: {len(rows)}")

        for row in rows:
            for col in COLUMNS:
                val = row[col]
                if not val:
                    continue
                if _is_encrypted(val):
                    already += 1
                    continue
                enc = encrypt_session(val)
                if len(enc) > MAX_LEN[col]:
                    rejected += 1
                    logger.error(
                        f"id={row['id']} col={col}: توکن {len(enc)} > سقف {MAX_LEN[col]} — "
                        f"ستون را بزرگ‌تر کنید و دوباره اجرا کنید."
                    )
                    continue
                encrypted += 1
                if apply_changes:
                    async with engine.begin() as conn:
                        await conn.execute(
                            text(f"UPDATE accounts SET {col} = :v WHERE id = :id"),
                            {"v": enc, "id": row["id"]},
                        )
                    logger.info(f"id={row['id']} col={col}: رمزنگاری شد ✔")

        logger.info(
            f"نتیجه [{'APPLY' if apply_changes else 'DRY-RUN'}]: "
            f"encrypt={encrypted} | already_encrypted={already} | rejected={rejected}"
        )
        if not apply_changes and encrypted:
            logger.info("→ برای نوشتن واقعی، همان دستور را با --apply اجرا کنید.")
    finally:
        await engine.dispose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SEC-2: encrypt plaintext 2FA/login-code")
    parser.add_argument("--apply", action="store_true", help="نوشتن در DB (پیش‌فرض: dry-run)")
    asyncio.run(run(parser.parse_args().apply))