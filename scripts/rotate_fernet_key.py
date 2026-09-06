#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
🔐 فاز ۱۰ (SEC-1) — چرخش FERNET_KEY با re-encrypt داده‌های موجود
ستون‌ها: accounts.session_string / two_step_password / last_login_code
منطق هر مقدار: decrypt با OLD موفق → re-encrypt با NEW؛
               decrypt با OLD ناموفق (plaintext) → مستقیم encrypt با NEW.
اجرا (db روشن، ربات ننویسد): docker compose run --rm -e OLD_FERNET_KEY=... -e NEW_FERNET_KEY=... bot python scripts/rotate_fernet_key.py [--apply]
⚠️ هیچ مقدار حساسی لاگ نمی‌شود.
"""
import argparse
import asyncio
import importlib.util
import logging
import os
import sys
from urllib.parse import quote_plus

from cryptography.fernet import Fernet
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("rotate_fernet_key")

COLUMNS = ("session_string", "two_step_password", "last_login_code")
MAX_LEN = {"session_string": None, "two_step_password": 512, "last_login_code": 255}


def _engine():
    name, user, pwd = os.getenv("DB_NAME"), os.getenv("DB_USER"), os.getenv("DB_PASS")
    if not all((name, user, pwd)):
        sys.exit("❌ DB_NAME/DB_USER/DB_PASS از env خوانده نشد — داخل کانتینر bot اجرا کنید.")
    driver = next((m for m in ("aiomysql", "asyncmy") if importlib.util.find_spec(m)), None)
    if not driver:
        sys.exit("❌ درایور async MySQL (aiomysql/asyncmy) یافت نشد.")
    host, port = os.getenv("DB_HOST", "db"), os.getenv("DB_PORT", "3306")
    return create_async_engine(
        f"mysql+{driver}://{quote_plus(user)}:{quote_plus(pwd)}@{host}:{port}/{name}"
    )


async def run(apply_changes: bool) -> None:
    old_key, new_key = os.getenv("OLD_FERNET_KEY"), os.getenv("NEW_FERNET_KEY")
    if not old_key or not new_key:
        sys.exit("❌ OLD_FERNET_KEY / NEW_FERNET_KEY ست نشده‌اند.")
    old_cipher, new_cipher = Fernet(old_key.encode()), Fernet(new_key.encode())

    stats = {c: 0 for c in COLUMNS}
    rejected = 0
    engine = _engine()
    try:
        async with engine.connect() as conn:
            rows = (await conn.execute(text(
                "SELECT id, session_string, two_step_password, last_login_code FROM accounts"
            ))).mappings().all()
        logger.info(f"accounts: {len(rows)}")

for row in rows:
            for col in COLUMNS:
                val = row[col]
                if not val:
                    continue
                    
                # اصلاح امن برای جلوگیری از خرابی داده‌های از قبل مهاجرت‌یافته
                try:
                    # بررسی اینکه آیا قبلاً با کلید جدید رمزنگاری شده است
                    new_cipher.decrypt(val.encode())
                    continue  # با موفقیت توسط کلید جدید باز شد، پس نادیده بگیر و رد شو
                except Exception:
                    try:
                        # اگر با کلید جدید باز نشد، با کلید قدیم رمزگشایی کن
                        plain = old_cipher.decrypt(val.encode()).decode()
                    except Exception:
                        plain = val  # plaintext (مثل داده‌های pre-SEC-2)
                        
                enc = new_cipher.encrypt(plain.encode()).decode()
                
                limit = MAX_LEN[col]
                if limit and len(enc) > limit:
                    rejected += 1
                    logger.error(f"id={row['id']} col={col}: توکن {len(enc)} > {limit} — ستون را بزرگ‌تر کنید.")
                    continue
                stats[col] += 1
                if apply_changes:
                    async with engine.begin() as conn:
                        await conn.execute(
                            text(f"UPDATE accounts SET {col} = :v WHERE id = :id"),
                            {"v": enc, "id": row["id"]},
                        )
        logger.info(f"نتیجه [{'APPLY' if apply_changes else 'DRY-RUN'}]: {stats} | rejected={rejected}")
    finally:
        await engine.dispose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SEC-1: rotate FERNET_KEY + re-encrypt")
    parser.add_argument("--apply", action="store_true", help="نوشتن در DB (پیش‌فرض: dry-run)")
    asyncio.run(run(parser.parse_args().apply))