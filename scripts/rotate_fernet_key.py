# scripts/rotate_fernet_key.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
🔐 فاز ۱۰ (SEC-1) — چرخش FERNET_KEY با re-encrypt داده‌های موجود
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

    if apply_changes:
        confirm = input("⚠️ آیا از دیتابیس بکاپ گرفته‌اید؟ (y/N): ")
        if confirm.lower() != 'y':
            sys.exit("❌ عملیات لغو شد. لطفاً ابتدا بکاپ بگیرید.")

    stats = {c: {"rotated": 0, "skipped": 0, "unrecoverable": 0} for c in COLUMNS}
    rejected = 0
    unrecoverable_items = []
    
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
                    
                # گام ۱: آیا قبلاً با کلید جدید رمز شده؟
                try:
                    new_cipher.decrypt(val.encode())
                    stats[col]["skipped"] += 1
                    continue
                except Exception:
                    pass

                # گام ۲: تلاش برای باز کردن با کلید قدیم
                plain = None
                try:
                    plain = old_cipher.decrypt(val.encode()).decode()
                except Exception:
                    # گام ۳: شاید کلاً Plaintext است (مانند داده‌های Pre-SEC-2)
                    if not val.startswith("gAAAAA"):
                        plain = val
                    else:
                        # 🚨 سایفرتکست خراب و غیرقابل بازیابی! نباید دوباره رمز شود.
                        stats[col]["unrecoverable"] += 1
                        unrecoverable_items.append(f"Account ID: {row['id']} | Column: {col}")
                        continue
                        
                # رمزنگاری با کلید جدید
                enc = new_cipher.encrypt(plain.encode()).decode()
                
                limit = MAX_LEN[col]
                if limit and len(enc) > limit:
                    rejected += 1
                    logger.error(f"id={row['id']} col={col}: توکن {len(enc)} > {limit} — ستون را بزرگ‌تر کنید.")
                    continue
                
                stats[col]["rotated"] += 1
                
                if apply_changes:
                    async with engine.begin() as conn:
                        await conn.execute(
                            text(f"UPDATE accounts SET {col} = :v WHERE id = :id"),
                            {"v": enc, "id": row["id"]},
                        )
                        
        logger.info(f"نتیجه [{'APPLY' if apply_changes else 'DRY-RUN'}]:")
        for c, s in stats.items():
            logger.info(f" - {c}: چرخش‌یافته={s['rotated']} | نادیده‌گرفته={s['skipped']} | غیرقابل‌بازیابی={s['unrecoverable']}")
        logger.info(f"Rejected (size limit): {rejected}")
        
        if unrecoverable_items:
            logger.warning("🚨 آیتم‌های غیرقابل بازیابی (نیاز به بررسی دستی):")
            for item in unrecoverable_items[:10]:
                logger.warning(f"   - {item}")
            if len(unrecoverable_items) > 10:
                logger.warning(f"   ... و {len(unrecoverable_items) - 10} مورد دیگر.")
                
    finally:
        await engine.dispose()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SEC-1: rotate FERNET_KEY + re-encrypt")
    parser.add_argument("--apply", action="store_true", help="نوشتن در DB (پیش‌فرض: dry-run)")
    asyncio.run(run(parser.parse_args().apply))