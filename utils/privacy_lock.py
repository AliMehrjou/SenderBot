# utils/privacy_lock.py
"""
🔒 (خواسته کارفرما) قفل حریم خصوصی اکانت‌های سندر
=====================================================
هر اکانتی که به ربات «اد» می‌شود (مسیر لاگین با شماره، فایل سشن تکی یا گروهی)
باید به‌صورت خودکار از نظر حریم خصوصی قفل شود:

  • شماره تلفن            → Nobody   (هیچ‌کس نبیند)
  • آخرین بازدید (LastSeen) → Nobody
  • افزودن به گروه/کانال   → Nobody   (کلاً بسته)
  • تماس‌ها                → Nobody
  • تماس P2P               → Nobody
  • فوروارد پیام‌ها        → Nobody
  • پیام‌های صوتی           → Nobody   (در صورت پریمیوم بودن اکانت)
  • عکس پروفایل            → Everybody (عمومی می‌ماند)
  • بیو (About)            → Everybody (عمومی می‌ماند — اگر لایه API پشتیبانی کند)

فقط و فقط «بیو» و «پروفایل» عمومی هستند؛ بقیه چیزها بسته می‌شوند.
"""

import asyncio
import logging
import random
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from pyrogram import Client, raw
from pyrogram.errors import (
    AuthKeyDuplicated,
    AuthKeyUnregistered,
    FloodWait,
    RPCError,
    SessionExpired,
    SessionRevoked,
    Unauthorized,
    UserDeactivated,
    UserDeactivatedBan,
)
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from database.engine import async_session
from database.models import Account

logger = logging.getLogger(__name__)

# سطرهای جدول خصوصی: (attribute_name روی raw.types, قانون, توضیح فارسی)
# ترتیب عمداً از حساس‌ترین چیزها شروع می‌شود (شماره / آخرین بازدید).
_PRIVACY_ROWS: List[Tuple[str, str, str]] = [
    ("InputPrivacyKeyPhoneNumber",     "InputPrivacyValueDisallowAll", "شماره تلفن"),
    ("InputPrivacyKeyStatusTimestamp", "InputPrivacyValueDisallowAll", "آخرین بازدید"),
    ("InputPrivacyKeyChatInvite",      "InputPrivacyValueDisallowAll", "افزودن به گروه/کانال"),
    ("InputPrivacyKeyPhoneCall",       "InputPrivacyValueDisallowAll", "تماس‌ها"),
    ("InputPrivacyKeyPhoneP2P",        "InputPrivacyValueDisallowAll", "تماس P2P"),
    ("InputPrivacyKeyForwards",        "InputPrivacyValueDisallowAll", "فوروارد پیام‌ها"),
    ("InputPrivacyKeyVoiceMessages",   "InputPrivacyValueDisallowAll", "پیام صوتی"),
    # 🔓 این دو مورد باید عمومی بمانند (خواسته صریح کارفرما):
    ("InputPrivacyKeyProfilePhoto",    "InputPrivacyValueAllowAll",    "عکس پروفایل"),
    ("InputPrivacyKeyAbout",           "InputPrivacyValueAllowAll",    "بیو"),
]

# فاصله انسانی بین هر فراخوانی SetPrivacy تا الگوی رباتیک تشخیص داده نشود
_CALL_GAP_MIN = 0.8
_CALL_GAP_MAX = 2.2

# حداکثر زمان مجاز انتظار برای FloodWait در لحظه (جلوگیری از فریز شدن لاگین/ورکر)
_MAX_FLOOD_WAIT_SECONDS = 30

# خطاهای کشنده سشن که در صورت وقوع، ادامه حلقه بی‌فایده است
_FATAL_SESSION_ERRORS = (
    AuthKeyUnregistered,
    AuthKeyDuplicated,
    SessionRevoked,
    SessionExpired,
    Unauthorized,
    UserDeactivated,
    UserDeactivatedBan,
)

# قفل انحصاری به ازای هر اکانت برای جلوگیری از اجرای همزمان دو تسک روی یک اکانت
_ACCOUNT_LOCKS: Dict[int, asyncio.Lock] = {}


def _get_account_lock(account_id: int) -> asyncio.Lock:
    lock = _ACCOUNT_LOCKS.get(account_id)
    if lock is None:
        lock = asyncio.Lock()
        _ACCOUNT_LOCKS[account_id] = lock
    return lock


def _resolve(key_name: str, rule_name: str):
    """پیدا کردن امن تایپ‌های خام؛ اگر لایه کتابخانه کلیدی را نداشت، None برمی‌گردد."""
    key_cls = getattr(raw.types, key_name, None)
    rule_cls = getattr(raw.types, rule_name, None)
    if key_cls is None or rule_cls is None:
        return None
    return key_cls(), [rule_cls()]


async def _apply_privacy_rules_internal(
    client: Client,
    account_id: Optional[int],
) -> Tuple[int, List[str], bool]:
    """
    اجرای داخلی قوانین حریم خصوصی.
    خروجی: (تعداد موفق، لیست نام‌های موفق، آیا حلقه بدون توقف اضطراری کامل شد؟)
    """
    success_count = 0
    applied_names: List[str] = []
    completed_cleanly = True

    who = f"user_{account_id}/" if account_id is not None else str(getattr(client, "name", "?"))

    if not getattr(client, "is_connected", False):
        logger.debug(f"PrivacyLock {who}: client not connected — skipping (will retry on next start).")
        return 0, [], False

    supported_rows = []
    for key_name, rule_name, fa_desc in _PRIVACY_ROWS:
        resolved = _resolve(key_name, rule_name)
        if resolved is None:
            logger.debug(f"PrivacyLock {who}: key '{key_name}' not supported by this layer — skipped.")
            continue
        supported_rows.append((key_name, rule_name, fa_desc, resolved[0], resolved[1]))

    total_supported = len(supported_rows)

    for idx, (key_name, rule_name, fa_desc, key_obj, rules) in enumerate(supported_rows):
        if not getattr(client, "is_connected", False):
            logger.warning(f"PrivacyLock {who}: client disconnected mid-execution at '{fa_desc}' — aborting.")
            completed_cleanly = False
            break

        try:
            await client.invoke(raw.functions.account.SetPrivacy(key=key_obj, rules=rules))
            success_count += 1
            applied_names.append(fa_desc)
            logger.info(f"PrivacyLock {who}: '{fa_desc}' locked ({rule_name}).")

        except FloodWait as fw:
            wait_sec = int(getattr(fw, "value", 0) or 0)
            if wait_sec > _MAX_FLOOD_WAIT_SECONDS:
                logger.warning(
                    f"PrivacyLock {who}: FloodWait {wait_sec}s on '{fa_desc}' exceeds threshold "
                    f"({_MAX_FLOOD_WAIT_SECONDS}s) — aborting remaining rules for next start."
                )
                completed_cleanly = False
                break

            logger.warning(f"PrivacyLock {who}: FloodWait {wait_sec}s while setting '{fa_desc}' — waiting.")
            await asyncio.sleep(wait_sec + 1)

            if not getattr(client, "is_connected", False):
                completed_cleanly = False
                break

            try:
                await client.invoke(raw.functions.account.SetPrivacy(key=key_obj, rules=rules))
                success_count += 1
                applied_names.append(fa_desc)
                logger.info(f"PrivacyLock {who}: '{fa_desc}' locked after FloodWait retry.")
            except FloodWait as fw2:
                logger.warning(
                    f"PrivacyLock {who}: second FloodWait ({getattr(fw2, 'value', '?')}s) on '{fa_desc}' — aborting."
                )
                completed_cleanly = False
                break
            except _FATAL_SESSION_ERRORS as fatal_err:
                logger.warning(f"PrivacyLock {who}: fatal session error on retry for '{fa_desc}': {fatal_err}")
                completed_cleanly = False
                break
            except Exception as retry_err:
                if "PREMIUM_ACCOUNT_REQUIRED" in str(retry_err):
                    logger.debug(
                        f"PrivacyLock {who}: '{fa_desc}' skipped (Telegram Premium required)."
                    )
                else:
                    logger.warning(f"PrivacyLock {who}: retry failed for '{fa_desc}': {retry_err}")
                    completed_cleanly = False

        except _FATAL_SESSION_ERRORS as fatal_err:
            logger.warning(f"PrivacyLock {who}: fatal session error while setting '{fa_desc}': {fatal_err} — aborting.")
            completed_cleanly = False
            break

        except RPCError as rpc_err:
            # در تلگرام InputPrivacyKeyVoiceMessages فقط برای اکانت‌های پریمیوم قابل قفل شدن است
            if "PREMIUM_ACCOUNT_REQUIRED" in str(rpc_err):
                logger.debug(
                    f"PrivacyLock {who}: '{fa_desc}' skipped (account is not Telegram Premium)."
                )
            else:
                logger.warning(f"PrivacyLock {who}: RPC error setting '{fa_desc}': {rpc_err}")
                completed_cleanly = False

        except (OSError, ConnectionError) as conn_err:
            logger.warning(f"PrivacyLock {who}: connection error on '{fa_desc}': {conn_err} — aborting.")
            completed_cleanly = False
            break

        except Exception as e:
            logger.warning(f"PrivacyLock {who}: unexpected error setting '{fa_desc}': {e}")
            completed_cleanly = False

        if idx < total_supported - 1:
            await asyncio.sleep(random.uniform(_CALL_GAP_MIN, _CALL_GAP_MAX))

    if success_count:
        # FIX: مخرجِ قبلی len(_PRIVACY_ROWS) بود که کلیدهای پشتیبانی‌نشده
        # (مثل InputPrivacyKeyAbout در pyrofork 2.2.0) را هم می‌شمرد و لاگ را
        # گمراه‌کننده می‌کرد (مثلاً «7/9» در حالی که فقط ۸ کلید پشتیبانی می‌شود).
        logger.info(
            f"PrivacyLock {who}: applied {success_count}/{total_supported} privacy rules "
            f"({', '.join(applied_names)})."
        )

    return success_count, applied_names, completed_cleanly


async def apply_employer_privacy_lock(client: Client, account_id: Optional[int] = None) -> Tuple[int, List[str]]:
    """
    اعمال قفل حریم خصوصی کارفرما روی یک کلاینتِ متصل.

    خروجی: (تعداد موفق، لیست نام موفق‌ها) — برای ثبت پرچم DB استفاده می‌شود.
    خطاها هرگز raise نمی‌شوند (best-effort) تا استارت ورکر را متوقف نکنند.
    """
    success_count, applied_names, _ = await _apply_privacy_rules_internal(client, account_id)
    return success_count, applied_names


async def ensure_account_privacy_locked(
    client: Client,
    account_id: int,
    *,
    session: Optional[AsyncSession] = None,
) -> bool:
    """
    idempotent & concurrency-safe: اگر برای این اکانت قبلاً قفل ثبت شده باشد،
    هیچ تماسی با تلگرام برقرار نمی‌شود (فقط یک SELECT سبک). در غیر این صورت
    قفل اعمال و پرچم privacy_locked_at در تراکنش مستقلی ثبت می‌شود.

    خروجی: True اگر اکانت همین حالا «قفل‌شده» است (از قبل یا تازه).
    """
    if not account_id:
        logger.warning("PrivacyLock: ensure_account_privacy_locked called with invalid account_id.")
        return False

    lock = _get_account_lock(account_id)
    async with lock:
        try:
            if session is not None:
                already = await session.scalar(
                    select(Account.privacy_locked_at).where(Account.id == account_id)
                )
            else:
                async with async_session() as chk:
                    already = await chk.scalar(
                        select(Account.privacy_locked_at).where(Account.id == account_id)
                    )
            if already is not None:
                return True
        except Exception as e:
            logger.warning(f"PrivacyLock user_{account_id}/: flag check failed ({e}); proceeding anyway.")

        success_count, _, completed_cleanly = await _apply_privacy_rules_internal(client, account_id)

        if success_count > 0 and completed_cleanly:
            # FIX: قبلاً func.now() (یعنی CURRENT_TIMESTAMP سرور MySQL) بود؛
            # اگر timezone سرور != UTC باشد مهر زمانی منحرف می‌شد (استاندارد پروژه UTC است —
            # مهاجرت DEP-7 همه‌ی داده‌ها را به UTC شیفت کرده). حالا مقدار UTC صریح از
            # سمت اپلیکیشن نوشته می‌شود.
            stmt = (
                update(Account)
                .where(Account.id == account_id, Account.privacy_locked_at.is_(None))
                .values(privacy_locked_at=datetime.now(timezone.utc))
                .execution_options(synchronize_session=False)
            )
            try:
                if session is not None:
                    res = await session.execute(stmt)
                    await session.flush()
                    rows = res.rowcount or 0
                else:
                    async with async_session() as wr:
                        res = await wr.execute(stmt)
                        await wr.commit()
                        rows = res.rowcount or 0

                if rows > 0:
                    logger.info(
                        f"PrivacyLock user_{account_id}/: account marked as privacy-locked "
                        f"({success_count} rules)."
                    )
                    return True

                async with async_session() as verify_sess:
                    is_now_set = await verify_sess.scalar(
                        select(Account.privacy_locked_at).where(Account.id == account_id)
                    )
                if is_now_set is not None:
                    return True

                logger.warning(
                    f"PrivacyLock user_{account_id}/: rules applied on Telegram, but DB row "
                    f"(id={account_id}) was not found or uncommitted in MySQL!"
                )
                return False
            except Exception as e:
                logger.warning(f"PrivacyLock user_{account_id}/: flag persist failed: {e}")
                return False

        if success_count > 0 and not completed_cleanly:
            logger.warning(
                f"PrivacyLock user_{account_id}/: applied {success_count} rules partially, "
                f"leaving privacy_locked_at unset so remaining rules retry on next start."
            )

        return False