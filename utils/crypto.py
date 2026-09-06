import base64
import logging
import os
import sqlite3
import struct

from cryptography.fernet import Fernet

logger = logging.getLogger(__name__)

SECRET_KEY = os.getenv("FERNET_KEY") 

# 🔴 منطق Fail-Fast: اگر کلید پیدا نشد، سیستم فوراً باید متوقف شود
if not SECRET_KEY:
    error_msg = "CRITICAL ERROR: FERNET_KEY is missing! Sessions cannot be decrypted."
    logger.critical(error_msg)
    raise ValueError(error_msg)

try:
    cipher = Fernet(SECRET_KEY.encode())
except ValueError as e:
    logger.critical(f"CRITICAL ERROR: Invalid FERNET_KEY format! {e}")
    raise

def encrypt_session(session_string: str) -> str | None:
    """رمزنگاری سشن استرینگ قبل از ذخیره در دیتابیس"""
    if not session_string:
        return None
    return cipher.encrypt(session_string.encode()).decode()

def decrypt_session(encrypted_string: str) -> str | None:
    """رمزگشایی سشن استرینگ پس از واکشی از دیتابیس"""
    if not encrypted_string:
        return None
    return cipher.decrypt(encrypted_string.encode()).decode()


def mask_phone(phone: str | None) -> str:
    """
    🛡 فاز ۱۰ (SEC-6): ماسک مشترک شماره تلفن برای لاگ‌ها و نمایش‌های حساس.

    قالب خروجی: +98••••6789 — پیش‌شمارهٔ تقریبی + حداکثر ۴ رقم آخر قابل مشاهده.
    شماره‌های خیلی کوتاه کامل ماسک می‌شوند؛ خروجی هرگز شمارهٔ کامل نیست.
    """
    if not phone:
        return "نامشخص"
    digits = "".join(ch for ch in str(phone) if ch.isdigit())
    if len(digits) < 8:
        return "+••••"
    return f"+{digits[:2]}••••{digits[-4:]}"


# ==========================================
# ⬇️ قابلیت دانلود فایل .session اکانت (ادمین)
# ==========================================

# دیتاسنترهای استاندارد تلگرام (production) — همان جدول DCهای pyrogram.
# server_address و port فایل سشن بر اساس dc_id از این جدول پر می‌شوند.
TELEGRAM_DC_ADDRESSES = {
    1: ("pluto.web.telegram.org", 443),
    2: ("venus.web.telegram.org", 443),
    3: ("aurora.web.telegram.org", 443),
    4: ("vesta.web.telegram.org", 443),
    5: ("flora.web.telegram.org", 443),
}

# فرمت‌های شناخته‌شدهٔ StringSession در نسخه‌ها/فورک‌های رایج pyrogram.
# تطبیق بر اساس «طول دادهٔ decoded» انجام می‌شود (طول‌ها یکتا هستند).
# ساختار هر آیتم: (فرمت struct، ایندکس dc_id، ایندکس auth_key، ایندکس user_id یا None)
_SESSION_STRING_FORMAT_SPECS = (
    (">B256sQ", 0, 1, 2),      # pyrogram v2.0.x: dc_id + auth_key + user_id (uint64)
    (">B256sI", 0, 1, 2),      # مشابه بالا با user_id (uint32)
    (">B256s", 0, 1, None),    # pyrogram قدیمی: فقط dc_id + auth_key (بدون user_id)
    (">BI?256sI?", 0, 3, 4),   # فورک‌های جدید: dc_id + port + test_mode + auth_key + user_id + is_bot
    (">BI?256sQ", 0, 3, 4),
    (">BI?256sQ?", 0, 3, 4),   # پشتیبانی از pyrofork 2.2.0 (طول ۲۷۱ بایت: دارای Q و is_bot انتهایی)
    (">B?256sI", 0, 2, 3),     # dc_id + test_mode + auth_key + user_id
    (">B?256sQ", 0, 2, 3),
)


def _read_session_field(obj, name: str):
    """خواندن فیلد از آبجکت StringSession — هم attribute/property و هم متد را پوشش می‌دهد."""
    val = getattr(obj, name, None)
    if callable(val):
        val = val()
    return val

def _parse_string_session(session_string: str) -> tuple[int, bytes, int | None]:
    """
    استخراج (dc_id, auth_key, user_id) از StringSession.

    ⚠️ امنیت: محتوای session_string / auth_key هرگز در لاگ نوشته نمی‌شود.

    نکته: لایه ۱ (استفاده از pyrogram.session.StringSession) به دلیل عدم وجود
    این کلاس در pyrofork 2.2.0 حذف شد. کد اکنون فقط به پارس دستی (لایه ۲) اتکا می‌کند.

    Raises:
        ValueError: اگر رشته با فرمت‌های شناخته‌شده سازگار نباشد یا base64 نامعتبر باشد.
    """
    # ── پارس دستی ساختار ──
    try:
        # اصلاح باگ ۲: پدینگ base64 بر مبنای مضرب ۴ محاسبه می‌شود
        decoded = base64.urlsafe_b64decode(session_string + "=" * (-len(session_string) % 4))
    except Exception:
        raise ValueError("Session string is not valid base64")

    for fmt, dc_idx, key_idx, uid_idx in _SESSION_STRING_FORMAT_SPECS:
        if struct.calcsize(fmt) != len(decoded):
            continue

        fields = struct.unpack(fmt, decoded)
        dc_id = int(fields[dc_idx])
        auth_key = bytes(fields[key_idx])
        user_id = int(fields[uid_idx]) if uid_idx is not None and fields[uid_idx] else None

        if 1 <= dc_id <= 5:
            return dc_id, auth_key, user_id

    # در صورت عدم تطبیق با هیچ‌یک از فرمت‌ها، طول رشته دیکدشده برای دیباگ نمایش داده می‌شود
    # ⚠️ هشدار: محتوای decoded یا خود auth_key مطلقاً نباید اینجا ضمیمه شود!
    raise ValueError(f"Session string format is not recognized (decoded length: {len(decoded)} bytes)")

def build_session_file(session_string: str, out_path: str) -> str:
    """
    ⬇️ تبدیل StringSession (رمزگشایی‌شده) به فایل .session استاندارد Pyrogram (SQLite).

    - dc_id / auth_key / user_id با `_parse_string_session` استخراج می‌شوند.
    - ساختار جداول دقیقاً مطابق schema کلاس SQLiteStorage پکیج pyrogram
      (pyrogram/storage/sqlite.py → متد create) بازتولید می‌شود تا خروجی با
      هر کلاینت Pyrogram دیگری قابل بازشدن باشد.
    - server_address و port از جدول دیتاسنترهای استاندارد تلگرام بر اساس dc_id پر می‌شوند.

    ⚠️ امنیت: محتوای session_string / auth_key هرگز در لاگ نوشته نمی‌شود.

    Raises:
        ValueError: اگر session_string خالی یا با فرمت‌های شناخته‌شده سازگار نباشد.
        sqlite3.Error / OSError: در صورت خطای ساخت فایل.
    """
    if not session_string:
        raise ValueError("session_string is empty")

    dc_id, auth_key, user_id = _parse_string_session(session_string)

    server_address, port = TELEGRAM_DC_ADDRESSES.get(dc_id, (None, None))
    if not server_address:
        raise ValueError(f"Unknown dc_id: {dc_id}")

    # اطمینان از وجود پوشهٔ مقصد
    out_dir = os.path.dirname(os.path.abspath(out_path))
    os.makedirs(out_dir, exist_ok=True)

    # اگر فایل قبلی با همین نام مانده باشد، sqlite روی فایل موجود جدول‌های ناقص
    # قبلی را نگه می‌دارد — پس همیشه از صفر شروع می‌کنیم
    if os.path.exists(out_path):
        os.remove(out_path)

    conn = sqlite3.connect(out_path)
    try:
        cur = conn.cursor()
        # ⚠️ این اسکریپت باید دقیقاً با SQLiteStorage.create (pyrogram/storage/sqlite.py)
        # هم‌خوان بماند — نسخهٔ فعلی schema: version = 2
        cur.executescript("""
            CREATE TABLE sessions(
                dc_id INTEGER PRIMARY KEY,
                server_address TEXT,
                port INTEGER,
                auth_key BLOB,
                test_mode INTEGER,
                user_id INTEGER,
                is_bot INTEGER
            );

            CREATE TABLE peers(
                id INTEGER PRIMARY KEY,
                hash INTEGER NOT NULL,
                name TEXT,
                username TEXT,
                phone TEXT
            );

            CREATE TABLE version(
                version INTEGER PRIMARY KEY
            );

            INSERT INTO version(version) VALUES(2);
        """)
        # user_id برای Pyrogram حیاتی است: بدون آن کلاینت وارد فلوی authorize
        # (درخواست کد) می‌شود — مقدار 0 یعنی «نامشخص» برای فرمت‌های خیلی قدیمی
        cur.execute(
            "INSERT INTO sessions(dc_id, server_address, port, auth_key, test_mode, user_id, is_bot) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (dc_id, server_address, port, auth_key, 0, user_id or 0, 0),
        )
        conn.commit()
    finally:
        conn.close()

    return out_path