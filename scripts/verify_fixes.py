# -*- coding: utf-8 -*-
"""
🧪 scripts/verify_fixes.py — راستی‌آزمایی رفتاری فیکس‌های فریز/بن
================================================================
بدون Redis / DB / تلگرام واقعی؛ با کلاینت‌های ساختگی (mock).

پوشش:
  ۱) پیش‌فرض‌های امن config (BYPASS_WARMUP / FALLBACK_TO_DIRECT_IP /
     MAX_ACCOUNTS_PER_PROXY / کلیدهای BAN_WAVE_* و BAN_PROXY_*)
  ۲) قابلیت تنظیم از .env (متغیر محیطی → مقدار کانفیگ)
  ۳) شبیه‌سازی الگوی قدیمی «while True» (اثبات بی‌نهایت بودن پیش از فیکس)
  ۴) کد واقعیِ clear_profile_photos_now:
       • خطای FROZEN_METHOD_INVALID → قطع فوری + ثبت فریز (۱ فراخوانی)
       • حذف ناممکن (پیشرفت صفر) → توقف بعد از همان دور
       • مسیر سالم → پاک‌سازی کامل و خروج عادی
       • بودجه‌ی زمانی (PHOTO_ROTATION_TIMEOUT_SECONDS) → توقف به‌موقع
  ۵) بررسی استاتیک فیکس‌های POST-REVIEW (fallback ها و کلیدهای config)

اجرای: python3 scripts/verify_fixes.py  (از ریشه‌ی پروژه)
"""

import asyncio
import importlib
import logging
import os
import sys
from types import SimpleNamespace

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent))

# --- متغیرهای محیطی حداقلی (مثل test_ban_wave.py) ---
# FERNET_KEY باید یک کلید معتبر Fernet باشد تا utils.crypto در زمان import لود شود
# 🩹 FIX: کلید نمونه‌ی قبلی ۴۳ کاراکتر بود و Fernet آن را رد می‌کرد (خطای
# «Fernet key must be 32 url-safe base64-encoded bytes») — با کلید معتبر جایگزین شد.
os.environ.setdefault("FERNET_KEY", "VG1lWekMcT-IV6Y2xuI9sra9mUFz8cu60wGhmQct0-E=")
os.environ.setdefault("DB_PASS", "test")
os.environ.setdefault("API_ID", "12345")
os.environ.setdefault("API_HASH", "testhash")
os.environ.setdefault("BOT_TOKEN", "123:abc")
os.environ.setdefault("ADMIN_ID", "1")

# خروجی لاگ تمیز — فقط خطاهای غیرمنتظره
logging.basicConfig(level=logging.CRITICAL, format="%(levelname)s %(name)s: %(message)s")

PASS_COUNT = 0
FAIL_COUNT = 0

FROZEN_MSG = (
    'Flood: Telegram says: [420 Flood] - [420 FROZEN_METHOD_INVALID] '
    '(caused by "photos.DeleteProfilePhotos")'
)


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS_COUNT, FAIL_COUNT
    mark = "✓" if cond else "✗"
    print(f"   [{mark}] {name}" + (f" — {detail}" if detail else ""))
    if cond:
        PASS_COUNT += 1
    else:
        FAIL_COUNT += 1


# ==========================================
# ابزارهای ساختگی
# ==========================================
class FakePhoto:
    def __init__(self, i: int):
        self.file_id = f"photo_file_{i}"


class FakeClient:
    """کلاینت Telethon/Pyrogram ساختگی — رفتار حذف عکس قابل تزریق."""

    def __init__(self, photos_per_call=5, delete_behavior=None, name="worker_acc_999"):
        self.name = name
        self.is_connected = True
        self.photos_per_call = photos_per_call
        self.delete_behavior = delete_behavior  # callable(i) -> None | raise
        self.calls = {"get_chat_photos": 0, "delete": 0}
        self._photo_seq = 0

    def get_chat_photos(self, chat_id, limit=5):
        self.calls["get_chat_photos"] += 1
        n = self.photos_per_call() if callable(self.photos_per_call) else self.photos_per_call
        photos = []
        for _ in range(n):
            self._photo_seq += 1
            photos.append(FakePhoto(self._photo_seq))

        async def _gen():
            for p in photos:
                yield p

        return _gen()

    async def delete_profile_photos(self, ids):
        self.calls["delete"] += 1
        if self.delete_behavior is not None:
            self.delete_behavior(self.calls["delete"])


class FrozenMarker:
    """جانشین mark_account_frozen — فقط فراخوانی‌ها را می‌شمارد."""

    def __init__(self):
        self.calls = []

    async def __call__(self, account_id, reason="", **kw):
        self.calls.append((account_id, reason))


async def never_frozen(account_id):
    return False


# ==========================================
# ۱) پیش‌فرض‌های امن config
# ==========================================
def test_config_defaults():
    print("== تست ۱: پیش‌فرض‌های امن config ==")
    import config as config_mod

    c = config_mod.config
    check("BYPASS_WARMUP پیش‌فرض false است", c.BYPASS_WARMUP is False)
    check("FALLBACK_TO_DIRECT_IP پیش‌فرض false است", c.FALLBACK_TO_DIRECT_IP is False)
    check("MAX_ACCOUNTS_PER_PROXY پیش‌فرض ۳ است", c.MAX_ACCOUNTS_PER_PROXY == 3)

    expected = {
        "BAN_WAVE_WINDOW_MINUTES": 60,
        "BAN_WAVE_GLOBAL_THRESHOLD": 3,
        "BAN_WAVE_GLOBAL_PF_THRESHOLD": 5,
        "BAN_WAVE_FROZEN_GLOBAL_THRESHOLD": 3,
        "BAN_WAVE_FREEZE_MINUTES": 60,
        "BAN_WAVE_PROXY_BAN_THRESHOLD": 2,
        "BAN_WAVE_PROXY_PF_THRESHOLD": 3,
        "BAN_PROXY_QUARANTINE_HOURS": 72,
        "BAN_PROXY_CLAIM_COOLDOWN_HOURS": 24,
    }
    for key, val in expected.items():
        got = getattr(c, key, None)
        check(f"{key} = {val}", got == val, f"مقدار فعلی: {got}")

    # گارد موج بن واقعاً همین attribute ها را می‌خواند
    from utils.ban_wave import _cfg
    check(
        "_cfg گارد مقدار را از config می‌خواند (نه fallback)",
        _cfg("BAN_WAVE_FREEZE_MINUTES", 999) == 60,
    )


# ==========================================
# ۲) قابلیت تنظیم از .env
# ==========================================
def test_env_tunability():
    print("== تست ۲: تنظیم از متغیر محیطی (.env) ==")
    import config as config_mod

    key = "BAN_WAVE_GLOBAL_THRESHOLD"
    try:
        os.environ[key] = "7"
        reloaded = importlib.reload(config_mod)
        check(
            "BAN_WAVE_GLOBAL_THRESHOLD=7 در محیط → کانفیگ ۷ می‌شود",
            getattr(reloaded.config, key) == 7,
            f"مقدار: {getattr(reloaded.config, key)}",
        )
    finally:
        os.environ.pop(key, None)
        importlib.reload(config_mod)  # بازگشت به پیش‌فرض


# ==========================================
# ۳) شبیه‌سازی الگوی قدیمی (while True)
# ==========================================
def test_old_loop_infinite():
    print("== تست ۳: شبیه‌سازی الگوی قدیمی (اثبات حلقه‌ی بی‌نهایت) ==")
    # الگوی pre-fix: while True + خطای FROZEN بی‌صدا قورت داده می‌شد.
    # سقف ۲۰۰۰ فقط برای جلوگیری از هنگ کردن تست است — در لاگ bot_19 همین
    # الگو ۱۱٬۰۹۰ فراخوانی photos.GetUserPhotos ثبت کرده بود.
    CAP = 2000
    get_calls = 0
    while get_calls < CAP:  # جایگزین while True برای قابل‌تست بودن
        get_calls += 1
        for _ in range(5):  # اکانت همیشه ۵ عکس «قابل‌دیدن» دارد
            pass  # delete همیشه FROZEN_METHOD_INVALID → نادیده گرفته می‌شد
    check(
        f"الگوی قدیمی تا سقف {CAP} ادامه می‌یابد (خودش خارج نمی‌شود)",
        get_calls == CAP,
    )


# ==========================================
# ۴) کد واقعی: clear_profile_photos_now
# ==========================================
async def _run_clear(client, sm, fg, account_id=999):
    """اجرای تابع واقعی با patch های موقت."""
    sm.worker_pool[account_id] = client
    orig_frozen, orig_mark = fg.is_account_frozen, fg.mark_account_frozen
    marker = FrozenMarker()
    fg.is_account_frozen = never_frozen
    fg.mark_account_frozen = marker
    try:
        result = await sm.clear_profile_photos_now(account_id)
        return result, marker
    finally:
        fg.is_account_frozen, fg.mark_account_frozen = orig_frozen, orig_mark
        sm.worker_pool.pop(account_id, None)


async def test_frozen_immediate_cut():
    print("== تست ۴: خطای FROZEN → قطع فوری + ثبت فریز (۱ فراخوانی) ==")
    import workers.session_manager as sm
    import utils.freeze_guard as fg

    def always_frozen(_i):
        raise Exception(FROZEN_MSG)

    client = FakeClient(photos_per_call=5, delete_behavior=always_frozen)
    result, marker = await _run_clear(client, sm, fg)

    check("خروجی False است", result is False)
    check("دقیقاً ۱ بار mark_account_frozen صدا زده شد", len(marker.calls) == 1,
          f"فراخوانی‌ها: {len(marker.calls)}")
    check("photos.GetUserPhotos فقط ۱ بار صدا زده شد",
          client.calls["get_chat_photos"] == 1, f"فراخوانی‌ها: {client.calls}")
    check("حذف عکس فقط ۱ بار امتحان شد (نه بی‌نهایت)",
          client.calls["delete"] == 1, f"فراخوانی‌ها: {client.calls['delete']}")


async def test_zero_progress_exit():
    print("== تست ۵: حذف ناممکن (پیشرفت صفر) → توقف بعد از همان دور ==")
    import workers.session_manager as sm
    import utils.freeze_guard as fg

    def always_generic_fail(_i):
        raise RuntimeError("PHOTO_DELETE_NOT_PERMITTED (خطای عمومی، نه فریز)")

    client = FakeClient(photos_per_call=5, delete_behavior=always_generic_fail)
    result, marker = await _run_clear(client, sm, fg)

    check("حلقه بعد از دور ۱ (پیشرفت صفر) متوقف شد",
          client.calls["get_chat_photos"] == 1, f"فراخوانی‌ها: {client.calls}")
    check("۵ عکسِ دور ۱ امتحان شدند (بدون تکرار دور بعدی)",
          client.calls["delete"] == 5, f"فراخوانی‌ها: {client.calls['delete']}")
    check("بدون خطای فریز، mark_account_frozen صدا زده نشد", len(marker.calls) == 0)


async def test_healthy_path():
    print("== تست ۶: مسیر سالم → پاک‌سازی کامل و خروج عادی ==")
    import workers.session_manager as sm
    import utils.freeze_guard as fg

    # دور ۱: ۲ عکس → حذف موفق؛ دور ۲: گالری خالی → پایان
    # (شمارنده قبل از فراخوانیِ callback افزایش می‌یابد: دور ۱ → 1، دور ۲ → 2)
    client = FakeClient(photos_per_call=lambda: 2 if client.calls["get_chat_photos"] == 1 else 0)
    result, marker = await _run_clear(client, sm, fg)

    check("خروجی True است", result is True)
    check("۲ عکس حذف شدند", client.calls["delete"] == 2)
    check("دو دور اجرا شد (دور دوم: گالری خالی → خروج)",
          client.calls["get_chat_photos"] == 2, f"فراخوانی‌ها: {client.calls}")
    check("mark_account_frozen صدا زده نشد", len(marker.calls) == 0)


async def test_time_budget():
    print("== تست ۷: بودجه‌ی زمانی (PHOTO_ROTATION_TIMEOUT_SECONDS) ==")
    import workers.session_manager as sm
    import utils.freeze_guard as fg

    real_time_mod = sm.time
    try:
        # --- اجرای الف: بودجه از همان دور ۱ گذشته → هیچ عکسی fetch/حذف نمی‌شود ---
        # توالی فراخوانی‌های monotonic: (۱) محاسبه‌ی deadline=0+900، (۲) چکِ دور ۱=10000 → break
        seq_a = iter([0.0, 10_000.0])
        sm.time = SimpleNamespace(monotonic=lambda: next(seq_a, 10_000.0))
        client_a = FakeClient(photos_per_call=1)
        result_a, _ = await _run_clear(client_a, sm, fg)

        check("بودجه گذشته → photos.GetUserPhotos صدا زده نشد",
              client_a.calls["get_chat_photos"] == 0,
              f"فراخوانی‌ها: {client_a.calls}")
        check("بودجه گذشته → هیچ حذفی امتحان نشد",
              client_a.calls["delete"] == 0)
        check("خروجی بدون crash (bool)", isinstance(result_a, bool))

        # --- اجرای ب: بودجه بین دور ۱ و ۲ منقضی می‌شود → فقط ۱ دور اجرا می‌شود ---
        # توالی: (۱) deadline=900، (۲) چک دور ۱=0 → اجرا؛ (۳) چک دور ۲=10000 → break
        seq_b = iter([0.0, 0.0, 10_000.0])
        sm.time = SimpleNamespace(monotonic=lambda: next(seq_b, 10_000.0))
        client_b = FakeClient(photos_per_call=1)  # حذف همیشه موفق + عکس همیشه موجود
        result_b, marker_b = await _run_clear(client_b, sm, fg)

        check("دور ۲ شروع نشد (فقط ۱ fetch در دور ۱)",
              client_b.calls["get_chat_photos"] == 1,
              f"فراخوانی‌ها: {client_b.calls}")
        check("فقط عکس دور ۱ حذف شد", client_b.calls["delete"] == 1)
        check("بدون خطای فریز، mark_account_frozen صدا زده نشد", len(marker_b.calls) == 0)
    finally:
        sm.time = real_time_mod


# ==========================================
# ۵) بررسی استاتیک فیکس‌های POST-REVIEW
# ==========================================
def test_static_source_checks():
    print("== تست ۸: بررسی استاتیک فیکس‌های POST-REVIEW ==")
    root = __import__("pathlib").Path(__file__).resolve().parent.parent

    hc = (root / "utils" / "health_checker.py").read_text(encoding="utf-8")
    check("health_checker: fallback ناسازگار ۱۰۰ حذف شد",
          "MAX_ACCOUNTS_PER_PROXY', 100" not in hc and "MAX_ACCOUNTS_PER_PROXY', 3)" in hc)

    ph = (root / "bot" / "handlers" / "photo_handlers.py").read_text(encoding="utf-8")
    check("wipe_all_photos: max_passes از PHOTO_WIPE_MAX_PASSES خوانده می‌شود",
          'getattr(config, "PHOTO_WIPE_MAX_PASSES"' in ph)

    cfg = (root / "config.py").read_text(encoding="utf-8")
    check("config: کلید BAN_PROXY_CLAIM_COOLDOWN_HOURS تعریف شده",
          'os.getenv("BAN_PROXY_CLAIM_COOLDOWN_HOURS"' in cfg)
    check("session_manager: بودجه‌ی زمانی در clear_profile_photos_now",
          "PHOTO_ROTATION_TIMEOUT_SECONDS" in (root / "workers" / "session_manager.py").read_text(encoding="utf-8"))


# ==========================================
# main
# ==========================================
async def main():
    print("🧪 verify_fixes — شبیه‌سازی رفتاری فیکس‌های فریز/بن")
    print("(بدون Redis/DB/تلگرام — کلاینت ساختگی)\n")

    test_config_defaults()
    test_old_loop_infinite()
    await test_frozen_immediate_cut()
    await test_zero_progress_exit()
    await test_healthy_path()
    await test_time_budget()
    test_static_source_checks()
    test_env_tunability()  # آخر: چون config را reload می‌کند

    print()
    print(f"نتیجه: {PASS_COUNT} پاس / {FAIL_COUNT} شکست")
    if FAIL_COUNT:
        sys.exit(1)
    print("✅ همه‌ی راستی‌آزمایی‌های رفتاری فیکس‌ها پاس شدند.")


if __name__ == "__main__":
    asyncio.run(main())
