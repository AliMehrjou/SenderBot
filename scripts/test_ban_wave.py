# -*- coding: utf-8 -*-
"""
تست عملکردی گارد ضد بنِ زنجیره‌ای (utils/ban_wave.py) بدون Redis/DB واقعی.
اجرای: python3 scripts/test_ban_wave.py (از ریشه‌ی پروژه)
"""
import asyncio
import os
import sys
import time
from types import SimpleNamespace

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent))

# --- متغیرهای محیطی حداقلی ---
os.environ.setdefault("FERNET_KEY", "x" * 44)
os.environ.setdefault("DB_PASS", "test")
os.environ.setdefault("API_ID", "12345")
os.environ.setdefault("API_HASH", "testhash")
os.environ.setdefault("BOT_TOKEN", "123:abc")
os.environ.setdefault("ADMIN_ID", "1")


class FakeRedis:
    """Redis درون‌حافظه‌ای مینیمال با پشتیبانی TTL واقعی (برای کلیدهای تست)."""
    def __init__(self):
        self.store = {}      # key -> (value, expire_at|None)

    def _alive(self, key):
        v = self.store.get(key)
        if v is None:
            return None
        val, exp = v
        if exp is not None and exp < time.time():
            del self.store[key]
            return None
        return val

    async def get(self, k):
        return self._alive(k)

    async def set(self, k, v, ex=None, nx=False, px=None):
        if nx and self._alive(k) is not None:
            return None
        exp = time.time() + (ex if ex else (px / 1000 if px else 0) or 0)
        self.store[k] = (v, exp if (ex or px) else None)
        return True

    async def incr(self, k):
        cur = self._alive(k)
        cur = int(cur) if cur else 0
        # نگهداری TTL موجود (اگر کلید از قبل expire دار بود)
        old = self.store.get(k)
        exp = old[1] if old else None
        self.store[k] = (str(cur + 1), exp)
        return cur + 1

    async def expire(self, k, seconds):
        v = self._alive(k)
        if v is None:
            return False
        self.store[k] = (v, time.time() + seconds)
        return True

    async def ttl(self, k):
        v = self.store.get(k)
        if v is None:
            return -2
        val, exp = v
        if exp is None:
            return -1
        remaining = int(exp - time.time())
        if remaining <= 0:
            del self.store[k]
            return -2
        return remaining

    async def exists(self, k):
        return 1 if self._alive(k) is not None else 0


async def main():
    fake = FakeRedis()

    # --- Patch کردن کلاینت Redis سندر ---
    import workers.sender as sender_mod
    sender_mod._redis_client = SimpleNamespace()  # جلوگیری از ساخت کلاینت واقعی
    sender_mod._get_redis = lambda: fake

    from utils import ban_wave

    # --- بدون DB واقعی: همه‌ی مسیرها باید fail-open کار کنند ---

    print("== تست ۱: شمارنده‌ی بن سراسری + فعال‌شدن فریز موج ==")
    for i in range(3):
        await ban_wave.register_ban_event(
            account_id=100 + i, proxy_string=f"socks5://u:p@10.0.0.{i}:1080", event_type="ban"
        )
    freeze_active = await ban_wave.is_banwave_freeze_active()
    snap = await ban_wave.get_banwave_snapshot()
    print(f"   freeze_active={freeze_active}, snapshot={snap}")
    assert freeze_active is True, "فریز باید بعد از ۳ بن فعال شود (آستانه پیش‌فرض=۳)"
    assert snap["global_bans"] == 3
    remaining = await ban_wave.banwave_freeze_remaining_seconds()
    assert 0 < remaining <= 60 * 60 + 5, f"باقی‌مانده فریز نامعتبر: {remaining}"
    print(f"   فریز فعال، {remaining}s باقی‌مانده ✓")

    print("== تست ۲: قرنطینه‌ی پروکسی سوخته (۲ بن روی یک پروکسی) ==")
    # فریز قبلی را پاک می‌کنیم تا فقط مسیر قرنطینه را ببینیم
    fake.store.pop("banwave:freeze_until", None)
    fake.store.pop("banwave:global:ban", None)

    proxy = "socks5://user:pass@185.10.10.10:9050"
    await ban_wave.register_ban_event(account_id=201, proxy_string=proxy, event_type="ban")
    # هنوز زیر آستانه (۱) → قرنطینه نباید فعال شود؛ فقط claim cooldown ست می‌شود
    cd_val = await fake.get(f"proxy_cd:{proxy}")
    print(f"   بعد از بن اول: claim_cooldown={'ست شده ✓' if cd_val else 'غیرفعال ✗'}")
    assert cd_val is not None, "claim cooldown بعد از بن اول باید ست شود"

    await ban_wave.register_ban_event(account_id=202, proxy_string=proxy, event_type="ban")
    # حالا ۲ بن روی همین پروکسی → قرنطینه (proxy_cd بلند + هشدار)
    ban_count = await fake.get(f"proxy_bans:{proxy}")
    print(f"   شمارنده‌ی بن روی پروکسی: {ban_count}")
    assert int(ban_count) == 2
    print("   مسیر قرنطینه اجرا شد (هشدار ادمین fire-and-forget؛ DB در محیط تست نیست) ✓")

    print("== تست ۳: فورنزیک — last_ban_proxy ==")
    last = await ban_wave.get_last_ban_proxy(202)
    print(f"   last_ban_proxy(202) = {last}")
    assert last == proxy, "پروکسی لحظه‌ی بن باید ثبت شود"

    print("== تست ۴: PeerFlood — شمارنده جدا و آستانه‌ی مستقل ==")
    fake.store.pop("banwave:global:peer_flood", None)
    fake.store.pop("banwave:freeze_until", None)
    for i in range(2):
        await ban_wave.register_ban_event(
            account_id=300 + i, proxy_string=f"1.2.3.{i}:1080", event_type="peer_flood"
        )
    pf = await fake.get("banwave:global:peer_flood")
    freeze_now = await ban_wave.is_banwave_freeze_active()
    print(f"   شمارنده PF: {pf} | فریز (باید False باشد چون آستانه PF=۵): {freeze_now}")
    assert int(pf) == 2
    assert freeze_now is False, "۲ PeerFlood نباید فریز فعال کند (آستانه ۵)"

    print("== تست ۵: نمایش امن پروکسی (_proxy_display بدون افشای user:pass) ==")
    d = ban_wave._proxy_display("socks5://ali:secret@185.10.10.10:9050")
    print(f"   display = {d}")
    assert "secret" not in d and d == "185.10.10.10:9050"
    assert ban_wave._proxy_display(None) == "بدون پروکسی (دایرکت!)"

    print()
    print("✅ همه‌ی تست‌های گارد ضد بنِ زنجیره‌ای پاس شدند.")


asyncio.run(main())
