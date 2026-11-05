# 🧾 POST_REVIEW_FIXES — فیکس‌های اعمال‌شده پس از ممیزی مستقل

> این سند مکمل `BAN_FREEZE_FIXES_REPORT.md` و `ANTICHAIN_BAN_FIXES.md` است.
> پس از یک بازبینی کامل خط‌به‌خط، ۵ مغایرت کلیدی + ۳ انحراف جزئی نسبت به
> ادعاهای آن گزارش‌ها شناسایی شد که **همه در این نسخه رفع شده‌اند**.

---

## ۱) خلاصه‌ی تغییرات

| # | فایل | تغییر | اولویت |
|---|---|---|---|
| 1 | `config.py` | **۸ کلید `BAN_WAVE_*` / `BAN_PROXY_*` تعریف شدند** (+ کلید منسوخ `BAN_FAILOVER_COOLDOWN_MINUTES` برای سازگاری .env). قبلاً این کلیدها فقط به‌صورت fallback داخل `_cfg()` در `ban_wave.py` وجود داشتند؛ یعنی مقداردهی در `.env` **هیچ اثری نداشت**. | 🔴 بالا |
| 2 | `config.py` | پیش‌فرض `BYPASS_WARMUP`: **true → false** — بدون `.env`، دوره‌ی گرم‌شدن ۲۴ ساعته‌ی اکانت‌های تازه دیگر بی‌صدا دور نمی‌شود. (اکانت‌های قدیمی بلاک نمی‌شوند — رفع legacy-warmup در `task_queue.py` از قبل موجود است.) | 🔴 بالا |
| 3 | `config.py` | پیش‌فرض `FALLBACK_TO_DIRECT_IP`: **true → false** — طبق گزارش ANTICHAIN؛ نبودِ پروکسی دیگر به «نشستن روی IP خام سرور» منجر نمی‌شود مگر با فعال‌سازی آگاهانه در `.env`. | 🟠 متوسط-بالا |
| 4 | `utils/health_checker.py` (خط ~530) | fallback ناسازگار `MAX_ACCOUNTS_PER_PROXY=100` → **۳** (هم‌راستا با پیش‌فرض امن؛ قبلاً در نبودِ attribute، چرخش به پروکسی پرشده مجاز می‌شد). | 🟡 کم |
| 5 | `workers/session_manager.py` (`direct_ip_fallback_enabled`) | fallback ناسازگار `FALLBACK_TO_DIRECT_IP=True` → **False** | 🟡 کم |
| 6 | `bot/handlers/order_handlers.py` (هولد پروکسی) | همان fallback ناسازگار `True` → **False** | 🟡 کم |
| 7 | `bot/handlers/photo_handlers.py` (`/wipe_all_photos`) | `max_passes` هاردکد ۴ → از **`PHOTO_WIPE_MAX_PASSES`** config (مثل بقیه‌ی حلقه‌های پاک‌سازی) | 🟠 متوسط |
| 8 | `workers/session_manager.py` (`clear_profile_photos_now`) | **بودجه‌ی زمانی ۱۵ دقیقه‌ای** (`PHOTO_ROTATION_TIMEOUT_SECONDS`) اضافه شد — همان الگوی `rotate_profile_photos`؛ قفل per-account هرگز برای همیشه اشغال نمی‌شود. | 🟠 متوسط |
| 9 | `scripts/verify_fixes.py` **(جدید)** | اسکریپت راستی‌آزمایی رفتاری که گزارش BAN_FREEZE به آن استناد می‌کرد ولی در بسته نبود. | 🟠 متوسط |
| 10 | `.env.example` **(جدید)** | مرجع کامل کلیدها با مقادیر امن پیشنهادی (چون `.env` واقعی gitignore است و در بسته نیست). | 🟠 متوسط |

---

## ۲) جزئیات فیکس ۱ — کلیدهای گارد موج بن

`utils/ban_wave.py` تنظیمات را با `_cfg(name, default)` می‌خواند که
`getattr(config, name, default)` است. تا قبل از این فیکس، نام هیچ‌کدام از
این کلیدها در `Config` تعریف نشده بود → همیشه مقدار fallback داخلی برمی‌گشت
و `.env` بی‌اثر بود. اکنون این کلیدها در `config.py` تعریف شده‌اند و از
محیط خوانده می‌شوند:

| کلید | پیش‌فرض | معنا |
|---|---|---|
| `BAN_WAVE_WINDOW_MINUTES` | 60 | پنجره‌ی شمارش موج بن |
| `BAN_WAVE_GLOBAL_THRESHOLD` | 3 | بن واقعی در پنجره → فریز کل دیسپچر |
| `BAN_WAVE_GLOBAL_PF_THRESHOLD` | 5 | PeerFlood در پنجره → فریز کل دیسپچر |
| `BAN_WAVE_FROZEN_GLOBAL_THRESHOLD` | 3 | موج فریز → فریز کل دیسپچر (از قبل موجود بود) |
| `BAN_WAVE_FREEZE_MINUTES` | 60 | مدت فریز سراسری |
| `BAN_WAVE_PROXY_BAN_THRESHOLD` | 2 | بن روی یک پروکسی در ۲۴h → قرنطینه |
| `BAN_WAVE_PROXY_PF_THRESHOLD` | 3 | PeerFlood روی یک پروکسی در ۲۴h → قرنطینه |
| `BAN_PROXY_QUARANTINE_HOURS` | 72 | مدت قرنطینه‌ی پروکسی سوخته |
| `BAN_PROXY_CLAIM_COOLDOWN_HOURS` | 24 | فاصله‌گذاری claim بعد از هر تک‌بن/فریز |
| `BAN_FAILOVER_COOLDOWN_MINUTES` | 30 | ⚠️ DEPRECATED — خوانده نمی‌شود |

---

## ۳) راستی‌آزمایی (خروجی واقعی اجرا در همین بسته)

### `python3 scripts/test_ban_wave.py` → ✅ ۵/۵ پاس
شمارنده‌ی بن سراسری + فعال‌شدن فریز موج، قرنطینه‌ی پروکسی سوخته،
فورنزیک `last_ban_proxy`، شمارنده‌ی مستقل PeerFlood، نمایش امن پروکسی.

### `python3 scripts/verify_fixes.py` → ✅ ۳۶/۳۶ پاس
- پیش‌فرض‌های امن کانفیگ (۱۳ چک)
- **اثبات رفتاری**: الگوی قدیمی تا سقف ۲۰۰۰ تکرار ادامه می‌یابد؛ کد جدید
  روی خطای `FROZEN_METHOD_INVALID` با **۱ فراخوانی** قطع می‌شود +
  `mark_account_frozen` دقیقاً ۱ بار
- خروج روی «پیشرفت صفر»، مسیر سالم، بودجه‌ی زمانی (۲ سناریو)
- بررسی استاتیک فیکس‌های همین سند + اثبات اثرگذاری `.env`
  (تغییر متغیر محیطی → تغییر مقدار کانفیگ)

### کامپایل: ✅ همه‌ی فایل‌های Python بدون خطا

---

## ۴) موارد عمداً تغییرنیافته (تصمیم طراحی، نه نقص)

1. **فریز خالص → قرنطینه‌ی پروکسی**: در `register_ban_event` فقط
   `ban`/`peer_flood` مستقیماً قرنطینه را ارزیابی می‌کنند؛ فریز از طریق
   شمارنده‌ی per-proxy (که فریز را هم می‌شمارد) و آستانه‌های موج عمل می‌کند —
   قرنطینه‌ی خودکار روی فریزِ خالص می‌تواند پروکسی سالم را بی‌دلیل بسوزاند.
2. **`BAN_FAILOVER_COOLDOWN_MINUTES`**: مطابق v2 گزارش ANTICHAIN منسوخ است
   (بن واقعی = لغوی فوری سفارش)؛ فقط برای سازگاری `.env` نگه داشته شده.

---

## ۵) چک‌لیست استقرار (قبل از اجرای سرور)

- [ ] `.env` واقعی سرور را با `.env.example` مقایسه کنید؛ به‌خصوص:
      `MAX_ACCOUNTS_PER_PROXY=3` (نه ۲۰/۱۰۰!)، `BYPASS_WARMUP=false`،
      `FALLBACK_TO_DIRECT_IP=false`
- [ ] `FERNET_KEY` موجود را نگه دارید (فقط در `.env.example` خالی است)
- [ ] اولین اجرا: ستون `photo_package_synced_id` خودکار به DB اضافه می‌شود
- [ ] ۴۸ ساعت اول، لاگ‌ها را برای `freeze_guard:` و `photo wipe pass` پایش کنید
