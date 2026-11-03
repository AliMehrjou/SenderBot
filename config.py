import os
from dataclasses import dataclass
from urllib.parse import quote_plus


def _env_flag(name: str, default: str = "false") -> bool:
    """
    🔒 پارس امن متغیر محیطی بولی — از 1/true/yes/on پشتیبانی می‌کند.
    هر مقدار مبهم دیگری False تلقی می‌شود تا در موارد تردید، مسیر امن انتخاب شود.
    """
    return os.getenv(name, default).strip().lower() in ("1", "true", "yes", "on", "y")


@dataclass
class Config:
    """
    Centralized configuration management for the Telegram Bulk Sender System.
    Values are loaded from environment variables with safe defaults.
    """
    # MySQL Database Settings
    DB_USER: str = os.getenv("DB_USER", "root")
    DB_PASS: str = os.getenv("DB_PASS", "password")
    DB_HOST: str = os.getenv("DB_HOST", "127.0.0.1")
    DB_PORT: int = int(os.getenv("DB_PORT", "3306"))
    DB_NAME: str = os.getenv("DB_NAME", "telegram_bulk_db")


    VIDEO_LINK: str = os.getenv("VIDEO_LINK", "")

    # 📢 (خواسته کارفرما) پیام‌های تبلیغاتی روزانه — هر روز ساعت ۹ صبح به وقت
    # ایران، یکی از پیام‌های DAILY_ADS (utils/daily_ads.py) به ترتیب برای
    # کاربران ربات (ادمین/ساب‌ادمین‌ها) ارسال می‌شود؛ بعد از آخری به اول برمی‌گردد.
    DAILY_ADS_ENABLED: bool = _env_flag("DAILY_ADS_ENABLED", "true")
    DAILY_ADS_HOUR: int = int(os.getenv("DAILY_ADS_HOUR", "9"))
    # Redis Settings
    REDIS_HOST: str = os.getenv("REDIS_HOST", "127.0.0.1")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))
    REDIS_DB: int = int(os.getenv("REDIS_DB", "0"))
    REDIS_PASS: str = os.getenv("REDIS_PASS", "")
    REDIS_USER: str = os.getenv("REDIS_USER", "")

    # Telegram API Settings
    API_ID: int = int(os.getenv("API_ID", "0"))
    API_HASH: str = os.getenv("API_HASH", "")
    BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")
    ADMIN_ID: int = int(os.getenv("ADMIN_ID", "0"))

    # تنظیم کانال‌های پیش‌فرض برای سیستم جوین اجباری
    FORCE_JOIN_CHANNELS: str = os.getenv("FORCE_JOIN_CHANNELS", "@linkdoonifun,@robotsfunlink")

    # 🛡 (خواسته کارفرما) کانال‌های حفاظت‌شده‌ی ورکرها — هر اکانت ورکر باید عضو این
    # دو کانال بماند و هیچ عملیات پاکسازی/leave اجازه خارج‌کردنش را ندارد.
    # (عمداً جدای از FORCE_JOIN_CHANNELS است؛ آن کلید برای جوین اجباری «کاربران ربات» است)
    WORKER_PROTECTED_CHANNELS: str = os.getenv(
        "WORKER_PROTECTED_CHANNELS", "@linkdoonifun,@robotsfunlink"
    )

    # 🧮 (خواسته کارفرما) سقف کل اکانت‌های قابل افزودن به ربات — بالاتر از این
    # تعداد، هیچ اکانت جدیدی ثبت نمی‌شود (لاگین تلفنی، سشن تکی و زیپ گروهی).
    MAX_TOTAL_ACCOUNTS: int = int(os.getenv("MAX_TOTAL_ACCOUNTS", "50"))

    # لاگین و اتصال مستقیم
    LOGIN_PROXY_URL: str = os.getenv("LOGIN_PROXY_URL", "")
    MAX_DIRECT_ACCOUNTS: int = int(os.getenv("MAX_DIRECT_ACCOUNTS", "50"))
    
    PROXY_REASSIGN_ENABLED: bool = _env_flag("PROXY_REASSIGN_ENABLED", "true")
    # 🛑 ANTI-CHAIN-BAN: فال‌بک IP مستقیم به‌صورت پیش‌فرض خاموش شد.
    # قبلاً true بود: با مرگ پروکسی‌ها، همه‌ی ورکرها روی IP دیتاسنتریِ خودِ سرور
    # می‌نشستند (۴۰ اکانت اسپمر از یک IP دیتاسنتر = پرچم فوری و موج بن جمعی).
    # برای فعال‌سازی آگاهانه: FALLBACK_TO_DIRECT_IP=true در .env
    FALLBACK_TO_DIRECT_IP: bool = _env_flag("FALLBACK_TO_DIRECT_IP", "false")

    # 🛑 ANTI-CHAIN-BAN: سقف اکانت روی هر پروکسی از ۲۰ به ۳ کاهش یافت.
    # ۲۰ اکانت روی یک IP از نظر تلگرام «کارخانه‌ی اسپم» است و سوختن IP یعنی
    # بنِ همه‌ی ۲۰ اکانت با هم (بن زنجیره‌ای). Ideal: 1:1 (اکانت↔IP اختصاصی).
    # bindingهای موجود sticky می‌مانند؛ فقط claimهای جدید از سقف جدید تبعیت می‌کنند.
    MAX_ACCOUNTS_PER_PROXY: int = int(os.getenv("MAX_ACCOUNTS_PER_PROXY", "3"))
    SHARED_API_ID_WARN_THRESHOLD: int = int(os.getenv("SHARED_API_ID_WARN_THRESHOLD", "5"))

    # افزوده شده برای یکدست‌سازی تاخیر حلقه‌ی Reconnect و Health Check (F17)
    RECONNECT_LOOP_INTERVAL_SECONDS: int = int(os.getenv("RECONNECT_LOOP_INTERVAL_SECONDS", "60"))
    DAILY_SEND_LIMIT_PER_ACCOUNT: int = int(os.getenv("DAILY_SEND_LIMIT_PER_ACCOUNT", "250"))
    
    SEND_DELAY_MIN: float = float(os.getenv("SEND_DELAY_MIN", "1.2"))
    SEND_DELAY_MAX: float = float(os.getenv("SEND_DELAY_MAX", "2.8"))
    GLOBAL_SLOWDOWN_FACTOR_MIN: float = float(os.getenv("GLOBAL_SLOWDOWN_FACTOR_MIN", "1.5"))
    GLOBAL_SLOWDOWN_FACTOR_MAX: float = float(os.getenv("GLOBAL_SLOWDOWN_FACTOR_MAX", "2.0"))
    
    SMARTFLOW_SEEN_TIMEOUT_MIN: float = float(os.getenv("SMARTFLOW_SEEN_TIMEOUT_MIN", "30.0"))
    SMARTFLOW_SEEN_TIMEOUT_MAX: float = float(os.getenv("SMARTFLOW_SEEN_TIMEOUT_MAX", "60.0"))
    SMARTFLOW_STAGE_DELAY_MIN: float = float(os.getenv("SMARTFLOW_STAGE_DELAY_MIN", "8.0"))
    SMARTFLOW_STAGE_DELAY_MAX: float = float(os.getenv("SMARTFLOW_STAGE_DELAY_MAX", "20.0"))
    BYPASS_WARMUP: bool = _env_flag("BYPASS_WARMUP", "false")

    NEW_ACCOUNT_DAYS: int = int(os.getenv("NEW_ACCOUNT_DAYS", "14"))
    NEW_ACCOUNT_DAILY_SEND_LIMIT: int = int(os.getenv("NEW_ACCOUNT_DAILY_SEND_LIMIT", "20"))

    CRM_RELEVANT_WINDOW_HOURS: int = int(os.getenv("CRM_RELEVANT_WINDOW_HOURS", "72"))
    CRM_NOTIFY_LIMIT_PER_ADMIN: int = int(os.getenv("CRM_NOTIFY_LIMIT_PER_ADMIN", "20"))
    CRM_NOTIFY_WINDOW_SECONDS: int = int(os.getenv("CRM_NOTIFY_WINDOW_SECONDS", "60"))

    ADMIN_ROLE_CACHE_TTL: int = int(os.getenv("ADMIN_ROLE_CACHE_TTL", "120"))
    ADMIN_REJECT_REPLY_COOLDOWN: int = int(os.getenv("ADMIN_REJECT_REPLY_COOLDOWN", "3600"))

    DISPATCH_BATCH_SIZE: int = int(os.getenv("DISPATCH_BATCH_SIZE", "6"))
    DISPATCH_INTERVAL_SECONDS: float = float(os.getenv("DISPATCH_INTERVAL_SECONDS", "1.0"))
    
    EXTRACT_PAUSE_MIN: float = float(os.getenv("EXTRACT_PAUSE_MIN", "0.25"))
    EXTRACT_PAUSE_MAX: float = float(os.getenv("EXTRACT_PAUSE_MAX", "0.5"))
    EXTRACT_JOIN_PAUSE_MIN: float = float(os.getenv("EXTRACT_JOIN_PAUSE_MIN", "0.4"))
    EXTRACT_JOIN_PAUSE_MAX: float = float(os.getenv("EXTRACT_JOIN_PAUSE_MAX", "0.8"))
    EXTRACT_MEMBERS_API_LIMIT: int = int(os.getenv("EXTRACT_MEMBERS_API_LIMIT", "10000"))
    EXTRACT_CHECKPOINT_EVERY: int = int(os.getenv("EXTRACT_CHECKPOINT_EVERY", "500"))
    EXTRACT_PARALLEL_MIN_MEMBERS: int = int(os.getenv("EXTRACT_PARALLEL_MIN_MEMBERS", "3000"))
    EXTRACT_PARALLEL_MAX_WORKERS: int = int(os.getenv("EXTRACT_PARALLEL_MAX_WORKERS", "3"))
    EXTRACT_AUTO_FALLBACK_TO_MESSAGES: bool = _env_flag("EXTRACT_AUTO_FALLBACK_TO_MESSAGES", "true")


    PROXY_HEALTH_CHECK_INTERVAL: int = int(os.getenv("PROXY_HEALTH_CHECK_INTERVAL", "600"))
    PROXY_WEAK_LATENCY_MS: int = int(os.getenv("PROXY_WEAK_LATENCY_MS", "1500"))
    PROXY_DEAD_CONSECUTIVE_FAILS: int = int(os.getenv("PROXY_DEAD_CONSECUTIVE_FAILS", "3"))
    PROXY_RECOVER_CONSECUTIVE_OKS: int = int(os.getenv("PROXY_RECOVER_CONSECUTIVE_OKS", "2"))

    
    PROGRESS_EDIT_INTERVAL_SECONDS: float = float(os.getenv("PROGRESS_EDIT_INTERVAL_SECONDS", "10.0"))
    PROGRESS_NOTIFY_ENABLED: bool = _env_flag("PROGRESS_NOTIFY_ENABLED", "true")


    # فاز ۵: تنظیمات چرخش پیشگیرانه پروکسی (Proactive Rotation)
    ROTATE_AFTER_CHUNKS: int = int(os.getenv("ROTATE_AFTER_CHUNKS", "5"))
    ROTATE_AFTER_MINUTES: int = int(os.getenv("ROTATE_AFTER_MINUTES", "60"))
    ROTATE_AFTER_CONSECUTIVE_ERRORS: int = int(os.getenv("ROTATE_AFTER_CONSECUTIVE_ERRORS", "3"))
    ROTATE_COOLDOWN_SECONDS: int = int(os.getenv("ROTATE_COOLDOWN_SECONDS", "300"))

    # Photo Package Config
    
    PACKAGE_PHOTO_COUNT: int = int(os.getenv("PACKAGE_PHOTO_COUNT", "3"))
    ACCOUNTS_PAGE_SIZE: int = int(os.getenv("ACCOUNTS_PAGE_SIZE", "8"))
    JOIN_REQUEST_TIMEOUT_SECONDS: int = int(os.getenv("JOIN_REQUEST_TIMEOUT_SECONDS", "60"))
    PENDING_APPROVAL_RETRY_LIMIT: int = int(os.getenv("PENDING_APPROVAL_RETRY_LIMIT", "25"))

    ORDER_MAX_CONCURRENT_SENDERS: int = int(os.getenv("ORDER_MAX_CONCURRENT_SENDERS", "1"))

    # رفع ارورهای getattr و اضافه شدن Type Hints
    MAX_CONSECUTIVE_ERRORS: int = int(os.getenv("MAX_CONSECUTIVE_ERRORS", "3"))
    # کاهش به ۶۰ دقیقه جهت جلوگیری از استراحت طولانی و فلج شدن سیستم بر اثر خطاهای موقت پروکسی (فاز ۱۰)
    COOLDOWN_MINUTES_ON_ERROR: int = int(os.getenv("COOLDOWN_MINUTES_ON_ERROR", "60"))
    # 🩹 FIX (hourly-cap): این مقدار «پیش‌فرض» قبلاً ۱۵ بود و مخفیانه جلوی «ظرفیت
    # ارسال هر ورکر» (send_limit_per_run که از پنل تنظیم می‌شود) را می‌گرفت؛ یعنی
    # ادمین ظرفیت را ۲۰ می‌گذاشت ولی ورکر با پیام «سقف ساعتی (15) پر شد» در ۱۵
    # متوقف می‌شد. پیش‌فرض به ۳۰ (بالاتر از مقادیر معمولِ پنل) تغییر کرد.
    # اگر در .env مقدار صریح دارید (مثلاً 15) آن را حذف یا >= ظرفیت پنل کنید.
    HOURLY_SEND_LIMIT_PER_ACCOUNT: int = int(os.getenv("HOURLY_SEND_LIMIT_PER_ACCOUNT", "30"))
    
    # 🟢 فاز ۲: حد نصاب شکست از کانال مبدا برای لغو کامل سفارش
    SOURCE_FAIL_MAX_WORKERS: int = int(os.getenv("SOURCE_FAIL_MAX_WORKERS", "3"))

    # اصلاح باگ ۲-الف: انتقال MIN_WARMUP_HOURS به داخل کانفیگ
    MIN_WARMUP_HOURS: int = int(os.getenv("MIN_WARMUP_HOURS", "24"))
    
    # اصلاح باگ ۲-ب: اضافه شدن فلگ مربوط به جهش متن
    TEXT_MUTATION_INVISIBLE_CHARS_ENABLED: bool = _env_flag("TEXT_MUTATION_INVISIBLE_CHARS_ENABLED", "false")

    WARMUP_THROTTLE_MIN: float = float(os.getenv("WARMUP_THROTTLE_MIN", "2.0"))
    WARMUP_THROTTLE_MAX: float = float(os.getenv("WARMUP_THROTTLE_MAX", "5.0"))

    # وضعیت چک پیشگیرانه (Preflight) با اسپم‌بات (همگام با متغیرهای محیطی)
    PREFLIGHT_SPAMBOT_CHECK_ENABLED: bool = _env_flag("PREFLIGHT_SPAMBOT_CHECK_ENABLED", "true")
    # بازه‌ی کش شدن وضعیت اسپم‌بات برای هر اکانت به ساعت
    PREFLIGHT_SPAMBOT_CACHE_HOURS: int = int(os.getenv("PREFLIGHT_SPAMBOT_CACHE_HOURS", "4"))

    # 🩹 FIX (باگ «مسدود» اشتباه):
    # مدت استراحت (دقیقه) برای «محدودیت نرم» اسپم‌بات — یعنی پیام
    # «I'm very sorry…» بدون تاریخ پایان. این پیام بن دائم نیست و اکانت معمولاً
    # همچنان می‌تواند به بقیه پیام بدهد؛ قبلاً این حالت بن دائم می‌شد. 0 = بدون توقف.
    SPAMBOT_SOFT_LIMIT_REST_MINUTES: int = int(os.getenv("SPAMBOT_SOFT_LIMIT_REST_MINUTES", "60"))
    # بازبینی دوره‌ای اسپم‌بات برای اکانت‌های مسدود (دقیقه) — اگر تلگرام اکانت را
    # آزاد کرده باشد (پیام «Good news»)، ربات خودکار وضعیتش را فعال می‌کند.
    SPAMBOT_RECHECK_INTERVAL_MINUTES: int = int(os.getenv("SPAMBOT_RECHECK_INTERVAL_MINUTES", "30"))

    # 🩹 FIX (آنلاین ۲۴ ساعته اکانت‌ها): حلقه‌ی Presence به صورت پیش‌فرض «خاموش» است.
    # قبلاً هر ۲۰ تا ۴۵ دقیقه UpdateStatus(offline=False) صدا زده می‌شد و چون
    # ورکرها ۲۴/۷ متصل بودند، اکانت‌ها در پیوی تارگت‌ها همیشه «آنلاین» دیده می‌شدند
    # (پرچم قرمز برای الگوریتم تلگرام + غیرطبیعی برای تارگت‌ها).
    PRESENCE_KEEPER_ENABLED: bool = _env_flag("PRESENCE_KEEPER_ENABLED", "false")
    PRESENCE_CYCLE_MIN_HOURS: float = float(os.getenv("PRESENCE_CYCLE_MIN_HOURS", "3"))
    PRESENCE_CYCLE_MAX_HOURS: float = float(os.getenv("PRESENCE_CYCLE_MAX_HOURS", "6"))
    # مدت «چشمک آنلاین» بر حسب ثانیه: مثل گوشی‌ای که بیدار می‌شود، سینک می‌کند و می‌خوابد
    PRESENCE_BLIP_MIN_SECONDS: float = float(os.getenv("PRESENCE_BLIP_MIN_SECONDS", "10"))
    PRESENCE_BLIP_MAX_SECONDS: float = float(os.getenv("PRESENCE_BLIP_MAX_SECONDS", "30"))

    # ==========================================
    # 🛑 ANTI-CHAIN-BAN (گارد ضد بنِ زنجیره‌ای) — utils/ban_wave.py
    # ==========================================
    # فاجعه‌ی ۳۴/۴۰ بن از سه کانال تغذیه می‌شد: IP مشترک + سوییچ فوری ورکر بعد از
    # بن + نبود واکنش به بن‌های متوالی. تنظیمات زیر سه لایه‌ی دفاع را کنترل می‌کنند:

    # پنجره‌ی زمانیِ «موج بن» (دقیقه) — شمارنده‌های سراسری در این پنجره ریست می‌شوند
    BAN_WAVE_WINDOW_MINUTES: int = int(os.getenv("BAN_WAVE_WINDOW_MINUTES", "60"))
    # آستانه‌ی بن واقعی در پنجره → فریز کامل دیسپچر
    BAN_WAVE_GLOBAL_THRESHOLD: int = int(os.getenv("BAN_WAVE_GLOBAL_THRESHOLD", "3"))
    # آستانه‌ی PeerFlood در پنجره → فریز کامل دیسپچر ( PeerFlood هم سیگنال اسپم است)
    BAN_WAVE_GLOBAL_PF_THRESHOLD: int = int(os.getenv("BAN_WAVE_GLOBAL_PF_THRESHOLD", "5"))
    # مدت فریز سراسری دیسپچر بعد از تشخیص موج (دقیقه)
    BAN_WAVE_FREEZE_MINUTES: int = int(os.getenv("BAN_WAVE_FREEZE_MINUTES", "60"))

    # آستانه‌ی بن واقعی روی «یک پروکسی» در ۲۴ ساعت → قرنطینه‌ی پروکسی (BURNED)
    BAN_WAVE_PROXY_BAN_THRESHOLD: int = int(os.getenv("BAN_WAVE_PROXY_BAN_THRESHOLD", "2"))
    # آستانه‌ی PeerFlood روی «یک پروکسی» در ۲۴ ساعت → قرنطینه‌ی پروکسی
    BAN_WAVE_PROXY_PF_THRESHOLD: int = int(os.getenv("BAN_WAVE_PROXY_PF_THRESHOLD", "3"))
    # مدت قرنطینه‌ی پروکسی سوخته (ساعت) — هیچ claim جدیدی در این مدت روی آن نمی‌نشیند
    BAN_PROXY_QUARANTINE_HOURS: int = int(os.getenv("BAN_PROXY_QUARANTINE_HOURS", "72"))
    # بعد از «یک» بن روی پروکسی، فاصله‌گذاری claim (ساعت) — حتی زیر آستانه
    BAN_PROXY_CLAIM_COOLDOWN_HOURS: int = int(os.getenv("BAN_PROXY_CLAIM_COOLDOWN_HOURS", "24"))

    # ⚠️ DEPRECATED (ANTI-CHAIN-BAN v2): این کلید دیگر استفاده نمی‌شود — بن واقعی
    # حالا سفارش را همان لحظه لغو می‌کند (الگوی «کمبود ورکر» در task_queue) و
    # PeerFlood (اسپم) بدون خنک‌سازی، بی‌درنگ به ورکر بعدی سوییچ می‌کند.
    # صرفاً برای سازگاری .env سرور نگه داشته شده است تا خطای خواندن کانفیگ ندهد.
    BAN_FAILOVER_COOLDOWN_MINUTES: int = int(os.getenv("BAN_FAILOVER_COOLDOWN_MINUTES", "30"))
    
    @property
    def MYSQL_URL(self) -> str:
        user_q = quote_plus(self.DB_USER)
        pass_q = quote_plus(self.DB_PASS)
        return (
            f"mysql+asyncmy://{user_q}:{pass_q}"
            f"@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
        )

    @property
    def FORCE_JOIN_CHANNEL_LIST(self) -> list:
        return [c.strip() for c in self.FORCE_JOIN_CHANNELS.split(",") if c.strip()]

    @property
    def WORKER_PROTECTED_CHANNEL_LIST(self) -> list:
        """کانال‌های حفاظت‌شده‌ی ورکرها به شکل @username (بدون موارد خالی/تکراری)."""
        seen: list = []
        for c in self.WORKER_PROTECTED_CHANNELS.replace(";", ",").split(","):
            c = c.strip().lstrip("@").strip()
            if c and c.lower() not in [s.lower() for s in seen]:
                seen.append(c)
        return [f"@{c}" for c in seen]

    @property
    def REDIS_URL(self) -> str:
        from urllib.parse import quote_plus
        auth = ""
        if self.REDIS_USER:
            auth = quote_plus(self.REDIS_USER)
            if self.REDIS_PASS:
                auth += f":{quote_plus(self.REDIS_PASS)}"
            auth += "@"
        elif self.REDIS_PASS:
            auth = f":{quote_plus(self.REDIS_PASS)}@"
        return f"redis://{auth}{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"


# سازگاری با ماژول‌های دیگری که مستقیما این نام را ایمپورت می‌کنند
# Global config instance
config = Config()
MIN_WARMUP_HOURS = config.MIN_WARMUP_HOURS
# 🔥 فاز ۶ (R7): حداقل ساعت گرم‌شدن اکانت تازه قبل از اولین chunk (توسط متغیرهای محیطی خوانده می‌شود)