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
    DB_PORT: int = int(os.getenv("DB_PORT", 3306))
    DB_NAME: str = os.getenv("DB_NAME", "telegram_bulk_db")

    # Redis Settings
    REDIS_HOST: str = os.getenv("REDIS_HOST", "127.0.0.1")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT", 6379))
    REDIS_DB: int = int(os.getenv("REDIS_DB", 0))
    REDIS_PASS: str = os.getenv("REDIS_PASS", "")

    # Telegram API Settings
    API_ID: int = int(os.getenv("API_ID", 0))
    API_HASH: str = os.getenv("API_HASH", "")
    BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")
    ADMIN_ID: int = int(os.getenv("ADMIN_ID", 0))

    FORCE_JOIN_CHANNELS: str = os.getenv("FORCE_JOIN_CHANNELS", "")
    FALLBACK_TO_DIRECT_IP: bool = _env_flag("FALLBACK_TO_DIRECT_IP", "false")

    MAX_ACCOUNTS_PER_PROXY: int = int(os.getenv("MAX_ACCOUNTS_PER_PROXY", "1"))
    SHARED_API_ID_WARN_THRESHOLD: int = int(os.getenv("SHARED_API_ID_WARN_THRESHOLD", "5"))
    DAILY_SEND_LIMIT_PER_ACCOUNT: int = int(os.getenv("DAILY_SEND_LIMIT_PER_ACCOUNT", "50"))

    NEW_ACCOUNT_DAYS: int = int(os.getenv("NEW_ACCOUNT_DAYS", "14"))
    NEW_ACCOUNT_DAILY_SEND_LIMIT: int = int(os.getenv("NEW_ACCOUNT_DAILY_SEND_LIMIT", "20"))

    CRM_RELEVANT_WINDOW_HOURS: int = int(os.getenv("CRM_RELEVANT_WINDOW_HOURS", "72"))
    CRM_NOTIFY_LIMIT_PER_ADMIN: int = int(os.getenv("CRM_NOTIFY_LIMIT_PER_ADMIN", "20"))
    CRM_NOTIFY_WINDOW_SECONDS: int = int(os.getenv("CRM_NOTIFY_WINDOW_SECONDS", "60"))

    ADMIN_ROLE_CACHE_TTL: int = int(os.getenv("ADMIN_ROLE_CACHE_TTL", "120"))
    ADMIN_REJECT_REPLY_COOLDOWN: int = int(os.getenv("ADMIN_REJECT_REPLY_COOLDOWN", "3600"))

    DISPATCH_BATCH_SIZE: int = int(os.getenv("DISPATCH_BATCH_SIZE", "3"))
    
    # اصلاح باگ ۲-الف: انتقال MIN_WARMUP_HOURS به داخل کانفیگ
    MIN_WARMUP_HOURS: int = int(os.getenv("MIN_WARMUP_HOURS", "24"))
    
    # اصلاح باگ ۲-ب: اضافه شدن فلگ مربوط به جهش متن
    TEXT_MUTATION_INVISIBLE_CHARS_ENABLED: bool = _env_flag("TEXT_MUTATION_INVISIBLE_CHARS_ENABLED", "false")

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
    def REDIS_URL(self) -> str:
        if self.REDIS_PASS:
            return f"redis://:{self.REDIS_PASS}@{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"
        return f"redis://{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"

# سازگاری با ماژول‌های دیگری که مستقیما این نام را ایمپورت می‌کنند


# Global config instance
config = Config()
MIN_WARMUP_HOURS = config.MIN_WARMUP_HOURS
# 🔥 فاز ۶ (R7): حداقل ساعت گرم‌شدن اکانت تازه قبل از اولین chunk (توسط متغیرهای محیطی خوانده می‌شود)
