import os
from dataclasses import dataclass

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

    # Redis Settings (Used for aiogram FSM states and Celery/background tasks)
    REDIS_HOST: str = os.getenv("REDIS_HOST", "127.0.0.1")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT", 6379))
    REDIS_DB: int = int(os.getenv("REDIS_DB", 0))
    REDIS_PASS: str = os.getenv("REDIS_PASS", "")

    # Telegram API Settings
    API_ID: int = int(os.getenv("API_ID", 0))
    API_HASH: str = os.getenv("API_HASH", "")
    ADMIN_ID: int = int(os.getenv("ADMIN_ID", 0))
    @property
    def MYSQL_URL(self) -> str:
        """Constructs the async SQLAlchemy connection string."""
        # Using asyncmy for maximum Python 3.11+ async compatibility and speed
        return f"mysql+asyncmy://{self.DB_USER}:{self.DB_PASS}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"

    @property
    def REDIS_URL(self) -> str:
        """Constructs the Redis connection string."""
        if self.REDIS_PASS:
            return f"redis://:{self.REDIS_PASS}@{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"
        return f"redis://{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"

# Global config instance
config = Config()