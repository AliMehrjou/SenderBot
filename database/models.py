import datetime
import enum
from typing import List, Optional

from sqlalchemy import String, Text, ForeignKey, DateTime, Enum as SQLAlchemyEnum
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func
from .base import Base
from sqlalchemy import Text
from typing import Optional
class OrderStatus(str, enum.Enum):
    pending = "pending"
    running = "running"
    completed = "completed"
    error = "error"

class APIKey(Base):
    __tablename__ = "api_keys"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    api_id: Mapped[int] = mapped_column(nullable=False)
    api_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    is_active: Mapped[bool] = mapped_column(default=True)

    @property
    def custom_id(self) -> str:
        return f"user_{self.id}/"

class Category(Base):
    __tablename__ = "categories"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False, unique=True)
    accounts: Mapped[List["Account"]] = relationship(back_populates="category", cascade="all, delete-orphan")
    orders: Mapped[List["Order"]] = relationship(back_populates="category", cascade="all, delete-orphan")

    @property
    def custom_id(self) -> str:
        return f"user_{self.id}/"

class Account(Base):
    __tablename__ = "accounts"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    phone_number: Mapped[str] = mapped_column(String(20), nullable=False, unique=True)
    session_string: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    category_id: Mapped[Optional[int]] = mapped_column(ForeignKey("categories.id"), nullable=True, index=True)
    proxy_string: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    is_banned: Mapped[bool] = mapped_column(default=False)
    flood_wait_until: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    category: Mapped[Optional["Category"]] = relationship(back_populates="accounts")

    @property
    def custom_id(self) -> str:
        return f"user_{self.id}/"

class Order(Base):
    __tablename__ = "orders"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    category_id: Mapped[int] = mapped_column(ForeignKey("categories.id"), nullable=False)
    order_type: Mapped[str] = mapped_column(String(50), nullable=False)
    target_data: Mapped[str] = mapped_column(Text, nullable=False)
    
    # فیلدهای جدید مارکتینگ
    message_text: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    media_path: Mapped[Optional[str]] = mapped_column(String(255), nullable=True) # مسیر فایل دانلود شده
    media_type: Mapped[Optional[str]] = mapped_column(String(50), nullable=True) # photo, video, document
    button_text: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    button_url: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    
    status: Mapped[OrderStatus] = mapped_column(SQLAlchemyEnum(OrderStatus), default=OrderStatus.pending, nullable=False, index=True)
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    
    category: Mapped["Category"] = relationship(back_populates="orders")
    logs: Mapped[list["OrderLog"]] = relationship(back_populates="order", cascade="all, delete-orphan")

    scheduled_for: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(timezone=True), nullable=True, index=True)

    
    @property
    def custom_id(self) -> str:
        return f"order_{self.id}/"

class GlobalSettings(Base):
    __tablename__ = "global_settings"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    max_accounts_per_api: Mapped[int] = mapped_column(default=5, nullable=False)
    send_limit_per_run: Mapped[int] = mapped_column(default=40, nullable=False)
    cooldown_hours: Mapped[int] = mapped_column(default=24, nullable=False)
    spam_penalty_days: Mapped[int] = mapped_column(default=3, nullable=False)

class OrderLog(Base):
    __tablename__ = "order_logs"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    order_id: Mapped[int] = mapped_column(ForeignKey("orders.id", ondelete="CASCADE"), nullable=False)
    account_id: Mapped[int] = mapped_column(ForeignKey("accounts.id", ondelete="CASCADE"), nullable=False)
    target: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(String(50), nullable=False) # مقادیر: 'success' یا 'error'
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

# اصلاح: استفاده از back_populates به جای backref برای جلوگیری از تداخل
    order: Mapped["Order"] = relationship(back_populates="logs")
    account: Mapped["Account"] = relationship()

# اضافه کردن به فایل models.py
class Proxy(Base):
    """
    جدول مدیریت استخر پراکسی‌ها
    """
    __tablename__ = "proxies"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    proxy_string: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    is_active: Mapped[bool] = mapped_column(default=True)
    fail_count: Mapped[int] = mapped_column(default=0)

    @property
    def custom_id(self) -> str:
        return f"proxy_{self.id}/"