import datetime
import enum
from typing import List, Optional
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func
from sqlalchemy.dialects.mysql import LONGTEXT  # ایمپورت اختصاصی برای حل مشکل Data too long
from .base import Base
from sqlalchemy import Table, Column, Integer, ForeignKey, String, Text, DateTime, Enum as SQLAlchemyEnum, BigInteger, UniqueConstraint, Index
class OrderStatus(str, enum.Enum):
    pending = "pending"
    running = "running"
    completed = "completed"
    error = "error"

order_category_assoc = Table(
    "order_category_assoc",
    Base.metadata,
    Column("order_id", Integer, ForeignKey("orders.id", ondelete="CASCADE"), primary_key=True),
    Column("category_id", Integer, ForeignKey("categories.id", ondelete="CASCADE"), primary_key=True)
)

class Admin(Base):
    """
    جدول مدیریت ادمین‌های فرعی سیستم
    """
    __tablename__ = "admins"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    telegram_id: Mapped[int] = mapped_column(BigInteger, unique=True, nullable=False)


class APIKey(Base):
    __tablename__ = "api_keys"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    api_id: Mapped[int] = mapped_column(nullable=False, unique=True)
    api_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    is_active: Mapped[bool] = mapped_column(default=True)

    @property
    def custom_id(self) -> str:
        return f"user_{self.id}/"

class Category(Base):
    __tablename__ = "categories"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False, unique=True)
    
    # 🛡 اصلاح فاز ۱۰: حذف delete-orphan برای جلوگیری از پاک شدن اکانت‌های زیرمجموعه
    accounts: Mapped[List["Account"]] = relationship(
        back_populates="category", 
        cascade="save-update, merge"
    )
    orders: Mapped[List["Order"]] = relationship(secondary=order_category_assoc, back_populates="categories")

    @property
    def custom_id(self) -> str:
        return f"user_{self.id}/"
    
class Account(Base):
    __tablename__ = "accounts"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    phone_number: Mapped[str] = mapped_column(String(20), nullable=False, unique=True)
    telegram_user_id: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True, index=True)
    session_string: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    
    # 🛡 اصلاح فاز ۱۰: اضافه شدن ondelete="SET NULL" برای همخوانی با فلسفه حفظ اکانت‌ها
    category_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("categories.id", ondelete="SET NULL"), 
        nullable=True, index=True
    )
    proxy_string: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    is_banned: Mapped[bool] = mapped_column(default=False)
    flood_wait_until: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(timezone=True), nullable=True, index=True)

    created_at: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=True)
    warmed_up_at: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    category: Mapped[Optional["Category"]] = relationship(
        back_populates="accounts", 
        lazy="selectin"
    )
    
    api_id: Mapped[Optional[int]] = mapped_column(ForeignKey("api_keys.id", ondelete="SET NULL"), nullable=True)
    api_key: Mapped[Optional["APIKey"]] = relationship(lazy="selectin")

    last_login_code: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    two_step_password: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)
    device_model: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    system_version: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    app_version: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)

    photo_package_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("profile_photo_packages.id", ondelete="SET NULL"),
        nullable=True, index=True
    )
    photo_package: Mapped[Optional["ProfilePhotoPackage"]] = relationship(back_populates="accounts")

    @property
    def custom_id(self) -> str:
        return f"user_{self.id}/"

class Order(Base):
    __tablename__ = "orders"
    
    # اضافه کردن ایندکس ترکیبی برای بهینه‌سازی وحشتناک سرعت دیسپچر
    __table_args__ = (
        Index("ix_orders_dispatch", "status", "is_approved", "scheduled_for"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    
    order_type: Mapped[str] = mapped_column(String(50), nullable=False)
    target_data: Mapped[str] = mapped_column(LONGTEXT, nullable=False)
    target_count: Mapped[Optional[int]] = mapped_column(nullable=True)
    filter_type: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    
    message_text: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    media_path: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    media_type: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    button_text: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    button_url: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    
    message_2_text: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    media_2_path: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    media_2_type: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    
    message_3_text: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    media_3_path: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    media_3_type: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    
    smart_flow: Mapped[bool] = mapped_column(default=False, nullable=False)
    inflight_data: Mapped[Optional[str]] = mapped_column(LONGTEXT, nullable=True)
    fail_streak: Mapped[int] = mapped_column(default=0, server_default="0", nullable=False)
    use_banner_pool: Mapped[bool] = mapped_column(default=False, nullable=False)

    source_channel_id: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    source_message_ids: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    status: Mapped[OrderStatus] = mapped_column(SQLAlchemyEnum(OrderStatus), default=OrderStatus.pending, nullable=False, index=True)
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    scheduled_for: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    tracking_code: Mapped[Optional[str]] = mapped_column(String(20), unique=True, index=True, nullable=True)

    is_approved: Mapped[bool] = mapped_column(default=False, server_default="0", nullable=False)
    reject_reason: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    retry_count: Mapped[int] = mapped_column(default=0, nullable=False)
    extracted_count: Mapped[Optional[int]] = mapped_column(nullable=True)

    # رفع N+1 هنگام باز کردن داشبورد سفارشات
    categories: Mapped[List["Category"]] = relationship(
        secondary=order_category_assoc, 
        back_populates="orders",
        lazy="selectin"
    )
    logs: Mapped[list["OrderLog"]] = relationship(back_populates="order", cascade="all, delete-orphan")

    @property
    def custom_id(self) -> str:
        return f"order_{self.id}/"
    
class OrderLog(Base):
    __tablename__ = "order_logs"
    
    # ایندکس ترکیبی برای سرعت بخشیدن به لود داشبورد سفارشات
    __table_args__ = (
        Index("ix_order_logs_order_status", "order_id", "status"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    
    order_id: Mapped[int] = mapped_column(ForeignKey("orders.id", ondelete="CASCADE"), nullable=False, index=True)
    account_id: Mapped[Optional[int]] = mapped_column(ForeignKey("accounts.id", ondelete="CASCADE"), nullable=True)
    
    target: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(String(50), nullable=False)
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    order: Mapped["Order"] = relationship(back_populates="logs")
    account: Mapped[Optional["Account"]] = relationship()


# 🚪 فاز ۶ (R3 — بهداشت Join/Leave): عضویت‌هایی که «به‌خاطر سفارش» ایجاد شده‌اند
class OrderJoin(Base):
    """
    🚪 فاز ۶ (R3-ب): اکانتی که «به‌خاطر این سفارش» عضو گروه هدف شده است.
    بعد از terminal شدن سفارش (completed/error — شامل Kill Switch)، sweep حلقه‌ی
    دیسپچر (workers/task_queue.py::leave_sweep_terminal_orders) این اکانت را از
    گروه leave می‌کند.
    - اکانتِ «از قبل عضو» (UserAlreadyParticipant) هرگز ردیف نمی‌گیرد → leave نمی‌شود.
    - chat_id = NULL یعنی فقط «درخواست عضویت» ارسال شده (InviteRequestSent) و
      عضویت هنوز رخ نداده؛ اگر بعداً تأیید شود، چرخه‌ی بعد chat_id را تکمیل می‌کند.
    ⚠️ برای دیتابیس‌های موجود این مهاجرت دستی لازم است (قبل از اجرا بکاپ بگیر):
       CREATE TABLE order_joins (...);  — دقیق در بخش SQL این گزارش
    """
    __tablename__ = "order_joins"
    __table_args__ = (UniqueConstraint("order_id", "account_id", name="uq_order_joins_order_account"),)

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    order_id: Mapped[int] = mapped_column(ForeignKey("orders.id", ondelete="CASCADE"), nullable=False, index=True)
    account_id: Mapped[int] = mapped_column(ForeignKey("accounts.id", ondelete="CASCADE"), nullable=False, index=True)
    # لینک اصلی گروه (بعد از resolve، target_data سفارش با لیست ممبرها جایگزین می‌شود؛
    # این ستون منبع «کدام گروه» برای leave است)
    group_link: Mapped[str] = mapped_column(String(255), nullable=False)
    chat_id: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    leave_done: Mapped[bool] = mapped_column(default=False, nullable=False)
    created_at: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=True)


class Banner(Base):
    """
    🎨 مخزن بنر (چرخش بنر): بنرهایی که به‌صورت تصادفی بین chunk های ارسال چرخش می‌خورند.
    هر chunk (هر اکانت در هر سیکل دیسپچ) یک بنر فعالِ تصادفی دریافت می‌کند و
    شمارنده‌ی usage_count همان بنر یک واحد زیاد می‌شود.
    """
    __tablename__ = "banners"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    text: Mapped[str] = mapped_column(Text, nullable=False)
    media_path: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    media_type: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)  # photo / video / None
    is_active: Mapped[bool] = mapped_column(default=True, nullable=False)
    usage_count: Mapped[int] = mapped_column(default=0, nullable=False)


# ==========================================
# 🖼 پکیج‌های ۳ تایی عکس پروفایل
# ==========================================
class ProfilePhotoPackage(Base):
    """
    🖼 پکیج عکس پروفایل — مجموعه‌ای استاندارد از ۳ عکس که هنگام استارت ورکر،
    در صورت روشن بودن auto_set_photo در GlobalSettings، جایگزین عکس‌های قبلی
    پروفایل اکانت می‌شود (utils/advanced_anti_ban.py::rotate_profile_photos).
    """
    __tablename__ = "profile_photo_packages"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False)

    # عکس‌های پکیج همیشه بر اساس position مرتب‌شده برمی‌گردند (ترتیب آپلود)
    photos: Mapped[List["ProfilePhoto"]] = relationship(
        back_populates="package",
        order_by="ProfilePhoto.position",
        cascade="all, delete-orphan",
    )
    # اکانت‌های متصل: با حذف پکیج فقط این ارتباط NULL می‌شود (اکانت حذف نمی‌شود)
    accounts: Mapped[List["Account"]] = relationship(back_populates="photo_package")

    @property
    def custom_id(self) -> str:
        return f"photo_pkg_{self.id}/"


class ProfilePhoto(Base):
    """
    🖼 تک‌عکسِ داخل پکیج — file_path مسیر فایل روی دیسک (profile_photos/{package_id}/)
    و position ترتیب آپلود روی پروفایل است (۱ تا ۳).
    """
    __tablename__ = "profile_photos"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    file_path: Mapped[str] = mapped_column(String(255), nullable=False)
    position: Mapped[int] = mapped_column(nullable=False)

    package_id: Mapped[int] = mapped_column(
        ForeignKey("profile_photo_packages.id", ondelete="CASCADE"),
        nullable=False, index=True
    )

    package: Mapped["ProfilePhotoPackage"] = relationship(back_populates="photos")


class GlobalSettings(Base):
    __tablename__ = "global_settings"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    max_accounts_per_api: Mapped[int] = mapped_column(default=5, nullable=False)
    send_limit_per_run: Mapped[int] = mapped_column(default=40, nullable=False)
    cooldown_hours: Mapped[int] = mapped_column(default=24, nullable=False)
    spam_penalty_days: Mapped[int] = mapped_column(default=3, nullable=False)
    
    auto_set_2fa: Mapped[bool] = mapped_column(default=True, nullable=False)         
    terminate_sessions: Mapped[bool] = mapped_column(default=False, nullable=False)    
    # 🎭 مدیریت پیشرفته پروفایل‌ها: سوئیچ واحد قبلی (auto_set_profile) به سه سوئیچ
    # مستقل تفکیک شد و قابلیت یوزرنیم (auto_set_username) کامل حذف گردید.
    auto_set_bio: Mapped[bool] = mapped_column(default=True, nullable=False)         # تغییر خودکار بیوگرافی
    auto_set_name: Mapped[bool] = mapped_column(default=True, nullable=False)        # تغییر خودکار نام (first/last)
    # 🖼 تغییر خودکار عکس پروفایل (پکیج‌های ۳ تایی) — مصرف‌کننده:
    # rotate_profile_photos که از session_manager فراخوانی می‌شود
    auto_set_photo: Mapped[bool] = mapped_column(default=False, nullable=False)
    public_order_access: Mapped[bool] = mapped_column(default=False, nullable=False)   


class Proxy(Base):
    """
    جدول مدیریت استخر پراکسی‌ها
    """
    __tablename__ = "proxies"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    proxy_string: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    is_active: Mapped[bool] = mapped_column(default=True)
    fail_count: Mapped[int] = mapped_column(default=0)

    # 🧲 فاز ۵ (BUG-14): شمارنده‌ی اکانت‌های اشغال‌کننده‌ی این پراکسی.
    # claim اتمیک با «UPDATE ... WHERE in_use < cap» نگه داشته می‌شود و
    # reconcile استارتاپی آن را با accounts.proxy_string همگام می‌کند.
    # ⚠️ برای دیتابیس‌های موجود این مهاجرت دستی لازم است (قبل از اجرا بکاپ بگیر):
    #    ALTER TABLE proxies ADD COLUMN in_use INT NOT NULL DEFAULT 0;
    #    (backfill اولیه به‌صورت خودکار توسط reconcile_proxy_usage در استارتاپ انجام می‌شود)
    in_use: Mapped[int] = mapped_column(default=0, nullable=False)

    @property
    def custom_id(self) -> str:
        return f"proxy_{self.id}/"