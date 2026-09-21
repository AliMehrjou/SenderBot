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
    on_hold_proxy = "on_hold_proxy"

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
    progress_notify: Mapped[bool] = mapped_column(default=True, nullable=False)


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
    
    # فاز جدید: پراکسی پرمیوم مشتری
    has_premium_proxy: Mapped[bool] = mapped_column(default=False, server_default="0", nullable=False)
    premium_proxy_string: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    # 🛡 اصلاح فاز ۱۰: حذف delete-orphan برای جلوگیری از پاک شدن اکانت‌های زیرمجموعه
    accounts: Mapped[List["Account"]] = relationship(
        back_populates="category", 
        cascade="save-update, merge"
    )
    orders: Mapped[List["Order"]] = relationship(secondary=order_category_assoc, back_populates="categories")

    @property
    def custom_id(self) -> str:
        return f"user_{self.id}/"
    
class AccountStatus(str, enum.Enum):
    active = "active"
    cooldown = "cooldown"
    blocked = "blocked"
    disabled = "disabled"


class Account(Base):
    __tablename__ = "accounts"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    phone_number: Mapped[str] = mapped_column(String(20), nullable=False, unique=True)
    telegram_user_id: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True, index=True)
    
    # 🩺 فاز ۱۰: سیستم سلامت اکانت
    status: Mapped[AccountStatus] = mapped_column(SQLAlchemyEnum(AccountStatus), default=AccountStatus.active, nullable=False, index=True)
    status_reason: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    expected_return_time: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    consecutive_errors: Mapped[int] = mapped_column(default=0, server_default="0", nullable=False)
    session_string: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    
    # 🛡 اصلاح فاز ۱۰: اضافه شدن ondelete="SET NULL" برای همخوانی با فلسفه حفظ اکانت‌ها
    category_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("categories.id", ondelete="SET NULL"), 
        nullable=True, index=True
    )
    proxy_string: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    
    # 🧱 فاز ۳: قرارداد وضعیت پروکسی و صف انتظار
    proxy_status: Mapped[str] = mapped_column(String(20), default="WAITING_PROXY", server_default="'WAITING_PROXY'", nullable=False, index=True)
    proxy_queue_joined_at: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    
    is_banned: Mapped[bool] = mapped_column(default=False)
    flood_wait_until: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(timezone=True), nullable=True, index=True)

    created_at: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=True)
    warmed_up_at: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    last_limit_type: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    spambot_report: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    spambot_checked_at: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    restricted_until: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

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

# مسیر فایل: database/models.py
# بلوک کد الحاقی (انتهای فایل):
class WorkerEvent(Base):
    """🩺 فاز ۱۰: ثبت رویدادهای تغییر وضعیت اکانت‌ها (لاگ‌های سلامت)"""
    __tablename__ = "worker_events"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    account_id: Mapped[int] = mapped_column(ForeignKey("accounts.id", ondelete="CASCADE"), nullable=False, index=True)
    old_status: Mapped[str] = mapped_column(String(50), nullable=False)
    new_status: Mapped[str] = mapped_column(String(50), nullable=False)
    reason: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    error_details: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    account: Mapped["Account"] = relationship()

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
    filter_type: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    
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
    
    hold_reason: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    server_ip_consent: Mapped[bool] = mapped_column(default=False, server_default="0", nullable=False)

    speed_mode: Mapped[Optional[str]] = mapped_column(String(8), nullable=True)

    source_channel_id: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    source_message_ids: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    status: Mapped[OrderStatus] = mapped_column(SQLAlchemyEnum(OrderStatus), default=OrderStatus.pending, nullable=False, index=True)
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    scheduled_for: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    tracking_code: Mapped[Optional[str]] = mapped_column(String(20), unique=True, index=True, nullable=True)

    user_id: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True, index=True)

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
    send_limit_per_run: Mapped[int] = mapped_column(default=80, nullable=False)
    cooldown_hours: Mapped[int] = mapped_column(default=1, nullable=False)
    spam_penalty_days: Mapped[int] = mapped_column(default=1, nullable=False)
    
    auto_set_2fa: Mapped[bool] = mapped_column(default=True, nullable=False)         
    terminate_sessions: Mapped[bool] = mapped_column(default=False, nullable=False)    
    auto_set_bio: Mapped[bool] = mapped_column(default=True, nullable=False)
    auto_set_name: Mapped[bool] = mapped_column(default=True, nullable=False)
    auto_set_photo: Mapped[bool] = mapped_column(default=True, nullable=False)
    public_order_access: Mapped[bool] = mapped_column(default=False, nullable=False) 

    # 🚀 فاز جدید: پروفایل سرعت و ریسک‌پذیری
# --- کد تغییر یافته ---
    extraction_speed_mode: Mapped[str] = mapped_column(String(8), default="safe", server_default="safe", nullable=False)
    turbo_risk_acknowledged: Mapped[bool] = mapped_column(default=False, server_default="0", nullable=False)
    worker_residency: Mapped[str] = mapped_column(String(15), default="ephemeral", server_default="'ephemeral'", nullable=False)
    smart_anti_ban: Mapped[bool] = mapped_column(default=True, server_default="1", nullable=False)
    
    # 🚀 فاز جدید: تنظیمات ارسال مستقیم یا با پراکسی
    use_proxy_for_sending: Mapped[bool] = mapped_column(default=False, server_default="0", nullable=False)

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

    # 🩺 فاز جدید: تفکیک نوع پراکسی و بررسی سلامت
    usage_type: Mapped[str] = mapped_column(String(20), default="both", server_default="'both'", nullable=False)
    is_healthy: Mapped[bool] = mapped_column(default=True, server_default="1", nullable=False)
    
    # فاز ۲: ماشین وضعیت سلامت پروکسی
    health_state: Mapped[str] = mapped_column(String(20), default="HEALTHY", server_default="'HEALTHY'", nullable=False)
    consecutive_successes: Mapped[int] = mapped_column(default=0, server_default="0", nullable=False)
    consecutive_failures: Mapped[int] = mapped_column(default=0, server_default="0", nullable=False)
    last_state_changed_at: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    
    ping_ms: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    last_checked_at: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    @property
    def custom_id(self) -> str:
        return f"proxy_{self.id}/"