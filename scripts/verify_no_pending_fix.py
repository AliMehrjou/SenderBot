# -*- coding: utf-8 -*-
"""
🧪 scripts/verify_no_pending_fix.py — راستی‌آزمایی فیکس «بسته‌شدن فوری سفارش
در صف دیسپچ» (خواسته‌ی کارفرما: «کلا نباید در انتظار دیسپچ بیوفته و pending»)
================================================================================
بدون Redis / DB / تلگرام واقعی؛ ترکیب تست رفتاریِ config و بررسی استاتیک
ساختاری (AST) روی workers/task_queue.py.

پوشش:
  ۱) کلید INSTANT_CLOSE_ON_NO_WORKER در config.py موجود است و پیش‌فرض true است
  ۲) قابلیت تنظیم از .env (متغیر محیطی → مقدار کانفیگ)
  ۳) بلوک Fail-Fast دیسپچر: بسته‌شدن «غیرمشروط» (وقتی فلگ روشن است) و
     حفظ رفتار قبلی فقط زیر فلگِ خاموش (Legacy)
  ۴) مسیر بسته‌شدن دقیقاً الگوی «کمبود ورکر» را کامل می‌کند:
     OrderLog + export file + reporter + پیام Push به کارفرما (_notify_owner)
     + اطلاع ادمین‌ها (broadcast_to_admins) + وضعیت completed/error بر اساس
     تعداد ارسال موفق
  ۵) حذف کامل رفتار قدیمی «ماندن در صف»: رشته‌های "staying pending" و
     مجموعه‌های _logged_pending_orders / _warmup_notified_orders دیگر وجود ندارند
  ۶) تشخیص دلیل (no_worker_reason) در پیام‌ها حضور دارد (پیام شفاف برای کارفرما)
  ۷) کامپایل بدون خطای هر دو فایل تغییریافته

اجرای: python3 scripts/verify_no_pending_fix.py  (از ریشه‌ی پروژه)
"""

import ast
import importlib
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# --- متغیرهای محیطی حداقلی (مثل verify_fixes.py) ---
os.environ.setdefault("FERNET_KEY", "3KJFDBx_iy0GOCV7Zb_qY_WaYPElHz1VpcRrB_r6Nk=")
os.environ.setdefault("DB_PASS", "test")
os.environ.setdefault("API_ID", "12345")
os.environ.setdefault("API_HASH", "testhash")
os.environ.setdefault("BOT_TOKEN", "123:abc")
os.environ.setdefault("ADMIN_ID", "1")

PASS_COUNT = 0
FAIL_COUNT = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS_COUNT, FAIL_COUNT
    mark = "✓" if cond else "✗"
    print(f"   [{mark}] {name}" + (f" — {detail}" if detail else ""))
    if cond:
        PASS_COUNT += 1
    else:
        FAIL_COUNT += 1


ROOT = Path(__file__).resolve().parent.parent
TQ_PATH = ROOT / "workers" / "task_queue.py"
CFG_PATH = ROOT / "config.py"


# ==========================================
# ۱) و ۲) رفتار واقعی config
# ==========================================
print("\n[۱] کانفیگ — کلید INSTANT_CLOSE_ON_NO_WORKER")

# ۱-الف) پیش‌فرض true (بدون ست کردن متغیر محیطی)
for key in ("INSTANT_CLOSE_ON_NO_WORKER",):
    os.environ.pop(key, None)
from config import config as cfg_default

check(
    "پیش‌فرض INSTANT_CLOSE_ON_NO_WORKER=true است",
    getattr(cfg_default, "INSTANT_CLOSE_ON_NO_WORKER", None) is True,
    "سفارش بدون ورکر آماده، همان لحظه بسته می‌شود (خواسته‌ی کارفرما)",
)

# ۱-ب) override از محیط
os.environ["INSTANT_CLOSE_ON_NO_WORKER"] = "false"
import config as config_module

config_module.config = importlib.reload(config_module).config
check(
    "متغیر محیطی INSTANT_CLOSE_ON_NO_WORKER=false اثر می‌گذارد",
    getattr(config_module.config, "INSTANT_CLOSE_ON_NO_WORKER", None) is False,
    "قابلیت بازگشت به رفتار قبلی (ماندن در صف) از .env",
)
os.environ.pop("INSTANT_CLOSE_ON_NO_WORKER", None)
config_module.config = importlib.reload(config_module).config


# ==========================================
# ۳) تا ۶) بررسی ساختاری (AST) روی task_queue.py
# ==========================================
print("\n[۲] ساختار Fail-Fast دیسپچر — workers/task_queue.py")

source = TQ_PATH.read_text(encoding="utf-8")
tree = ast.parse(source)

# --- یافتن تابع order_dispatcher_loop ---
dispatcher_fn = None
for node in ast.walk(tree):
    if isinstance(node, ast.AsyncFunctionDef) and node.name == "order_dispatcher_loop":
        dispatcher_fn = node
        break
check("تابع order_dispatcher_loop پیدا شد", dispatcher_fn is not None)


def _src(node) -> str:
    return ast.get_source_segment(source, node) or ""


def _contains_call(node, func_name: str) -> bool:
    for sub in ast.walk(node):
        if isinstance(sub, ast.Call):
            f = sub.func
            name = ""
            if isinstance(f, ast.Name):
                name = f.id
            elif isinstance(f, ast.Attribute):
                name = f.attr
            if name == func_name:
                return True
    return False


# --- یافتن بلوک if not dispatched_anything and not empty_target_notify_code ---
fail_fast = None
for node in ast.walk(dispatcher_fn):
    if isinstance(node, ast.If) and "not dispatched_anything" in _src(node.test):
        fail_fast = node
        break
check(
    "بلوک Fail-Fast (if not dispatched_anything...) موجود است",
    fail_fast is not None,
)

ff_src = _src(fail_fast) if fail_fast else ""

check(
    "خواندن فلگ INSTANT_CLOSE_ON_NO_WORKER با پیش‌فرض True",
    'getattr(config, "INSTANT_CLOSE_ON_NO_WORKER", True)' in ff_src,
)

# close_now باید مستقیماً از فلگ مقدار بگیرد (رفتار غیرمشروط وقتی فلگ روشن است)
close_now_from_flag = any(
    isinstance(stmt, ast.Assign)
    and any(isinstance(t, ast.Name) and t.id == "close_now" for t in stmt.targets)
    and "INSTANT_CLOSE_ON_NO_WORKER" in _src(stmt)
    for stmt in fail_fast.body
) if fail_fast else False
check(
    "close_now مستقیماً از فلگ مقدار می‌گیرد (بسته‌شدن غیرمشروط وقتی فلگ روشن است)",
    close_now_from_flag,
)

# شرط قدیمی فقط باید داخل شاخه‌ی Legacy (if not close_now) باشد
legacy_if = None
close_if = None
if fail_fast:
    for stmt in fail_fast.body:
        if isinstance(stmt, ast.If) and isinstance(stmt.test, ast.UnaryOp) and isinstance(stmt.test.op, ast.Not) \
                and isinstance(stmt.test.operand, ast.Name) and stmt.test.operand.id == "close_now":
            legacy_if = stmt
        if isinstance(stmt, ast.If) and isinstance(stmt.test, ast.Name) and stmt.test.id == "close_now":
            close_if = stmt

check(
    "رفتار قبلی (busy/warmup wait) فقط زیر شاخه‌ی Legacy (if not close_now) حفظ شده",
    legacy_if is not None and "is_any_worker_busy" in _src(legacy_if)
    and "is_waiting_for_warmup" in _src(legacy_if),
)
check(
    "شاخه‌ی بسته‌شدن (if close_now:) موجود است",
    close_if is not None,
)

close_src = _src(close_if) if close_if else ""
check(
    "وضعیت نهایی: completed اگر ارسال موفق > 0 وگرنه error",
    "OrderStatus.completed if success_count > 0 else OrderStatus.error" in close_src,
)
check(
    "ثبت OrderLog سیستمی برای دلیل بسته‌شدن",
    "OrderLog(" in close_src and "System" in close_src,
)
check(
    "reject_reason با دلیل دقیق (no_worker_reason) ثبت می‌شود",
    "no_worker_reason" in close_src and "reject_reason" in close_src,
)
check(
    "ارسال فایل خروجی به کارفرما (الگوی کمبود ورکر)",
    "_send_export_file" in close_src,
)
check(
    "بروزرسانی داشبورد لایو (reporter.finish / reporter.fail)",
    "reporter.finish" in close_src and "reporter.fail" in close_src,
)
check(
    "🟢 پیام Push مستقیم به کارفرما (_notify_owner) — تکمیل الگوی کمبود ورکر",
    "_notify_owner" in close_src,
)
check(
    "اطلاع‌رسانی به ادمین‌ها (broadcast_to_admins)",
    "broadcast_to_admins" in close_src,
)
check(
    "پیام‌ها دلیل شفاف دارند (بدون ارسال، صراحتاً «هیچ ارسالی انجام نشد»)",
    "هیچ ارسالی انجام نشد" in close_src,
)

# --- بلوک تشخیص دلیل داخل if not eligible_accounts ---
reason_block_ok = False
if dispatcher_fn:
    for node in ast.walk(dispatcher_fn):
        if isinstance(node, ast.Assign):
            for t in node.targets:
                if isinstance(t, ast.Name) and t.id == "no_worker_reason":
                    reason_block_ok = True
check(
    "تشخیص دلیل نبود ورکر (no_worker_reason) برای پیام کارفرما",
    reason_block_ok,
)

# --- ۵) حذف رفتار قدیمی ---
check(
    "رشته‌ی 'staying pending' کاملاً حذف شده است",
    "staying pending" not in source,
)
check(
    "مجموعه‌ی _logged_pending_orders کاملاً حذف شده است",
    "_logged_pending_orders" not in source,
)
check(
    "مجموعه‌ی _warmup_notified_orders کاملاً حذف شده است",
    "_warmup_notified_orders" not in source,
)
check(
    "پیام قدیمی «این سفارش در صف انتظار باقی می‌ماند» حذف شده است",
    "در صف انتظار باقی می‌ماند" not in source,
)

# مسیرهای عمدیِ pending که نباید دست بخورند، همچنان سر جایشان هستند
for must_keep in (
    "Order.status == OrderStatus.pending",      # کوئری دیسپچر
    "status=OrderStatus.pending",               # ریست زامبی/ریتِری
    "on_hold_proxy",                            # هولد پروکسی دست‌نخورده
):
    check(f"حفظ شده: {must_keep}", must_keep in source)

# ==========================================
# ۷) کامپایل
# ==========================================
print("\n[۳] کامپایل")
import py_compile

for path in (TQ_PATH, CFG_PATH):
    ok = True
    try:
        py_compile.compile(str(path), doraise=True)
    except Exception as e:  # pragma: no cover
        ok = False
        print(f"      {e}")
    check(f"py_compile: {path.relative_to(ROOT)}", ok)

# ==========================================
print("\n" + "=" * 50)
print(f"نتیجه: {PASS_COUNT} پاس / {FAIL_COUNT} خطا")
sys.exit(1 if FAIL_COUNT else 0)
