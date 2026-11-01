# utils/send_stats.py
"""
📤 (خواسته کارفرما) آمار ارسال ورکرها — بر پایه واقعیت جدول order_logs
=====================================================================
کاربر باید بتواند ببیند هر ورکر (اکانت) چند «ارسال» واقعی زده است.

تعریف «یک ارسال» دقیقاً منطبق بر شمارنده‌های داخلی سیستم است
(workers/sender.py — در هر جایی که sent_in_chunk += 1 و
incr_hourly_sent_count صدا زده می‌شود):
    • status = 'success'  → ارسال کامل (همه استیج‌ها تحویل شد)
    • status = 'partial'  → استیج اول تحویل شد اما ادامه‌اش خطا خورد
                            (در سیستم به‌عنوان یک ارسالِ ناقص شمرده می‌شود)

خطاها ('error' / 'flood' / 'restricted') هرگز جزو ارسال‌ها نیستند و
'info' صرفاً رویداد سیستمی است.

منبع داده: جدول order_logs (دیتای ماندگار و قابل بازسازی) —
شمارنده‌های Redis (ساعتی/روزانه) گذرا هستند و بعد از ری‌استارت
یا انقضای TTL صفر می‌شوند؛ به همین دلیل مبنا اینجا OrderLog است.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, Optional

from sqlalchemy import case, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from database.models import Account, OrderLog

logger = logging.getLogger(__name__)

# وضعیت‌هایی که «یک ارسال» حساب می‌شوند (منطبق بر شمارنده‌های داخلی sender.py)
SENT_STATUSES: tuple = ("success", "partial")

# وضعیت‌های شکست (برای نمایش جداگانه)
ERROR_STATUSES: tuple = ("error", "flood", "restricted")

# شمارش ردیف‌های نمایش در نمای رتبه‌بندی
RANKING_TOP_N = 15


def _utc_naive(dt: Optional[datetime]) -> Optional[datetime]:
    """نرمال‌سازی به naive-UTC — MySQL DATETIME بدون tz برمی‌گردد؛ سیستم همه‌جا UTC ذخیره می‌کند."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


async def get_accounts_send_totals(
    session: AsyncSession,
    account_ids: Optional[Iterable[int]] = None,
) -> Dict[int, Dict[str, int]]:
    """
    شمارش یک‌جای ارسال‌های اکانت‌ها (همه، یا مجموعه‌ای خاص) با یک کوئری GROUP BY.

    خروجی: {account_id: {"success": n, "partial": m, "total": n+m}}
    اکانت‌هایی که هیچ لاگی ندارند در خروجی نیستند → caller باید با
    totals.get(acc_id) یا مقادیر صفر بخواند.
    """
    try:
        stmt = (
            select(
                OrderLog.account_id,
                func.sum(case((OrderLog.status == "success", 1), else_=0)),
                func.sum(case((OrderLog.status == "partial", 1), else_=0)),
            )
            .where(
                OrderLog.status.in_(SENT_STATUSES),
                OrderLog.account_id.is_not(None),
            )
            .group_by(OrderLog.account_id)
        )
        if account_ids is not None:
            ids = list(account_ids)
            if not ids:
                return {}
            stmt = stmt.where(OrderLog.account_id.in_(ids))

        rows = (await session.execute(stmt)).all()

        result: Dict[int, Dict[str, int]] = {}
        for acc_id, ok_cnt, partial_cnt in rows:
            ok = int(ok_cnt or 0)
            partial = int(partial_cnt or 0)
            result[int(acc_id)] = {"success": ok, "partial": partial, "total": ok + partial}
        return result
    except Exception as e:
        logger.warning(f"send_stats: get_accounts_send_totals failed ({e}); returning empty.")
        return {}


async def get_account_send_stats(session: AsyncSession, account_id: int) -> Dict:
    """
    آمار تفصیلی ارسال‌های یک اکانت خاص:
      all_time     → {"success", "partial", "total"}
      last_24h     → {"success", "partial", "total"}
      errors       → تعداد کل خطاهای ارسال (error/flood/restricted)
      last_send_at → datetime|None (آخرین ارسال کامل/ناقص)
    هر خطا باعث نمی‌شود نمای والد کرش کند؛ مقادیر صفر برمی‌گردند.
    """
    empty = {
        "all_time": {"success": 0, "partial": 0, "total": 0},
        "last_24h": {"success": 0, "partial": 0, "total": 0},
        "errors": 0,
        "last_send_at": None,
    }
    try:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        day_ago = now - timedelta(hours=24)
        is_sent = OrderLog.status.in_(SENT_STATUSES)

        # کل دوران + ۲۴ ساعت اخیر در یک اجرا (دو کوئری سبک)
        stmt_all = select(
            func.sum(case((OrderLog.status == "success", 1), else_=0)),
            func.sum(case((OrderLog.status == "partial", 1), else_=0)),
            func.max(OrderLog.created_at),
        ).where(is_sent, OrderLog.account_id == account_id)
        row_all = (await session.execute(stmt_all)).one()

        stmt_24h = select(
            func.sum(case((OrderLog.status == "success", 1), else_=0)),
            func.sum(case((OrderLog.status == "partial", 1), else_=0)),
        ).where(is_sent, OrderLog.account_id == account_id, OrderLog.created_at >= day_ago)
        row_24h = (await session.execute(stmt_24h)).one()

        err_cnt = await session.scalar(
            select(func.count(OrderLog.id)).where(
                OrderLog.account_id == account_id,
                OrderLog.status.in_(ERROR_STATUSES),
            )
        ) or 0

        ok_all = int(row_all[0] or 0)
        partial_all = int(row_all[1] or 0)
        ok_day = int(row_24h[0] or 0)
        partial_day = int(row_24h[1] or 0)

        return {
            "all_time": {"success": ok_all, "partial": partial_all, "total": ok_all + partial_all},
            "last_24h": {"success": ok_day, "partial": partial_day, "total": ok_day + partial_day},
            "errors": int(err_cnt),
            "last_send_at": _utc_naive(row_all[2]),
        }
    except Exception as e:
        logger.warning(f"send_stats: get_account_send_stats failed for user_{account_id}/ ({e}).")
        return empty


def format_send_count_line(totals: Optional[Dict[str, int]]) -> str:
    """
    خطِ فشردهٔ «📤 ارسال‌ها» برای لیست‌ها:
        📤 ارسال‌ها: ۱۲۳ (کامل ۱۰۰ | ناقص ۲۳)
    """
    if not totals:
        totals = {}
    total = int(totals.get("total", 0) or 0)
    ok = int(totals.get("success", 0) or 0)
    partial = int(totals.get("partial", 0) or 0)
    if total <= 0:
        return "📤 ارسال‌ها: ۰"
    return f"📤 ارسال‌ها: {total:,} (کامل {ok:,} | ناقص {partial:,})"


async def build_worker_send_ranking_view(session: AsyncSession) -> str:
    """
    🏆 نمای رتبه‌بندی ورکرها بر اساس تعداد ارسال —
    «هر ورکر چند تا ارسال زده» در یک نگاه، به‌همراه جمع کل سیستم.
    مرتب‌سازی: بیشترین ارسال کل؛ اکانت‌های بدون لاگ در انتها (صفر).
    """
    try:
        accounts = (await session.scalars(select(Account).order_by(Account.id.asc()))).all()
    except Exception as e:
        logger.warning(f"send_stats: ranking could not load accounts ({e}).")
        await session.rollback()
        accounts = []

    totals = await get_accounts_send_totals(session)

    rows = []
    for acc in accounts:
        t = totals.get(acc.id) or {"success": 0, "partial": 0, "total": 0}
        rows.append((acc, t))
    rows.sort(key=lambda x: x[1]["total"], reverse=True)

    grand_total = sum(t["total"] for _, t in rows)
    grand_ok = sum(t["success"] for _, t in rows)
    grand_partial = sum(t["partial"] for _, t in rows)

    text = (
        "🏆 <b>آمار ارسال ورکرها</b>\n"
        "«هر ورکر چند پیام واقعاً ارسال کرده» — مبنا: لاگ واقعی سفارش‌ها\n\n"
        f"Σ مجموع کل سیستم: <b>{grand_total:,}</b> ارسال "
        f"(کامل {grand_ok:,} | ناقص {grand_partial:,})\n"
        f"🤖 تعداد ورکرها: <b>{len(rows)}</b>\n\n"
    )

    if not rows:
        text += "⚠️ هنوز هیچ ورکری ثبت نشده است."
        return text

    medals = ("🥇", "🥈", "🥉")
    for idx, (acc, t) in enumerate(rows[:RANKING_TOP_N]):
        rank = medals[idx] if idx < 3 else f"{idx + 1}."
        phone = str(acc.phone_number) if acc.phone_number else f"ID-{acc.id}"
        if t["total"] > 0:
            text += (
                f"{rank} <code>{phone}</code>\n"
                f"   └ 📤 {t['total']:,} ارسال (کامل {t['success']:,} | ناقص {t['partial']:,}) — 🔸 /status_{acc.id}\n"
            )
        else:
            text += (
                f"{rank} <code>{phone}</code>\n"
                f"   └ 📤 بدون ارسال — 🔸 /status_{acc.id}\n"
            )

    if len(rows) > RANKING_TOP_N:
        text += f"\n… و {len(rows) - RANKING_TOP_N} ورکر دیگر (جزئیات هر ورکر: /status_آیدی)\n"

    return text
