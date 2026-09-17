# utils/progress_reporter.py
"""
Phase 5 — live, throttled, exception-safe progress reporting for jobs.

Design contract:
- ONE Telegram message per (task_type, task_id). The message id and the last
  rendered fields live in Redis (48h TTL) so a restarted process re-attaches
  to the same message instead of spamming a new one.
- Every public method is fully guarded (try/except + warning log): a reporting
  failure (Redis down, message deleted, network error…) must NEVER propagate
  into the extraction/send job flow.
- Edits are throttled by config.PROGRESS_EDIT_INTERVAL_SECONDS (default 10s).
  If editing fails twice in a row (message deleted / too old) the reporter
  falls back to sending a fresh message and keeps tracking that one.
"""
from __future__ import annotations

import asyncio
import html
import logging
import os
import time
from typing import Dict, Optional

from aiogram import Bot
from aiogram.exceptions import TelegramBadRequest
from aiogram.types import FSInputFile, LinkPreviewOptions

from config import config
# Shared edit-error classification (single source of truth with the panel UI)
from utils.safe_edit import is_message_gone_error, is_not_modified_error

logger = logging.getLogger(__name__)

# Whitelisted update() fields — anything else is ignored.
_FIELD_KEYS = ("done", "total", "speed", "eta", "status", "account", "extra")

# Redis state TTL (spec: 48h).
_STATE_TTL_SECONDS = 48 * 3600

_BAR_LENGTH = 10

# User-facing fallback labels (Persian).
_STATUS_DEFAULT = "در حال اجرا…"
_STATUS_PREPARING = "در حال آماده‌سازی و انتخاب اکانت…"
_STATUS_RESUMED = "ادامهٔ کار پس از راه‌اندازی مجدد…"


def _edit_interval() -> float:
    try:
        return max(1.0, float(getattr(config, "PROGRESS_EDIT_INTERVAL_SECONDS", 10.0)))
    except (TypeError, ValueError):
        return 10.0


def _get_redis_client():
    """Reuse the shared Redis client from workers.sender (lazy import avoids import cycles)."""
    try:
        from workers.sender import _get_redis
        return _get_redis()
    except Exception as e:
        logger.warning(f"ProgressReporter: Redis client unavailable: {e}")
        return None


def _s(value) -> str:
    """Decode bytes -> str (Redis returns either depending on decode_responses)."""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value if isinstance(value, str) else str(value)


def _to_float(value) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _fmt_int(value: Optional[float]) -> str:
    if value is None:
        return "—"
    return f"{int(value):,}"


def _fmt_eta_seconds(seconds: float) -> str:
    s = int(max(0, seconds))
    if s < 60:
        return f"{s} ثانیه"
    minutes, sec = divmod(s, 60)
    if minutes < 60:
        base = f"{minutes} دقیقه"
        return base + (f" و {sec} ثانیه" if sec else "")
    hours, minutes = divmod(minutes, 60)
    if hours < 24:
        base = f"{hours} ساعت"
        return base + (f" و {minutes} دقیقه" if minutes else "")
    days, hours = divmod(hours, 24)
    base = f"{days} روز"
    return base + (f" و {hours} ساعت" if hours else "")


def _fmt_speed(value) -> str:
    v = _to_float(value)
    if v is not None:
        return f"{v:g} در دقیقه"
    if isinstance(value, str) and value.strip():
        return value.strip()
    return "—"


def _fmt_eta(value) -> str:
    v = _to_float(value)
    if v is not None:
        return _fmt_eta_seconds(v)
    if isinstance(value, str) and value.strip():
        return value.strip()
    return "—"


class ProgressReporter:
    """
    Live, throttled, exception-safe progress reporter that edits ONE message
    per task. State (message_id, last_payload) is kept in Redis so a restarted
    process re-attaches to the same message instead of spamming a new one.
    NEVER raises into the job flow — every public method is fully guarded
    (try/except + log).
    """

    def __init__(self, bot: Bot, chat_id: int, task_type: str, task_id: int, title: str):
        self.bot = bot
        self.chat_id = int(chat_id)
        self.task_type = str(task_type)
        self.task_id = int(task_id)
        self.title = str(title or "")
        self._msg_key = f"progress:msg:{self.task_type}:{self.task_id}"
        self._data_key = f"progress:data:{self.task_type}:{self.task_id}"
        # In-memory mirrors of the Redis state (Redis stays the source of truth
        # so restarts can re-attach; memory is the fallback when Redis is down).
        self._message_id: Optional[int] = None
        self._fields: Dict[str, str] = {}
        self._last_render_ts: float = 0.0
        self._edit_failures: int = 0
        self._speed_base = (None, 0.0)  # (baseline_done, baseline_ts)
        self._lock = asyncio.Lock()
        self._finished = False
        self._ever_updated = False

    # ------------------------------------------------------------------
    # Public API — all guarded, must never raise into the job flow
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Send (or re-attach via Redis key progress:msg:{task_type}:{task_id})
        the initial Persian status message and store its message_id."""
        try:
            async with self._lock:
                await self._load_state()
                if self._message_id is not None:
                    # Re-attach: process restart or a later chunk of the same task.
                    text = self._render(status_override=_STATUS_RESUMED)
                    await self._edit_or_send(text)
                else:
                    self._fields["status"] = _STATUS_PREPARING
                    text = (
                        "📊 <b>گزارش پیشرفت زنده</b>\n"
                        f"🧾 <b>سفارش:</b> {html.escape(self.title)}\n\n"
                        f"⏳ {_STATUS_PREPARING}"
                    )
                    await self._send_new(text)
                self._last_render_ts = time.time()
                self._fields["ts"] = f"{self._last_render_ts:.3f}"
                await self._persist()
        except Exception as e:
            logger.warning(f"ProgressReporter.start failed for {self.task_type}#{self.task_id}: {e}")

    async def update(self, **fields) -> None:
        """
        fields: done, total, speed, eta, status, account, extra.
        Throttled edit (PROGRESS_EDIT_INTERVAL_SECONDS); falls back to sending
        a new message if editing fails twice (deleted/too old).
        """
        try:
            if self._finished:
                return
            async with self._lock:
                now = time.time()
                # The first update after start() is always rendered so the user
                # sees live progress immediately (later calls are throttled).
                force_render = not self._ever_updated
                self._ever_updated = True

                had_speed = "speed" in fields and fields["speed"] is not None
                had_eta = "eta" in fields and fields["eta"] is not None

                for key in _FIELD_KEYS:
                    if key in fields and fields[key] is not None:
                        self._fields[key] = self._stringify(fields[key])

                self._derive_speed_and_eta(now, derive_speed=not had_speed, derive_eta=not had_eta)

                done_val = _to_float(self._fields.get("done"))
                total_val = _to_float(self._fields.get("total"))
                current_pct = 0.0
                if done_val is not None and total_val is not None and total_val > 0:
                    current_pct = (done_val / total_val) * 100.0

                last_pct = getattr(self, '_last_rendered_pct', 0.0)
                is_significant_jump = (current_pct - last_pct) >= 5.0
                is_completed = current_pct >= 100.0

                if not force_render and not is_significant_jump and not is_completed and (now - self._last_render_ts) < 4.0:
                    # Throttled: persist the latest fields so the next render
                    # (or a restarted process) continues from the newest state.
                    await self._persist()
                    return

                self._last_rendered_pct = current_pct

                text = self._render()
                await self._edit_or_send(text)
                self._last_render_ts = now
                self._fields["ts"] = f"{now:.3f}"
                await self._persist()
        except Exception as e:
            logger.warning(f"ProgressReporter.update failed for {self.task_type}#{self.task_id}: {e}")

    async def milestone(self, pct: float) -> None:
        """Force an immediate (unthrottled) progress edit at a milestone percent."""
        try:
            if self._finished:
                return
            try:
                pct = max(0.0, min(100.0, float(pct)))
            except (TypeError, ValueError):
                return
            async with self._lock:
                text = self._render(pct_override=pct)
                await self._edit_or_send(text)
                self._last_render_ts = time.time()
                self._fields["ts"] = f"{self._last_render_ts:.3f}"
                await self._persist()
        except Exception as e:
            logger.warning(f"ProgressReporter.milestone failed for {self.task_type}#{self.task_id}: {e}")

    async def finish(self, summary_html: str, document_path: str | None = None) -> None:
        """Final success message (+ optional result document) and state cleanup."""
        try:
            self._finished = True
            async with self._lock:
                text = self._render_final(summary_html)
                if document_path and os.path.exists(document_path):
                    # Telegram caption hard limit is 1024 — keep it short if needed.
                    caption = (
                        summary_html
                        if len(summary_html) <= 900
                        else "✅ عملیات تکمیل شد — فایل نتیجه پیوست شد."
                    )
                    try:
                        await self.bot.send_document(
                            chat_id=self.chat_id,
                            document=FSInputFile(document_path),
                            caption=caption,
                            parse_mode="HTML",
                        )
                    except Exception as e:
                        logger.warning(
                            f"ProgressReporter: result file delivery failed for "
                            f"{self.task_type}#{self.task_id}: {e}"
                        )
                if self._message_id is not None:
                    await self._edit_or_send(text)
                else:
                    await self._send_new(text)
                await self._clear_state()
        except Exception as e:
            logger.warning(f"ProgressReporter.finish failed for {self.task_type}#{self.task_id}: {e}")

    async def fail(self, error_html: str) -> None:
        """Final failure message and state cleanup."""
        try:
            self._finished = True
            async with self._lock:
                text = self._render_final(error_html)
                if self._message_id is not None:
                    await self._edit_or_send(text)
                else:
                    await self._send_new(text)
                await self._clear_state()
        except Exception as e:
            logger.warning(f"ProgressReporter.fail failed for {self.task_type}#{self.task_id}: {e}")

    # ------------------------------------------------------------------
    # Rendering (Persian, HTML, consistent with the project's emoji style)
    # ------------------------------------------------------------------

    def _render(self, pct_override: Optional[float] = None, status_override: Optional[str] = None) -> str:
        done = _to_float(self._fields.get("done"))
        total = _to_float(self._fields.get("total"))
        status = status_override or self._fields.get("status") or _STATUS_DEFAULT
        account = self._fields.get("account")
        extra = self._fields.get("extra")

        pct: Optional[float] = pct_override
        if pct is None and done is not None and total is not None and total > 0:
            pct = min(100.0, max(0.0, done / total * 100.0))

        lines = [
            "📊 <b>گزارش پیشرفت زنده</b>",
            f"🧾 <b>سفارش:</b> {html.escape(self.title)}",
            "",
        ]
        if pct is not None:
            lines.append(self._bar(pct))
            lines.append("")

        progress_line = f"📈 <b>پیشرفت:</b> {_fmt_int(done)}"
        if total is not None and total > 0:
            progress_line += f" از {_fmt_int(total)}"
        lines.append(progress_line)
        lines.append(f"⚡ <b>سرعت:</b> {_fmt_speed(self._fields.get('speed'))}")
        lines.append(f"⏳ <b>زمان باقی‌مانده:</b> {_fmt_eta(self._fields.get('eta'))}")
        lines.append(f"⚙️ <b>وضعیت:</b> {html.escape(status)}")
        if account:
            lines.append(f"👤 <b>اکانت:</b> {html.escape(account)}")
        if extra:
            lines.append(f"📎 <b>جزئیات:</b> {html.escape(extra)}")
        return "\n".join(lines)

    @staticmethod
    def _bar(pct: float) -> str:
        filled = int(round(max(0.0, min(100.0, pct)) / 100.0 * _BAR_LENGTH))
        filled = max(0, min(_BAR_LENGTH, filled))
        return "▓" * filled + "░" * (_BAR_LENGTH - filled) + f" {int(round(pct))}٪"

    def _render_final(self, body_html: str) -> str:
        return (
            "📊 <b>گزارش پیشرفت زنده</b>\n"
            f"🧾 <b>سفارش:</b> {html.escape(self.title)}\n\n"
            f"{body_html}"
        )

    def _derive_speed_and_eta(self, now: float, derive_speed: bool, derive_eta: bool) -> None:
        """
        When the caller did not pass speed/eta explicitly, derive them from the
        monotonic growth of `done` (auto-resets when a new chunk restarts the
        counter). Results are stored back into the field map so renders stay pure.
        """
        done = _to_float(self._fields.get("done"))
        if done is None:
            return
        base_done, base_ts = self._speed_base
        if base_done is None or done < base_done or base_ts <= 0:
            # New baseline (first sample or a chunk counter reset).
            self._speed_base = (done, now)
            if derive_speed:
                self._fields.pop("speed", None)
            if derive_eta and not self._fields.get("speed"):
                self._fields.pop("eta", None)
            return
        elapsed_min = max((now - base_ts) / 60.0, 1e-9)
        speed = (done - base_done) / elapsed_min
        if derive_speed:
            if speed > 0:
                self._fields["speed"] = f"{speed:.1f}"
        else:
            speed = _to_float(self._fields.get("speed")) or 0.0
        if derive_eta:
            total = _to_float(self._fields.get("total"))
            if speed > 0 and total is not None and total > done:
                self._fields["eta"] = str(int((total - done) / speed * 60))
            else:
                self._fields.pop("eta", None)

    # ------------------------------------------------------------------
    # Telegram I/O — mirrors utils/safe_edit.py semantics. The helpers there
    # operate on live Message objects, while this reporter only holds a
    # (chat_id, message_id) pair that must survive process restarts, so the
    # shared error-classification helpers are reused instead.
    # ------------------------------------------------------------------

    async def _edit_or_send(self, text: str) -> None:
        if self._message_id is None:
            await self._send_new(text)
            return
        try:
            await self.bot.edit_message_text(
                chat_id=self.chat_id,
                message_id=self._message_id,
                text=text,
                parse_mode="HTML",
                link_preview_options=LinkPreviewOptions(is_disabled=True),
            )
            self._edit_failures = 0
        except TelegramBadRequest as e:
            if is_not_modified_error(e):
                # Identical content — nothing to change, not a failure.
                self._edit_failures = 0
                return
            self._edit_failures += 1
            reason = "message deleted/too old" if is_message_gone_error(e) else "bad request"
            logger.warning(
                f"ProgressReporter: edit failed ({reason}: {e}); "
                f"attempt {self._edit_failures}/2."
            )
            if self._edit_failures >= 2:
                logger.warning(
                    f"ProgressReporter: editing message {self._message_id} failed twice; "
                    f"falling back to a new message."
                )
                await self._send_new(text)
        except Exception as e:
            logger.warning(f"ProgressReporter: edit_message_text error: {e}")

    async def _send_new(self, text: str) -> None:
        try:
            msg = await self.bot.send_message(
                chat_id=self.chat_id,
                text=text,
                parse_mode="HTML",
                link_preview_options=LinkPreviewOptions(is_disabled=True),
            )
            self._message_id = msg.message_id
            self._edit_failures = 0
            await self._store_msg_id()
        except Exception as e:
            logger.warning(f"ProgressReporter: send_message failed: {e}")

    # ------------------------------------------------------------------
    # Redis state (best-effort: Redis being down only degrades re-attach)
    # ------------------------------------------------------------------

    async def _load_state(self) -> None:
        redis = _get_redis_client()
        if redis is None:
            return
        try:
            raw_msg_id = await redis.get(self._msg_key)
            if raw_msg_id is not None:
                self._message_id = int(_s(raw_msg_id))
            data = await redis.hgetall(self._data_key)
            if data:
                self._fields = {_s(k): _s(v) for k, v in data.items()}
            ts = _to_float(self._fields.get("ts"))
            if ts is not None:
                # Clamp against clock skew so throttling never blocks forever.
                self._last_render_ts = min(ts, time.time())
        except Exception as e:
            logger.warning(f"ProgressReporter: state load failed: {e}")

    async def _store_msg_id(self) -> None:
        if self._message_id is None:
            return
        redis = _get_redis_client()
        if redis is None:
            return
        try:
            await redis.set(self._msg_key, str(self._message_id), ex=_STATE_TTL_SECONDS)
        except Exception as e:
            logger.warning(f"ProgressReporter: message-id store failed: {e}")

    async def _persist(self) -> None:
        redis = _get_redis_client()
        if redis is None:
            return
        try:
            mapping = dict(self._fields)
            mapping["ts"] = f"{time.time():.3f}"
            await redis.hset(self._data_key, mapping=mapping)
            await redis.expire(self._data_key, _STATE_TTL_SECONDS)
        except Exception as e:
            logger.warning(f"ProgressReporter: state persist failed: {e}")

    async def _clear_state(self) -> None:
        redis = _get_redis_client()
        if redis is None:
            return
        try:
            await redis.delete(self._msg_key, self._data_key)
        except Exception as e:
            logger.warning(f"ProgressReporter: state clear failed: {e}")

    @staticmethod
    def _stringify(value) -> str:
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, float):
            return f"{value:g}"
        return str(value)