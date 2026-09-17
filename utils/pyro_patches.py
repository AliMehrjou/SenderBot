"""
Runtime patches for pyrofork==2.2.0 (do NOT modify site-packages).

WHY: pyrogram/utils.py get_peer_type() uses MIN_CHANNEL_ID =
-1002147483647 (legacy int32 bound). Telegram now issues 64-bit channel
ids; e.g. raw id 2870780413 -> marked id -1002870780413 which falls outside
the recognized range -> ValueError("Peer id invalid") inside a
fire-and-forget update task -> whole update batch silently dropped.
Import this module ONCE at process startup, BEFORE any Client is created.
"""
import pyrogram.utils as _utils

# New floor for marked channel ids: -1009999999999999
# (marked = -1000000000000 - raw_id; raw ids stay far below 1e12).
PATCHED_MIN_CHANNEL_ID = -1009999999999999


def _patched_get_peer_type(peer_id: int) -> str:
    if peer_id < 0:
        if _utils.MIN_CHAT_ID <= peer_id:
            return "chat"
        if PATCHED_MIN_CHANNEL_ID <= peer_id < _utils.MAX_CHANNEL_ID:
            return "channel"
    elif 0 < peer_id <= _utils.MAX_USER_ID:
        return "user"
    raise ValueError(f"Peer id invalid: {peer_id}")


def apply_pyrogram_patches() -> None:
    # get_peer_type() reads module globals at call time, and every pyrofork
    # call site does `from pyrogram import utils` (attribute access), so
    # patching the module is sufficient:
    _utils.MIN_CHANNEL_ID = PATCHED_MIN_CHANNEL_ID
    _utils.get_peer_type = _patched_get_peer_type


apply_pyrogram_patches()