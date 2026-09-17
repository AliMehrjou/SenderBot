import asyncio
from dataclasses import dataclass
from typing import Tuple, Optional
from database.engine import async_session
from database.models import GlobalSettings
from sqlalchemy import select

@dataclass
class SpeedProfile:
    name: str
    pre_join_sleep: Tuple[float, float]
    join_pause: Tuple[float, float]
    iter_pause: Tuple[float, float]
    approval_cycle_seconds: int
    approval_retry_limit: int
    dispatch_batch: int
    dispatch_interval_seconds: float
    extract_parallel_min_members: int
    floodwait_padding_join: Tuple[float, float]
    floodwait_padding_iter: Tuple[float, float]
    estimate_on_critical_path: bool
    rejoin_on_redispatch: bool
    adaptive_penalty_factor: float
    join_stagger_seconds: Tuple[float, float]
    join_rate_cap_per_hour: int
    leave_dwell_hours: Tuple[float, float]

SAFE_PROFILE = SpeedProfile(
    name="safe",
    pre_join_sleep=(2.0, 5.0),
    join_pause=(0.6, 1.2),
    iter_pause=(0.4, 0.9),
    approval_cycle_seconds=360,
    approval_retry_limit=60,
    dispatch_batch=3,
    dispatch_interval_seconds=1.0,
    extract_parallel_min_members=3000,
    floodwait_padding_join=(1.0, 3.0),
    floodwait_padding_iter=(2.0, 5.0),
    estimate_on_critical_path=True,
    rejoin_on_redispatch=True,
    adaptive_penalty_factor=1.0,
    join_stagger_seconds=(0.0, 0.0),
    join_rate_cap_per_hour=999999,
    leave_dwell_hours=(0.0, 0.0)
)

FAST_PROFILE = SpeedProfile(
    name="fast",
    pre_join_sleep=(0.8, 1.5),
    join_pause=(0.4, 0.8),
    iter_pause=(0.25, 0.5),
    approval_cycle_seconds=60,
    approval_retry_limit=25,
    dispatch_batch=6,
    dispatch_interval_seconds=1.0,
    extract_parallel_min_members=1500,
    floodwait_padding_join=(1.0, 3.0),
    floodwait_padding_iter=(2.0, 5.0),
    estimate_on_critical_path=False,
    rejoin_on_redispatch=False,
    adaptive_penalty_factor=1.0,
    join_stagger_seconds=(3.0, 5.0),
    join_rate_cap_per_hour=4,
    leave_dwell_hours=(1.0, 3.0)
)

TURBO_PROFILE = SpeedProfile(
    name="turbo",
    pre_join_sleep=(0.3, 0.8),
    join_pause=(0.1, 0.3),
    iter_pause=(0.15, 0.35),
    approval_cycle_seconds=18,
    approval_retry_limit=30,
    dispatch_batch=8,
    dispatch_interval_seconds=0.7,
    extract_parallel_min_members=0,
    floodwait_padding_join=(0.5, 1.0),
    floodwait_padding_iter=(1.0, 2.0),
    estimate_on_critical_path=False,
    rejoin_on_redispatch=False,
    adaptive_penalty_factor=1.0,
    join_stagger_seconds=(2.0, 4.0),
    join_rate_cap_per_hour=5,
    leave_dwell_hours=(1.0, 6.0)
)

PROFILES = {
    "safe": SAFE_PROFILE,
    "fast": FAST_PROFILE,
    "turbo": TURBO_PROFILE
}

async def get_speed_profile(mode_override: Optional[str] = None) -> SpeedProfile:
    """دریافت پروفایل با کش Redis. اگر اورراید داده شود مستقیماً برمی‌گردد."""
    if mode_override and mode_override in PROFILES:
        return PROFILES[mode_override]
    
    try:
        from workers.sender import _get_redis
        redis = _get_redis()
        cached = await redis.get("settings:speed_mode")
        if cached:
            mode = cached.decode("utf-8") if isinstance(cached, bytes) else cached
            return PROFILES.get(mode, SAFE_PROFILE)
            
        async with async_session() as session:
            settings = (await session.scalars(select(GlobalSettings).limit(1))).first()
            mode = settings.extraction_speed_mode if settings else "safe"
            
            await redis.set("settings:speed_mode", mode, ex=30)
            return PROFILES.get(mode, SAFE_PROFILE)
    except Exception:
        return SAFE_PROFILE