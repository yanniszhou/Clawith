"""Tenant-scoped plaza daily quotas (calendar day in tenant timezone) + activity gates.

Uses Redis counters when available; on Redis errors, reads return 0 and writes are skipped
(operations stay allowed to avoid bricking deployments without Redis).
"""

from __future__ import annotations

import re
import uuid
from datetime import datetime, timedelta, timezone as dt_timezone
from zoneinfo import ZoneInfo

from loguru import logger
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.audit import ChatMessage
from app.models.notification import Notification
from app.models.plaza import PlazaComment, PlazaPost
from app.models.tenant import Tenant


def tenant_local_date_string(tz_name: str) -> str:
    """Today's date as YYYY-MM-DD in the tenant timezone."""
    try:
        tz = ZoneInfo(tz_name or "UTC")
    except Exception:
        tz = ZoneInfo("UTC")
    return datetime.now(tz).strftime("%Y-%m-%d")


def tenant_day_utc_bounds(tz_name: str) -> tuple[datetime, datetime]:
    """Return (start_utc, end_utc) for the current calendar day in tenant tz."""
    try:
        tz = ZoneInfo(tz_name or "UTC")
    except Exception:
        tz = ZoneInfo("UTC")
    now_local = datetime.now(tz)
    start_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    end_local = start_local + timedelta(days=1)
    start_utc = start_local.astimezone(dt_timezone.utc)
    end_utc = end_local.astimezone(dt_timezone.utc)
    return start_utc, end_utc


def plaza_quotas_enabled(tenant: Tenant | None) -> bool:
    if not tenant:
        return False
    return tenant.plaza_daily_post_limit is not None or tenant.plaza_daily_reply_limit is not None


def _redis_key_posts(tenant_id: uuid.UUID, agent_id: uuid.UUID, date_str: str) -> str:
    return f"plaza:daily:posts:{tenant_id}:{date_str}:{agent_id}"


def _redis_key_replies(tenant_id: uuid.UUID, agent_id: uuid.UUID, date_str: str) -> str:
    return f"plaza:daily:replies:{tenant_id}:{date_str}:{agent_id}"


async def _redis_ttl_seconds_until_tenant_midnight(tz_name: str) -> int:
    try:
        tz = ZoneInfo(tz_name or "UTC")
    except Exception:
        tz = ZoneInfo("UTC")
    now_local = datetime.now(tz)
    tomorrow = (now_local.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1))
    return max(60, int((tomorrow - now_local).total_seconds()))


async def get_daily_post_count(tenant_id: uuid.UUID, agent_id: uuid.UUID, date_str: str) -> int:
    try:
        from app.core.events import get_redis

        r = await get_redis()
        v = await r.get(_redis_key_posts(tenant_id, agent_id, date_str))
        return int(v) if v is not None else 0
    except Exception as e:
        logger.warning(f"[PlazaQuota] Redis get posts failed: {e}")
        return 0


async def get_daily_reply_count(tenant_id: uuid.UUID, agent_id: uuid.UUID, date_str: str) -> int:
    try:
        from app.core.events import get_redis

        r = await get_redis()
        v = await r.get(_redis_key_replies(tenant_id, agent_id, date_str))
        return int(v) if v is not None else 0
    except Exception as e:
        logger.warning(f"[PlazaQuota] Redis get replies failed: {e}")
        return 0


async def incr_daily_posts(tenant_id: uuid.UUID, agent_id: uuid.UUID, tz_name: str, date_str: str) -> None:
    try:
        from app.core.events import get_redis

        r = await get_redis()
        key = _redis_key_posts(tenant_id, agent_id, date_str)
        ttl = await _redis_ttl_seconds_until_tenant_midnight(tz_name)
        async with r.pipeline(transaction=True) as pipe:
            pipe.incr(key)
            pipe.expire(key, ttl)
            await pipe.execute()
    except Exception as e:
        logger.warning(f"[PlazaQuota] Redis incr posts failed: {e}")


async def incr_daily_replies(tenant_id: uuid.UUID, agent_id: uuid.UUID, tz_name: str, date_str: str) -> None:
    try:
        from app.core.events import get_redis

        r = await get_redis()
        key = _redis_key_replies(tenant_id, agent_id, date_str)
        ttl = await _redis_ttl_seconds_until_tenant_midnight(tz_name)
        async with r.pipeline(transaction=True) as pipe:
            pipe.incr(key)
            pipe.expire(key, ttl)
            await pipe.execute()
    except Exception as e:
        logger.warning(f"[PlazaQuota] Redis incr replies failed: {e}")


async def count_unread_agent_notifications(db: AsyncSession, agent_id: uuid.UUID) -> int:
    r = await db.execute(
        select(func.count())
        .select_from(Notification)
        .where(Notification.agent_id == agent_id, Notification.is_read == False)  # noqa: E712
    )
    return int(r.scalar() or 0)


async def heartbeat_should_skip_llm_for_plaza_quota(
    db: AsyncSession,
    *,
    agent_id: uuid.UUID,
    tenant: Tenant,
) -> bool:
    """If plaza quotas are on, no unread agent notifications, and no post/reply quota left, skip LLM."""
    if not plaza_quotas_enabled(tenant):
        return False
    unread = await count_unread_agent_notifications(db, agent_id)
    if unread > 0:
        return False

    tz = tenant.timezone or "UTC"
    ds = tenant_local_date_string(tz)
    can_post = tenant.plaza_daily_post_limit is None or (
        await get_daily_post_count(tenant.id, agent_id, ds) < tenant.plaza_daily_post_limit
    )
    can_reply = tenant.plaza_daily_reply_limit is None or (
        await get_daily_reply_count(tenant.id, agent_id, ds) < tenant.plaza_daily_reply_limit
    )
    return not can_post and not can_reply


async def had_qualifying_activity_today(db: AsyncSession, agent_id: uuid.UUID, tz_name: str) -> bool:
    """True if today (tenant tz) the agent had user chat, A2A inbound, or plaza-related inbox signal.

    Signals:
    - Any ChatMessage with role=user for this agent (web / feishu / A2A session ids).
    - Any Notification for this agent of type mention / plaza_reply / plaza_comment created today.
    """
    start_utc, end_utc = tenant_day_utc_bounds(tz_name)

    um = await db.execute(
        select(func.count())
        .select_from(ChatMessage)
        .where(
            ChatMessage.agent_id == agent_id,
            ChatMessage.role == "user",
            ChatMessage.created_at >= start_utc,
            ChatMessage.created_at < end_utc,
        )
    )
    if int(um.scalar() or 0) > 0:
        return True

    nm = await db.execute(
        select(func.count())
        .select_from(Notification)
        .where(
            Notification.agent_id == agent_id,
            Notification.type.in_(["mention", "plaza_reply", "plaza_comment"]),
            Notification.created_at >= start_utc,
            Notification.created_at < end_utc,
        )
    )
    return int(nm.scalar() or 0) > 0


_CASE_TAG_RE = re.compile(r"#\s*CASE[-\s]?", re.I)


def post_related_to_agent_for_reply(
    post: PlazaPost,
    *,
    agent_id: uuid.UUID,
    agent_name: str,
    db_has_prior_comment: bool,
) -> bool:
    """Rule-based: agent may add a proactive comment only if the thread relates to them."""
    if post.author_id == agent_id and post.author_type == "agent":
        return True
    if db_has_prior_comment:
        return True
    body = (post.content or "").lower()
    name = (agent_name or "").strip().lower()
    if name and len(name) >= 2 and name in body:
        return True
    # Workflow case tags on shared tenant cases
    if _CASE_TAG_RE.search(post.content or ""):
        return True
    return False


async def agent_has_prior_comment_on_post(
    db: AsyncSession, post_id: uuid.UUID, agent_id: uuid.UUID
) -> bool:
    r = await db.execute(
        select(func.count())
        .select_from(PlazaComment)
        .where(PlazaComment.post_id == post_id, PlazaComment.author_id == agent_id)
    )
    return int(r.scalar() or 0) > 0
