"""Shared helper: find-or-create ChatSession by external channel conv_id.

Used by feishu.py, slack.py, discord_bot.py, wecom.py, teams.py — eliminates in-process caches.
"""
import uuid as _uuid
from datetime import datetime, timezone

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.audit import ChatMessage
from app.models.chat_session import ChatSession


async def find_or_create_channel_session(
    db: AsyncSession,
    agent_id: _uuid.UUID,
    user_id: _uuid.UUID,
    external_conv_id: str,
    source_channel: str,
    first_message_title: str,
    is_group: bool = False,
    group_name: str | None = None,
) -> ChatSession:
    """Find an existing ChatSession by (agent_id, external_conv_id), or create one.

    Relies on the UNIQUE constraint on (agent_id, external_conv_id) in the DB.

    Args:
        is_group: True for group chat sessions (Feishu group, Slack channel, etc.).
                  Group sessions keep user_id as the agent creator (placeholder) and
                  are excluded from the user's "mine" session list.
        group_name: Display name for group sessions (e.g. IM group/channel name).
    """
    result = await db.execute(
        select(ChatSession).where(
            ChatSession.agent_id == agent_id,
            ChatSession.external_conv_id == external_conv_id,
        )
    )
    session = result.scalar_one_or_none()

    if session is None:
        now = datetime.now(timezone.utc)
        session = ChatSession(
            agent_id=agent_id,
            user_id=user_id,
            title=group_name[:40] if (is_group and group_name) else first_message_title[:40],
            source_channel=source_channel,
            external_conv_id=external_conv_id,
            is_group=is_group,
            group_name=group_name,
            created_at=now,
        )
        db.add(session)
        await db.flush()  # populate session.id
    else:
        # For P2P sessions: re-attribute to the correct user
        # (fixes legacy sessions stored under creator_id)
        if not session.is_group and session.user_id != user_id:
            session.user_id = user_id

        # For group sessions: update group_name if it changed
        if session.is_group and group_name and session.group_name != group_name:
            session.group_name = group_name
            session.title = group_name[:40]

    return session


async def pick_best_feishu_p2p_session_by_message_count(
    db: AsyncSession,
    agent_id: _uuid.UUID,
    candidates: list[ChatSession],
) -> ChatSession:
    """Pick the Feishu P2P ChatSession that should own the thread (user_id vs open_id duplicates).

    Prefer the row with more chat_messages so proactive outbound lands in the same session
    the user already chats in, not an empty duplicate keyed only by tenant user_id.
    """
    if not candidates:
        raise ValueError("pick_best_feishu_p2p_session_by_message_count: empty candidates")
    if len(candidates) == 1:
        return candidates[0]
    best = candidates[0]
    best_n = -1
    for s in candidates:
        r = await db.execute(
            select(func.count(ChatMessage.id)).where(
                ChatMessage.agent_id == agent_id,
                ChatMessage.conversation_id == str(s.id),
            )
        )
        n = r.scalar() or 0
        if n > best_n:
            best_n = n
            best = s
        elif n == best_n:
            s_lm = s.last_message_at or s.created_at
            b_lm = best.last_message_at or best.created_at
            if s_lm and b_lm and s_lm > b_lm:
                best = s
    return best
