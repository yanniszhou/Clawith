#!/usr/bin/env python3
"""Diagnose why on_message may not see Feishu replies (agent_id / user / snapshot).

Run from repo backend/ with DATABASE_URL set (e.g. docker compose postgres):

  cd backend && DATABASE_URL=postgresql+asyncpg://clawith:clawith@localhost:5432/clawith \\
    uv run python scripts/diagnose_feishu_on_message.py

Optional: filter by human-readable agent name substring:
  uv run python scripts/diagnose_feishu_on_message.py --agent 数字

Optional: resolve users whose display_name/username matches (same tenant as agents):
  uv run python scripts/diagnose_feishu_on_message.py --from-user 闫洲
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys


def _setup_path() -> None:
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if root not in sys.path:
        sys.path.insert(0, root)


async def main() -> None:
    _setup_path()

    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--agent",
        default="",
        help="Substring to filter agents by name (case-sensitive contains)",
    )
    p.add_argument(
        "--from-user",
        default="",
        help="Match User.display_name / Identity.username ilike %%value%% (joined to agents by tenant)",
    )
    args = p.parse_args()

    from sqlalchemy import cast as sa_cast, or_, select, String as SaString

    from app.database import async_session
    from app.models.agent import Agent
    from app.models.audit import ChatMessage
    from app.models.channel_config import ChannelConfig
    from app.models.chat_session import ChatSession
    from app.models.trigger import AgentTrigger
    from app.models.user import Identity, User

    agent_filter = args.agent.strip()

    print("=== Feishu channel configs (is_configured) ===")
    feishu_agent_ids: set = set()
    rows: list = []
    async with async_session() as db:
        q = (
            select(ChannelConfig, Agent)
            .join(Agent, ChannelConfig.agent_id == Agent.id)
            .where(
                ChannelConfig.channel_type == "feishu",
                ChannelConfig.is_configured.is_(True),
            )
        )
        if agent_filter:
            q = q.where(Agent.name.contains(agent_filter))
        r = await db.execute(q)
        rows = r.all()
        for cc, ag in rows:
            feishu_agent_ids.add(ag.id)
            mode = (cc.extra_config or {}).get("connection_mode") or "webhook"
            print(
                f"  agent_id={ag.id}  name={ag.name!r}  mode={mode}  "
                f"app_id={cc.app_id!r}"
            )

        by_app: dict[str, list[str]] = {}
        for cc, ag in rows:
            if cc.app_id:
                by_app.setdefault(cc.app_id, []).append(f"{ag.name} ({ag.id})")
        dup = {k: v for k, v in by_app.items() if len(v) > 1}
        if dup:
            print("\n⚠️  Same Feishu app_id on multiple agents:")
            for app_id, names in dup.items():
                print(f"  app_id={app_id!r} -> {names}")
        elif rows:
            print("\n✓ No duplicate Feishu app_id across agents in this DB snapshot.")

    print("\n=== Agents with both interval + on_message enabled ===")
    async with async_session() as db:
        q = (
            select(AgentTrigger, Agent)
            .join(Agent, AgentTrigger.agent_id == Agent.id)
            .where(AgentTrigger.is_enabled.is_(True), AgentTrigger.type.in_(["interval", "on_message"]))
        )
        if agent_filter:
            q = q.where(Agent.name.contains(agent_filter))
        r = await db.execute(q)
        all_rows = r.all()

    by_agent: dict = {}
    for t, ag in all_rows:
        by_agent.setdefault(ag.id, {"name": ag.name, "triggers": []})
        by_agent[ag.id]["triggers"].append(t)

    for aid, info in sorted(by_agent.items(), key=lambda x: x[1]["name"]):
        types = {x.type for x in info["triggers"]}
        if "interval" not in types or "on_message" not in types:
            continue
        has_feishu = aid in feishu_agent_ids
        mark = "OK" if has_feishu else "❌ NO FEISHU CHANNEL ON THIS AGENT"
        print(f"\n  Agent {info['name']!r}  id={aid}  feishu_configured={has_feishu}  [{mark}]")
        for t in sorted(info["triggers"], key=lambda x: (x.type, x.name)):
            cfg = t.config or {}
            extra = ""
            if t.type == "on_message":
                extra = (
                    f" from_user_name={cfg.get('from_user_name')!r} "
                    f"from_agent_name={cfg.get('from_agent_name')!r} "
                    f"_since_ts={cfg.get('_since_ts')!r}"
                )
            elif t.type == "interval":
                extra = f" minutes={cfg.get('minutes')!r}"
            print(
                f"    - {t.type} name={t.name!r} cooldown_s={t.cooldown_seconds} "
                f"fire_count={t.fire_count}{extra}"
            )

    print("\n=== Enabled on_message but agent has no Feishu channel ===")
    async with async_session() as db:
        q = (
            select(AgentTrigger, Agent)
            .join(Agent, AgentTrigger.agent_id == Agent.id)
            .where(AgentTrigger.is_enabled.is_(True), AgentTrigger.type == "on_message")
        )
        if agent_filter:
            q = q.where(Agent.name.contains(agent_filter))
        r = await db.execute(q)
        bad = [(t, ag) for t, ag in r.all() if ag.id not in feishu_agent_ids]
        if not bad:
            print("  (none)")
        for t, ag in bad:
            print(f"  ❌ agent={ag.name!r} id={ag.id} trigger={t.name!r} config={t.config}")

    from_user = args.from_user.strip()
    if from_user:
        print(f"\n=== Users matching ilike %{from_user}% (by tenant with any Agent) ===")
        async with async_session() as db:
            q = (
                select(User, Agent)
                .join(Agent, Agent.tenant_id == User.tenant_id)
                .outerjoin(Identity, User.identity_id == Identity.id)
                .where(
                    or_(
                        User.display_name.ilike(f"%{from_user}%"),
                        Identity.username.ilike(f"%{from_user}%"),
                    ),
                )
                .distinct()
            )
            if agent_filter:
                q = q.where(Agent.name.contains(agent_filter))
            r = await db.execute(q.limit(30))
            urows = r.all()
            if not urows:
                print("  (no rows — trigger fallback path will not match by User id either)")
            for u, ag in urows:
                print(
                    f"  user_id={u.id} display_name={u.display_name!r} "
                    f"identity_id={u.identity_id} sample_agent={ag.name!r}"
                )

    print("\n=== Latest Feishu user ChatMessage (up to 15) ===")
    async with async_session() as db:
        q = (
            select(ChatMessage, ChatSession, Agent)
            .join(ChatSession, ChatMessage.conversation_id == sa_cast(ChatSession.id, SaString))
            .join(Agent, ChatMessage.agent_id == Agent.id)
            .where(ChatSession.source_channel == "feishu", ChatMessage.role == "user")
            .order_by(ChatMessage.created_at.desc())
            .limit(15)
        )
        if agent_filter:
            q = q.where(Agent.name.contains(agent_filter))
        r = await db.execute(q)
        for msg, sess, ag in r.all():
            preview = (msg.content or "")[:80].replace("\n", " ")
            print(
                f"  at={msg.created_at} agent={ag.name!r} agent_id={msg.agent_id} "
                f"session.user_id={sess.user_id} ext_conv={sess.external_conv_id!r} "
                f"msg.user_id={msg.user_id} | {preview!r}"
            )


if __name__ == "__main__":
    asyncio.run(main())
