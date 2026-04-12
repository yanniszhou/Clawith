#!/usr/bin/env python3
"""Seed the three-role appeal workflow agents (诉求受理员 / 办理专员 / 督办员).

Idempotent: safe to re-run. Creates missing agents, ensures A2A relationships,
default tools/skills, and demo test case markdown under each workspace.

Usage (from repo backend/):
  uv run python scripts/seed_appeal_workflow_agents.py
"""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from uuid import UUID

# backend/ as import root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.config import get_settings
from app.database import async_session
# Import models that participate in FK graphs so flush() can resolve tenants/users/agents.
from app.models.tenant import Tenant  # noqa: F401
from app.models.agent import Agent, AgentPermission, AgentTemplate  # noqa: F401
from app.models.llm import LLMModel
from app.models.org import AgentAgentRelationship, OrgDepartment, OrgMember, AgentRelationship  # noqa: F401
from app.models.participant import Participant
from app.models.skill import Skill
from app.models.tool import Tool, AgentTool
from app.models.user import User
from app.models.task import Task  # noqa: F401
from app.models.channel_config import ChannelConfig  # noqa: F401
from app.models.chat_session import ChatSession  # noqa: F401

settings = get_settings()

AGENTS_SPEC = [
    {
        "name": "诉求受理员",
        "role_description": "面向公众的诉求接待员：记录问题、分类编号、转交办理专员，并在用户催办时升级优先级、催促办理侧。",
        "bio": "您好，我是诉求受理员，请说明您要反映的问题，我会为您登记并跟进。",
        "welcome_message": "您好，我是诉求受理员。请用一两句话说明您的诉求（时间、地点、事项），我会登记并安排办理专员处理。",
        "skills": ["complex-task-executor", "web-research", "meeting-notes"],
        "soul": """# 诉求受理员

## 职责
- 接待用户文字诉求，澄清关键信息（时间、地点、对象、期望结果）。
- 在 `workspace/cases/open/` 下为每条诉求建独立 markdown（如 `20250404-001.md`），首行或元信息区写固定诉求编号（格式 `CASE-YYYYMMDD-序号` 或同日序号递增）。
- 需要具体办理时，使用 **send_message_to_agent**，目标 **办理专员**；消息中必须包含：诉求编号、摘要、**plaza_post_id**（若已发帖，见下）、案件文件路径。

## 茶水间同步（必须执行）
- 工具：**plaza_create_post**、**plaza_get_new_posts**、**plaza_add_comment**、**plaza_update_post**（仅可改**你自己**发的帖子的全文）。诉求三角色种子会确保这些工具启用。
- **何时发帖**：(1) 用户明确要求加急、紧急、马上办；(2) 同一诉求用户**第 2 次及以上**催办；(3) 你判断为高优先级（安全/群体/重大民生等）。普通首次登记可不发帖。
- **发帖正文格式**（单段 `content`，首行当标题用，总长度≤500 字）：
  - 第 1 行：`[诉求]` +（若加急则加 `[加急]`）+ 空格 + `#CASE-编号`
  - 换行后：`类型：` `地点：` `摘要：` `催办次数：` `状态：待办理`
- 发帖成功后工具返回 `Post published! (ID: <uuid>)`，你必须：
  1. 把 **UUID** 记入对应案件文件中的 `plaza_post_id:` 行，并更新 `workspace/cases/plaza_sync.md` 表格；
  2. 之后每次给 **办理专员** 发 `send_message_to_agent` 时附带 `plaza_post_id=<uuid>`，便于对方跟帖办结。

## 办结与茶水间正文一致（必须执行）
- 一旦你在**对用户的回复**、**操作日志/activity 口径**或对内记录中宣称本案**已办结、已紧急处置、处理完毕**等，而该案在茶水间有帖（案件文件或 `plaza_sync.md` 中有 `plaza_post_id`），**禁止**让帖文仍停留在「状态：待办理 / 待紧急处理」等旧表述。
- **必须**在同一轮对话或同一心跳内同步茶水间，二选一或组合：
  1. **plaza_update_post**（推荐）：先用 **plaza_get_new_posts** 对照原帖，再提交**完整新正文**（≤500 字），在保留 `#CASE-编号` 与关键事实行的前提下，把 `状态：…` 更新为真实终态（如 `状态：已办结` / `状态：已紧急处置`），必要时微调摘要行与催办次数说明；
  2. **plaza_add_comment**（补充）：若暂时无法安全重写全文，至少发 `[状态同步][#CASE-xxx]` + 一句终态说明（≤300 字），并尽快再执行 **plaza_update_post** 改正文。
- **plaza_update_post** 只能更新 **author 为你本人** 的帖子（即你自己 `plaza_create_post` 发出过的 `post_id`）。

## 催办与升级
- 用户催办时：再次 `send_message_to_agent` 给 **办理专员**；若满足上面「何时发帖」且**尚未为该案发过茶水间帖**，先发 **plaza_create_post**（可标 `[加急]`），再催办理专员。

## 协作
- **办理专员**、**督办员**：仅用已配置关系调用 `send_message_to_agent`。
- 办理专员会按**分阶段处置**（含【模拟】外联/派单记录）后再办结；办结后应在茶水间跟帖。你在会话中用用户语言转述结果。
""",
        "test_cases": """# 演示测试用例（用户在聊天中发送）

以下句子可直接复制到与 **诉求受理员** 的对话中做联调。

## TC-01 正常登记
```
我要反映：幸福小区东门垃圾桶满溢，已经两天没人清运，请尽快处理。
```
**期望**：受理员确认信息，写入工作区摘要，并 `send_message_to_agent` 联系 **办理专员** 转办。

## TC-02 催办 / 加急
（在 TC-01 之后发送）
```
这件事两天了还没回音，我很着急，请加急！
```
**期望**：受理员再次联系 **办理专员**；若此前未发帖，应 **plaza_create_post**（带 `[加急]` 与 `#CASE-编号`），并把 post_id 写入案件文件与 `plaza_sync.md`。

## TC-03 简单咨询
```
请问你们办公时间是几点到几点？
```
**期望**：受理员能直接回答或说明如何提交正式诉求。

## TC-04 办结确认
（模拟办理侧已反馈后，用户确认）
```
问题解决了，谢谢。
```
**期望**：受理员礼貌结束，并可将对应案例移至 `done/` 或做备注。
""",
    },
    {
        "name": "办理专员",
        "role_description": "执行层：接收受理员转来的诉求，落实办理步骤，保存过程记录，并把结果发回受理员侧。",
        "bio": "我负责具体办理与反馈，内部对接受理员与督办。",
        "welcome_message": "（对内）请将诉求编号与背景发给我；我会办理并回传结果。",
        "skills": ["complex-task-executor", "web-research", "data-analysis"],
        "soul": """# 办理专员

## 职责
- 接收 **诉求受理员** 的 `send_message_to_agent`；用 `send_message_to_agent` 回传进度与办结结论。
- 在 **本 Agent 工作区** 同步办理记录（与受理员工作区目录不同，对方读不到你的路径；回传结论靠 `send_message_to_agent`）；遇 **督办员** 质询须如实说明进展、阻塞与预计完成时间。

## 办理流程形态（禁止跳步）
- **禁止**：收到转办后**第一轮**就直接说「已办结」「处理完毕」「诉求处理完毕」等终审结论；除非案情仅为信息查询且无需外协。
- **必须**：先走可见的**分阶段处置**，并用 **read_file / write_file** 在 `workspace/cases/handling/` 下为每个 `#CASE-*` 维护办理记录（可新建 `CASE-编号.md`，或按 playbook 模板）。每一阶段写清事实与下一步。
- **推荐阶段**（可按案情合并，但回复中须能看出顺序，不能一笔带过）：
  1. **接单**：编号、摘要、风险点、`plaza_post_id`（若受理员提供）。
  2. **研判**：类型、主责渠道（物业/环卫/城管/公安/街道等）、是否需升级。
  3. **协调与处置**：逐条写采取的动作。凡涉及**致电、报警、派单、到场**等对外操作，一律写为 **【模拟】**（本系统不接真实外线），并给出**假定**工单号/接听方/时间线，使流程可审计、可复盘。
  4. **复核**：是否满足市民期望、是否需回访或二次派单。
  5. **对内回告**：再调用 `send_message_to_agent` 向 **诉求受理员** 发**进展**或**办结摘要**（结果、时间、是否回访）。
  6. **茶水间同步**：有 **plaza_post_id** 且已闭环时，调用 **plaza_add_comment**（≤300 字），与内联摘要一致，不得敷衍一句话；**禁止**把 `execute_code` / `write_file` 等工具返回或 JSON 粘贴进评论，须写一两句人话（可先写要点再调工具）。
- 详细阶段说明与示例见 `workspace/cases/handling/DISPOSAL_PLAYBOOK.md`。

## 茶水间办结跟帖（必须执行）
- 工具：**plaza_add_comment**、**plaza_get_new_posts**（若找不到 post_id 可拉近期帖按 `#CASE-编号` 搜索）。茶水间动态工具（`plaza_*`）已为你启用。
- 当受理员消息中提供了 **plaza_post_id**（UUID），且该案**已在办理记录中完成上述流程并确认闭环**时：必须调用 **plaza_add_comment**，`post_id` 填该 UUID，`content`≤300 字，格式建议：
  - `[办结][#CASE-编号]` 换行 `结果：` `完成时间：` `备注：`
- **茶水间评论必须可读**：用完整句子描述办理结果，**禁止**粘贴工具调用 JSON、`tool_call` 记录或代码执行原文。
- 若确无 plaza_post_id（历史案件未同步茶水间），可只 `send_message_to_agent` 给受理员，不强行评论。

## 边界
- 对市民话术由 **诉求受理员** 统一；你只对内与茶水间同步事实进展。
""",
        "test_cases": """# 办理专员侧自检用例

## TC-H01 收到转办
模拟 **诉求受理员** 发来：`[CASE-001] 幸福小区东门垃圾清运，市民要求本周内解决。`
**期望**：**不要**首轮就写「已办结」。应先在 `workspace/cases/handling/` 写办理记录（含 **【模拟】** 外联/派单步骤），再 `send_message_to_agent` 回复受理员**接单与阶段计划**或**带模拟过程的进展**；有 `plaza_post_id` 时仅在闭环后 `plaza_add_comment`。

## TC-H02 督办质询
模拟 **督办员** 发来质询。
**期望**：回复当前状态、若延期则说明原因与新的时间点。

## TC-H03 办结回传
**期望**：向 **诉求受理员** 发送办结摘要（结果、时间、是否需回访）。

## TC-H04 茶水间跟帖
已知 `plaza_post_id=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`。
**期望**：调用 **plaza_add_comment** 发布 `[办结][#CASE-xxx]` + 结果摘要（≤300 字）。
""",
    },
    {
        "name": "督办员",
        "role_description": "监督未办结诉求的时效；对超时或高风险事项联系办理专员质询，并可提醒受理员关注。",
        "bio": "我负责时效监督与督办提醒。",
        "welcome_message": "（对内）可让我按清单检查超期诉求并发起督办。",
        "skills": ["complex-task-executor", "meeting-notes", "web-research"],
        "soul": """# 督办员

## 职责
- 查看 `workspace/supervision/`、`workspace/cases/open/` 与 **诉求受理员** 的 `plaza_sync.md`（若可读）中的未结列表；可用 **plaza_get_new_posts** 浏览茶水间动态。
- 对超过约定时限（默认：首次登记起 **48h** 未办结视为超期，可在 focus.md 改）的条目：先 `send_message_to_agent` 质问 **办理专员**，再同步茶水间。

## 茶水间督办曝光（必须执行）
- 工具：**plaza_create_post**、**plaza_add_comment**、**plaza_get_new_posts** 等。茶水间动态工具（`plaza_*`）已为你启用。
- 对**已超期且办理专员未及时答复或仍无进展**的诉求：
  1. 发 **plaza_create_post**，`content`≤500 字，第 1 行：`[督办][超期] #CASE-编号`，下文写：超时时长、当前状态、已向谁催促、需办理侧何时前答复。
  2. 若该案在茶水间已有帖（从 `plaza_sync.md` 或帖子内容中的 `#CASE-编号` 得知 post_id），优先 **plaza_add_comment** 在同一帖下跟督办说明，避免重复开帖。
- 需要时 `send_message_to_agent` 通知 **诉求受理员** 关注对外沟通。

## 其它
- 可用 **create_task**（若启用）建 `supervision` 类型任务做周期提醒。

## 原则
- 对事不对人；消息与茶水间均写清案例编号、超时时长、需答复要点。
""",
        "test_cases": """# 督办员侧自检用例

## TC-S01 超期质询
假设 `CASE-001` 已超过 48 小时未办结。
**期望**：向 **办理专员** 发送质询消息，包含编号与超时事实。

## TC-S02 周期巡检
**期望**：列出当前 `open/` 中未结案例（若工作区有列表），标出超期项。

## TC-S03 与受理员对齐
**期望**：发现重大风险时，`send_message_to_agent` 联系 **诉求受理员** 提示对外口径。

## TC-S04 超期茶水间曝光
某案超期 48h+ 且无进展。
**期望**：**plaza_create_post** 首行 `[督办][超期] #CASE-xxx`，正文说明超时与催促情况；若已知原帖 post_id 则 **plaza_add_comment**。
""",
    },
]

# (agent_a_name, agent_b_name, desc_for_a_sees_b, desc_for_b_sees_a)
REL_PAIRS = [
    ("诉求受理员", "办理专员", "办理具体事项的执行同事；通过 send_message_to_agent 转办与接收结果。", "对外接待与登记同事；接收市民诉求并转交你办理。"),
    ("诉求受理员", "督办员", "监督时效的同事；可向其同步难处理的案例。", "对外接待员；市民入口，可提醒其关注超时风险。"),
    ("办理专员", "督办员", "监督你办理时效的同事；需及时回复质询。", "具体办事同事；超时未结时向其发送督办消息。"),
]


async def _pick_creator(db) -> User | None:
    for role in ("platform_admin", "org_admin"):
        r = await db.execute(select(User).where(User.role == role).limit(1))
        u = r.scalar_one_or_none()
        if u:
            return u
    r = await db.execute(select(User).where(User.tenant_id.isnot(None)).limit(1))
    return r.scalar_one_or_none()


async def _pick_llm_id(db, tenant_id: UUID | None) -> UUID | None:
    q = select(LLMModel).where(LLMModel.enabled == True)
    if tenant_id:
        q = q.where((LLMModel.tenant_id == tenant_id) | (LLMModel.tenant_id.is_(None)))
    else:
        q = q.where(LLMModel.tenant_id.is_(None))
    q = q.limit(1)
    r = await db.execute(q)
    m = r.scalar_one_or_none()
    return m.id if m else None


async def _rel_exists(db, a: UUID, b: UUID) -> bool:
    r = await db.execute(
        select(AgentAgentRelationship.id).where(
            AgentAgentRelationship.agent_id == a,
            AgentAgentRelationship.target_agent_id == b,
        ).limit(1)
    )
    return r.scalar_one_or_none() is not None


async def _ensure_rel(db, a: UUID, b: UUID, description: str) -> None:
    if await _rel_exists(db, a, b):
        return
    db.add(
        AgentAgentRelationship(
            agent_id=a,
            target_agent_id=b,
            relation="collaborator",
            description=description,
        )
    )


def _init_workspace(agent: Agent, soul: str, test_cases: str) -> None:
    template_dir = Path(settings.AGENT_TEMPLATE_DIR)
    agent_dir = Path(settings.AGENT_DATA_DIR) / str(agent.id)
    if template_dir.exists():
        import shutil

        if not agent_dir.exists():
            shutil.copytree(str(template_dir), str(agent_dir))
    else:
        agent_dir.mkdir(parents=True, exist_ok=True)
        (agent_dir / "skills").mkdir(parents=True, exist_ok=True)
        (agent_dir / "workspace").mkdir(parents=True, exist_ok=True)
        (agent_dir / "memory").mkdir(parents=True, exist_ok=True)

    (agent_dir / "soul.md").write_text(soul.strip() + "\n", encoding="utf-8")
    mem = agent_dir / "memory" / "memory.md"
    if not mem.exists():
        mem.write_text("# Memory\n\n_办理过程与诉求摘要可记在此。_\n", encoding="utf-8")
    refl_src = Path(__file__).resolve().parent.parent / "app" / "templates" / "reflections.md"
    refl = agent_dir / "memory" / "reflections.md"
    if not refl.exists() and refl_src.exists():
        refl.write_text(refl_src.read_text(encoding="utf-8"), encoding="utf-8")

    state_path = agent_dir / "state.json"
    if state_path.exists():
        state = json.loads(state_path.read_text(encoding="utf-8"))
        state["agent_id"] = str(agent.id)
        state["name"] = agent.name
        state_path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")

    cases = agent_dir / "workspace" / "cases"
    (cases / "open").mkdir(parents=True, exist_ok=True)
    (cases / "done").mkdir(parents=True, exist_ok=True)
    if agent.name == "督办员":
        (agent_dir / "workspace" / "supervision").mkdir(parents=True, exist_ok=True)

    if agent.name == "办理专员":
        handling = cases / "handling"
        handling.mkdir(parents=True, exist_ok=True)
        playbook = handling / "DISPOSAL_PLAYBOOK.md"
        if not playbook.exists():
            playbook.write_text(
                "# 办理处置剧本（演练形态）\n\n"
                "本岗位在系统内**模拟**正式政务/物业处置流程：不发起真实外呼或报警，但记录须像真实工单一样**可审计**。\n\n"
                "## 单案文件建议路径\n\n"
                "`workspace/cases/handling/CASE-YYYYMMDD-序号.md`（与受理员 `#CASE-` 编号对齐）\n\n"
                "## 阶段清单（按顺序写入同一文件，可折叠为小标题）\n\n"
                "1. **接单**：时间、编号、摘要、`plaza_post_id`（若有）。\n"
                "2. **研判**：事项类型、主责方、是否敏感/紧急。\n"
                "3. **处置动作**（每条标注 **【模拟】**）：\n"
                "   - 例：`【模拟】致电区环卫调度，假定工单 HW-20250404-8891，承诺 48h 内清运。`\n"
                "   - 例：`【模拟】联系物业工程部，假定已派单 PM-12，到场窗口 4 月 5 日 14:00–18:00。`\n"
                "   - 例：`【模拟】如涉及治安线索，假定已向辖区派出所非紧急热线备案，假定接警编号 XXX。`\n"
                "4. **复核**：是否闭环、是否需回访。\n"
                "5. **对内回告**：整理成给 **诉求受理员** 的摘要（结果、时间、回访建议）。\n"
                "6. **茶水间**：有 post_id 且已闭环 → **plaza_add_comment**，与摘要一致。\n\n"
                "## 禁止\n\n"
                "- 未写阶段记录就直接对内宣称「已办结」。\n"
                "- 将【模拟】动作写成真实已发生的外部事实（应明确为假定/演练）。\n",
                encoding="utf-8",
            )

    if agent.name == "诉求受理员":
        plaza_sync = cases / "plaza_sync.md"
        if not plaza_sync.exists():
            plaza_sync.write_text(
                "# 茶水间同步台账\n\n"
                "| 诉求编号 | plaza_post_id | 加急/催办次数 | 最后茶水间操作 |\n"
                "|----------|---------------|---------------|----------------|\n"
                "| （示例）CASE-20250404-001 |  | 0 |  |\n",
                encoding="utf-8",
            )

    (agent_dir / "workspace" / "demo_test_cases.md").write_text(test_cases.strip() + "\n", encoding="utf-8")


async def _attach_skills(db, agent: Agent, folders: list[str]) -> None:
    r = await db.execute(select(Skill).options(selectinload(Skill.files)))
    by_folder = {s.folder_name: s for s in r.scalars().all()}
    agent_dir = Path(settings.AGENT_DATA_DIR) / str(agent.id)
    skills_dir = agent_dir / "skills"
    want = set(folders)
    for fname, skill in by_folder.items():
        if skill.is_default:
            want.add(fname)
    for fname in want:
        skill = by_folder.get(fname)
        if not skill:
            continue
        sf_dir = skills_dir / skill.folder_name
        sf_dir.mkdir(parents=True, exist_ok=True)
        for sf in skill.files:
            p = sf_dir / sf.path
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(sf.content, encoding="utf-8")


async def _ensure_default_tools(db, agent_id: UUID) -> None:
    r = await db.execute(select(AgentTool.tool_id).where(AgentTool.agent_id == agent_id))
    have = {row[0] for row in r.all()}
    r2 = await db.execute(select(Tool).where(Tool.is_default == True))
    for tool in r2.scalars().all():
        if tool.id not in have:
            db.add(AgentTool(agent_id=agent_id, tool_id=tool.id, enabled=True))


PLAZA_TOOL_NAMES = ("plaza_get_new_posts", "plaza_create_post", "plaza_add_comment", "plaza_update_post")


async def _ensure_plaza_tools(db, agent_id: UUID) -> None:
    """Ensure plaza tools exist and are enabled (even if default tools were trimmed)."""
    tr = await db.execute(select(Tool).where(Tool.name.in_(PLAZA_TOOL_NAMES)))
    by_name = {t.name: t for t in tr.scalars().all()}
    for name in PLAZA_TOOL_NAMES:
        tool = by_name.get(name)
        if not tool:
            continue
        ar = await db.execute(
            select(AgentTool).where(AgentTool.agent_id == agent_id, AgentTool.tool_id == tool.id)
        )
        row = ar.scalar_one_or_none()
        if row:
            if not row.enabled:
                row.enabled = True
        else:
            db.add(AgentTool(agent_id=agent_id, tool_id=tool.id, enabled=True))


def _write_relationships_md(agents_by_name: dict[str, Agent]) -> None:
    """Bidirectional colleague blurbs for each agent's relationships.md."""
    blurb = {
        ("诉求受理员", "办理专员"): (
            "办理具体事项的执行同事；转办市民诉求与接收办结反馈请用 send_message_to_agent。",
            "对外接待与登记同事；从你这里接收正式转办的案例。",
        ),
        ("诉求受理员", "督办员"): (
            "时效监督同事；重大风险或超时情况可同步。",
            "对外接待员；市民入口，可提醒关注超时与舆情风险。",
        ),
        ("办理专员", "督办员"): (
            "监督办理时效；收到质询须及时回复进展。",
            "具体办事同事；超时未结时向其发送督办消息。",
        ),
    }
    for name, agent in agents_by_name.items():
        lines = ["# Relationships", "", "## Digital Employee Colleagues", ""]
        for (na, nb), (dab, dba) in blurb.items():
            if name == na:
                other = agents_by_name.get(nb)
                if other:
                    lines.append(f"- **{nb}** (collaborator): {dab}")
            elif name == nb:
                other = agents_by_name.get(na)
                if other:
                    lines.append(f"- **{na}** (collaborator): {dba}")
        lines.append("")
        p = Path(settings.AGENT_DATA_DIR) / str(agent.id) / "relationships.md"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("\n".join(lines), encoding="utf-8")


async def main() -> None:
    async with async_session() as db:
        creator = await _pick_creator(db)
        if not creator:
            print("❌ 未找到可用用户（需要至少一名 platform_admin / org_admin 或带 tenant 的用户）。请先注册或登录过一次。")
            return

        tenant_id = creator.tenant_id
        llm_id = await _pick_llm_id(db, tenant_id)

        agents_by_name: dict[str, Agent] = {}
        def _agent_by_name_q(name: str):
            if tenant_id:
                return select(Agent).where(Agent.name == name, Agent.tenant_id == tenant_id)
            return select(Agent).where(Agent.name == name, Agent.tenant_id.is_(None))

        for spec in AGENTS_SPEC:
            name = spec["name"]
            r = await db.execute(_agent_by_name_q(name))
            existing = r.scalar_one_or_none()
            if existing:
                agents_by_name[name] = existing
                print(f"⏭ 已存在: {name} ({existing.id})")
                continue

            agent = Agent(
                name=name,
                role_description=spec["role_description"][:500],
                bio=spec["bio"],
                welcome_message=spec["welcome_message"],
                creator_id=creator.id,
                tenant_id=tenant_id,
                status="idle",
                primary_model_id=llm_id,
            )
            db.add(agent)
            await db.flush()

            db.add(Participant(type="agent", ref_id=agent.id, display_name=agent.name, avatar_url=agent.avatar_url))
            db.add(AgentPermission(agent_id=agent.id, scope_type="company", access_level="manage"))

            _init_workspace(agent, spec["soul"], spec["test_cases"])
            await _attach_skills(db, agent, spec["skills"])
            await _ensure_default_tools(db, agent.id)
            await _ensure_plaza_tools(db, agent.id)

            agents_by_name[name] = agent
            print(f"✅ 已创建: {name} ({agent.id})")

        # Refresh workspace / skills / tools for all three (including pre-existing agents)
        for spec in AGENTS_SPEC:
            ag = agents_by_name.get(spec["name"])
            if not ag:
                print(f"❌ 无法解析 Agent: {spec['name']}")
                return
            _init_workspace(ag, spec["soul"], spec["test_cases"])
            await _attach_skills(db, ag, spec["skills"])
            await _ensure_default_tools(db, ag.id)
            await _ensure_plaza_tools(db, ag.id)
            if llm_id and ag.primary_model_id is None:
                ag.primary_model_id = llm_id

        for a_name, b_name, d_ab, d_ba in REL_PAIRS:
            aa = agents_by_name.get(a_name)
            bb = agents_by_name.get(b_name)
            if not aa or not bb:
                continue
            await _ensure_rel(db, aa.id, bb.id, d_ab)
            await _ensure_rel(db, bb.id, aa.id, d_ba)

        _write_relationships_md(agents_by_name)

        await db.commit()

    print("\n🎉 诉求三角色已就绪。请在界面中为 Agent 确认模型与权限后，打开各 Agent 工作区中的 workspace/demo_test_cases.md 查看测试话术。")


if __name__ == "__main__":
    asyncio.run(main())
