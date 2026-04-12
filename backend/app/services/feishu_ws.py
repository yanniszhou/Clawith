"""Feishu WebSocket Long Connection Manager."""

import asyncio
import json
from typing import Any

import uuid

from loguru import logger

try:
    import lark_oapi as lark
    import lark_oapi.ws as ws

    _HAS_LARK = True
except ImportError:
    lark = None  # type: ignore
    ws = None  # type: ignore
    _HAS_LARK = False

from app.database import async_session
from app.models.channel_config import ChannelConfig
from sqlalchemy import select


if not _HAS_LARK:
    logger.warning(
        "[Feishu WS] lark-oapi package not installed. "
        "Feishu WebSocket features will be disabled. "
        "Install with: pip install lark-oapi"
    )


def _lark_ws_payload_to_body_dict(data: Any) -> dict | None:
    """Turn Lark SDK WS callback payload into the same dict shape as the HTTP webhook body.

    The sync callback often runs on a worker thread (no running asyncio loop); parsing here
    keeps a single code path before we hand off to the main loop.
    """
    raw_body = getattr(data, "raw_body", None)
    if isinstance(data, dict):
        if data.get("header") is not None or data.get("event") is not None:
            return data
        return None

    if raw_body:
        try:
            return json.loads(raw_body.decode("utf-8"))
        except Exception as e:
            logger.warning(f"[Feishu WS] Failed to parse raw_body: {e}")
            return None

    body_dict: dict = {}
    if hasattr(data, "header"):
        header_obj = data.header
        if hasattr(header_obj, "__dict__"):
            body_dict["header"] = dict(vars(header_obj))
        else:
            body_dict["header"] = {
                "event_type": getattr(header_obj, "event_type", "im.message.receive_v1"),
                "event_id": getattr(header_obj, "event_id", ""),
                "create_time": getattr(header_obj, "create_time", ""),
            }
        if "event_type" not in body_dict["header"]:
            body_dict["header"]["event_type"] = getattr(
                header_obj, "event_type", "im.message.receive_v1"
            )
    else:
        body_dict["header"] = {"event_type": "im.message.receive_v1"}

    if hasattr(data, "event"):
        ev = data.event
        if isinstance(ev, dict):
            body_dict["event"] = ev
        elif hasattr(ev, "__dict__"):
            body_dict["event"] = dict(vars(ev))
        else:
            body_dict["event"] = ev
    elif hasattr(data, "content") and isinstance(getattr(data, "content", None), str):
        try:
            body_dict["event"] = json.loads(data.content)
        except json.JSONDecodeError:
            body_dict["event"] = {"content": data.content}
    else:
        logger.warning(
            f"[Feishu WS] Unrecognized event payload (no raw_body / event): {type(data)}"
        )
        return None

    return body_dict


class FeishuWSManager:
    """Manages Feishu WebSocket clients for all agents."""

    def __init__(self):
        self._clients: dict[uuid.UUID, ws.Client] = {}
        self._tasks: dict[uuid.UUID, asyncio.Task] = {}
        # Lark invokes handle_message on a background thread; dispatch via this loop.
        self._main_loop: asyncio.AbstractEventLoop | None = None

    def _ensure_main_loop(self) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        if self._main_loop is None:
            self._main_loop = loop
            logger.info("[Feishu WS] Bound main asyncio loop for cross-thread dispatch")

    def _create_event_handler(self, agent_id: uuid.UUID) -> lark.EventDispatcherHandler:
        """Create an event dispatcher for a specific agent."""

        def handle_message(data: Any) -> None:
            """Handle im.message.receive_v1 events from Feishu WebSocket (may run off the main thread)."""
            body_dict = _lark_ws_payload_to_body_dict(data)
            if not body_dict:
                return

            logger.info(
                f"[Feishu WS] Received WS event for agent {agent_id}: "
                f"type={body_dict.get('header', {}).get('event_type', 'N/A')}"
            )

            loop = self._main_loop
            if loop is None or not loop.is_running():
                logger.error(
                    "[Feishu WS] Main event loop not ready; cannot dispatch Feishu event. "
                    "Ensure start_client/start_all ran on the FastAPI loop."
                )
                return

            def _log_future(fut: asyncio.Future) -> None:
                try:
                    fut.result()
                except asyncio.CancelledError:
                    pass
                except Exception:
                    logger.exception(f"[Feishu WS] process_feishu_event failed for agent {agent_id}")

            fut = asyncio.run_coroutine_threadsafe(
                self._async_handle_message(agent_id, body_dict), loop
            )
            fut.add_done_callback(_log_future)

        dispatcher = (
            lark.EventDispatcherHandler.builder("", "")
            .register_p2_customized_event("im.message.receive_v1", handle_message)
            .build()
        )
        return dispatcher

    async def _async_handle_message(self, agent_id: uuid.UUID, body_dict: dict) -> None:
        """Run DB-backed Feishu handler on the main loop (called via run_coroutine_threadsafe)."""
        try:
            event_type = body_dict.get("header", {}).get("event_type", "unknown")
            logger.info(f"[Feishu WS] Dispatching event for agent {agent_id}: {event_type}")

            from app.api.feishu import process_feishu_event

            async with async_session() as db:
                await process_feishu_event(agent_id, body_dict, db)

        except Exception as e:
            logger.exception(f"[Feishu WS] Error processing event for {agent_id}: {e}")

    async def start_client(
        self,
        agent_id: uuid.UUID,
        app_id: str,
        app_secret: str,
        stop_existing: bool = True,
    ):
        """Spawns a Feishu WebSocket client on the current asyncio loop."""
        self._ensure_main_loop()

        if not _HAS_LARK:
            logger.warning("[Feishu WS] lark-oapi not installed, cannot start client")
            return
        if not app_id or not app_secret:
            logger.warning(f"[Feishu WS] Missing app_id or app_secret for {agent_id}, skipping")
            return

        logger.info(f"[Feishu WS] Starting async WS client for agent {agent_id} (App ID: {app_id})")

        if stop_existing and agent_id in self._tasks:
            old_task = self._tasks.pop(agent_id, None)
            if old_task and not old_task.done():
                old_task.cancel()
                logger.info(f"[Feishu WS] Cancelled old WS task for {agent_id}")

        try:
            event_handler = self._create_event_handler(agent_id)
        except Exception as e:
            logger.exception(f"[Feishu WS] Failed to create event handler for {agent_id}: {e}")
            return

        client = ws.Client(
            app_id,
            app_secret,
            event_handler=event_handler,
            log_level=lark.LogLevel.INFO,
        )
        self._clients[agent_id] = client

        async def _run_async_client():
            try:
                await client._connect()
                asyncio.create_task(client._ping_loop())
                while True:
                    await asyncio.sleep(3600)
            except asyncio.CancelledError:
                logger.info(f"[Feishu WS] Async client task cancelled for {agent_id}")
                await client._disconnect()
                raise
            except Exception as e:
                logger.exception(f"[Feishu WS] Async client exception for {agent_id}: {e}")
                await client._disconnect()
                self._clients.pop(agent_id, None)

        task = asyncio.create_task(_run_async_client(), name=f"feishu-ws-async-{str(agent_id)[:8]}")
        self._tasks[agent_id] = task
        logger.info(f"[Feishu WS] Async WS task scheduled for agent {agent_id}")

    async def stop_client(self, agent_id: uuid.UUID):
        """Stops an actively running WebSocket client for an agent."""
        if agent_id in self._tasks:
            task = self._tasks.pop(agent_id, None)
            if task and not task.done():
                task.cancel()
                logger.info(f"[Feishu WS] Stopped client task for {agent_id}")
        if agent_id in self._clients:
            client = self._clients.pop(agent_id)
            try:
                await client._disconnect()
            except Exception as e:
                logger.error(f"[Feishu WS] Error disconnecting client for {agent_id}: {e}")

    async def start_all(self):
        """Start WS clients for all configured Feishu agents."""
        self._ensure_main_loop()

        if not _HAS_LARK:
            logger.info("[Feishu WS] lark-oapi not installed, skipping Feishu WS initialization")
            return
        logger.info("[Feishu WS] Initializing all active Feishu channels...")
        async with async_session() as db:
            result = await db.execute(
                select(ChannelConfig).where(
                    ChannelConfig.is_configured.is_(True),
                    ChannelConfig.channel_type == "feishu",
                )
            )
            configs = result.scalars().all()

        for config in configs:
            extra = config.extra_config or {}
            mode = str(extra.get("connection_mode") or "webhook").strip().lower()
            if mode == "websocket":
                if config.app_id and config.app_secret:
                    await self.start_client(
                        config.agent_id, config.app_id, config.app_secret, stop_existing=False
                    )
                else:
                    logger.warning(f"[Feishu WS] Skipping agent {config.agent_id}: missing credentials")

    def status(self) -> dict:
        """Return status of all active WS tasks."""
        return {str(aid): not self._tasks[aid].done() for aid in self._tasks}


feishu_ws_manager = FeishuWSManager()
