"""MCP (Model Context Protocol) Client — connects to external MCP servers.

Supports two transport modes:
1. Streamable HTTP (modern) — single URL, POST JSON-RPC, response as JSON or SSE
2. SSE Transport (legacy but widely used) — GET /sse for event stream, POST /messages for requests

Transport is auto-detected: tries Streamable HTTP first, falls back to SSE.
Reference: https://modelcontextprotocol.io/docs
"""

import httpx
import json
import asyncio
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

from loguru import logger


class MCPClient:
    """Client for connecting to MCP servers via Streamable HTTP or SSE transport.

    Auto-detects the transport mode on first request.
    """

    def __init__(self, server_url: str, api_key: str | None = None, transport: str | None = None):
        # Extract apiKey from URL query params and move to Authorization header
        parsed = urlparse(server_url)
        qs = parse_qs(parsed.query, keep_blank_values=True)

        self.api_key = api_key
        if not self.api_key and "apiKey" in qs:
            self.api_key = qs.pop("apiKey")[0]

        # Rebuild URL without apiKey in query string
        remaining_qs = urlencode({k: v[0] for k, v in qs.items()}) if qs else ""
        self.server_url = urlunparse(parsed._replace(query=remaining_qs)).rstrip("/")
        # User-configured root (never mutated) — used to try common subpaths like /mcp
        self._configured_root = self.server_url.rstrip("/")
        # Pinned after first successful connection (Streamable POST URL or SSE GET URL)
        self._streamable_endpoint: str | None = None
        self._sse_listen_url: str | None = None

        # Transport state ("streamable" | "sse" | None = auto-detect)
        hint = (transport or "").strip().lower()
        if hint in ("sse", "streamable", "streamable-http", "http"):
            self._transport = "sse" if hint == "sse" else "streamable"
        else:
            self._transport = None
        self._session_id: str | None = None
        self._sse_messages_url: str | None = None  # POST endpoint for SSE transport

    def _headers(self) -> dict:
        """Build request headers with proper MCP and auth headers."""
        h = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
        }
        if self.api_key:
            h["Authorization"] = f"Bearer {self.api_key}"
        if self._session_id:
            h["Mcp-Session-Id"] = self._session_id
        return h

    def _parse_response(self, resp: httpx.Response) -> dict:
        """Parse response — handles both JSON and SSE (text/event-stream) formats."""
        content_type = resp.headers.get("content-type", "")

        # Save session ID if the server returns one
        session_id = resp.headers.get("mcp-session-id")
        if session_id:
            self._session_id = session_id

        if "text/event-stream" in content_type:
            return self._parse_sse_response(resp.text)
        else:
            return resp.json()

    def _parse_sse_response(self, text: str) -> dict:
        """Extract the last JSON-RPC result from an SSE stream."""
        last_data = None
        for line in text.splitlines():
            if line.startswith("data:"):
                raw = line[5:].strip()
                if raw and raw != "[DONE]":
                    try:
                        last_data = json.loads(raw)
                    except json.JSONDecodeError:
                        pass
        if last_data is None:
            raise Exception("No valid JSON found in SSE response")
        return last_data

    def _streamable_post_candidates(self) -> list[str]:
        """URLs to try for Streamable HTTP POST (many servers use /mcp, not site root)."""
        u = self._configured_root.rstrip("/")
        out = [u]
        if not u.endswith("/mcp"):
            out.append(f"{u}/mcp")
        return list(dict.fromkeys(out))

    def _sse_listen_candidates(self) -> list[str]:
        """URLs to try for legacy SSE GET.

        Order covers: SQLBot-style GET on same base as config URL (see sqlbot.org MCP docs),
        /path/sse, /mcp/sse, and host /sse.
        """
        u = self._configured_root.rstrip("/")
        if u.endswith("/sse"):
            return [u]
        c: list[str] = []
        # e.g. SQLBot: url is http://host:8001/mcp; some clients use that exact URL for SSE (GET).
        if u.endswith("/mcp"):
            c.append(u)
        c.append(f"{u}/sse")
        if not u.endswith("/mcp"):
            c.append(f"{u}/mcp/sse")
        parsed = urlparse(u)
        root = f"{parsed.scheme}://{parsed.netloc}"
        root_sse = f"{root}/sse"
        if root_sse not in c:
            c.append(root_sse)
        return list(dict.fromkeys(c))

    # ── Streamable HTTP Transport ────────────────────────────────

    async def _streamable_initialize(self, client: httpx.AsyncClient, endpoint_url: str) -> None:
        """Send MCP initialize + initialized handshake (Streamable HTTP)."""
        try:
            resp = await client.post(
                endpoint_url,
                json={
                    "jsonrpc": "2.0",
                    "id": 0,
                    "method": "initialize",
                    "params": {
                        "protocolVersion": "2024-11-05",
                        "capabilities": {},
                        "clientInfo": {"name": "clawith", "version": "1.0"},
                    },
                },
                headers=self._headers(),
            )
            if resp.status_code == 200:
                self._parse_response(resp)  # captures Mcp-Session-Id if present
            # Send initialized notification (required by MCP spec before other requests)
            await client.post(
                endpoint_url,
                json={"jsonrpc": "2.0", "method": "notifications/initialized"},
                headers=self._headers(),
            )
        except Exception:
            pass  # initialization failure is non-fatal — server may be stateless

    async def _streamable_request(self, method: str, params: dict | None = None) -> dict:
        """Send a JSON-RPC request via Streamable HTTP transport."""
        endpoints = (
            [self._streamable_endpoint]
            if self._streamable_endpoint
            else self._streamable_post_candidates()
        )
        body: dict = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params or {}}
        last_err: Exception | None = None

        for ep in endpoints:
            try:
                async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
                    if not self._streamable_endpoint:
                        self._session_id = None
                    if not self._session_id:
                        await self._streamable_initialize(client, ep)

                    resp = await client.post(ep, json=body, headers=self._headers())
                    if resp.status_code not in (200, 201):
                        raise Exception(f"HTTP {resp.status_code}")
                    out = self._parse_response(resp)

                self._streamable_endpoint = ep
                self.server_url = ep.rstrip("/")
                return out
            except Exception as e:
                last_err = e
                if not self._streamable_endpoint:
                    self._session_id = None

        raise last_err if last_err else Exception("Streamable HTTP failed")

    # ── SSE Transport ────────────────────────────────────────────

    async def _sse_request_at(self, sse_url: str, method: str, params: dict | None = None) -> dict:
        """Send one JSON-RPC request via SSE transport using a specific SSE listen URL."""
        parsed = urlparse(sse_url)
        base_url = f"{parsed.scheme}://{parsed.netloc}"

        headers_sse = {"Accept": "text/event-stream"}
        headers_post = {"Content-Type": "application/json", "Accept": "application/json, text/event-stream"}
        if self.api_key:
            headers_sse["Authorization"] = f"Bearer {self.api_key}"
            headers_post["Authorization"] = f"Bearer {self.api_key}"

        body: dict = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params or {}}

        timeout = 60 if method == "tools/call" else 30

        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
            async with client.stream("GET", sse_url, headers=headers_sse) as sse_resp:
                if sse_resp.status_code != 200:
                    raise Exception(f"SSE connect failed: HTTP {sse_resp.status_code}")

                messages_url = None
                event_type = ""

                line_iter = sse_resp.aiter_lines()
                async for line in line_iter:
                    line = line.strip()
                    if line.startswith("event:"):
                        event_type = line[6:].strip()
                    elif line.startswith("data:"):
                        data = line[5:].strip()
                        if event_type == "endpoint" and data:
                            if data.startswith("http"):
                                messages_url = data
                            else:
                                messages_url = base_url + data
                            break

                if not messages_url:
                    raise Exception("SSE endpoint did not return a messages URL")

                init_body = {
                    "jsonrpc": "2.0",
                    "id": 0,
                    "method": "initialize",
                    "params": {
                        "protocolVersion": "2024-11-05",
                        "capabilities": {},
                        "clientInfo": {"name": "clawith", "version": "1.0"},
                    },
                }
                await client.post(messages_url, json=init_body, headers=headers_post)
                await client.post(
                    messages_url,
                    json={"jsonrpc": "2.0", "method": "notifications/initialized"},
                    headers=headers_post,
                )

                post_resp = await client.post(messages_url, json=body, headers=headers_post)

                if post_resp.status_code == 200:
                    ct = post_resp.headers.get("content-type", "")
                    if "application/json" in ct:
                        return post_resp.json()

                result = None
                async for line in line_iter:
                    line = line.strip()
                    if line.startswith("event:"):
                        event_type = line[6:].strip()
                    elif line.startswith("data:"):
                        data = line[5:].strip()
                        if event_type == "message" and data:
                            try:
                                parsed_data = json.loads(data)
                                if isinstance(parsed_data, dict) and parsed_data.get("id") in (0, 1):
                                    result = parsed_data
                                    if parsed_data.get("id") == 1:
                                        break
                            except json.JSONDecodeError:
                                pass

                if result is None:
                    raise Exception("No response received from SSE transport")
                return result

    async def _sse_request(self, method: str, params: dict | None = None) -> dict:
        """Send a JSON-RPC request via SSE transport (tries common listen paths)."""
        if self._sse_listen_url:
            listen_urls = [self._sse_listen_url]
        else:
            listen_urls = self._sse_listen_candidates()

        last_err: Exception | None = None
        for sse_url in listen_urls:
            try:
                out = await self._sse_request_at(sse_url, method, params)
                self._sse_listen_url = sse_url
                return out
            except Exception as e:
                last_err = e

        raise last_err if last_err else Exception("SSE transport failed")

    # ── Auto-detect Transport ────────────────────────────────────

    async def _detect_and_request(self, method: str, params: dict | None = None) -> dict:
        """Auto-detect transport and send request.

        Strategy: If transport is already known, use it directly.
        Otherwise try Streamable HTTP first, fall back to SSE.
        """
        if self._transport == "sse":
            return await self._sse_request(method, params)
        if self._transport == "streamable":
            return await self._streamable_request(method, params)

        # Auto-detect: try Streamable HTTP first
        # Exception names in `except X as e` are cleared when the handler ends (PEP 3110);
        # keep a copy for the nested handler below.
        streamable_failure: str | None = None
        try:
            result = await self._streamable_request(method, params)
            self._transport = "streamable"
            return result
        except Exception as streamable_err:
            streamable_failure = str(streamable_err)
            logger.info(
                f"[MCPClient] Streamable HTTP failed ({streamable_failure}), trying SSE transport..."
            )

        # Fallback to SSE
        try:
            result = await self._sse_request(method, params)
            self._transport = "sse"
            return result
        except Exception as sse_err:
            raise Exception(
                f"Both transports failed. "
                f"Streamable HTTP: {streamable_failure}; "
                f"SSE: {sse_err}"
            )

    # ── Public API ───────────────────────────────────────────────

    async def list_tools(self) -> list[dict]:
        """Fetch available tools from the MCP server."""
        try:
            data = await self._detect_and_request("tools/list")

            if "error" in data:
                err = data["error"]
                msg = err.get("message", str(err)) if isinstance(err, dict) else str(err)
                raise Exception(f"MCP error: {msg}")

            result = data.get("result", {})
            tools = result.get("tools", []) if isinstance(result, dict) else []
            return [
                {
                    "name": t.get("name", ""),
                    "description": t.get("description", ""),
                    "inputSchema": t.get("inputSchema", {}),
                }
                for t in tools
            ]
        except httpx.HTTPError as e:
            raise Exception(f"Connection failed: {str(e)[:200]}")

    async def call_tool(self, tool_name: str, arguments: dict) -> str:
        """Execute a tool on the MCP server."""
        try:
            data = await self._detect_and_request(
                "tools/call",
                {"name": tool_name, "arguments": arguments},
            )

            if "error" in data:
                err = data["error"]
                msg = err.get("message", str(err)) if isinstance(err, dict) else str(err)
                return f"❌ MCP tool execution error: {msg[:200]}"

            result = data.get("result", {})
            if isinstance(result, str):
                return result

            # MCP returns content as list of content blocks
            content_blocks = result.get("content", []) if isinstance(result, dict) else []
            texts = []
            for block in content_blocks:
                if isinstance(block, str):
                    texts.append(block)
                elif isinstance(block, dict):
                    if block.get("type") == "text":
                        texts.append(block.get("text", ""))
                    elif block.get("type") == "image":
                        texts.append(f"[Image: {block.get('mimeType', 'image')}]")
                    else:
                        texts.append(str(block))
                else:
                    texts.append(str(block))

            return "\n".join(texts) if texts else str(result)

        except httpx.HTTPError as e:
            return f"❌ MCP connection failed: {str(e)[:200]}"
