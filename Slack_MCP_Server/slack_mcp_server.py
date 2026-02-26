#!/usr/bin/env python3
"""Slack Tools MCP Server - Read/post Slack messages and run keyword-triggered actions via polling."""

import os
import sys
import json
import re
import time
import asyncio
import logging
from datetime import datetime, timezone

import httpx
from mcp.server.fastmcp import FastMCP

# Configure logging to stderr
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("slack_mcp-server")

mcp = FastMCP("slack_mcp")

# --- Environment configuration ---
SLACK_BOT_TOKEN = os.environ.get("SLACK_BOT_TOKEN", "")
SLACK_API_BASE = os.environ.get("SLACK_API_BASE", "https://slack.com/api").rstrip("/")
SLACK_STATE_PATH = os.environ.get("SLACK_STATE_PATH", "/tmp/slack_mcp_state.json")

SLACK_DEFAULT_POLL_INTERVAL = os.environ.get("SLACK_DEFAULT_POLL_INTERVAL", "120")
SLACK_DEFAULT_PATTERN = os.environ.get("SLACK_DEFAULT_PATTERN", "")
SLACK_DEFAULT_PATTERN_MODE = os.environ.get("SLACK_DEFAULT_PATTERN_MODE", "keywords")  # keywords|regex
SLACK_DEFAULT_ACTION_TYPE = os.environ.get("SLACK_DEFAULT_ACTION_TYPE", "reaction")  # reaction|reply|forward
SLACK_DEFAULT_ACTION_VALUE = os.environ.get("SLACK_DEFAULT_ACTION_VALUE", "eyes")
SLACK_DEFAULT_IGNORE_BOTS = os.environ.get("SLACK_DEFAULT_IGNORE_BOTS", "true")
SLACK_DEFAULT_CHANNELS = os.environ.get("SLACK_DEFAULT_CHANNELS", "")  # comma-separated channel IDs or names

# --- In-process monitor state ---
_monitor_task = None
_monitor_stop_event = asyncio.Event()


# ---------------- Utilities ----------------
def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _sanitize_simple(s: str = "", max_len: str = "2000") -> str:
    try:
        ml = int(max_len) if str(max_len).strip() else 2000
    except Exception:
        ml = 2000
    if s is None:
        s = ""
    s = str(s)
    s = s.replace("\x00", "")
    if len(s) > ml:
        s = s[:ml]
    return s


def _truthy(s: str = "") -> bool:
    v = (s or "").strip().lower()
    return v in ("1", "true", "yes", "y", "on")


def _to_int(s: str = "", default: int = 0, min_value: int = 0, max_value: int = 10_000) -> int:
    try:
        v = int(str(s).strip()) if str(s).strip() else int(default)
    except Exception:
        v = int(default)
    if v < min_value:
        v = min_value
    if v > max_value:
        v = max_value
    return v


def _require_token() -> str:
    if not SLACK_BOT_TOKEN.strip():
        return "❌ Error: SLACK_BOT_TOKEN is not set. Configure it via Docker/MCP secrets."
    return ""


def _slack_headers() -> dict:
    return {
        "Authorization": f"Bearer {SLACK_BOT_TOKEN.strip()}",
        "Content-Type": "application/json; charset=utf-8",
        "User-Agent": "slack_mcp/1.0",
    }


def _format_message_brief(msg: dict) -> str:
    ts = msg.get("ts", "")
    user = msg.get("user", msg.get("username", ""))
    text = msg.get("text", "")
    text = _sanitize_simple(text, "400")
    subtype = msg.get("subtype", "")
    bot_id = msg.get("bot_id", "")
    flags = []
    if subtype:
        flags.append(f"subtype={subtype}")
    if bot_id:
        flags.append("bot")
    flag_txt = f" ({', '.join(flags)})" if flags else ""
    return f"- ts={ts} user={user}{flag_txt}: {text}"


async def _slack_api_call(endpoint: str = "", method: str = "GET", params: dict = None, payload: dict = None) -> dict:
    endpoint = _sanitize_simple(endpoint, "200")
    method = (_sanitize_simple(method, "10") or "GET").upper()
    if not endpoint.strip():
        return {"ok": False, "error": "missing_endpoint"}

    url = f"{SLACK_API_BASE}/{endpoint.lstrip('/')}"
    headers = _slack_headers()

    timeout = httpx.Timeout(20.0, connect=10.0)

    async with httpx.AsyncClient(timeout=timeout) as client:
        for attempt in range(1, 4):
            try:
                if method == "GET":
                    resp = await client.get(url, headers=headers, params=params or {})
                else:
                    resp = await client.post(url, headers=headers, json=payload or {})

                if resp.status_code == 429:
                    retry_after = resp.headers.get("Retry-After", "1")
                    wait_s = _to_int(retry_after, default=1, min_value=1, max_value=60)
                    logger.warning("Slack rate limited (429). Waiting %ss (attempt %s).", wait_s, attempt)
                    await asyncio.sleep(wait_s)
                    continue

                resp.raise_for_status()
                data = resp.json()
                return data
            except httpx.HTTPStatusError as e:
                logger.error("Slack HTTP error: %s %s -> %s", method, url, str(e))
                return {"ok": False, "error": f"http_status_{e.response.status_code}"}
            except Exception as e:
                logger.error("Slack request error: %s", str(e))
                if attempt < 3:
                    await asyncio.sleep(1.0 * attempt)
                    continue
                return {"ok": False, "error": "request_failed"}
    return {"ok": False, "error": "unknown_error"}


def _default_state() -> dict:
    return {
        "version": 1,
        "updatedAt": _now_iso(),
        "monitor": {
            "enabled": False,
            "channels": [c.strip() for c in (SLACK_DEFAULT_CHANNELS or "").split(",") if c.strip()],
            "pollIntervalSeconds": _to_int(SLACK_DEFAULT_POLL_INTERVAL, default=120, min_value=15, max_value=3600),
            "pattern": SLACK_DEFAULT_PATTERN or "",
            "patternMode": (SLACK_DEFAULT_PATTERN_MODE or "keywords").strip().lower(),
            "actionType": (SLACK_DEFAULT_ACTION_TYPE or "reaction").strip().lower(),
            "actionValue": SLACK_DEFAULT_ACTION_VALUE or "eyes",
            "ignoreBots": _truthy(SLACK_DEFAULT_IGNORE_BOTS),
            "lastSeenTsByChannel": {},
        },
    }


def _load_state() -> dict:
    try:
        if not os.path.exists(SLACK_STATE_PATH):
            return _default_state()
        with open(SLACK_STATE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return _default_state()
        if "monitor" not in data or not isinstance(data.get("monitor"), dict):
            data["monitor"] = _default_state()["monitor"]
        if "lastSeenTsByChannel" not in data["monitor"] or not isinstance(data["monitor"].get("lastSeenTsByChannel"), dict):
            data["monitor"]["lastSeenTsByChannel"] = {}
        return data
    except Exception as e:
        logger.error("Failed to load state: %s", str(e))
        return _default_state()


def _save_state(state: dict) -> bool:
    try:
        state["updatedAt"] = _now_iso()
        tmp_path = f"{SLACK_STATE_PATH}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, SLACK_STATE_PATH)
        return True
    except Exception as e:
        logger.error("Failed to save state: %s", str(e))
        return False


def _parse_channels(channels: str = "") -> list:
    channels = _sanitize_simple(channels, "2000")
    items = [c.strip() for c in channels.split(",") if c.strip()]
    return items


def _looks_like_channel_id(s: str = "") -> bool:
    s = (s or "").strip()
    if len(s) < 6:
        return False
    # Slack channel IDs often start with C (public), G (private), D (IM)
    return s[0] in ("C", "G", "D") and s.replace("_", "").isalnum()


async def _resolve_channel_id(channel: str = "") -> str:
    channel = _sanitize_simple(channel, "200").strip()
    if not channel:
        return ""
    if _looks_like_channel_id(channel):
        return channel
    if channel.startswith("#"):
        channel = channel[1:].strip()
    if not channel:
        return ""

    cursor = ""
    for _ in range(0, 10):
        params = {"limit": 200}
        if cursor:
            params["cursor"] = cursor
        data = await _slack_api_call("conversations.list", method="GET", params=params)
        if not data.get("ok"):
            return ""
        for ch in data.get("channels", []) or []:
            if (ch.get("name") or "").strip().lower() == channel.lower():
                return ch.get("id", "") or ""
        cursor = ((data.get("response_metadata") or {}).get("next_cursor") or "").strip()
        if not cursor:
            break
    return ""


def _compile_matcher(pattern: str = "", pattern_mode: str = "keywords"):
    pattern = _sanitize_simple(pattern, "400").strip()
    pattern_mode = (_sanitize_simple(pattern_mode, "20") or "keywords").strip().lower()

    if not pattern:
        return {"ok": True, "mode": pattern_mode, "matcher": None, "detail": "No pattern set; nothing will match."}

    if pattern_mode == "regex":
        try:
            rx = re.compile(pattern, re.IGNORECASE)
            return {"ok": True, "mode": "regex", "matcher": rx, "detail": f"Regex compiled: /{pattern}/i"}
        except Exception as e:
            return {"ok": False, "error": f"Invalid regex: {str(e)}"}

    # keywords mode
    keywords = [k.strip() for k in pattern.split(",") if k.strip()]
    if not keywords:
        return {"ok": True, "mode": "keywords", "matcher": [], "detail": "No valid keywords found; nothing will match."}
    keywords = keywords[:50]
    return {"ok": True, "mode": "keywords", "matcher": keywords, "detail": f"Keywords: {', '.join(keywords)}"}


def _text_matches(text: str, matcher_info: dict) -> bool:
    text = (text or "")
    if not matcher_info or not matcher_info.get("ok"):
        return False
    matcher = matcher_info.get("matcher")
    if matcher is None:
        return False
    mode = matcher_info.get("mode")
    if mode == "regex":
        try:
            return bool(matcher.search(text))
        except Exception:
            return False
    # keywords
    low = text.lower()
    for k in matcher:
        if k.lower() in low:
            return True
    return False


async def _monitor_act_on_message(channel_id: str, msg: dict, monitor_cfg: dict) -> str:
    action_type = (monitor_cfg.get("actionType") or "reaction").strip().lower()
    action_value = monitor_cfg.get("actionValue") or "eyes"

    ts = msg.get("ts", "")
    if not ts:
        return "⚠️ Skipped message without ts."

    if action_type == "reaction":
        reaction = _sanitize_simple(action_value, "50").strip().strip(":")
        if not reaction:
            reaction = "eyes"
        data = await _slack_api_call(
            "reactions.add",
            method="POST",
            payload={"channel": channel_id, "timestamp": ts, "name": reaction},
        )
        if data.get("ok"):
            return f"✅ Added reaction :{reaction}: to ts={ts}"
        return f"❌ Failed to add reaction to ts={ts} (error={data.get('error','unknown')})"

    if action_type == "reply":
        reply_text = _sanitize_simple(action_value, "2000").strip()
        if not reply_text:
            reply_text = "Thanks—flagging this for review."
        data = await _slack_api_call(
            "chat.postMessage",
            method="POST",
            payload={"channel": channel_id, "thread_ts": ts, "text": reply_text},
        )
        if data.get("ok"):
            return f"✅ Replied in thread ts={ts}"
        return f"❌ Failed to reply to ts={ts} (error={data.get('error','unknown')})"

    if action_type == "forward":
        target = _sanitize_simple(action_value, "200").strip()
        target_id = await _resolve_channel_id(target) if target else ""
        if not target_id:
            return "❌ Forward failed: actionValue must be a valid target channel ID or #name."
        text = _sanitize_simple(msg.get("text", ""), "1500").strip()
        forward_text = f"Forwarded from <#{channel_id}> ts={ts}:\n{text}"
        data = await _slack_api_call(
            "chat.postMessage",
            method="POST",
            payload={"channel": target_id, "text": forward_text},
        )
        if data.get("ok"):
            return f"✅ Forwarded message ts={ts} to <#{target_id}>"
        return f"❌ Failed to forward ts={ts} (error={data.get('error','unknown')})"

    return f"⚠️ Unknown actionType={action_type}; no action taken."


async def _poll_once(state: dict) -> list:
    monitor_cfg = state.get("monitor") or {}
    channels = monitor_cfg.get("channels") or []
    ignore_bots = bool(monitor_cfg.get("ignoreBots", True))
    last_seen = monitor_cfg.get("lastSeenTsByChannel") or {}
    pattern = monitor_cfg.get("pattern") or ""
    pattern_mode = monitor_cfg.get("patternMode") or "keywords"
    matcher_info = _compile_matcher(pattern, pattern_mode)

    results = []
    if not matcher_info.get("ok"):
        results.append(f"❌ Monitor pattern error: {matcher_info.get('error','invalid_pattern')}")
        return results

    for ch in channels:
        ch = (ch or "").strip()
        if not ch:
            continue
        channel_id = await _resolve_channel_id(ch)
        if not channel_id:
            results.append(f"❌ Could not resolve channel: {ch}")
            continue

        oldest = (last_seen.get(channel_id) or "").strip()
        params = {"channel": channel_id, "limit": 200}
        if oldest:
            params["oldest"] = oldest

        data = await _slack_api_call("conversations.history", method="GET", params=params)
        if not data.get("ok"):
            results.append(f"❌ Read failed for <#{channel_id}> (error={data.get('error','unknown')})")
            continue

        messages = data.get("messages", []) or []
        # Slack returns newest-first; process oldest-first
        messages = list(reversed(messages))

        max_ts = oldest
        acted = 0
        scanned = 0

        for msg in messages:
            scanned += 1
            ts = (msg.get("ts") or "").strip()
            if ts and (not max_ts or float(ts) > float(max_ts)):
                max_ts = ts

            if ignore_bots and (msg.get("bot_id") or msg.get("subtype") in ("bot_message", "message_changed")):
                continue

            text = msg.get("text", "") or ""
            if _text_matches(text, matcher_info):
                action_res = await _monitor_act_on_message(channel_id, msg, monitor_cfg)
                logger.info("Monitor action: %s", action_res)
                results.append(f"⚡ <#{channel_id}> {action_res}")
                acted += 1

        if max_ts:
            last_seen[channel_id] = max_ts

        results.append(f"🔍 <#{channel_id}> scanned={scanned} acted={acted} lastSeenTs={last_seen.get(channel_id,'')}")
        state["monitor"]["lastSeenTsByChannel"] = last_seen
        _save_state(state)

    return results


async def _monitor_loop():
    global _monitor_task
    logger.info("Monitor loop started.")
    try:
        while not _monitor_stop_event.is_set():
            state = _load_state()
            monitor_cfg = state.get("monitor") or {}
            if not bool(monitor_cfg.get("enabled", False)):
                await asyncio.sleep(2.0)
                continue

            interval = _to_int(str(monitor_cfg.get("pollIntervalSeconds", "120")), default=120, min_value=15, max_value=3600)

            try:
                await _poll_once(state)
            except Exception as e:
                logger.error("Monitor poll error: %s", str(e))

            await asyncio.sleep(float(interval))
    finally:
        logger.info("Monitor loop stopped.")
        _monitor_task = None


# ---------------- MCP Tools ----------------
@mcp.tool()
async def slack_healthcheck() -> str:
    """Check server health and Slack auth presence."""
    token_err = _require_token()
    if token_err:
        return token_err
    return "✅ slack_mcp is running and SLACK_BOT_TOKEN is set."


@mcp.tool()
async def slack_list_channels(limit: str = "200", include_archived: str = "false") -> str:
    """List Slack channels visible to the bot."""
    token_err = _require_token()
    if token_err:
        return token_err

    lim = _to_int(limit, default=200, min_value=1, max_value=1000)
    inc_arch = _truthy(include_archived)

    channels_out = []
    cursor = ""
    fetched = 0

    for _ in range(0, 10):
        params = {"limit": min(200, lim - fetched) if lim - fetched > 0 else 1, "exclude_archived": (0 if inc_arch else 1)}
        if cursor:
            params["cursor"] = cursor
        data = await _slack_api_call("conversations.list", method="GET", params=params)
        if not data.get("ok"):
            return f"❌ Slack API error: {data.get('error','unknown')}"
        for ch in data.get("channels", []) or []:
            if fetched >= lim:
                break
            channels_out.append(f"- {ch.get('name','')} (id={ch.get('id','')}, private={bool(ch.get('is_private', False))})")
            fetched += 1
        cursor = ((data.get("response_metadata") or {}).get("next_cursor") or "").strip()
        if not cursor or fetched >= lim:
            break

    if not channels_out:
        return "⚠️ No channels returned (bot may not have access or is not in any channels)."

    return "✅ Channels:\n" + "\n".join(channels_out)


@mcp.tool()
async def slack_get_channel_info(channel: str = "") -> str:
    """Get Slack channel info by channel ID or #name."""
    token_err = _require_token()
    if token_err:
        return token_err

    channel = _sanitize_simple(channel, "200").strip()
    if not channel:
        return "❌ Error: channel is required (ID like C123... or #name)."

    channel_id = await _resolve_channel_id(channel)
    if not channel_id:
        return f"❌ Error: Could not resolve channel: {channel}"

    data = await _slack_api_call("conversations.info", method="GET", params={"channel": channel_id})
    if not data.get("ok"):
        return f"❌ Slack API error: {data.get('error','unknown')}"

    ch = data.get("channel") or {}
    return (
        "✅ Channel info:\n"
        f"- name: {ch.get('name','')}\n"
        f"- id: {ch.get('id','')}\n"
        f"- is_private: {bool(ch.get('is_private', False))}\n"
        f"- is_archived: {bool(ch.get('is_archived', False))}\n"
        f"- created: {ch.get('created','')}\n"
        f"- topic: {(ch.get('topic') or {}).get('value','')}\n"
        f"- purpose: {(ch.get('purpose') or {}).get('value','')}"
    )


@mcp.tool()
async def slack_read_recent_messages(channel: str = "", limit: str = "20", oldest: str = "") -> str:
    """Read recent Slack messages from a channel (optionally from oldest timestamp)."""
    token_err = _require_token()
    if token_err:
        return token_err

    channel = _sanitize_simple(channel, "200").strip()
    if not channel:
        return "❌ Error: channel is required (ID like C123... or #name)."

    lim = _to_int(limit, default=20, min_value=1, max_value=200)
    oldest = _sanitize_simple(oldest, "50").strip()

    channel_id = await _resolve_channel_id(channel)
    if not channel_id:
        return f"❌ Error: Could not resolve channel: {channel}"

    params = {"channel": channel_id, "limit": lim}
    if oldest:
        params["oldest"] = oldest

    data = await _slack_api_call("conversations.history", method="GET", params=params)
    if not data.get("ok"):
        return f"❌ Slack API error: {data.get('error','unknown')}"

    msgs = data.get("messages", []) or []
    if not msgs:
        return f"⚠️ No messages found in <#{channel_id}>."

    lines = ["✅ Messages:"]
    for msg in reversed(msgs):
        lines.append(_format_message_brief(msg))
    return "\n".join(lines)


@mcp.tool()
async def slack_post_message(channel: str = "", text: str = "") -> str:
    """Post a new Slack message to a channel."""
    token_err = _require_token()
    if token_err:
        return token_err

    channel = _sanitize_simple(channel, "200").strip()
    text = _sanitize_simple(text, "3000").strip()

    if not channel:
        return "❌ Error: channel is required (ID like C123... or #name)."
    if not text:
        return "❌ Error: text is required."

    channel_id = await _resolve_channel_id(channel)
    if not channel_id:
        return f"❌ Error: Could not resolve channel: {channel}"

    data = await _slack_api_call("chat.postMessage", method="POST", payload={"channel": channel_id, "text": text})
    if not data.get("ok"):
        return f"❌ Slack API error: {data.get('error','unknown')}"

    ts = (data.get("ts") or "").strip()
    return f"✅ Posted message to <#{channel_id}> (ts={ts})"


@mcp.tool()
async def slack_reply_in_thread(channel: str = "", thread_ts: str = "", text: str = "") -> str:
    """Reply to a Slack message thread by thread_ts."""
    token_err = _require_token()
    if token_err:
        return token_err

    channel = _sanitize_simple(channel, "200").strip()
    thread_ts = _sanitize_simple(thread_ts, "50").strip()
    text = _sanitize_simple(text, "3000").strip()

    if not channel:
        return "❌ Error: channel is required (ID like C123... or #name)."
    if not thread_ts:
        return "❌ Error: thread_ts is required."
    if not text:
        return "❌ Error: text is required."

    channel_id = await _resolve_channel_id(channel)
    if not channel_id:
        return f"❌ Error: Could not resolve channel: {channel}"

    data = await _slack_api_call(
        "chat.postMessage",
        method="POST",
        payload={"channel": channel_id, "thread_ts": thread_ts, "text": text},
    )
    if not data.get("ok"):
        return f"❌ Slack API error: {data.get('error','unknown')}"

    ts = (data.get("ts") or "").strip()
    return f"✅ Replied in thread in <#{channel_id}> (thread_ts={thread_ts}, ts={ts})"


@mcp.tool()
async def slack_add_reaction(channel: str = "", timestamp: str = "", reaction: str = "eyes") -> str:
    """Add a reaction emoji to a Slack message."""
    token_err = _require_token()
    if token_err:
        return token_err

    channel = _sanitize_simple(channel, "200").strip()
    timestamp = _sanitize_simple(timestamp, "50").strip()
    reaction = _sanitize_simple(reaction, "50").strip().strip(":")

    if not channel:
        return "❌ Error: channel is required (ID like C123... or #name)."
    if not timestamp:
        return "❌ Error: timestamp is required (message ts)."
    if not reaction:
        return "❌ Error: reaction is required (e.g., eyes)."

    channel_id = await _resolve_channel_id(channel)
    if not channel_id:
        return f"❌ Error: Could not resolve channel: {channel}"

    data = await _slack_api_call(
        "reactions.add",
        method="POST",
        payload={"channel": channel_id, "timestamp": timestamp, "name": reaction},
    )
    if not data.get("ok"):
        return f"❌ Slack API error: {data.get('error','unknown')}"

    return f"✅ Added :{reaction}: to message ts={timestamp} in <#{channel_id}>"


@mcp.tool()
async def slack_get_permalink(channel: str = "", message_ts: str = "") -> str:
    """Get a permalink URL for a Slack message."""
    token_err = _require_token()
    if token_err:
        return token_err

    channel = _sanitize_simple(channel, "200").strip()
    message_ts = _sanitize_simple(message_ts, "50").strip()

    if not channel:
        return "❌ Error: channel is required (ID like C123... or #name)."
    if not message_ts:
        return "❌ Error: message_ts is required."

    channel_id = await _resolve_channel_id(channel)
    if not channel_id:
        return f"❌ Error: Could not resolve channel: {channel}"

    data = await _slack_api_call("chat.getPermalink", method="GET", params={"channel": channel_id, "message_ts": message_ts})
    if not data.get("ok"):
        return f"❌ Slack API error: {data.get('error','unknown')}"

    return f"✅ Permalink: {data.get('permalink','')}"


@mcp.tool()
async def slack_set_monitor_config(
    channels: str = "",
    pattern: str = "",
    pattern_mode: str = "keywords",
    action_type: str = "reaction",
    action_value: str = "eyes",
    poll_interval_seconds: str = "120",
    ignore_bots: str = "true",
) -> str:
    """Configure polling monitor (channels, keyword/regex pattern, and action)."""
    token_err = _require_token()
    if token_err:
        return token_err

    channels = _sanitize_simple(channels, "2000").strip()
    pattern = _sanitize_simple(pattern, "400").strip()
    pattern_mode = (_sanitize_simple(pattern_mode, "20") or "keywords").strip().lower()
    action_type = (_sanitize_simple(action_type, "20") or "reaction").strip().lower()
    action_value = _sanitize_simple(action_value, "2000").strip()
    poll_interval_seconds = _sanitize_simple(poll_interval_seconds, "10").strip()
    ignore_bots_bool = _truthy(ignore_bots)

    if pattern_mode not in ("keywords", "regex"):
        return "❌ Error: pattern_mode must be 'keywords' or 'regex'."
    if action_type not in ("reaction", "reply", "forward"):
        return "❌ Error: action_type must be 'reaction', 'reply', or 'forward'."

    interval = _to_int(poll_interval_seconds, default=120, min_value=15, max_value=3600)

    state = _load_state()
    monitor_cfg = state.get("monitor") or {}

    if channels.strip():
        monitor_cfg["channels"] = _parse_channels(channels)

    monitor_cfg["pattern"] = pattern
    monitor_cfg["patternMode"] = pattern_mode
    monitor_cfg["actionType"] = action_type
    monitor_cfg["actionValue"] = action_value if action_value else monitor_cfg.get("actionValue", "eyes")
    monitor_cfg["pollIntervalSeconds"] = interval
    monitor_cfg["ignoreBots"] = ignore_bots_bool

    state["monitor"] = monitor_cfg
    ok = _save_state(state)

    matcher_info = _compile_matcher(pattern, pattern_mode)
    if not matcher_info.get("ok"):
        return f"❌ Saved config, but pattern is invalid: {matcher_info.get('error','invalid_pattern')}"

    return (
        "✅ Monitor configuration saved.\n"
        f"- channels: {', '.join(monitor_cfg.get('channels') or []) or '(not set)'}\n"
        f"- pollIntervalSeconds: {monitor_cfg.get('pollIntervalSeconds')}\n"
        f"- ignoreBots: {bool(monitor_cfg.get('ignoreBots', True))}\n"
        f"- patternMode: {pattern_mode}\n"
        f"- pattern: {pattern or '(empty)'}\n"
        f"- actionType: {action_type}\n"
        f"- actionValue: {monitor_cfg.get('actionValue','')}\n"
        f"- stateSaved: {ok}\n"
        f"- matcher: {matcher_info.get('detail','')}"
    )


@mcp.tool()
async def slack_get_monitor_config() -> str:
    """Get current polling monitor configuration."""
    state = _load_state()
    m = state.get("monitor") or {}
    return (
        "📊 Monitor configuration:\n"
        f"- enabled: {bool(m.get('enabled', False))}\n"
        f"- channels: {', '.join(m.get('channels') or []) or '(not set)'}\n"
        f"- pollIntervalSeconds: {m.get('pollIntervalSeconds', '')}\n"
        f"- ignoreBots: {bool(m.get('ignoreBots', True))}\n"
        f"- patternMode: {m.get('patternMode', '')}\n"
        f"- pattern: {m.get('pattern', '') or '(empty)'}\n"
        f"- actionType: {m.get('actionType', '')}\n"
        f"- actionValue: {m.get('actionValue', '')}\n"
        f"- statePath: {SLACK_STATE_PATH}\n"
        f"- updatedAt: {state.get('updatedAt','')}"
    )


@mcp.tool()
async def slack_start_monitor() -> str:
    """Start the background polling monitor loop."""
    global _monitor_task
    token_err = _require_token()
    if token_err:
        return token_err

    state = _load_state()
    m = state.get("monitor") or {}
    channels = m.get("channels") or []
    if not channels:
        return "❌ Error: Monitor channels not configured. Use slack_set_monitor_config(channels=...)."

    m["enabled"] = True
    state["monitor"] = m
    _save_state(state)

    if _monitor_task and not _monitor_task.done():
        return "✅ Monitor already running."

    _monitor_stop_event.clear()
    _monitor_task = asyncio.create_task(_monitor_loop())
    return "✅ Monitor started (polling in background)."


@mcp.tool()
async def slack_stop_monitor() -> str:
    """Stop the background polling monitor loop."""
    global _monitor_task
    state = _load_state()
    m = state.get("monitor") or {}
    m["enabled"] = False
    state["monitor"] = m
    _save_state(state)

    if not _monitor_task or _monitor_task.done():
        _monitor_stop_event.set()
        return "✅ Monitor is not running (disabled in config)."

    _monitor_stop_event.set()
    try:
        await asyncio.wait_for(_monitor_task, timeout=5.0)
    except Exception:
        pass
    return "✅ Monitor stop requested (disabled in config)."


@mcp.tool()
async def slack_monitor_status() -> str:
    """Get background monitor runtime status."""
    state = _load_state()
    m = state.get("monitor") or {}
    running = bool(_monitor_task and not _monitor_task.done())
    return (
        "📊 Monitor status:\n"
        f"- enabledInConfig: {bool(m.get('enabled', False))}\n"
        f"- runningInProcess: {running}\n"
        f"- pollIntervalSeconds: {m.get('pollIntervalSeconds', '')}\n"
        f"- channels: {', '.join(m.get('channels') or []) or '(not set)'}\n"
        f"- statePath: {SLACK_STATE_PATH}\n"
        f"- updatedAt: {state.get('updatedAt','')}"
    )


@mcp.tool()
async def slack_poll_once_now() -> str:
    """Run one polling cycle immediately and report actions taken."""
    token_err = _require_token()
    if token_err:
        return token_err

    state = _load_state()
    m = state.get("monitor") or {}
    if not (m.get("channels") or []):
        return "❌ Error: Monitor channels not configured. Use slack_set_monitor_config(channels=...)."

    lines = await _poll_once(state)
    if not lines:
        return "⚠️ Poll completed with no output."
    return "✅ Poll completed:\n" + "\n".join(lines)


# --- Server startup ---
if __name__ == "__main__":
    logger.info("Starting Slack Tools MCP server (slack_mcp)...")
    if not SLACK_BOT_TOKEN.strip():
        logger.warning("SLACK_BOT_TOKEN is not set. Tools will return an error until configured.")
    try:
        mcp.run(transport="stdio")
    except Exception as e:
        logger.error("Server error: %s", str(e), exc_info=True)
        sys.exit(1)