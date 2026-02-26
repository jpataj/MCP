
# VS Code (Cline) Integration Notes for slack_mcp

This project is designed to be used with Cline in VS Code to call Slack tools.

## Key Files
- `slack_mcp_server.py` — MCP server implementation (stdio transport)
- `Dockerfile` — Container image runs as non-root user
- `requirements.txt` — Python dependencies

## Secrets and Configuration
Use Docker/MCP secrets for:
- `SLACK_BOT_TOKEN`

Non-secret env vars (optional):
- `SLACK_STATE_PATH`
- `SLACK_DEFAULT_POLL_INTERVAL`

## Monitor Behavior
- The monitor is a background asyncio task started via `slack_start_monitor`.
- It polls `conversations.history` every `pollIntervalSeconds` (default 120s).
- It ignores bot/app messages by default.
- It persists per-channel last-seen timestamps in `SLACK_STATE_PATH` so it can continue after restarts.

## Safe Configuration Pattern
1) Use `slack_list_channels` to discover channel names/ids the bot can access.
2) Set monitor config using IDs or #names.
3) Start monitor.
4) Validate with `slack_poll_once_now` and `slack_monitor_status`.

## Adding New Tools
1) Implement a new async function in `slack_mcp_server.py`
2) Decorate with `@mcp.tool()`
3) Ensure:
   - Single-line docstring
   - Only simple parameters with default empty strings (or simple defaults like "120")
   - Return type is always a string
   - Graceful error handling
4) Rebuild the Docker image
5) Update your MCP catalog entry tool list

## Notes on Slack API Scopes
If a tool fails with Slack `missing_scope`, add the required scope in your Slack app config, reinstall the app, and retry.

## Instalation Instruction

### Clone the repo
```bash
git clone https://github.com/jpataj/MCP.git
cd MCP/Slack_MCP_Server
 ```
