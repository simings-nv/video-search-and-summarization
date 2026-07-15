# TOOLS.md - VSS Hermes Tools

## VSS Orchestrator Command Bridge

The host-side VSS Orchestrator MCP server is exposed inside the sandbox through
this command:

```bash
/sandbox/bin/vss-orchestrator
```

The command talks to:

```text
http://host.openshell.internal:9988/mcp
```

Start the host MCP server before connecting to Hermes. If Hermes was already
connected, reconnect the session after setup changes.

Use the bridge for host Docker work. Do not run `docker compose`,
`deploy/docker/scripts/dev-profile.sh`, `nvidia-smi`, Docker prerequisite shell
probes, or raw host deployment commands from inside this sandbox.

## Commands

```bash
/sandbox/bin/vss-orchestrator list
/sandbox/bin/vss-orchestrator profiles
/sandbox/bin/vss-orchestrator prereqs
/sandbox/bin/vss-orchestrator docker_generate '{"profile":"base"}'
/sandbox/bin/vss-orchestrator docker_up '{"docker_compose_id":"<id>"}'
/sandbox/bin/vss-orchestrator docker_status '{"docker_compose_id":"<id>"}'
/sandbox/bin/vss-orchestrator docker_logs '{"docker_compose_id":"<id>"}'
/sandbox/bin/vss-orchestrator docker_down '{"docker_compose_id":"<id>"}'
```

Tool arguments are JSON objects. For larger payloads, write the JSON to a file
and pass `@path`, or pass `-` and provide JSON on stdin.

For long deploys, report one short progress update after each status poll and
continue until the operation reaches `success`, `error`, or `cancelled`.
