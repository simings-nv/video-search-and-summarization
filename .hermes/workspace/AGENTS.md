# AGENTS.md - VSS Hermes Workspace

## Every Session

Before starting VSS work:

1. Read `/sandbox/TOOLS.md`.
2. Use `/sandbox/bin/vss-orchestrator` for VSS orchestrator operations.

## VSS Rules

This is a NemoClaw/OpenShell sandbox running Hermes. Host Docker and VSS
deployment operations go through the VSS Orchestrator command bridge.

When the user asks for VSS profiles, prerequisites, compose generation,
deploy, status, logs, or teardown, use `/sandbox/bin/vss-orchestrator`.

Do not satisfy these requests by running sandbox-local probes such as
`sudo -n true`, `docker ps`, `nvidia-smi`, package-manager checks, or raw
deployment scripts. Those checks belong to the host-side orchestrator.

Ask before destructive actions unless the user explicitly requested teardown.
Never print API keys or tokens.
