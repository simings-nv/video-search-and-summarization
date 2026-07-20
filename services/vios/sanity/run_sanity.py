# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Orchestrate a VIOS+NVStreamer sanity run: drive each use-case, capture evidence,
and emit a PDF (snapshots + http links). Assumes a running, overlay-capable
deployment (VST reachable; metadata backends per SanityContext). Deployment /
reconfiguration is a separate concern (see the vios-sanity skill).

  python3 run_sanity.py --base-url http://localhost:30888 --broker redis \
      --stream-id warehouse_sample --out /tmp/vios_sanity/report.pdf
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from sanity_common import SanityContext, run_usecase
from usecases import USECASES
from report import build_pdf

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("sanity")

_RTSP_COPIES = 4   # identical copies made for single-file / sync_wall provisioning

# VIOS (in-container) reaches the host-side fake-ES over the docker bridge; matches the
# metadata_service --es-port and index. Wired into overlay.video_metadata_server so the
# download/replay overlay path queries it (the live/recent path uses the broker instead).
_FAKE_ES = "172.17.0.1:19200/mdx-bev-test*"


def _find_chrome():
    """A codec-capable browser for WebRTC capture (H.264/H.265): VIOS_SANITY_CHROME override,
    then system Google Chrome; else None (Playwright's bundled Chromium -> black WebRTC panel)."""
    import os
    import shutil
    return (os.environ.get("VIOS_SANITY_CHROME") or shutil.which("google-chrome")
            or shutil.which("google-chrome-stable")
            or ("/opt/google/chrome/chrome" if os.path.exists("/opt/google/chrome/chrome") else None))


def _check_prereqs():
    """Fail fast with a clear message if a runtime prerequisite is missing (a WARNING for
    Chrome, which only WebRTC capture needs). Points at --install-deps."""
    import importlib
    import shutil
    missing = [t for t in ("ffmpeg", "docker") if not shutil.which(t)]
    for mod, pip in (("av", "av"), ("requests", "requests"), ("yaml", "PyYAML"),
                     ("playwright", "playwright"), ("redis", "redis")):
        try:
            importlib.import_module(mod)
        except Exception:  # noqa: BLE001
            missing.append(f"python:{pip}")
    if not _find_chrome():
        log.warning("Google Chrome not found -> WebRTC capture renders black. Install Chrome or "
                    "set VIOS_SANITY_CHROME (run_sanity.py --install-deps installs it).")
    if missing:
        log.error("missing prerequisites: %s", ", ".join(missing))
        log.error("install everything with:  python3 services/vios/sanity/run_sanity.py --install-deps")
        raise SystemExit(2)


def _start_file_server(host_ip=""):
    """Serve the evidence share_dir over HTTP so the PDF's links resolve, without the user
    starting a server by hand. Daemon thread (dies with the process). No-op if an external
    server is set via VIOS_SANITY_FILE_SERVER, or if the port is already taken."""
    import functools
    import os
    import threading
    from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
    if os.environ.get("VIOS_SANITY_FILE_SERVER"):
        return
    ctx = SanityContext(host_ip=host_ip)
    share, port = str(ctx.share_dir), int(os.environ.get("VIOS_SANITY_FILE_SERVER_PORT", "18080"))
    handler = functools.partial(SimpleHTTPRequestHandler, directory=share)
    try:
        srv = ThreadingHTTPServer(("0.0.0.0", port), handler)
    except OSError as e:  # noqa: BLE001
        log.warning("evidence file server not started on :%d (%s); assuming one is already up", port, e)
        return
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    log.info("serving evidence: %s -> http://%s:%d/", share, ctx.host_ip, port)


def _install_deps():
    """Install ALL prerequisites: pip deps, the Playwright browser, ffmpeg, and Google Chrome.
    ffmpeg/Chrome go through install_deps.sh (apt/.deb; Debian/Ubuntu, may prompt for sudo)."""
    import shutil
    import subprocess
    import sys
    here = Path(__file__).resolve().parent

    def run(cmd):
        log.info("$ %s", " ".join(cmd))
        return subprocess.run(cmd, check=False).returncode

    run([sys.executable, "-m", "pip", "install", "-r", str(here / "requirements.txt")])
    run([sys.executable, "-m", "playwright", "install", "chromium"])
    script = here / "install_deps.sh"
    if script.exists():
        run(["bash", str(script)])
    ok = bool(shutil.which("ffmpeg")) and bool(_find_chrome())
    log.info("dependency install complete (ffmpeg=%s, chrome=%s)",
             bool(shutil.which("ffmpeg")), bool(_find_chrome()))
    return 0 if ok else 1


def _provision(ctx, video_path, copies, sync_wall=False, max_streams=4, variants=False,
               deploy_vios=None):
    from provision import provision
    try:
        info = provision(ctx, Path(video_path), n_copies=copies, sync_wall=sync_wall,
                         max_streams=max_streams, variants=variants, deploy_vios=deploy_vios)
        ctx.provisioned_streams = info["rtsp_streams"]
        ctx.file_sensor = info["file_sensor"]
        log.info("provisioned streams=%s file_sensor=%s stream_id=%s",
                 ctx.provisioned_streams, ctx.file_sensor, ctx.stream_id)
    except Exception as e:  # noqa: BLE001
        log.warning("provisioning failed (continuing): %s", e)


def _dump_results(results, plan_meta, ctx, when, path):
    """Persist a run so the PDF can be re-rendered later WITHOUT re-running."""
    data = {"when": when, "host_ip": ctx.host_ip, "base_url": ctx.base_url,
            "stream_id": ctx.stream_id, "broker": ctx.broker, "plan_meta": plan_meta,
            "results": [{"name": r.name, "status": r.status, "detail": r.detail,
                         "duration_s": r.duration_s, "image": str(r.image) if r.image else None,
                         "links": r.links, "plan": r.plan, "group": r.group,
                         "metrics": r.metrics, "evidence": r.evidence,
                         "request": getattr(r, "request", {}) or {}}
                        for r in results]}
    Path(path).write_text(json.dumps(data, indent=2))
    log.info("saved results -> %s (re-render with --from-json)", path)


def _plan_tag(name: str) -> str:
    """Short filesystem tag for a plan (e.g. 'Plan-2 | ... kafka' -> 'plan2')."""
    import re
    m = re.search(r"[Pp]lan-?(\d+)", name or "")
    return f"plan{m.group(1)}" if m else re.sub(r"[^a-z0-9]+", "_", (name or "sanity").lower())[:16]


def _capture_container_logs(ctx, containers=("sensor-ms", "streamprocessing-ms-1"), tag=""):
    """Save the FULL logs (from container start -- no --tail) of the key VIOS containers so a
    reader can trace a failed case to the service-side cause from the very beginning. MUST be
    called while the containers are still alive (a stop --clean or restore_configs recreate
    replaces them and drops the history). `tag` namespaces the file per plan. Returns
    {container: http_link}."""
    import subprocess
    links = {}
    suffix = f"_{tag}" if tag else ""
    for c in containers:
        try:
            out = subprocess.run(["docker", "logs", c],   # full log, from container start
                                 capture_output=True, text=True, timeout=120)
            log_path = ctx.out_dir / f"{c}{suffix}.log"
            log_path.write_text((out.stdout or "") + (out.stderr or ""))
            links[c] = ctx.publish(log_path, f"vios_sanity_{c}{suffix}.log")
        except Exception as e:  # noqa: BLE001
            log.warning("capture logs for %s failed: %s", c, e)
    return links


def _dump_failures(results, ctx, path):
    """Write a failures manifest -- every FAIL with the plan, stream/group, the exact
    request (api + startTime/endTime/params/streamid), and the error detail -- and serve it.
    Container logs are captured PER PLAN during the run (while alive), not here. Returns
    {'link', 'count'}."""
    fails = [r for r in results if r.status == "FAIL"]
    rows = [{"name": r.name, "plan": getattr(r, "plan", "") or "", "status": r.status,
             "group": getattr(r, "group", "") or "",
             "request": getattr(r, "request", {}) or {}, "detail": r.detail}
            for r in fails]
    Path(path).write_text(json.dumps(rows, indent=2))
    if not fails:
        log.info("no failed cases")
        return {"link": "", "count": 0}
    link = ""
    try:
        link = ctx.publish(Path(path), "vios_sanity_failed_cases.json")
    except Exception as e:  # noqa: BLE001
        log.warning("publish failures manifest failed: %s", e)
    log.info("%d failed case(s) -> %s", len(fails), link or path)
    return {"link": link, "count": len(fails)}


def _load_results(path):
    from sanity_common import UseCaseResult
    d = json.loads(Path(path).read_text())
    results = []
    for x in d["results"]:
        r = UseCaseResult(name=x["name"], status=x["status"], detail=x["detail"])
        r.duration_s = x.get("duration_s", 0.0)
        r.image = Path(x["image"]) if x.get("image") else None
        r.links = x.get("links", []); r.plan = x.get("plan", ""); r.group = x.get("group", "")
        r.metrics = x.get("metrics", {}); r.evidence = x.get("evidence", False)
        r.request = x.get("request", {})
        results.append(r)
    ctx = SanityContext(host_ip=d["host_ip"], base_url=d["base_url"],
                        stream_id=d["stream_id"], broker=d["broker"])
    return results, d.get("plan_meta", {}), ctx, d["when"]


def _health(base_url, timeout: int = 120):
    """Wait (retrying) until VIOS answers health, so a fresh deploy is fully up before
    provisioning. The oneclick deploy already blocks on docker healthchecks, but the HTTP
    API can lag a few seconds past 'healthy'."""
    import requests
    import time as _t
    deadline = _t.time() + timeout
    while _t.time() < deadline:
        try:
            h = requests.get(f"{base_url}/health", timeout=10)
            if h.status_code == 200:
                log.info("VST %s /health -> 200", base_url)
                return
        except Exception:  # noqa: BLE001
            pass
        _t.sleep(3)
    log.warning("VST health not ready after %ds (%s); continuing", timeout, base_url)


def _start_metadata_service(ctx, wait_s: int = 12):
    """Start the EVENT-DRIVEN overlay plugin (the DeepStream stand-in): it subscribes to
    VIOS's camera_streaming events and, per stream, reads its SEI off the VIOS rtsp proxy and
    publishes a per-frame bbox (objectId = the incrementing frame number) to the plan's broker
    AND the fake-ES on :19200. The overlay use-cases consume this. Idempotent per ctx (returns
    the already-running proc); start it with wait_s=0 BEFORE the VIOS deploy so it is
    subscribed before camera_streaming fires. Returns the Popen (or None)."""
    existing = getattr(ctx, "_mds", None)
    if existing is not None and existing.poll() is None:
        return existing
    import subprocess
    import time as _t
    svc = Path(__file__).resolve().parents[1] / "test/bdd_tests/scripts/overlay/metadata_service.py"
    cmd = ["python3", str(svc), "--broker", ctx.broker, "--base-url", ctx.base_url,
           "--nvstreamer-url", ctx.nvstreamer_url, "--es-port", "19200"]
    if ctx.broker == "kafka" and getattr(ctx, "kafka_brokers", None):
        cmd += ["--kafka", ctx.kafka_brokers]
    log.info("starting event-driven overlay plugin (broker=%s, subscribing camera_streaming)", ctx.broker)
    proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    ctx._mds = proc
    if wait_s:
        _t.sleep(wait_s)   # let it bind fake-ES + subscribe before use-cases run
    return proc


def _stop_metadata_service(proc):
    if not proc:
        return
    try:
        proc.terminate()          # SIGTERM -> service XTRIMs the broker + clears ES on exit
        proc.wait(timeout=20)
    except Exception:  # noqa: BLE001
        try:
            proc.kill()
        except Exception:  # noqa: BLE001
            pass
    log.info("stopped continuous metadata service (broker + ES cleared)")


def _run_plans(plans_path: str, deploy_only: bool = False, images: dict = None, host_ip: str = ""):
    from plans import load_plans, expand_usecases
    from provision import backup_configs, restore_configs, apply_images, apply_deploy_env
    defaults, plans = load_plans(plans_path)
    results = []
    plan_meta = {}
    # Make the host-specific bits implicit: set HOST_IP + repo-local paths, and point the
    # deployment at the requested images (CLI overrides sanity_plans.yaml `defaults.images`),
    # pulling them -- so the user only supplies images, never edits compose.env.
    apply_deploy_env(host_ip)
    imgs = {**(defaults.get("images") or {}), **(images or {})}
    if imgs:
        apply_images(imgs.get("streamprocessing"), imgs.get("sensor"), imgs.get("nvstreamer"))
    # Snapshot configs before mutating so we can leave the box exactly as we found it.
    backup_configs()
    try:
        return _run_plans_inner(plans, deploy_only, results, plan_meta, expand_usecases)
    finally:
        if deploy_only:
            log.info("deploy-only: configs left applied for inspection (.sanity-bak kept; "
                     "run restore_configs() to revert)")
        else:
            restore_configs()   # revert every config change; recreate services


def _plan_systems(plan):
    """A plan targets one or more systems. New schema: `systems:` map (name -> conf with
    `enabled`). Back-compat: a single `system:` dict is treated as one 'local' system.
    Returns [(sysname, sysconf), ...] for the ENABLED systems only."""
    systems = plan.get("systems")
    if not systems:
        return [("local", plan.get("system", {}) or {})]
    out = []
    for sysname, sysconf in systems.items():
        sysconf = sysconf or {}
        if sysconf.get("enabled", False):
            out.append((sysname, sysconf))
    return out


def _deploy_provision_nvstreamer(ctx, name, setup, plan, sync_wall):
    """The NVStreamer-first flow: write config, stop --clean, deploy NVStreamer, provision +
    verify its RTSP sources, then deploy VIOS against a live NVStreamer, then the recording
    gate. Provisions ctx.provisioned_streams / ctx.file_sensor. (Plans 1 & 2.)"""
    from provision import (apply_vst_config, apply_nvstreamer_config, clean_stop, deploy_target,
                           wipe_nvstreamer_videos, recreate_service, _wait_ready,
                           _wait_recording_current, verify_nvstreamer_stream_count)
    import requests as _rq
    video_path = plan.get("video_path") or plan.get("input_mp4")
    # Point VIOS's download/replay overlay at the plugin's fake-ES (host bridge :19200).
    # Without this VIOS never queries it, so replay/download of recorded windows draw no box
    # (the live/recent path goes through the broker and is unaffected).
    vst_over = {"video_metadata_server": _FAKE_ES}
    if setup.get("consumer"):
        vst_over.update({"use_message_broker_consumer": setup["consumer"],
                         "enable_notification_consumer": True,
                         "message_broker_topic_consumer": "vst-overlay-test"})
        if setup["consumer"] == "kafka":
            vst_over["kafka_server_address"] = setup.get("broker_addr", "172.17.0.1:9092")
    vst_over.update(setup.get("vst_config", {}) or {})
    try:
        apply_vst_config(vst_over, recreate=False)
        apply_nvstreamer_config(setup.get("nvstreamer_config", {}) or {}, recreate=False)
    except Exception as e:  # noqa: BLE001
        log.warning("config write failed: %s", e)
    clean_stop()
    wipe_nvstreamer_videos()
    deploy_target("nvstreamer")
    _wait_ready(f"{ctx.nvstreamer_url}/vst/api/v1/sensor/list", 120)
    if not (video_path and setup.get("nvstreamer")):
        return

    def _bring_up_vios(copy_names):
        if sync_wall:
            recreate_service("nvstreamer")
            _wait_ready(f"{ctx.nvstreamer_url}/vst/api/v1/sensor/list", 90)
        verify_nvstreamer_stream_count(ctx.nvstreamer_url, len(copy_names) or 4)
        # event-driven overlay plugin subscribes BEFORE VIOS fires camera_streaming.
        _start_metadata_service(ctx, wait_s=0)
        deploy_target("vst")
        _health(ctx.base_url, timeout=180)

    _provision(ctx, video_path, _RTSP_COPIES, sync_wall,
               plan.get("max_streams", 4), bool(setup.get("variants")),
               deploy_vios=_bring_up_vios)
    # DEPLOYMENT-READY GATE: proceed only once every provisioned RTSP stream is recording
    # continuous-to-now with >=60s of history -- a full minute so the overlay use-cases (whose
    # historical window sits early in the recording) have settled metadata everywhere, and the
    # first-live-frame backfill has bridged the ramp-up gap.
    if ctx.provisioned_streams:
        _wait_recording_current(ctx.base_url, ctx.provisioned_streams[0],
                                ctx.verify_ssl, timeout=300, min_span_s=60)
        not_rec = [s for s in ctx.provisioned_streams
                   if not _wait_recording_current(ctx.base_url, s, ctx.verify_ssl,
                                                  timeout=90, min_span_s=55)]
        try:
            sensors = _rq.get(f"{ctx.base_url}/vst/api/v1/sensor/list",
                              timeout=20, verify=ctx.verify_ssl).json() or []
        except Exception:  # noqa: BLE001
            sensors = []
        rtsp = [s for s in sensors if str(s.get("type", "")).endswith("rtsp")]
        if not_rec:
            log.warning("DEPLOYMENT NOT READY: %d sensor(s), %d RTSP; NOT recording: %s",
                        len(sensors), len(rtsp), not_rec)
        else:
            log.info("DEPLOYMENT READY: %d sensor(s) (%d RTSP), all RTSP recording",
                     len(sensors), len(rtsp))


def _deploy_adaptor_plan(ctx, name, adaptor, setup):
    """Deploy VIOS with a VMS adaptor enabled -- no NVStreamer. VIOS discovers cameras from
    the adaptor: 'milestone' (Milestone server via milestone_onvif, no overlay) or 'onvif'
    (ONVIF network discovery, full overlay). For onvif, the overlay plugin is started before
    VIOS. Populates ctx.provisioned_streams with the ONLINE camera ids."""
    from provision import (apply_vst_config, apply_adaptor_config, clean_stop, deploy_target,
                           discover_online_cameras)
    overlay = adaptor != "milestone"
    vst_over = {"video_metadata_server": _FAKE_ES} if overlay else {}
    if overlay and setup.get("consumer"):
        vst_over.update({"use_message_broker_consumer": setup["consumer"],
                         "enable_notification_consumer": True,
                         "message_broker_topic_consumer": "vst-overlay-test"})
        if setup["consumer"] == "kafka":
            vst_over["kafka_server_address"] = setup.get("broker_addr", "172.17.0.1:9092")
    vst_over.update(setup.get("vst_config", {}) or {})
    try:
        apply_vst_config(vst_over, recreate=False)
        apply_adaptor_config(adaptor, setup)     # enable adaptor + write server config
    except Exception as e:  # noqa: BLE001
        log.warning("adaptor config write failed: %s", e)
    clean_stop()
    if overlay:                                  # onvif: plugin subscribes before VIOS
        _start_metadata_service(ctx, wait_s=0)
    deploy_target("vst")
    _health(ctx.base_url, timeout=240)           # adaptor connect/discovery takes longer
    cams = discover_online_cameras(ctx.base_url, verify_ssl=ctx.verify_ssl, timeout=180)
    ctx.provisioned_streams = cams
    log.info("plan '%s' (adaptor=%s): %d ONLINE camera(s): %s", name, adaptor, len(cams), cams)


def _run_plan_on_system(plan, base_name, sysname, system, deploy_only,
                        results, plan_meta, expand_usecases):
    setup = plan.get("setup", {}) or {}
    name = base_name if sysname in ("local", "default") else f"{base_name} @ {sysname}"
    ctx = SanityContext(base_url=system.get("base_url", "http://localhost:30888"),
                        broker=setup.get("consumer", "redis"),
                        stream_id=plan.get("stream_id", "warehouse_sample"))
    if setup.get("broker_addr"):
        ctx.kafka_brokers = setup["broker_addr"]
    target = system.get("target", "local")
    adaptor = setup.get("adaptor")               # None -> NVStreamer; 'milestone' | 'onvif'
    sync_wall = bool(setup.get("sync_wall"))
    overlay = adaptor != "milestone"             # milestone has NO overlay
    log.info("===================== PLAN: %s (target=%s, adaptor=%s) =====================",
             name, target, adaptor or "nvstreamer")

    if target == "local" and not adaptor and setup.get("nvstreamer"):
        _deploy_provision_nvstreamer(ctx, name, setup, plan, sync_wall)
    elif target == "local" and adaptor in ("milestone", "onvif"):
        _deploy_adaptor_plan(ctx, name, adaptor, setup)
    elif target == "remote":
        log.warning("plan '%s' is remote: ssh deploy not implemented; running API use-cases "
                    "against %s", name, ctx.base_url)

    plan_meta[name] = {
        "consumer": setup.get("consumer", ctx.broker), "target": target, "system": sysname,
        "adaptor": adaptor or "nvstreamer", "base_url": system.get("base_url", ctx.base_url),
        "nvstreamer": ctx.nvstreamer_url, "streams": list(ctx.provisioned_streams),
        "file_sensor": ctx.file_sensor, "stream_id": ctx.stream_id,
    }
    if deploy_only:
        log.info("deploy-only: plan '%s' provisioned (%s); skipping use-cases",
                 name, ctx.provisioned_streams)
        return

    # Overlay plugin only for overlay-capable plans (nvstreamer/onvif) -- milestone has none.
    mds = _start_metadata_service(ctx) if (target == "local" and overlay) else None
    try:
        if plan.get("usecases"):
            suite = expand_usecases(plan["usecases"], ctx)
        elif adaptor == "milestone":
            from usecases import milestone_suite
            suite = milestone_suite(ctx)
        else:
            from usecases import default_suite
            # ONVIF adaptor has no NVStreamer/file sensor -> nvstreamer=False (RTSP + overlay only).
            suite = default_suite(ctx, sync_wall, nvstreamer=(adaptor is None))
        for label, fn, meta in suite:
            res = run_usecase(label, fn, ctx)
            res.plan = name
            res.evidence = bool(meta.get("evidence"))
            results.append(res)
    finally:
        _stop_metadata_service(mds)
    # Capture THIS plan-run's container logs while the containers are still alive.
    if target == "local" and any(r.plan == name and r.status == "FAIL" for r in results):
        plan_meta[name]["logs"] = _capture_container_logs(ctx, tag=_plan_tag(name))


def _run_plans_inner(plans, deploy_only, results, plan_meta, expand_usecases):
    for plan in plans:
        base_name = plan.get("name", "plan")
        if not plan.get("enabled"):
            log.info("skip disabled plan: %s", base_name)
            continue
        for sysname, system in _plan_systems(plan):    # one run per enabled system
            _run_plan_on_system(plan, base_name, sysname, system, deploy_only,
                                results, plan_meta, expand_usecases)
    return results, plan_meta


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--plans", help="run all ENABLED plans from a sanity_plans.yaml")
    ap.add_argument("--base-url", default="http://localhost:30888")
    ap.add_argument("--stream-id", default="warehouse_sample")
    ap.add_argument("--broker", default="redis", choices=["redis", "kafka"])
    ap.add_argument("--host-ip", default="",
                    help="host IP for evidence links (default: $VIOS_SANITY_HOST_IP or auto-detect)")
    ap.add_argument("--only", help="comma-separated subset of use-case names")
    ap.add_argument("--input-mp4", help="a clip/dir to provision from (RTSP copies/set + file sensor)")
    ap.add_argument("--max-streams", type=int, default=4,
                    help="cap RTSP streams provisioned from a video directory")
    ap.add_argument("--out", default="/tmp/vios_sanity/report.pdf")
    ap.add_argument("--from-json", help="re-render the PDF from a saved results.json (no run)")
    ap.add_argument("--deploy-only", action="store_true",
                    help="apply plan config + provision streams, but do NOT run use-cases")
    ap.add_argument("--restore-config", action="store_true",
                    help="restore the configs the sanity backed up (.sanity-bak) + recreate, then exit")
    # Containers under test -> written into compose.env + pulled automatically (no hand-editing).
    ap.add_argument("--streamprocessing-image", help="VST stream-processor image to test")
    ap.add_argument("--sensor-image", help="VST sensor image to test")
    ap.add_argument("--nvstreamer-image", help="NVStreamer image to test")
    ap.add_argument("--no-serve", action="store_true",
                    help="do NOT auto-start the evidence file server (serve share_dir on :18080)")
    ap.add_argument("--install-deps", action="store_true",
                    help="install all prerequisites (pip deps, playwright chromium, Google Chrome, ffmpeg) and exit")
    a = ap.parse_args()

    if a.install_deps:
        return _install_deps()

    if a.restore_config:
        from provision import restore_configs
        restore_configs()
        log.info("configs restored to their pre-sanity snapshot")
        return 0

    # Re-render only: rebuild the PDF from a saved run (format tweaks need no re-run).
    if a.from_json:
        results, plan_meta, ctx, when = _load_results(a.from_json)
        out = Path(a.out); out.parent.mkdir(parents=True, exist_ok=True)
        fails_info = _dump_failures(results, ctx, out.with_name("failed_cases.json"))
        build_pdf(results, ctx, when, out, plan_meta, failures=fails_info)
        link = ctx.publish(out, "vios_sanity_report.pdf")
        print(f"\nPDF report: {out}\nServed at:  {link}")
        return 0

    _check_prereqs()
    if not a.no_serve:
        _start_file_server(a.host_ip)

    if a.plans:
        images = {k: v for k, v in (("streamprocessing", a.streamprocessing_image),
                                    ("sensor", a.sensor_image),
                                    ("nvstreamer", a.nvstreamer_image)) if v}
        results, plan_meta = _run_plans(a.plans, deploy_only=a.deploy_only,
                                        images=images, host_ip=a.host_ip)
        if a.deploy_only:
            log.info("deploy-only complete: stack deployed + plan(s) provisioned; no use-cases run")
            return 0
        ctx = SanityContext()   # for publish() + PDF metadata only
    else:
        ctx = SanityContext(base_url=a.base_url, stream_id=a.stream_id, broker=a.broker, host_ip=a.host_ip)
        _health(a.base_url)
        if a.input_mp4:
            _provision(ctx, a.input_mp4, _RTSP_COPIES, max_streams=a.max_streams)
        wanted = set(a.only.split(",")) if a.only else None
        # Same single metadata source as plan mode: ONE continuous metadata_service (generator
        # + fake-ES + broker publisher) for the whole run; the overlay verbs consume it.
        mds = _start_metadata_service(ctx)
        try:
            results = [run_usecase(n, f, ctx) for n, f in USECASES if not (wanted and n not in wanted)]
        finally:
            _stop_metadata_service(mds)
        plan_meta = {"Sanity": {"consumer": a.broker, "target": "local", "base_url": a.base_url,
                                "nvstreamer": ctx.nvstreamer_url, "streams": list(ctx.provisioned_streams),
                                "file_sensor": ctx.file_sensor, "stream_id": a.stream_id}}

    when = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    out = Path(a.out); out.parent.mkdir(parents=True, exist_ok=True)
    _dump_results(results, plan_meta, ctx, when, out.with_suffix(".results.json"))
    fails_info = _dump_failures(results, ctx, out.with_name("failed_cases.json"))
    build_pdf(results, ctx, when, out, plan_meta, failures=fails_info)
    pdf_link = ctx.publish(out, "vios_sanity_report.pdf")

    npass = sum(1 for r in results if r.status == "PASS")
    nfail = sum(1 for r in results if r.status == "FAIL")
    nskip = sum(1 for r in results if r.status == "SKIP")
    log.info("SANITY SUMMARY: %d PASS / %d FAIL / %d SKIP", npass, nfail, nskip)
    for r in results:
        log.info("  [%s] %-24s %-4s  %s", (r.plan or "-")[:20], r.name, r.status, r.detail[:60])
    print(f"\nPDF report: {out}\nServed at:  {pdf_link}")
    return 1 if nfail else 0


if __name__ == "__main__":
    sys.exit(main())
