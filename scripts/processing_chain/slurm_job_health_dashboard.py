#!/usr/bin/env python3
"""Track Slurm job health over time and build an interactive dashboard.

This script samples `sstat` and `sacct` in parsable mode (`-P`) and stores
time-series snapshots as NDJSON. It can also render the snapshots to an
interactive Plotly HTML dashboard.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


SSTAT_FIELDS = [
    "JobID",
    "AveCPU",
    "AveRSS",
    "MaxRSS",
    "MaxVMSize",
    "NTasks",
]

SACCT_FIELDS = [
    "JobIDRaw",
    "State",
    "ExitCode",
    "Elapsed",
    "ElapsedRaw",
    "MaxRSS",
    "ReqMem",
    "AllocCPUS",
]

TERMINAL_STATES = {
    "BOOT_FAIL",
    "CANCELLED",
    "COMPLETED",
    "DEADLINE",
    "FAILED",
    "NODE_FAIL",
    "OUT_OF_MEMORY",
    "PREEMPTED",
    "TIMEOUT",
}

MEM_RE = re.compile(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*([KMGTPE]?)\s*([cn]?)\s*$", re.IGNORECASE)


def run_command(cmd: list[str]) -> tuple[int, str, str]:
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    return proc.returncode, proc.stdout.strip(), proc.stderr.strip()


def parse_parsable_row(line: str, fields: list[str]) -> dict[str, str]:
    parts = line.split("|")
    if len(parts) < len(fields):
        parts.extend([""] * (len(fields) - len(parts)))
    return {key: parts[idx].strip() for idx, key in enumerate(fields)}


def parse_elapsed_seconds(value: str) -> float | None:
    if not value:
        return None
    raw = value.strip()
    if raw in {"Unknown", "N/A", "NONE"}:
        return None

    day_part = 0
    time_part = raw
    if "-" in raw:
        d, time_part = raw.split("-", 1)
        if d.isdigit():
            day_part = int(d)

    chunks = time_part.split(":")
    if not chunks or len(chunks) > 3:
        return None
    try:
        chunks_i = [int(c) for c in chunks]
    except ValueError:
        return None

    if len(chunks_i) == 3:
        hours, minutes, seconds = chunks_i
    elif len(chunks_i) == 2:
        hours, minutes, seconds = 0, chunks_i[0], chunks_i[1]
    else:
        hours, minutes, seconds = 0, 0, chunks_i[0]

    return float(day_part * 86400 + hours * 3600 + minutes * 60 + seconds)


def parse_mem_to_bytes(value: str, alloc_cpus: int | None = None) -> float | None:
    if not value:
        return None
    raw = value.strip()
    if raw in {"Unknown", "N/A", "NONE", "0"}:
        return None

    match = MEM_RE.match(raw)
    if not match:
        return None

    number = float(match.group(1))
    unit = match.group(2).upper()
    qualifier = match.group(3).lower()

    multiplier = {
        "": 1.0,
        "K": 1024.0,
        "M": 1024.0**2,
        "G": 1024.0**3,
        "T": 1024.0**4,
        "P": 1024.0**5,
        "E": 1024.0**6,
    }[unit]
    total = number * multiplier

    if qualifier == "c" and alloc_cpus and alloc_cpus > 0:
        total *= alloc_cpus

    return total


def safe_int(value: str) -> int | None:
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def state_is_terminal(state: str | None) -> bool:
    if not state:
        return False
    normalized = state.split()[0].split("+")[0].strip().upper()
    return normalized in TERMINAL_STATES


def select_sacct_job_row(rows: list[dict[str, str]], job_id: str) -> dict[str, str] | None:
    if not rows:
        return None

    for row in rows:
        if row.get("JobIDRaw", "") == job_id:
            return row
    for row in rows:
        rid = row.get("JobIDRaw", "")
        if rid and "." not in rid and "_" not in rid:
            return row
    return rows[0]


def select_sstat_row(rows: list[dict[str, str]], job_id: str) -> dict[str, str] | None:
    if not rows:
        return None
    target = f"{job_id}.batch"
    for row in rows:
        if row.get("JobID", "") == target:
            return row
    return rows[0]


def sample_job(job_id: str) -> dict[str, Any]:
    sample: dict[str, Any] = {
        "timestamp_epoch_s": time.time(),
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "job_id": job_id,
    }

    rc_sacct, out_sacct, err_sacct = run_command(
        ["sacct", "-j", job_id, "-P", "-n", "--format=" + ",".join(SACCT_FIELDS)]
    )
    sacct_rows: list[dict[str, str]] = []
    if rc_sacct == 0 and out_sacct:
        sacct_rows = [parse_parsable_row(line, SACCT_FIELDS) for line in out_sacct.splitlines() if line.strip()]
    sample["sacct_error"] = err_sacct if rc_sacct != 0 else ""
    sample["sacct_row"] = select_sacct_job_row(sacct_rows, job_id) or {}

    rc_sstat, out_sstat, err_sstat = run_command(
        ["sstat", "-j", f"{job_id}.batch", "-P", "-n", "--format=" + ",".join(SSTAT_FIELDS)]
    )
    sstat_rows: list[dict[str, str]] = []
    if rc_sstat == 0 and out_sstat:
        sstat_rows = [parse_parsable_row(line, SSTAT_FIELDS) for line in out_sstat.splitlines() if line.strip()]
    sample["sstat_error"] = err_sstat if rc_sstat != 0 else ""
    sample["sstat_row"] = select_sstat_row(sstat_rows, job_id) or {}

    sacct_row = sample["sacct_row"]
    sstat_row = sample["sstat_row"]
    alloc_cpus = safe_int(sacct_row.get("AllocCPUS", ""))

    state = sacct_row.get("State", "")
    exit_code = sacct_row.get("ExitCode", "")
    elapsed_seconds = parse_elapsed_seconds(sacct_row.get("Elapsed", "")) or parse_elapsed_seconds(
        sstat_row.get("AveCPU", "")
    )

    ave_cpu_seconds = parse_elapsed_seconds(sstat_row.get("AveCPU", ""))
    ave_rss_bytes = parse_mem_to_bytes(sstat_row.get("AveRSS", ""))
    max_rss_bytes = parse_mem_to_bytes(sstat_row.get("MaxRSS", ""))
    max_vm_bytes = parse_mem_to_bytes(sstat_row.get("MaxVMSize", ""))

    if max_rss_bytes is None:
        max_rss_bytes = parse_mem_to_bytes(sacct_row.get("MaxRSS", ""))

    req_mem_bytes = parse_mem_to_bytes(sacct_row.get("ReqMem", ""), alloc_cpus=alloc_cpus)
    max_rss_pct_req = None
    if max_rss_bytes and req_mem_bytes and req_mem_bytes > 0:
        max_rss_pct_req = (max_rss_bytes / req_mem_bytes) * 100.0

    sample["metrics"] = {
        "state": state,
        "exit_code": exit_code,
        "elapsed_seconds": elapsed_seconds,
        "ave_cpu_seconds": ave_cpu_seconds,
        "ave_rss_bytes": ave_rss_bytes,
        "max_rss_bytes": max_rss_bytes,
        "max_vm_bytes": max_vm_bytes,
        "req_mem_bytes": req_mem_bytes,
        "max_rss_pct_req": max_rss_pct_req,
        "alloc_cpus": alloc_cpus,
    }
    return sample


def append_ndjson(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row) + "\n")


def load_ndjson(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            text = line.strip()
            if not text:
                continue
            try:
                rows.append(json.loads(text))
            except json.JSONDecodeError:
                continue
    rows.sort(key=lambda x: float(x.get("timestamp_epoch_s", 0.0)))
    return rows


def fmt_gib(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value / (1024.0 ** 3):.2f} GiB"


def render_dashboard(samples: list[dict[str, Any]], job_id: str, output_html: Path) -> None:
    if not samples:
        raise RuntimeError("No samples available for dashboard rendering.")
    latest = samples[-1].get("metrics", {})
    latest_state = latest.get("state", "n/a")
    latest_exit = latest.get("exit_code", "n/a")
    latest_max_rss = fmt_gib(latest.get("max_rss_bytes"))
    latest_req_mem = fmt_gib(latest.get("req_mem_bytes"))
    latest_pressure = latest.get("max_rss_pct_req")
    pressure_txt = "n/a" if latest_pressure is None else f"{latest_pressure:.1f}%"

    payload = {
        "job_id": job_id,
        "samples": samples,
        "latest": {
            "state": latest_state,
            "exit_code": latest_exit,
            "max_rss": latest_max_rss,
            "req_mem": latest_req_mem,
            "pressure_pct": pressure_txt,
        },
    }

    html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Slurm Job Health Dashboard {job_id}</title>
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
  <style>
    body {{ font-family: system-ui, sans-serif; margin: 20px; background: #fafafa; color: #111; }}
    h1 {{ margin-bottom: 0.2rem; }}
    .subtitle {{ margin-top: 0; color: #444; }}
    .grid {{ display: grid; grid-template-columns: 1fr; gap: 16px; }}
    .panel {{ background: #fff; border: 1px solid #ddd; border-radius: 8px; padding: 10px; }}
    .plot {{ width: 100%; height: 280px; }}
  </style>
</head>
<body>
  <h1>Slurm Job Health Dashboard: {job_id}</h1>
  <p class="subtitle">
    latest state={latest_state}, exit={latest_exit}, MaxRSS={latest_max_rss},
    ReqMem={latest_req_mem}, pressure={pressure_txt}
  </p>
  <div class="grid">
    <div class="panel"><div id="plot-memory" class="plot"></div></div>
    <div class="panel"><div id="plot-pressure" class="plot"></div></div>
    <div class="panel"><div id="plot-cpu" class="plot"></div></div>
    <div class="panel"><div id="plot-state" class="plot"></div></div>
  </div>
  <script>
    const payload = {json.dumps(payload)};
    const samples = payload.samples || [];

    const x = samples.map((s) => s.timestamp_utc || null);
    const metrics = samples.map((s) => s.metrics || {{}});
    const toGiB = (v) => (v == null ? null : v / (1024 ** 3));
    const aveRssGiB = metrics.map((m) => toGiB(m.ave_rss_bytes));
    const maxRssGiB = metrics.map((m) => toGiB(m.max_rss_bytes));
    const reqMemGiB = metrics.map((m) => toGiB(m.req_mem_bytes));
    const pressurePct = metrics.map((m) => m.max_rss_pct_req ?? null);
    const aveCpuSec = metrics.map((m) => m.ave_cpu_seconds ?? null);
    const elapsedSec = metrics.map((m) => m.elapsed_seconds ?? null);
    const states = metrics.map((m) => m.state || "");

    const uniqueStates = [...new Set(states.filter((s) => s))].sort();
    const stateToNum = new Map(uniqueStates.map((s, i) => [s, i]));
    const stateNum = states.map((s) => (s ? stateToNum.get(s) : null));

    const baseLayout = {{
      margin: {{ l: 60, r: 20, t: 20, b: 40 }},
      xaxis: {{ title: "time (UTC)" }},
      hovermode: "x unified",
      legend: {{ orientation: "h" }}
    }};

    Plotly.newPlot("plot-memory", [
      {{ x, y: aveRssGiB, mode: "lines+markers", name: "AveRSS" }},
      {{ x, y: maxRssGiB, mode: "lines+markers", name: "MaxRSS" }},
      {{ x, y: reqMemGiB, mode: "lines", name: "ReqMem", line: {{ dash: "dash", width: 2 }} }}
    ], {{
      ...baseLayout,
      title: "Memory (GiB)",
      yaxis: {{ title: "GiB" }}
    }}, {{ responsive: true }});

    Plotly.newPlot("plot-pressure", [
      {{ x, y: pressurePct, mode: "lines+markers", name: "MaxRSS/ReqMem %" }},
      {{
        x: [x[0], x[x.length - 1]],
        y: [100, 100],
        mode: "lines",
        name: "100%",
        line: {{ dash: "dash", color: "red" }}
      }}
    ], {{
      ...baseLayout,
      title: "Memory Pressure (% of ReqMem)",
      yaxis: {{ title: "%"}}
    }}, {{ responsive: true }});

    Plotly.newPlot("plot-cpu", [
      {{ x, y: aveCpuSec, mode: "lines+markers", name: "AveCPU(s)" }},
      {{ x, y: elapsedSec, mode: "lines+markers", name: "Elapsed(s)" }}
    ], {{
      ...baseLayout,
      title: "CPU / Elapsed (seconds)",
      yaxis: {{ title: "seconds"}}
    }}, {{ responsive: true }});

    Plotly.newPlot("plot-state", [
      {{ x, y: stateNum, text: states, mode: "lines+markers", name: "State", hovertemplate: "state=%{{text}}<extra></extra>" }}
    ], {{
      ...baseLayout,
      title: "Job State",
      yaxis: {{
        title: "state",
        tickmode: "array",
        tickvals: uniqueStates.map((s) => stateToNum.get(s)),
        ticktext: uniqueStates
      }}
    }}, {{ responsive: true }});
  </script>
</body>
</html>
"""
    output_html.parent.mkdir(parents=True, exist_ok=True)
    output_html.write_text(html, encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sample Slurm job metrics and render a health dashboard.")
    parser.add_argument("--job-id", required=True, help="Slurm job ID, e.g. 23706854")
    parser.add_argument(
        "--mode",
        choices=("watch", "render", "watch-render"),
        default="watch-render",
        help="watch: sample only; render: render only; watch-render: do both",
    )
    parser.add_argument("--interval", type=float, default=15.0, help="Sampling interval in seconds (watch modes).")
    parser.add_argument("--max-samples", type=int, default=0, help="Stop after N samples (0 = until terminal state).")
    parser.add_argument(
        "--samples-file",
        default=None,
        help="NDJSON path for sampled snapshots (default: scripts/processing_chain/logs/job-<id>-health.ndjson)",
    )
    parser.add_argument(
        "--output-html",
        default=None,
        help="Output dashboard HTML (default: scripts/processing_chain/logs/job-<id>-health.html)",
    )
    return parser.parse_args()


def default_artifacts(job_id: str) -> tuple[Path, Path]:
    here = Path(__file__).resolve().parent
    logs = here / "logs"
    return logs / f"job-{job_id}-health.ndjson", logs / f"job-{job_id}-health.html"


def print_sample_summary(sample: dict[str, Any]) -> None:
    metrics = sample.get("metrics", {})
    state = metrics.get("state") or "n/a"
    max_rss = fmt_gib(metrics.get("max_rss_bytes"))
    req_mem = fmt_gib(metrics.get("req_mem_bytes"))
    pressure = metrics.get("max_rss_pct_req")
    pressure_txt = "n/a" if pressure is None else f"{pressure:.1f}%"
    print(
        f"[{sample.get('timestamp_utc', 'n/a')}] "
        f"state={state} max_rss={max_rss} req_mem={req_mem} pressure={pressure_txt}",
        flush=True,
    )


def main() -> int:
    args = parse_args()
    samples_file_default, output_html_default = default_artifacts(args.job_id)
    samples_file = Path(args.samples_file) if args.samples_file else samples_file_default
    output_html = Path(args.output_html) if args.output_html else output_html_default

    if args.mode in {"watch", "watch-render"}:
        count = 0
        while True:
            snapshot = sample_job(args.job_id)
            append_ndjson(samples_file, snapshot)
            print_sample_summary(snapshot)

            count += 1
            state = snapshot.get("metrics", {}).get("state", "")
            terminal = state_is_terminal(state)
            reached_max = args.max_samples > 0 and count >= args.max_samples
            if reached_max or terminal:
                break
            time.sleep(max(args.interval, 0.5))

    if args.mode in {"render", "watch-render"}:
        samples = load_ndjson(samples_file)
        render_dashboard(samples, args.job_id, output_html)
        print(f"Dashboard written: {output_html}")
        print(f"Samples file: {samples_file}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
