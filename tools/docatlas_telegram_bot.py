from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import time
from pathlib import Path
from typing import Any
from urllib import parse, request


SUPPORTED_EXTS = {".pdf", ".doc", ".docx", ".ppt", ".pptx", ".xls", ".xlsx"}
DEFAULT_ESTIMATE_SEC_PER_FILE = 50.0
DEFAULT_ESTIMATE_SEC_PER_MB = 1.5


def api_get(api_root: str, method: str, params: dict[str, Any] | None = None, timeout: int = 35) -> dict[str, Any]:
    params = params or {}
    url = api_root + "/" + method + "?" + parse.urlencode(params)
    with request.urlopen(url, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def api_post(api_root: str, method: str, data: dict[str, Any], timeout: int = 35) -> dict[str, Any]:
    body = parse.urlencode(data).encode()
    req = request.Request(api_root + "/" + method, data=body)
    with request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def send_message(api_root: str, chat_id: str, text: str) -> None:
    api_post(api_root, "sendMessage", {"chat_id": chat_id, "text": text})


def safe_read(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return ""


def tail_lines(path: Path, limit: int = 10) -> list[str]:
    lines = [line for line in safe_read(path).splitlines() if line.strip()]
    return lines[-limit:]


def parse_summary(summary_path: Path) -> dict[str, str]:
    info: dict[str, str] = {}
    for line in safe_read(summary_path).splitlines():
        if line.startswith("Total documents:"):
            info["documents"] = line.split(":", 1)[1].strip()
        elif line.startswith("Duplicate documents:"):
            info["duplicates"] = line.split(":", 1)[1].strip()
        elif line.startswith("- total_unsupported_files:"):
            info["unsupported"] = line.split(":", 1)[1].strip()
        elif line.startswith("- ocr_used:"):
            info["ocr_used"] = line.split(":", 1)[1].strip()
        elif line.startswith("- total_tokens:"):
            info["tokens"] = line.split(":", 1)[1].strip()
    return info


def parse_last_run_stats(run_dir: Path) -> dict[str, Any]:
    stats_path = run_dir / "last_run_stats.json"
    if not stats_path.exists():
        return {}
    try:
        return json.loads(safe_read(stats_path))
    except Exception:
        return {}


def find_latest_completed_run(output_root: Path, app_slug: str | None = None) -> dict[str, Any] | None:
    candidates: list[tuple[float, str, Path]] = []
    if not output_root.exists():
        return None
    app_dirs = [output_root / app_slug] if app_slug else [p for p in output_root.iterdir() if p.is_dir()]
    for app_dir in app_dirs:
        charter_dir = app_dir / "charter"
        if not charter_dir.exists():
            continue
        for run_dir in charter_dir.iterdir():
            if not run_dir.is_dir():
                continue
            stats = run_dir / "last_run_stats.json"
            summary = run_dir / "summary_report.txt"
            if not stats.exists() and not summary.exists():
                continue
            stamp = 0.0
            if stats.exists():
                stamp = max(stamp, stats.stat().st_mtime)
            if summary.exists():
                stamp = max(stamp, summary.stat().st_mtime)
            candidates.append((stamp, app_dir.name, run_dir))
    if not candidates:
        return None
    _stamp, app_slug, run_dir = max(candidates, key=lambda item: item[0])
    return {"app_slug": app_slug, "run_dir": run_dir, "run_tag": run_dir.name}


def detect_active_run() -> dict[str, Any] | None:
    try:
        pids = [pid for pid in os.listdir("/proc") if pid.isdigit()]
    except Exception:
        return None
    for pid in sorted(pids, key=int):
        cmdline_path = Path("/proc") / pid / "cmdline"
        try:
            raw = cmdline_path.read_bytes().split(b"\x00")
            args = [part.decode("utf-8", errors="replace") for part in raw if part]
        except Exception:
            continue
        joined = " ".join(args)
        if "docatlas.py" not in joined:
            continue
        output_dir = None
        input_dir = None
        app_name = None
        for idx, arg in enumerate(args):
            if arg == "--output" and idx + 1 < len(args):
                output_dir = args[idx + 1]
            elif arg == "--input" and idx + 1 < len(args):
                input_dir = args[idx + 1]
            elif arg == "--app" and idx + 1 < len(args):
                app_name = args[idx + 1]
        if not output_dir:
            continue
        run_dir = Path(output_dir)
        return {
            "pid": int(pid),
            "app": app_name or run_dir.parent.parent.name,
            "app_slug": run_dir.parent.parent.name,
            "run_dir": run_dir,
            "run_tag": run_dir.name,
            "input_dir": Path(input_dir) if input_dir else None,
        }
    return None


def count_input_stats(input_dir: Path | None) -> dict[str, float]:
    count = 0
    total_size = 0
    if not input_dir or not input_dir.exists():
        return {"count": 0, "total_size_mb": 0.0}
    for root, _dirs, files in os.walk(input_dir):
        for name in files:
            p = Path(root) / name
            if p.suffix.lower() in SUPPORTED_EXTS:
                count += 1
                try:
                    total_size += p.stat().st_size
                except Exception:
                    pass
    return {"count": count, "total_size_mb": total_size / 1024.0 / 1024.0}


def estimate_total_seconds(active_run: dict[str, Any], output_root: Path) -> tuple[float, str, dict[str, float]]:
    input_stats = count_input_stats(active_run.get("input_dir"))
    count = input_stats.get("count", 0)
    total_mb = input_stats.get("total_size_mb", 0.0)
    baseline = find_latest_completed_run(output_root, active_run.get("app_slug"))
    if baseline and baseline["run_dir"] != active_run["run_dir"]:
        stats = parse_last_run_stats(baseline["run_dir"])
        processed = int(stats.get("processed_files", 0) or 0)
        elapsed = float(stats.get("elapsed_sec", 0.0) or 0.0)
        base_mb = float(stats.get("total_size_mb", 0.0) or 0.0)
        if processed > 0 and elapsed > 0:
            est_by_file = (elapsed / processed) * count
            est_by_mb = (elapsed / base_mb) * total_mb if base_mb > 0 else 0.0
            return max(est_by_file, est_by_mb), "baseline", input_stats
    est_by_file = DEFAULT_ESTIMATE_SEC_PER_FILE * count
    est_by_mb = DEFAULT_ESTIMATE_SEC_PER_MB * total_mb
    return max(est_by_file, est_by_mb), "heuristic", input_stats


def elapsed_seconds(pid: int) -> int:
    try:
        return int(subprocess.check_output(["ps", "-p", str(pid), "-o", "etimes="], text=True).strip() or "0")
    except Exception:
        return 0


def fmt_hours(seconds: float) -> str:
    return f"{seconds / 3600.0:.2f} h" if seconds else "0.00 h"


def describe_run(run: dict[str, Any], include_log_tail: bool = True) -> str:
    run_dir = run["run_dir"]
    summary = parse_summary(run_dir / "summary_report.txt")
    stats = parse_last_run_stats(run_dir)
    app = run.get("app") or run.get("app_slug") or run_dir.parent.parent.name
    lines = [f"App: {app}", f"Run: {run.get('run_tag', run_dir.name)}", f"Folder: {run_dir}"]
    processed = stats.get("processed_files")
    if processed is not None:
        lines.append(f"Processed files: {processed}")
    elapsed = float(stats.get("elapsed_sec", 0.0) or 0.0)
    if elapsed:
        lines.append(f"Runtime: {elapsed / 3600:.2f} h")
    total_size = stats.get("total_size_mb")
    if total_size is not None:
        lines.append(f"Size: {float(total_size):.1f} MB")
    if "documents" in summary:
        lines.append(f"Total documents: {summary['documents']}")
    if "duplicates" in summary:
        lines.append(f"Duplicate documents: {summary['duplicates']}")
    if "unsupported" in summary:
        lines.append(f"Unsupported files: {summary['unsupported']}")
    if "ocr_used" in summary:
        lines.append(f"OCR used: {summary['ocr_used']}")
    if "tokens" in summary:
        lines.append(f"Total tokens: {summary['tokens']}")
    if include_log_tail:
        tail = tail_lines(run_dir / "docatlas.log", limit=2)
        if tail:
            lines.append("Latest log:")
            lines.extend(tail)
    return "\n".join(lines)


def estimate_text(active: dict[str, Any], output_root: Path) -> str:
    total_sec, source, input_stats = estimate_total_seconds(active, output_root)
    elapsed = elapsed_seconds(active["pid"])
    remaining = max(0.0, total_sec - elapsed)
    lines = [
        "DocAtlas estimate:",
        f"App: {active.get('app', active.get('app_slug'))}",
        f"Run: {active['run_tag']}",
        f"Supported input files: {int(input_stats.get('count', 0))}",
        f"Supported input size: {input_stats.get('total_size_mb', 0.0):.1f} MB",
        f"Estimate source: {source}",
        f"Estimated total runtime: {fmt_hours(total_sec)}",
        f"Elapsed so far: {fmt_hours(elapsed)}",
        f"Estimated remaining: {fmt_hours(remaining)}",
    ]
    return "\n".join(lines)


def current_status(output_root: Path) -> str:
    active = detect_active_run()
    if active:
        try:
            etime = subprocess.check_output(["ps", "-p", str(active["pid"]), "-o", "etime="], text=True).strip() or "n/a"
        except Exception:
            etime = "n/a"
        parts = [
            "DocAtlas status: running",
            f"PID: {active['pid']}",
            f"Elapsed: {etime}",
            describe_run(active),
            estimate_text(active, output_root),
        ]
        return "\n".join(parts)
    latest = find_latest_completed_run(output_root)
    if latest:
        return "DocAtlas status: no active run\nLatest completed run:\n" + describe_run(latest)
    return "DocAtlas status: no active run and no completed runs found"


def latest_summary(output_root: Path) -> str:
    latest = find_latest_completed_run(output_root)
    if not latest:
        return "No completed runs found."
    return "Latest completed run:\n" + describe_run(latest, include_log_tail=False)


def latest_tail(output_root: Path) -> str:
    target = detect_active_run() or find_latest_completed_run(output_root)
    if not target:
        return "No active or completed run found."
    lines = tail_lines(target["run_dir"] / "docatlas.log", limit=10)
    if not lines:
        return f"No log lines found for {target['run_tag']}."
    return f"Tail for {target['run_tag']}:\n" + "\n".join(lines)


def latest_errors(output_root: Path) -> str:
    target = detect_active_run() or find_latest_completed_run(output_root)
    if not target:
        return "No active or completed run found."
    log_text = safe_read(target["run_dir"] / "docatlas.log")
    warn_lines = [line for line in log_text.splitlines() if "[WARNING]" in line or "[ERROR]" in line or "Traceback" in line]
    warn_lines = warn_lines[-20:]
    if not warn_lines:
        return f"No warnings or errors found for {target['run_tag']}."
    return f"Warnings/errors for {target['run_tag']}:\n" + "\n".join(warn_lines)


def disk_status(docatlas_root: Path) -> str:
    checks = [("/", "system"), (str(docatlas_root), "docatlas")]
    lines = ["Disk usage:"]
    seen: set[str] = set()
    for path, label in checks:
        try:
            out = subprocess.check_output(["df", "-h", path], text=True).strip().splitlines()
            if len(out) >= 2:
                row = re.sub(r"\s+", " ", out[1].strip())
                if row not in seen:
                    lines.append(f"{label}: {row}")
                    seen.add(row)
        except Exception as exc:
            lines.append(f"{label}: unavailable ({exc})")
    return "\n".join(lines)


def help_text() -> str:
    return "\n".join(
        [
            "Commands:",
            "status - active run if any, with estimate, otherwise latest completed run",
            "estimate - estimate for active run",
            "latest - latest completed run summary",
            "summary - detailed summary of latest completed run",
            "tail - last log lines from active/latest run",
            "errors - recent warnings/errors from active/latest run",
            "disk - disk usage for system and DocAtlas storage",
            "help - show commands",
        ]
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Telegram status bot for DocAtlas runs.")
    parser.add_argument("--token", required=True)
    parser.add_argument("--chat-id", required=True)
    parser.add_argument("--docatlas-root", default="/mnt/nas/faisal/DocAtlas")
    parser.add_argument("--poll-timeout", type=int, default=30)
    parser.add_argument("--idle-sleep", type=int, default=5)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    api_root = f"https://api.telegram.org/bot{args.token}"
    docatlas_root = Path(args.docatlas_root)
    output_root = docatlas_root / "output"

    start = api_get(api_root, "getUpdates", {"timeout": 1})
    offset = None
    if start.get("ok") and start.get("result"):
        offset = max(item["update_id"] for item in start["result"]) + 1

    while True:
        try:
            params: dict[str, Any] = {"timeout": args.poll_timeout}
            if offset is not None:
                params["offset"] = offset
            data = api_get(api_root, "getUpdates", params=params, timeout=args.poll_timeout + 5)
            if not data.get("ok"):
                time.sleep(args.idle_sleep)
                continue
            for item in data.get("result", []):
                offset = item["update_id"] + 1
                msg = item.get("message") or {}
                chat = msg.get("chat") or {}
                text = (msg.get("text") or "").strip().lower()
                if str(chat.get("id")) != str(args.chat_id):
                    continue
                if text == "status":
                    send_message(api_root, args.chat_id, current_status(output_root))
                elif text == "estimate":
                    active = detect_active_run()
                    send_message(api_root, args.chat_id, estimate_text(active, output_root) if active else "No active run to estimate.")
                elif text in {"latest", "summary"}:
                    send_message(api_root, args.chat_id, latest_summary(output_root))
                elif text == "tail":
                    send_message(api_root, args.chat_id, latest_tail(output_root))
                elif text == "errors":
                    send_message(api_root, args.chat_id, latest_errors(output_root))
                elif text == "disk":
                    send_message(api_root, args.chat_id, disk_status(docatlas_root))
                elif text in {"help", "/help"}:
                    send_message(api_root, args.chat_id, help_text())
        except Exception:
            time.sleep(args.idle_sleep)


if __name__ == "__main__":
    raise SystemExit(main())
