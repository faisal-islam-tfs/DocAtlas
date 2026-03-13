#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path


def slugify_app_name(name: str) -> str:
    text = re.sub(r"[^A-Za-z0-9]+", "_", name.strip()).strip("_").lower()
    return text or "uncategorized"


def load_applications(config_path: Path) -> list[str]:
    data = json.loads(config_path.read_text(encoding="utf-8"))
    apps = data.get("applications", {})
    if not isinstance(apps, dict) or not apps:
        raise ValueError(f"No applications found in {config_path}")
    return list(apps.keys())


def build_structure(base_dir: Path, app_names: list[str]) -> list[Path]:
    created: list[Path] = []
    archive_dirs = [
        base_dir / "input",
        base_dir / "output",
        base_dir / "archive",
        base_dir / "archive" / "zips",
        base_dir / "archive" / "old_runs",
    ]
    for path in archive_dirs:
        path.mkdir(parents=True, exist_ok=True)
        created.append(path)

    for app_name in app_names:
        app_slug = slugify_app_name(app_name)
        app_dirs = [
            base_dir / "input" / app_slug,
            base_dir / "output" / app_slug,
            base_dir / "output" / app_slug / "charter",
            base_dir / "output" / app_slug / "atlas",
        ]
        for path in app_dirs:
            path.mkdir(parents=True, exist_ok=True)
            created.append(path)
    return created


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create DocAtlas input/output folder structure from applications.json")
    parser.add_argument("--base", required=True, help="Base DocAtlas root folder to create, e.g. /mnt/nas/faisal/DocAtlas")
    parser.add_argument("--config", default="applications.json", help="Path to applications.json")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    base_dir = Path(args.base).expanduser()
    config_path = Path(args.config).expanduser()
    app_names = load_applications(config_path)
    created = build_structure(base_dir, app_names)
    print(f"Created/verified {len(created)} directories under {base_dir}")
    for app_name in app_names:
        app_slug = slugify_app_name(app_name)
        print(f"- {app_name} -> {app_slug}")
        print(f"  input : {base_dir / 'input' / app_slug}")
        print(f"  output: {base_dir / 'output' / app_slug}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
