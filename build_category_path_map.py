#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd


HIERARCHY_COLS = ["Main Category", "Sub Categories", "Unnamed: 2", "Unnamed: 3", "Unnamed: 4"]
SKIP_VALUES = {
    "",
    "top level (1)",
    "mid level (2)",
    "mid level (3)",
    "bottom level (4)",
}
SKIP_KEYS = {
    "standard guides",
    "color code",
    "*content reuse categories",
    "*duplicate categories",
}


def norm(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


def load_structure_rows(path: Path, sheet_name: str) -> List[Tuple[str, str]]:
    df = pd.read_excel(path, sheet_name=sheet_name)
    for col in HIERARCHY_COLS:
        if col in df.columns:
            df[col] = df[col].ffill()
        else:
            df[col] = ""
    if "Keys" not in df.columns:
        df["Keys"] = ""

    rows: List[Tuple[str, str]] = []
    for _, row in df.iterrows():
        key = norm(str(row.get("Keys", "") or ""))
        if key in SKIP_KEYS:
            continue
        parts: List[str] = []
        for col in HIERARCHY_COLS:
            val = str(row.get(col, "") or "").strip()
            if not val:
                continue
            if norm(val) in SKIP_VALUES:
                continue
            parts.append(val)
        if not parts:
            continue
        path_str = "/".join(parts)
        rows.append((path_str, norm(path_str)))
    return rows


def choose_path_for_category(category: str, structure_rows: List[Tuple[str, str]], app_name: str) -> str:
    ncat = norm(category)
    if not ncat:
        return app_name

    exact = [orig for orig, npath in structure_rows if any(norm(seg) == ncat for seg in npath.split("/"))]
    if exact:
        return min(exact, key=lambda p: len(p))

    contains = [orig for orig, npath in structure_rows if ncat in npath]
    if contains:
        return min(contains, key=lambda p: len(p))

    tokens = [t for t in ncat.split(" ") if len(t) > 2]
    if tokens:
        scored: List[Tuple[int, str]] = []
        for orig, npath in structure_rows:
            score = sum(1 for t in tokens if t in npath)
            if score > 0:
                scored.append((score, orig))
        if scored:
            scored.sort(key=lambda x: (-x[0], len(x[1])))
            return scored[0][1]

    return f"{app_name}/{category}"


def build_map(applications_json: Path, structure_xlsx: Path, sheet_name: str) -> Dict[str, Dict[str, str]]:
    data = json.loads(applications_json.read_text(encoding="utf-8"))
    apps = data.get("applications", {})
    structure_rows = load_structure_rows(structure_xlsx, sheet_name=sheet_name)
    out: Dict[str, Dict[str, str]] = {}
    for app_name, categories in apps.items():
        if not isinstance(categories, list):
            continue
        mapped: Dict[str, str] = {}
        for cat in categories:
            cat_str = str(cat).strip()
            if not cat_str:
                continue
            mapped[cat_str] = choose_path_for_category(cat_str, structure_rows, app_name)
        out[str(app_name)] = mapped
    return out


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build category_path_map.json from a structure workbook")
    p.add_argument("--applications", default="applications.json", help="Path to applications.json")
    p.add_argument("--structure", required=True, help="Path to site structure workbook (.xlsx)")
    p.add_argument("--sheet", default="LS KB Structure", help="Sheet name in structure workbook")
    p.add_argument("--output", default="category_path_map.json", help="Output JSON path")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    result = build_map(Path(args.applications), Path(args.structure), args.sheet)
    Path(args.output).write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Wrote {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
