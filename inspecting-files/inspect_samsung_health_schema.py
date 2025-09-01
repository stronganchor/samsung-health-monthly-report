#!/usr/bin/env python3
"""
inspect_samsung_health_schema.py

Lightweight introspection to help build the master summary script.

Usage:
    python inspect_samsung_health_schema.py /path/to/export [--limit-sample N]

Prints for each CSV:
  * shape
  * detected possible time columns
  * metric-like columns (steps, heart rate, sleep, weight, water, calories, stress)
  * For step data: aggregates from detailed vs day summary for sanity checking.
"""

import sys
import pathlib
import pandas as pd
import csv

# ---------- helpers ----------
def inspect_header_lines(path):
    with open(path, encoding="utf-8", errors="ignore") as f:
        lines = []
        for _ in range(3):
            line = f.readline()
            if not line:
                break
            lines.append(line.rstrip("\n"))
    counts = [len(line.split(",")) for line in lines]
    return lines, counts

def load_csv(path, debug=False):
    # Heuristic to skip initial metadata line if needed
    header_row = 0
    lines, counts = inspect_header_lines(path)
    if len(counts) >= 2 and counts[0] < 5 and counts[1] >= 5:
        header_row = 1

    def try_read(hdr):
        try:
            df = pd.read_csv(
                path,
                header=hdr,
                engine="python",
                sep=",",
                on_bad_lines="warn",
                dtype=str,
            )
            return df
        except Exception:
            return pd.DataFrame()

    df = try_read(header_row)
    if df.empty and header_row != 0:
        df = try_read(0)
    if df.empty:
        # last resort: no header promotion
        try:
            raw = pd.read_csv(
                path,
                header=None,
                engine="python",
                sep=",",
                on_bad_lines="warn",
                dtype=str,
            )
            if raw.shape[0] >= 2:
                new_header = raw.iloc[1].fillna("").astype(str).tolist()
                df = raw.iloc[2:].copy()
                df.columns = new_header
            else:
                df = raw
        except Exception:
            df = pd.DataFrame()
    if debug:
        print(f"[DEBUG] {path.name} first lines: {lines}")
    return df

def find_candidates(df, keywords):
    out = []
    lower = {c: c.lower() for c in df.columns}
    for c in df.columns:
        for kw in keywords:
            if kw.lower() in lower[c]:
                out.append(c)
                break
    return out

def try_parse_time_series(df, col):
    if col not in df.columns:
        return None
    series = df[col]
    # try numeric epoch-ms
    try:
        num = pd.to_numeric(series, errors="coerce")
        ts = pd.to_datetime(num, unit="ms", errors="coerce")
        if not ts.isna().all():
            return ts
    except Exception:
        pass
    # fallback string parse
    try:
        ts2 = pd.to_datetime(series, errors="coerce")
        if not ts2.isna().all():
            return ts2
    except Exception:
        pass
    return None

# ---------- main ----------
def main():
    if len(sys.argv) < 2:
        print("Usage: python inspect_samsung_health_schema.py /path/to/export [--limit-sample N]")
        sys.exit(1)
    base = pathlib.Path(sys.argv[1])
    if not base.is_dir():
        print(f"{base} is not a directory")
        sys.exit(1)

    limit_sample = None
    if "--limit-sample" in sys.argv:
        try:
            idx = sys.argv.index("--limit-sample")
            limit_sample = int(sys.argv[idx + 1])
        except Exception:
            pass

    # Collect step summary containers
    pedometer_steps = {}
    day_summary_steps = {}

    print(f"Inspecting CSVs in {base}\n")

    for csv_path in sorted(base.glob("*.csv")):
        print(f"--- {csv_path.name} ---")
        df = load_csv(csv_path, debug=False)
        print(f"shape: {df.shape}")
        if df.empty:
            print("Warning: empty or failed to load.")
            continue

        # Show first few columns
        print("Columns (first 10):", list(df.columns)[:10])
        # Candidate time columns
        time_cands = find_candidates(df, ["start_time", "end_time", "time", "day_time", "run_time", "wake_up", "bed_time"])
        print("Possible time columns:", time_cands if time_cands else "None detected")

        # Metric candidates
        steps_cands = find_candidates(df, ["step", "count"])
        hr_cands = find_candidates(df, ["heart", "bpm"])
        sleep_cands = find_candidates(df, ["sleep", "bed", "wake"])
        weight_cands = find_candidates(df, ["weight"])
        water_cands = find_candidates(df, ["water", "amount", "volume"])
        calorie_cands = find_candidates(df, ["calorie"])
        stress_cands = find_candidates(df, ["stress", "score", "max", "min"])

        if steps_cands:
            print("Step-like columns:", steps_cands)
        if hr_cands:
            print("Heart-rate-like columns:", hr_cands)
        if sleep_cands:
            print("Sleep-related columns:", sleep_cands)
        if weight_cands:
            print("Weight-related columns:", weight_cands)
        if water_cands:
            print("Water-related columns:", water_cands)
        if calorie_cands:
            print("Calorie-related columns:", calorie_cands)
        if stress_cands:
            print("Stress-related columns:", stress_cands)

        # Special logic for step comparison
        if "pedometer_step_count" in csv_path.name or "movement" in csv_path.name:
            # aggregate detailed steps
            cnt_col = steps_cands[0] if steps_cands else None
            ts = None
            for t in ["start_time", "time"]:
                ts = try_parse_time_series(df, t)
                if ts is not None:
                    break
            if cnt_col and ts is not None:
                df2 = df.copy()
                df2["ts"] = ts
                df2["steps"] = pd.to_numeric(df2[cnt_col], errors="coerce").fillna(0)
                df2["month"] = df2["ts"].dt.to_period("M")
                summary = df2.groupby("month")["steps"].sum()
                print("Detailed step sums by month:")
                for m, v in summary.items():
                    print(f"  {m}: {int(v)}")
        if "day_summary" in csv_path.name:
            # show the daily step_count
            step_col = None
            for c in df.columns:
                if "step" in c.lower():
                    step_col = c
                    break
            ts = None
            for t in ["day_time", "start_time", "time"]:
                ts = try_parse_time_series(df, t)
                if ts is not None:
                    break
            if step_col and ts is not None:
                df2 = df.copy()
                df2["ts"] = ts
                df2["step_count"] = pd.to_numeric(df2[step_col], errors="coerce")
                df2["month"] = df2["ts"].dt.to_period("M")
                summary = df2.groupby("month")["step_count"].sum()
                print("Day-summary step sums by month:")
                for m, v in summary.items():
                    print(f"  {m}: {int(v)}")

        # Sample rows if small or limited
        if limit_sample:
            print("Sample rows:")
            with pd.option_context("display.max_columns", None):
                print(df.head(limit_sample).to_string(index=False))
        print()

if __name__ == "__main__":
    main()
