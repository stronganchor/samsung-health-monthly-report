# steps.py
import pandas as pd
from pathlib import Path

# ---------- helpers ----------
def clean_ts(series):
    series = series.copy()
    ts = pd.Series([pd.NaT] * len(series))
    num = pd.to_numeric(series, errors="coerce")
    if num.notna().any():
        candidate = pd.to_datetime(num, unit="ms", errors="coerce")
        if candidate.notna().any():
            ts = candidate
    if ts.isna().all():
        try:
            parsed = pd.to_datetime(series, errors="coerce")
            if parsed.notna().any():
                ts = parsed
        except Exception:
            pass
    return ts.where(ts.dt.year >= 2005, pd.NaT)

def smart_load(path, expect=None, debug=False):
    if not path.exists():
        if debug:
            print(f"[steps] [DEBUG] missing {path.name}")
        return pd.DataFrame()
    def try_read(h):
        try:
            return pd.read_csv(
                path, header=h, sep=",", engine="python", dtype=str, on_bad_lines="skip"
            )
        except Exception:
            return pd.DataFrame()
    df0 = try_read(0)
    df1 = try_read(1)
    pick = df0
    if expect:
        has1 = any(e.lower() in " ".join(str(c).lower() for c in df1.columns) for e in expect)
        has0 = any(e.lower() in " ".join(str(c).lower() for c in df0.columns) for e in expect)
        if has1 and (not has0 or len(df1.columns) >= len(df0.columns)):
            pick = df1
    else:
        if len(df1.columns) > len(df0.columns):
            pick = df1
    if debug:
        hdr = "1" if pick is df1 else "0"
        print(f"[steps] [DEBUG] Loading {path.name}: header={hdr}, shape={pick.shape}")
        print(f"[steps] [DEBUG] Columns (first 20): {list(pick.columns)[:20]}")
        with pd.option_context("display.max_columns", None, "display.width", 220):
            print(f"[steps] [DEBUG] Sample:\n{pick.head(2).to_string(index=False)}")
    return pick

def load_day_summary_manual(path, debug=False):
    if not path.exists():
        if debug:
            print(f"[steps] [DEBUG] day_summary file missing: {path}")
        return pd.DataFrame()
    with open(path, encoding="utf-8", errors="ignore") as f:
        lines = f.read().splitlines()
    if len(lines) < 2:
        return pd.DataFrame()
    header = lines[1].split(",")
    records = []
    for line in lines[2:]:
        if not line.strip():
            continue
        parts = line.split(",")
        if len(parts) > len(header):
            parts = parts[: len(header)]
        if len(parts) < len(header):
            parts += [""] * (len(header) - len(parts))
        records.append(dict(zip(header, parts)))
    df = pd.DataFrame.from_records(records)
    if debug:
        print(f"[steps] [DEBUG] Loaded day_summary manually: shape={df.shape}")
        with pd.option_context("display.max_columns", None, "display.width", 220):
            print(f"[steps] [DEBUG] Columns: {list(df.columns)}")
            print(f"[steps] [DEBUG] Sample:\n{df.head(2).to_string(index=False)}")
    return df

# ---------- aggregators ----------
def aggregate_day_summary(df):
    if df.empty:
        return pd.DataFrame()
    df = df.copy()
    step_col = next((c for c in df.columns if "step_count" in c.lower()), None)
    if step_col is None:
        return pd.DataFrame()
    df["step_val"] = pd.to_numeric(df[step_col], errors="coerce")
    if "day_time" in df.columns:
        df["ts"] = clean_ts(pd.to_numeric(df["day_time"], errors="coerce"))
    else:
        fallback = next((c for c in df.columns if "start_time" in c.lower()), None)
        if fallback is None:
            return pd.DataFrame()
        df["ts"] = clean_ts(df[fallback])
    df = df.dropna(subset=["step_val", "ts"])
    if df.empty:
        return pd.DataFrame()
    if "day_time" in df.columns:
        df = (
            df.sort_values(["day_time", "step_val"], ascending=[True, False])
            .drop_duplicates("day_time", keep="first")
        )
    df["month"] = df["ts"].dt.to_period("M")
    df["day"] = df["ts"].dt.normalize()
    total = df.groupby("month")["step_val"].sum().rename("merged")
    days = df.groupby("month")["day"].nunique()
    avg = (total / days).round(1).rename("avg_daily")
    return pd.concat([total, avg], axis=1)

def aggregate_pedometer_detailed(df):
    if df.empty:
        return pd.Series(dtype=float)
    df = df.copy()
    ts_col = next((c for c in df.columns if "start_time" in c.lower()), None)
    if ts_col is None:
        return pd.Series(dtype=float)
    df["ts"] = clean_ts(df[ts_col])
    run_col = next((c for c in df.columns if "run_step" in c.lower()), None)
    walk_col = next((c for c in df.columns if "walk_step" in c.lower()), None)
    if run_col or walk_col:
        run_vals = pd.to_numeric(df[run_col], errors="coerce") if run_col else pd.Series(0, index=df.index)
        walk_vals = pd.to_numeric(df[walk_col], errors="coerce") if walk_col else pd.Series(0, index=df.index)
        df["steps"] = run_vals.fillna(0) + walk_vals.fillna(0)
    else:
        count_col = next((c for c in df.columns if "count" in c.lower()), None)
        if count_col is None:
            return pd.Series(dtype=float)
        df["steps"] = pd.to_numeric(df[count_col], errors="coerce")
    df = df.dropna(subset=["steps", "ts"])
    if df.empty:
        return pd.Series(dtype=float)
    df["month"] = df["ts"].dt.to_period("M")
    return df.groupby("month")["steps"].sum().rename("detailed")

def aggregate_trend(df):
    if df.empty:
        return pd.Series(dtype=float)
    df = df.copy()
    ts_col = next((c for c in df.columns if "day_time" in c.lower()), None)
    if ts_col is None:
        return pd.Series(dtype=float)
    df["ts"] = clean_ts(pd.to_numeric(df[ts_col], errors="coerce"))
    count_col = next((c for c in df.columns if "count" in c.lower()), None)
    if count_col is None:
        return pd.Series(dtype=float)
    df["steps"] = pd.to_numeric(df[count_col], errors="coerce")
    df = df.dropna(subset=["steps", "ts"])
    if df.empty:
        return pd.Series(dtype=float)
    df["month"] = df["ts"].dt.to_period("M")
    return df.groupby("month")["steps"].sum().rename("trend")

# ---------- public interface ----------
def summarize_steps(base_path: Path, debug=False):
    ped = smart_load(base_path / "com.samsung.shealth.tracker.pedometer_step_count.20250801145008.csv",
                     expect=["run_step", "walk_step"], debug=debug)
    day = load_day_summary_manual(base_path / "com.samsung.shealth.tracker.pedometer_day_summary.20250801145008.csv",
                                  debug=debug)
    trn = smart_load(base_path / "com.samsung.shealth.step_daily_trend.20250801145008.csv",
                     expect=["count"], debug=debug)

    merged = aggregate_day_summary(day)  # DataFrame with merged + avg_daily
    detailed = aggregate_pedometer_detailed(ped)  # Series
    trend = aggregate_trend(trn)  # Series

    # Build a combined per-month dict for easier formatting upstream
    return {
        "merged": merged,        # DataFrame
        "detailed": detailed,    # Series
        "trend": trend,          # Series
    }

def format_steps_section(summary_dict):
    merged = summary_dict.get("merged", pd.DataFrame())
    detailed = summary_dict.get("detailed", pd.Series(dtype=float))
    trend = summary_dict.get("trend", pd.Series(dtype=float))

    # Collect months present across sources
    months = set()
    if isinstance(merged, pd.DataFrame):
        months.update(str(m) for m in merged.index)
    if isinstance(detailed, pd.Series):
        months.update(str(m) for m in detailed.index)
    if isinstance(trend, pd.Series):
        months.update(str(m) for m in trend.index)
    if not months:
        return "=== Steps ===\nNo step data found.\n"

    months = sorted(months)
    lines = ["=== Steps ==="]
    for m in months:
        p = pd.Period(m)
        # best authoritative value: merged > detailed > trend
        best = None
        avg = "?"
        if isinstance(merged, pd.DataFrame) and p in merged.index:
            best = merged.loc[p, "merged"]
            avg = merged.loc[p, "avg_daily"]
        elif isinstance(detailed, pd.Series) and p in detailed.index:
            best = detailed.loc[p]
        elif isinstance(trend, pd.Series) and p in trend.index:
            best = trend.loc[p]

        best_fmt = "?" if best is None or pd.isna(best) else f"{int(best):,}"
        lines.append(f"\n===== {m} =====")
        lines.append(f"Steps: {best_fmt}, avg/day ~{avg}")
        comps = []
        if isinstance(merged, pd.DataFrame) and p in merged.index:
            comps.append(f"merged={int(merged.loc[p,'merged']):,}")
        if isinstance(detailed, pd.Series) and p in detailed.index:
            comps.append(f"detailed={int(detailed.loc[p]):,}")
        if isinstance(trend, pd.Series) and p in trend.index:
            comps.append(f"trend={int(trend.loc[p]):,}")
        if comps:
            lines.append("  (" + ", ".join(comps) + ")")
    return "\n".join(lines) + "\n"
