# hrv.py
import json
import os
from pathlib import Path
import pandas as pd


def _smart_load_hrv_csv(path: Path, debug=False) -> pd.DataFrame:
    if not path.exists():
        if debug:
            print(f"[hrv] [DEBUG] missing {path.name}")
        return pd.DataFrame()
    def try_read(h):
        try:
            return pd.read_csv(path, header=h, engine="python", dtype=str, on_bad_lines="skip")
        except Exception:
            return pd.DataFrame()
    df0 = try_read(0)
    df1 = try_read(1)
    pick = df1 if ("start_time" in df1.columns and "binning_data" in df1.columns) else df0
    if debug:
        hdr = "1" if pick is df1 else "0"
        print(f"[hrv] [DEBUG] Loading {path.name}: header={hdr}, shape={pick.shape}")
        print(f"[hrv] [DEBUG] Columns: {list(pick.columns)}")
        with pd.option_context("display.max_columns", None, "display.width", 220):
            print(f"[hrv] [DEBUG] Sample:\n{pick.head(2).to_string(index=False)}")
    return pick


def _extract_date_from_json(json_obj):
    if not isinstance(json_obj, dict):
        return None
    for key in ("date", "day_time", "recorded_date", "recorded_at", "timestamp", "start_time"):
        if key in json_obj:
            val = json_obj[key]
            try:
                num = float(val)
                dt = pd.to_datetime(num, unit="ms", errors="coerce")
                if not pd.isna(dt):
                    return dt
            except Exception:
                try:
                    dt = pd.to_datetime(val, errors="coerce")
                    if not pd.isna(dt):
                        return dt
                except Exception:
                    pass
    return None


def _date_from_row_fields(row):
    for col in ("update_time", "create_time"):
        val = row.get(col)
        if isinstance(val, str):
            try:
                dt = pd.to_datetime(val, errors="coerce")
                if not pd.isna(dt) and dt.year >= 2005:
                    return dt
            except Exception:
                pass
    return None


def _build_json_index(json_dir: Path, debug=False):
    """
    Walk json_dir once and index all .binning_data.json filenames to their full paths.
    Returns dict: filename -> [Path, ...]
    """
    index = {}
    if not json_dir.exists():
        if debug:
            print(f"[hrv] JSON directory {json_dir} does not exist.")
        return index
    for p in json_dir.rglob("*.binning_data.json"):
        name = p.name
        index.setdefault(name, []).append(p)
    if debug:
        print(f"[hrv] Indexed {sum(len(v) for v in index.values())} HRV histogram JSON(s) across {len(index)} unique names.")
    return index


def summarize_hrv(base_path: Path, debug=False):
    hrv_csv = next(base_path.glob("com.samsung.health.hrv*.csv"), None)
    if hrv_csv is None:
        if debug:
            print("[hrv] No HRV CSV found.")
        return {"daily": pd.DataFrame(), "monthly": pd.DataFrame()}

    df = _smart_load_hrv_csv(hrv_csv, debug=debug)
    if df.empty:
        return {"daily": pd.DataFrame(), "monthly": pd.DataFrame()}

    json_dir = base_path / "jsons"
    json_index = _build_json_index(json_dir, debug=debug)

    records = []
    for idx, row in df.iterrows():
        bin_ref = row.get("binning_data")
        if not (isinstance(bin_ref, str) and bin_ref.strip().endswith(".binning_data.json")):
            bin_ref = None
            for v in row.values:
                if isinstance(v, str) and v.strip().endswith(".binning_data.json"):
                    bin_ref = v.strip()
                    break
        if not bin_ref:
            if debug:
                print(f"[hrv] no valid binning_data field in row {idx}; skipping")
            continue

        bin_ref_clean = bin_ref.strip()
        json_path = None

        # exact lookup in prebuilt index
        candidates = json_index.get(bin_ref_clean)
        if candidates:
            json_path = candidates[0]
        else:
            # fallback: any indexed name that contains the token substring
            for name, paths in json_index.items():
                if bin_ref_clean in name:
                    json_path = paths[0]
                    break

        if not json_path or not json_path.exists():
            if debug:
                print(f"[hrv] histogram JSON {bin_ref_clean} not found for row {idx} via index.")
            continue

        try:
            raw = json.loads(json_path.read_text(encoding="utf-8"))
        except Exception as e:
            if debug:
                print(f"[hrv] failed to parse JSON {json_path}: {e}")
            continue

        if isinstance(raw, list):
            if not raw:
                if debug:
                    print(f"[hrv] empty list in {json_path}, skipping.")
                continue
            j_dict = raw[0]
        elif isinstance(raw, dict):
            j_dict = raw
        else:
            if debug:
                print(f"[hrv] unexpected JSON top-level type {type(raw)} in {json_path}, skipping.")
            continue

        date = _extract_date_from_json(j_dict) or _date_from_row_fields(row)
        if date is None:
            try:
                mtime = os.path.getmtime(json_path)
                date = pd.to_datetime(mtime, unit="s", errors="coerce")
            except Exception:
                date = None
        if date is None or pd.isna(date):
            if debug:
                print(f"[hrv] could not determine date for histogram {bin_ref_clean} (row {idx}); skipping.")
            continue
        date = pd.to_datetime(date).normalize()

        sdnn = j_dict.get("sdnn")
        rmssd = j_dict.get("rmssd")
        total_samples = j_dict.get("total_samples")

        records.append(
            {
                "date": date,
                "sdnn": float(sdnn) if sdnn is not None else None,
                "rmssd": float(rmssd) if rmssd is not None else None,
                "total_samples": int(total_samples) if total_samples is not None else None,
                "create_sh_ver": row.get("create_sh_ver"),
                "modify_sh_ver": row.get("modify_sh_ver"),
                "deviceuuid": row.get("deviceuuid"),
            }
        )

    if not records:
        if debug:
            print("[hrv] No usable HRV histogram records after extraction.")
        return {"daily": pd.DataFrame(), "monthly": pd.DataFrame()}

    daily_df = pd.DataFrame.from_records(records)
    daily_df = (
        daily_df.sort_values(["deviceuuid", "date", "modify_sh_ver"])
        .drop_duplicates(subset=["deviceuuid", "date"], keep="last")
    )
    daily_df["month"] = daily_df["date"].dt.to_period("M")

    monthly = (
        daily_df.groupby("month")
        .agg(
            avg_rmssd=pd.NamedAgg(column="rmssd", aggfunc=lambda x: round(pd.to_numeric(x, errors="coerce").mean(), 1)),
            avg_sdnn=pd.NamedAgg(column="sdnn", aggfunc=lambda x: round(pd.to_numeric(x, errors="coerce").mean(), 1)),
            days_with_hrv=pd.NamedAgg(column="date", aggfunc="nunique"),
        )
        .reset_index()
        .set_index("month")
    )

    return {"daily": daily_df, "monthly": monthly}


def format_hrv_section(summary_dict):
    monthly = summary_dict.get("monthly", pd.DataFrame())
    if monthly.empty:
        return "=== HRV ===\nNo HRV data found or unable to determine date for records.\n"
    lines = ["=== HRV ==="]
    for m in sorted(monthly.index.astype(str)):
        row = monthly.loc[pd.Period(m)]
        lines.append(f"\n===== {m} =====")
        rmssd = row.get("avg_rmssd", "?")
        sdnn = row.get("avg_sdnn", "?")
        days = row.get("days_with_hrv", 0)
        lines.append(f"Days with HRV: {int(days)}")
        lines.append(f"Average RMSSD: {rmssd} ms")
        lines.append(f"Average SDNN: {sdnn} ms")
    return "\n".join(lines) + "\n"
