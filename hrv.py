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

    # More thorough search - look for any JSON files
    json_files = list(json_dir.rglob("*.json"))
    if debug:
        print(f"[hrv] [DEBUG] Found {len(json_files)} total JSON files in {json_dir}")
        if json_files:
            print(f"[hrv] [DEBUG] Sample JSON files: {[f.name for f in json_files[:5]]}")

    # Index both full names and base names (without .binning_data.json)
    for p in json_files:
        name = p.name
        index.setdefault(name, []).append(p)

        # Also index by the UUID part if it's a .binning_data.json file
        if name.endswith('.binning_data.json'):
            uuid_part = name.replace('.binning_data.json', '')
            index.setdefault(uuid_part + '.binning_data.json', []).append(p)

    if debug:
        print(f"[hrv] Indexed {len(index)} unique JSON file patterns.")
        print(f"[hrv] [DEBUG] Index keys (first 10): {list(index.keys())[:10]}")
    return index


def summarize_hrv(base_path: Path, debug=False):
    hrv_files = list(base_path.glob("com.samsung.health.hrv*.csv"))

    if debug:
        print(f"[hrv] [DEBUG] Found HRV files: {[f.name for f in hrv_files]}")
        print(f"[hrv] [DEBUG] Base path: {base_path}")

    if not hrv_files:
        if debug:
            print("[hrv] No HRV CSV found.")
        return {"daily": pd.DataFrame(), "monthly": pd.DataFrame()}

    hrv_csv = hrv_files[0]  # Use the first HRV file found
    if debug:
        print(f"[hrv] [DEBUG] Using HRV file: {hrv_csv.name}")

    df = _smart_load_hrv_csv(hrv_csv, debug=debug)
    if df.empty:
        return {"daily": pd.DataFrame(), "monthly": pd.DataFrame()}

    json_dir = base_path / "jsons"
    json_index = _build_json_index(json_dir, debug=debug)

    records = []
    processed_count = 0
    found_count = 0

    for idx, row in df.iterrows():
        processed_count += 1
        if debug and processed_count % 1000 == 0:
            print(f"[hrv] [DEBUG] Processed {processed_count} rows, found {found_count} JSON files")

        bin_ref = row.get("binning_data")
        if not (isinstance(bin_ref, str) and bin_ref.strip().endswith(".binning_data.json")):
            bin_ref = None
            for v in row.values:
                if isinstance(v, str) and v.strip().endswith(".binning_data.json"):
                    bin_ref = v.strip()
                    break
        if not bin_ref:
            if debug and processed_count <= 5:  # Only show first few
                print(f"[hrv] no valid binning_data field in row {idx}; skipping")
            continue

        bin_ref_clean = bin_ref.strip()
        json_path = None

        # Try multiple lookup strategies
        # 1. exact lookup in prebuilt index
        candidates = json_index.get(bin_ref_clean)
        if candidates:
            json_path = candidates[0]
        else:
            # 2. fallback: any indexed name that contains the token substring
            for name, paths in json_index.items():
                if bin_ref_clean in name or name in bin_ref_clean:
                    json_path = paths[0]
                    break

        if not json_path or not json_path.exists():
            if debug and found_count < 5:  # Only show first few failures
                print(f"[hrv] histogram JSON {bin_ref_clean} not found for row {idx} via index.")
            continue

        found_count += 1
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
            if debug and found_count <= 5:
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

    if debug:
        print(f"[hrv] [DEBUG] Final stats: processed {processed_count} rows, found {found_count} JSON files, extracted {len(records)} records")

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
