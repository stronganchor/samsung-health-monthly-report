#!/usr/bin/env python3
import sys
import pathlib
from steps import summarize_steps, format_steps_section
from hrv import summarize_hrv, format_hrv_section

def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    debug = "--debug" in sys.argv

    if len(args) >= 1:
        base = pathlib.Path(args[0])
    else:
        base = pathlib.Path(__file__).resolve().parent
        print(f"[INFO] No path given; using script directory: {base}")

    if not base.is_dir():
        print(f"Error: {base} is not a directory.")
        sys.exit(1)

    sections = []

    # Steps
    step_summary = summarize_steps(base, debug=debug)
    sections.append(format_steps_section(step_summary))

    # HRV
    hrv_summary = summarize_hrv(base, debug=debug)
    sections.append(format_hrv_section(hrv_summary))

    # Future: other modules here...

    output_text = "\n".join(sections)
    print(output_text)
    try:
        (base / "monthly_summary.txt").write_text(output_text, encoding="utf-8")
        print(f"Wrote summary to {base/'monthly_summary.txt'}")
    except Exception as e:
        print(f"Failed to write summary: {e}")

if __name__ == "__main__":
    main()
