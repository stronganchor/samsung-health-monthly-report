#!/usr/bin/env python3
import os

def main():
    # Absolute path of this script
    try:
        script_path = os.path.abspath(__file__)
    except NameError:
        # Fallback if __file__ is not defined (e.g., interactive), use cwd
        script_path = os.path.join(os.getcwd(), "")
    dirpath = os.path.dirname(script_path)

    output_filename = "file_list.txt"
    output_path = os.path.join(dirpath, output_filename)

    filenames = []
    for entry in os.listdir(dirpath):
        full_path = os.path.join(dirpath, entry)
        if not os.path.isfile(full_path):
            continue  # skip directories
        if os.path.abspath(full_path) == os.path.abspath(script_path):
            continue  # skip the script itself
        filenames.append(entry)

    filenames.sort()
    with open(output_path, "w", encoding="utf-8") as out:
        for name in filenames:
            out.write(name + "\n")

    print(f"Wrote {len(filenames)} filenames to '{output_filename}'")

if __name__ == "__main__":
    main()
