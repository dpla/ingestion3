#!/usr/bin/env python3
"""
Local test of the NARA preprocessing logic.

Runs the unzip / deletes-move / rename / recompress steps on your Mac
so you can verify the output structure before running the real thing on EC2.

Output goes to /tmp/nara-test/<date>/ by default (or --output-dir).

Usage:
    python3 run/nara/test_preprocessing_nara.py --date 20260501 --zip ~/Downloads/nara-delta.zip
"""

import argparse
import glob
import os
import re
import shutil
import subprocess
import sys


def run(cmd, cwd=None):
    """Run a shell command locally, print it, raise on failure."""
    print(f"  $ {cmd}")
    result = subprocess.run(cmd, shell=True, cwd=cwd, capture_output=True, text=True)
    if result.stdout.strip():
        print(result.stdout.rstrip())
    if result.stderr.strip():
        print(result.stderr.rstrip())
    if result.returncode != 0:
        raise RuntimeError(f"Command failed (exit {result.returncode}): {cmd}")


def main():
    parser = argparse.ArgumentParser(description="Test NARA preprocessing locally")
    parser.add_argument("--date", required=True, help="Delta date YYYYMMDD e.g. 20260501")
    parser.add_argument("--zip",  required=True, help="Path to local NARA delta ZIP")
    parser.add_argument("--output-dir", default="/tmp/nara-test",
                        help="Root output dir (default: /tmp/nara-test)")
    parser.add_argument("--clean", action="store_true",
                        help="Wipe output dir before running (fresh start)")
    args = parser.parse_args()

    if not re.match(r"^\d{8}$", args.date):
        sys.exit(f"--date must be YYYYMMDD, got: {args.date!r}")
    zip_path = os.path.expanduser(args.zip)
    if not os.path.isfile(zip_path):
        sys.exit(f"ZIP not found: {zip_path}")

    date_str    = args.date
    delta_dir   = os.path.join(args.output_dir, date_str)
    deletes_dir = os.path.join(delta_dir, "deletes")

    print(f"\nNARA PREPROCESS TEST — {date_str}")
    print(f"ZIP:        {zip_path}")
    print(f"Output dir: {delta_dir}\n")

    # Clean if requested
    if args.clean and os.path.exists(delta_dir):
        print(f"  Removing existing {delta_dir}...")
        shutil.rmtree(delta_dir)

    # Step 1: Create dirs
    print("── Step 1: Create directory structure")
    os.makedirs(deletes_dir, exist_ok=True)
    print(f"  Created: {delta_dir}/")
    print(f"  Created: {deletes_dir}/")

    # Step 2: Copy ZIP in (mirrors EC2 downloading from S3)
    print("\n── Step 2: Copy ZIP into delta dir")
    local_zip = os.path.join(delta_dir, "nara-delta.zip")
    shutil.copy2(zip_path, local_zip)
    print(f"  Copied → {local_zip}")

    # Step 3: Unzip
    print("\n── Step 3: Unzip")
    run(f"unzip -o nara-delta.zip", cwd=delta_dir)

    # Step 4: Move and rename deletes_*.xml
    print("\n── Step 4: Move deletes_*.xml into deletes/ with date prefix")
    delete_files = glob.glob(os.path.join(delta_dir, "deletes_*.xml"))
    if not delete_files:
        print("  [CAUTION] No deletes_*.xml files found at the top level of the ZIP.")
        print("  They may be in a subdirectory, or this ZIP has no deletes.")
    for src in sorted(delete_files):
        fname   = os.path.basename(src)
        dst     = os.path.join(deletes_dir, f"{date_str}_{fname}")
        os.rename(src, dst)
        print(f"  moved: {fname} → deletes/{date_str}_{fname}")

    # Step 5: Recompress update files
    print("\n── Step 5: Recompress update files (excluding deletes/ and .zip/.tar.gz)")
    tar_name = f"{date_str}-nara-delta.tar.gz"
    run(
        f"tar czf {tar_name} "
        f"--exclude='./deletes' --exclude='*.zip' --exclude='*.tar.gz' .",
        cwd=delta_dir,
    )
    tar_path = os.path.join(delta_dir, tar_name)
    tar_size = os.path.getsize(tar_path) / 1024 / 1024
    print(f"  Created: {tar_name}  ({tar_size:.1f} MB)")

    # Step 6: Show final structure
    print("\n── Final structure")
    print(f"\n  {delta_dir}/")
    for entry in sorted(os.listdir(delta_dir)):
        full = os.path.join(delta_dir, entry)
        if os.path.isdir(full):
            sub = sorted(os.listdir(full))
            print(f"    {entry}/  ({len(sub)} files)")
            for s in sub[:5]:
                print(f"      {s}")
            if len(sub) > 5:
                print(f"      ... ({len(sub) - 5} more)")
        else:
            size = os.path.getsize(full) / 1024 / 1024
            print(f"    {entry}  ({size:.1f} MB)")

    print(f"\n✓ Preprocessing test complete. Output at: {delta_dir}")
    print("  If the structure looks right, the real run_nara.py will produce")
    print("  the same layout on EC2 under ~/nara/delta/<date>/\n")


if __name__ == "__main__":
    main()