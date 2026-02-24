#!/usr/bin/env python3
"""
delete-from-jsonl.py - Delete DPLA records from JSONL files in S3

Usage:
    ./delete-from-jsonl.py --hub <hub> -f <file-with-ids>
    ./delete-from-jsonl.py --hub <hub> <id1> [id2] [id3] ...

Examples:
    ./delete-from-jsonl.py --hub cdl -f ids-to-delete.txt
    ./delete-from-jsonl.py --hub cdl 57d000d66004c73c5e31ea7dc7f57201
    DRY_RUN=true ./delete-from-jsonl.py --hub cdl -f ids.txt

Requirements:
    pip install boto3
"""

import argparse
import gzip
import json
import os
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:
    print("Error: boto3 is required but not installed.", file=sys.stderr)
    print("Install it with: pip install boto3", file=sys.stderr)
    sys.exit(1)


# Configuration
DEFAULT_BUCKET = "dpla-master-dataset"
DEFAULT_PROFILE = "dpla"
MAX_WORKERS = 4  # Parallel file processing


def find_latest_jsonl_export(s3_client, bucket: str, hub: str) -> str:
    """Find the most recent JSONL export directory for a hub."""
    prefix = f"{hub}/jsonl/"

    response = s3_client.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix,
        Delimiter="/"
    )

    if "CommonPrefixes" not in response:
        raise ValueError(f"No JSONL exports found in s3://{bucket}/{prefix}")

    # Get all export directories and sort by name (descending) to get most recent
    exports = [p["Prefix"].rstrip("/").split("/")[-1] for p in response["CommonPrefixes"]]
    exports.sort(reverse=True)

    if not exports:
        raise ValueError(f"No JSONL exports found in s3://{bucket}/{prefix}")

    return exports[0]


def list_jsonl_files(s3_client, bucket: str, prefix: str) -> list[str]:
    """List all JSONL files in an S3 prefix."""
    files = []
    paginator = s3_client.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".gz") or key.endswith(".jsonl"):
                files.append(key)

    return files


def filter_jsonl_file(
    s3_client,
    bucket: str,
    s3_key: str,
    exclude_ids: set[str],
    dry_run: bool = False
) -> dict:
    """
    Download a JSONL file, filter out records with excluded IDs, and re-upload.

    Returns stats about the operation.
    """
    filename = s3_key.split("/")[-1]
    is_gzipped = filename.endswith(".gz")

    with tempfile.TemporaryDirectory() as temp_dir:
        local_path = Path(temp_dir) / filename
        filtered_path = Path(temp_dir) / f"filtered_{filename}"

        # Download
        s3_client.download_file(bucket, s3_key, str(local_path))

        # Filter
        before_count = 0
        after_count = 0

        open_read = gzip.open if is_gzipped else open
        open_write = gzip.open if is_gzipped else open

        with open_read(local_path, "rt", encoding="utf-8") as infile, \
             open_write(filtered_path, "wt", encoding="utf-8") as outfile:
            for line in infile:
                before_count += 1
                line = line.rstrip("\n")
                if not line:
                    continue
                try:
                    record = json.loads(line)
                    record_id = record.get("_source", {}).get("id", "")
                    if record_id not in exclude_ids:
                        outfile.write(line + "\n")
                        after_count += 1
                except json.JSONDecodeError:
                    # Keep malformed lines
                    outfile.write(line + "\n")
                    after_count += 1

        removed = before_count - after_count

        # Upload if records were removed (and not dry run)
        uploaded = False
        if removed > 0 and not dry_run:
            s3_client.upload_file(str(filtered_path), bucket, s3_key)
            uploaded = True

        return {
            "file": filename,
            "s3_key": s3_key,
            "before": before_count,
            "after": after_count,
            "removed": removed,
            "uploaded": uploaded
        }


def load_ids_from_file(filepath: str) -> set[str]:
    """Load IDs from a file, one per line. Supports '-' for stdin."""
    ids = set()

    if filepath == "-":
        f = sys.stdin
    else:
        f = open(filepath, "r")

    try:
        for line in f:
            # Strip comments and whitespace
            line = line.split("#")[0].strip()
            if line:
                ids.add(line)
    finally:
        if filepath != "-":
            f.close()

    return ids


def main():
    parser = argparse.ArgumentParser(
        description="Delete DPLA records from JSONL files in S3",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --hub cdl -f ids-to-delete.txt
  %(prog)s --hub cdl -y -f ids.txt              # Skip confirmation
  DRY_RUN=true %(prog)s --hub cdl -f ids.txt    # Preview only
  %(prog)s --hub cdl 57d000d66004c73c5e31ea7dc7f57201 83851552c94d5b8372f99f8d635fc23e

Environment variables:
  AWS_PROFILE  AWS profile name (default: dpla)
  S3_BUCKET    S3 bucket name (default: dpla-master-dataset)
  DRY_RUN      Set to 'true' to preview without modifying S3
        """
    )

    parser.add_argument(
        "--hub",
        required=True,
        help="Hub/provider short name (e.g., cdl, mdl, pa)"
    )
    parser.add_argument(
        "-f", "--file",
        help="Read IDs from file (one per line), use '-' for stdin"
    )
    parser.add_argument(
        "-y", "--yes",
        action="store_true",
        help="Skip confirmation prompt"
    )
    parser.add_argument(
        "--bucket",
        default=os.environ.get("S3_BUCKET", DEFAULT_BUCKET),
        help=f"S3 bucket name (default: {DEFAULT_BUCKET})"
    )
    parser.add_argument(
        "--profile",
        default=os.environ.get("AWS_PROFILE", DEFAULT_PROFILE),
        help=f"AWS profile name (default: {DEFAULT_PROFILE})"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=os.environ.get("DRY_RUN", "").lower() == "true",
        help="Preview without modifying S3"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=MAX_WORKERS,
        help=f"Number of parallel workers (default: {MAX_WORKERS})"
    )
    parser.add_argument(
        "ids",
        nargs="*",
        help="DPLA IDs to delete"
    )

    args = parser.parse_args()

    # Collect IDs
    exclude_ids = set(args.ids) if args.ids else set()

    if args.file:
        exclude_ids.update(load_ids_from_file(args.file))

    if not exclude_ids:
        print("Error: No IDs provided", file=sys.stderr)
        parser.print_help()
        sys.exit(1)

    # Initialize S3 client with profile
    try:
        session = boto3.Session(profile_name=args.profile)
        s3_client = session.client("s3")
    except Exception as e:
        print(f"Error: Could not initialize AWS session with profile '{args.profile}': {e}", file=sys.stderr)
        sys.exit(1)

    # Find latest export
    print(f"Finding latest JSONL export for hub '{args.hub}'...")
    try:
        latest_export = find_latest_jsonl_export(s3_client, args.bucket, args.hub)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    s3_prefix = f"{args.hub}/jsonl/{latest_export}/"
    print(f"Found: s3://{args.bucket}/{s3_prefix}")
    print()

    # List JSONL files
    print("Listing JSONL files...")
    jsonl_files = list_jsonl_files(s3_client, args.bucket, s3_prefix)

    if not jsonl_files:
        print(f"Error: No JSONL files found in s3://{args.bucket}/{s3_prefix}", file=sys.stderr)
        sys.exit(1)

    # Print summary
    print()
    print("S3 JSONL Delete")
    print("=" * 40)
    print(f"Profile:    {args.profile}")
    print(f"Bucket:     {args.bucket}")
    print(f"Hub:        {args.hub}")
    print(f"Export:     {latest_export}")
    print(f"Files:      {len(jsonl_files)} file(s)")
    print(f"IDs:        {len(exclude_ids)} record(s) to delete")
    print(f"Workers:    {args.workers}")
    print()

    if args.dry_run:
        print("[DRY RUN] Would process the following files:")
        for f in jsonl_files[:20]:
            print(f"  {f.split('/')[-1]}")
        if len(jsonl_files) > 20:
            print(f"  ... and {len(jsonl_files) - 20} more")
        print()
        print("IDs to delete:")
        for id_ in list(exclude_ids)[:20]:
            print(f"  {id_}")
        if len(exclude_ids) > 20:
            print(f"  ... and {len(exclude_ids) - 20} more")
        sys.exit(0)

    # Confirm
    if not args.yes:
        response = input(f"This will modify {len(jsonl_files)} file(s) in S3. Continue? [y/N] ")
        if response.lower() != "y":
            print("Aborted.")
            sys.exit(0)

    # Process files in parallel
    print("Processing files...")
    print()

    total_removed = 0
    processed = 0

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(
                filter_jsonl_file,
                s3_client,
                args.bucket,
                s3_key,
                exclude_ids,
                args.dry_run
            ): s3_key
            for s3_key in jsonl_files
        }

        for future in as_completed(futures):
            processed += 1
            s3_key = futures[future]

            try:
                result = future.result()
                total_removed += result["removed"]

                status = ""
                if result["removed"] > 0:
                    status = f"removed {result['removed']} (was {result['before']}, now {result['after']})"
                    if result["uploaded"]:
                        status += " ✓"
                else:
                    status = "no matches"

                print(f"[{processed}/{len(jsonl_files)}] {result['file']}: {status}")

            except Exception as e:
                print(f"[{processed}/{len(jsonl_files)}] {s3_key.split('/')[-1]}: ERROR - {e}", file=sys.stderr)

    print()
    print("=" * 40)
    print("Complete!")
    print(f"Total records removed: {total_removed}")
    print(f"Files processed: {processed}")


if __name__ == "__main__":
    main()
