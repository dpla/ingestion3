"""S3 utilities for generating presigned URLs and building S3 paths."""

import os
import subprocess
from pathlib import Path
from typing import Optional
from dataclasses import dataclass


@dataclass
class S3Paths:
    """S3 paths for a hub's ingest output."""
    hub: str
    s3_prefix: str
    bucket: str
    mapping_dir: Optional[str] = None
    summary_key: Optional[str] = None
    logs_key: Optional[str] = None
    summary_url: Optional[str] = None
    logs_url: Optional[str] = None


def get_latest_dir(base_path: Path) -> Optional[Path]:
    """Get the most recent directory in a path (by name, descending)."""
    if not base_path.exists():
        return None

    dirs = [d for d in base_path.iterdir() if d.is_dir() and not d.name.startswith('.')]
    if not dirs:
        return None

    return max(dirs, key=lambda d: d.name)


def generate_presigned_url(
    bucket: str,
    key: str,
    expiry_seconds: int = 604800,  # 7 days
    aws_profile: str = "dpla"
) -> Optional[str]:
    """Generate a presigned URL for an S3 object.

    Args:
        bucket: S3 bucket name
        key: S3 object key
        expiry_seconds: URL expiry time (default 7 days)
        aws_profile: AWS profile to use

    Returns:
        Presigned URL or None if generation fails
    """
    try:
        import boto3
        from botocore.config import Config as BotoConfig

        # Create session with profile
        session = boto3.Session(profile_name=aws_profile)
        s3_client = session.client(
            's3',
            config=BotoConfig(signature_version='s3v4')
        )

        url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket, 'Key': key},
            ExpiresIn=expiry_seconds
        )
        return url
    except ImportError:
        # boto3 not installed, fall back to CLI
        return _generate_presigned_url_cli(bucket, key, expiry_seconds, aws_profile)
    except Exception as e:
        print(f"Warning: Failed to generate presigned URL: {e}")
        return None


def _generate_presigned_url_cli(
    bucket: str,
    key: str,
    expiry_seconds: int,
    aws_profile: str
) -> Optional[str]:
    """Generate presigned URL using AWS CLI as fallback."""
    try:
        cmd = [
            "aws", "s3", "presign",
            f"s3://{bucket}/{key}",
            "--expires-in", str(expiry_seconds),
            "--profile", aws_profile
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            return result.stdout.strip()
        return None
    except Exception:
        return None


def get_console_url(bucket: str, key: str, region: str = "us-east-1") -> str:
    """Generate AWS Console URL for an S3 object (fallback when presigning fails)."""
    return f"https://s3.console.aws.amazon.com/s3/object/{bucket}?region={region}&prefix={key}"


def get_s3_uri(bucket: str, key: str) -> str:
    """Generate s3:// URI for an S3 object."""
    return f"s3://{bucket}/{key}"


def build_s3_paths(
    hub: str,
    data_dir: Path,
    bucket: str,
    s3_prefix: str,
    aws_profile: str = "dpla",
    generate_urls: bool = True
) -> S3Paths:
    """Build S3 paths and optionally presigned URLs for a hub's ingest output.

    Args:
        hub: Hub name
        data_dir: Local data directory (e.g. /Users/scott/dpla/data)
        bucket: S3 bucket name
        s3_prefix: S3 prefix for the hub (from config.get_s3_prefix)
        aws_profile: AWS profile for presigning
        generate_urls: Whether to generate presigned URLs

    Returns:
        S3Paths with keys and optional presigned URLs
    """
    paths = S3Paths(
        hub=hub,
        s3_prefix=s3_prefix,
        bucket=bucket
    )

    # Find latest mapping directory
    mapping_base = data_dir / hub / "mapping"
    latest_mapping = get_latest_dir(mapping_base)

    if latest_mapping:
        paths.mapping_dir = latest_mapping.name

        # Build S3 keys
        paths.summary_key = f"{s3_prefix}/mapping/{latest_mapping.name}/_SUMMARY"
        paths.logs_key = f"{s3_prefix}/mapping/{latest_mapping.name}/_LOGS"

        if generate_urls:
            # Try to generate presigned URLs
            paths.summary_url = generate_presigned_url(
                bucket, paths.summary_key, aws_profile=aws_profile
            )
            # For logs, try the zip file if it exists
            logs_zip_key = f"{s3_prefix}/mapping/{latest_mapping.name}/_LOGS/logs.zip"
            paths.logs_url = generate_presigned_url(
                bucket, logs_zip_key, aws_profile=aws_profile
            )

            # Fall back to console URLs if presigning failed
            if not paths.summary_url:
                paths.summary_url = get_console_url(bucket, paths.summary_key)
            if not paths.logs_url:
                paths.logs_url = get_console_url(bucket, paths.logs_key)

    return paths


def count_avro_records(avro_dir: Path) -> Optional[int]:
    """Count records in an Avro directory (fallback when _SUMMARY is missing).

    This is a rough estimate based on file sizes or actual record counting.
    """
    if not avro_dir.exists():
        return None

    try:
        # Try using avro-tools or pyarrow if available
        import pyarrow.parquet as pq

        # Avro files in the directory
        avro_files = list(avro_dir.glob("*.avro")) + list(avro_dir.glob("part-*.avro"))
        if not avro_files:
            return None

        # This requires pyarrow with avro support, which may not be available
        # Fall back to file-based estimation
        raise ImportError("Use file-based estimation")
    except ImportError:
        pass

    # Fallback: estimate from file sizes (rough approximation)
    try:
        avro_files = list(avro_dir.glob("*.avro")) + list(avro_dir.glob("part-*.avro"))
        if not avro_files:
            return None

        total_size = sum(f.stat().st_size for f in avro_files)
        # Rough estimate: ~1KB per record on average (very approximate)
        return total_size // 1024 if total_size > 0 else None
    except Exception:
        return None


def count_jsonl_records(jsonl_dir: Path) -> Optional[int]:
    """Count records in a JSONL directory."""
    if not jsonl_dir.exists():
        return None

    try:
        jsonl_files = list(jsonl_dir.glob("*.jsonl")) + list(jsonl_dir.glob("*.json"))
        if not jsonl_files:
            # Check for subdirectories with jsonl files
            jsonl_files = list(jsonl_dir.glob("**/*.jsonl")) + list(jsonl_dir.glob("**/*.json"))

        if not jsonl_files:
            return None

        total_lines = 0
        for f in jsonl_files:
            with open(f, 'r') as fp:
                total_lines += sum(1 for _ in fp)

        return total_lines if total_lines > 0 else None
    except Exception:
        return None
