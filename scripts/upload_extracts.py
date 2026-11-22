#!/usr/bin/env python3
"""Upload the raw CSV extracts to the raw S3 bucket (made by infra/terraform).

Each file goes to s3://<raw bucket>/extracts/<table>/<sha256 prefix>.csv,
with its full SHA-256, table and original file name as object metadata.
The key is derived from the content, so uploading the same file again is a
no-op, like scripts/load_raw.py skipping a file already in raw._load_log.
Nothing is ever overwritten or deleted.

Endpoint: AWS_ENDPOINT_URL (boto3 reads it), default the Moto emulator from
infra/docker-compose.yml; set it to an empty string for real AWS, where
credentials come from the standard chain (environment, profile or role).

Usage:
    python scripts/upload_extracts.py
    python scripts/upload_extracts.py --raw-dir DIR --bucket NAME
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from pathlib import Path

import boto3
from load_raw import DEFAULT_RAW_DIR, TABLES, sha256_of

DEFAULT_ENDPOINT = "http://localhost:15000"
DEFAULT_BUCKET = "ecom-dev-raw"


@dataclass(frozen=True)
class Upload:
    table: str
    key: str
    uploaded: bool  # False when the same content was already there


def s3_client(region: str = "ca-central-1"):
    endpoint = os.environ.get("AWS_ENDPOINT_URL", DEFAULT_ENDPOINT)
    if not endpoint:
        return boto3.client("s3", region_name=region)
    return boto3.client(
        "s3",
        region_name=region,
        endpoint_url=endpoint,
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
    )


def object_key(table: str, checksum: str) -> str:
    return f"extracts/{table}/{checksum[:16]}.csv"


def exists(s3, bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
    except s3.exceptions.ClientError as err:
        if err.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return False
        raise
    return True


def upload_all(raw_dir: Path, bucket: str, s3=None) -> list[Upload]:
    s3 = s3 or s3_client()
    missing = [t for t in TABLES if not (raw_dir / f"{t}.csv").exists()]
    if missing:
        raise FileNotFoundError(f"missing extracts in {raw_dir}: {', '.join(missing)}")
    results = []
    for table in TABLES:
        path = raw_dir / f"{table}.csv"
        checksum = sha256_of(path)
        key = object_key(table, checksum)
        if exists(s3, bucket, key):
            results.append(Upload(table, key, uploaded=False))
            continue
        s3.upload_file(
            str(path),
            bucket,
            key,
            ExtraArgs={
                "Metadata": {
                    "sha256": checksum,
                    "table": table,
                    "file-name": path.name,
                },
                "ContentType": "text/csv",
            },
        )
        results.append(Upload(table, key, uploaded=True))
    return results


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW_DIR)
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    args = parser.parse_args(argv)
    try:
        results = upload_all(args.raw_dir, args.bucket)
    except FileNotFoundError as err:
        print(f"{err} (run `make data` first)", file=sys.stderr)
        return 2
    uploaded = sum(r.uploaded for r in results)
    for r in results:
        print(f"{'uploaded' if r.uploaded else 'skipped '} s3://{args.bucket}/{r.key}")
    skipped = len(results) - uploaded
    print(f"{uploaded} of {len(results)} extracts uploaded, {skipped} already there")
    return 0


if __name__ == "__main__":
    sys.exit(main())
