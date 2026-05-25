"""
Lambda Worker para download de dados TLC NYC para S3.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any

import boto3
import requests
from botocore.config import Config
from botocore.exceptions import ClientError

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

TARGET_BUCKET = os.environ.get("TARGET_BUCKET", "datalake-geral-trusted")
TARGET_PREFIX = os.environ.get("TARGET_PREFIX", "landing/prd").strip("/")
HTTP_TIMEOUT = int(os.environ.get("HTTP_TIMEOUT", "60"))
MULTIPART_THRESHOLD_MB = int(os.environ.get("MULTIPART_THRESHOLD_MB", "50"))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")

VALID_DATASETS = {
    "yellow_tripdata",
    "green_tripdata",
    "fhv_tripdata",
    "fhvhv_tripdata",
}

STREAM_CHUNK_SIZE = 8 * 1024 * 1024

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
)
log = logging.getLogger("tlc_worker")

_s3 = boto3.client(
    "s3",
    config=Config(
        retries={"max_attempts": 5, "mode": "adaptive"},
        connect_timeout=10,
        read_timeout=120,
    ),
)


@dataclass(frozen=True)
class TripFile:
    dataset: str
    year: int
    month: int

    @property
    def filename(self) -> str:
        return f"{self.dataset}_{self.year}-{self.month:02d}.parquet"

    @property
    def source_url(self) -> str:
        return f"{BASE_URL}/{self.filename}"

    @property
    def s3_key(self) -> str:
        return (
            f"{TARGET_PREFIX}/{self.dataset}/"
            f"year={self.year}/month={self.month:02d}/{self.filename}"
        )


def parse_event(event: dict) -> tuple[TripFile, bool]:
    """Validate event and return TripFile and overwrite flag."""
    try:
        dataset = event["dataset"]
        year = int(event["year"])
        month = int(event["month"])
    except (KeyError, TypeError, ValueError) as e:
        raise ValueError(f"Invalid event - expected dataset/year/month: {e}")

    if dataset not in VALID_DATASETS:
        raise ValueError(
            f"Invalid dataset '{dataset}'. Valid options: {sorted(VALID_DATASETS)}"
        )
    if not (1 <= month <= 12):
        raise ValueError(f"Invalid month: {month}")
    if not (2009 <= year <= 2099):
        raise ValueError(f"Year out of range: {year}")

    overwrite = bool(event.get("overwrite", True))
    return TripFile(dataset, year, month), overwrite


def object_exists(bucket: str, key: str) -> bool:
    try:
        _s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def stream_download_to_s3(trip: TripFile) -> dict[str, Any]:
    """
    Download file via HTTP streaming and upload to S3 using multipart upload.
    No local disk usage.
    """
    log.info(f"GET {trip.source_url}")
    with requests.get(trip.source_url, stream=True, timeout=HTTP_TIMEOUT) as r:
        r.raise_for_status()
        content_length = r.headers.get("Content-Length")
        size_hint_mb = (int(content_length) / (1024 * 1024)) if content_length else None
        if size_hint_mb:
            log.info(f"Content size: {size_hint_mb:.1f} MB")

        log.info(f"Starting multipart upload to s3://{TARGET_BUCKET}/{trip.s3_key}")
        mpu = _s3.create_multipart_upload(
            Bucket=TARGET_BUCKET,
            Key=trip.s3_key,
            ContentType="application/octet-stream",
            Metadata={
                "source": "nyc-tlc",
                "dataset": trip.dataset,
                "year": str(trip.year),
                "month": f"{trip.month:02d}",
            },
        )
        upload_id = mpu["UploadId"]
        parts: list[dict[str, Any]] = []
        part_number = 1
        buffer = bytearray()
        total_bytes = 0

        try:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if not chunk:
                    continue
                buffer.extend(chunk)
                if len(buffer) >= STREAM_CHUNK_SIZE:
                    parts.append(_upload_part(trip.s3_key, upload_id, part_number, bytes(buffer)))
                    total_bytes += len(buffer)
                    log.info(
                        f"Part {part_number} ({len(buffer) / 1024 / 1024:.1f} MB) "
                        f"- total {total_bytes / 1024 / 1024:.1f} MB"
                    )
                    buffer.clear()
                    part_number += 1

            if buffer:
                parts.append(_upload_part(trip.s3_key, upload_id, part_number, bytes(buffer)))
                total_bytes += len(buffer)
                log.info(
                    f"Final part {part_number} ({len(buffer) / 1024 / 1024:.1f} MB) "
                    f"- total {total_bytes / 1024 / 1024:.1f} MB"
                )

            _s3.complete_multipart_upload(
                Bucket=TARGET_BUCKET,
                Key=trip.s3_key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )
            log.info(f"Upload completed ({total_bytes / 1024 / 1024:.1f} MB)")
            return {
                "bytes": total_bytes,
                "parts": len(parts),
                "s3_uri": f"s3://{TARGET_BUCKET}/{trip.s3_key}",
            }

        except Exception:
            log.exception("Upload failed, aborting multipart")
            _s3.abort_multipart_upload(
                Bucket=TARGET_BUCKET, Key=trip.s3_key, UploadId=upload_id
            )
            raise


def _upload_part(key: str, upload_id: str, part_number: int, data: bytes) -> dict:
    resp = _s3.upload_part(
        Bucket=TARGET_BUCKET,
        Key=key,
        PartNumber=part_number,
        UploadId=upload_id,
        Body=data,
    )
    return {"PartNumber": part_number, "ETag": resp["ETag"]}


def lambda_handler(event: dict, context: Any) -> dict:  # noqa: ARG001
    log.info(f"Event received: {event}")
    trip, overwrite = parse_event(event)

    log.info(
        f"Target: s3://{TARGET_BUCKET}/{trip.s3_key} (overwrite={overwrite})"
    )

    if not overwrite and object_exists(TARGET_BUCKET, trip.s3_key):
        log.info("Object already exists and overwrite=False - skipping")
        return {
            "status": "skipped",
            "reason": "already_exists",
            "s3_uri": f"s3://{TARGET_BUCKET}/{trip.s3_key}",
        }

    result = stream_download_to_s3(trip)
    return {"status": "success", **result}
