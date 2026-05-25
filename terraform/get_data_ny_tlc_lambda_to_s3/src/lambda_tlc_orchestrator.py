"""
Lambda Orchestrator para processamento TLC NYC.
"""
from __future__ import annotations

import json
import logging
import os
from typing import Any

import boto3

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
log = logging.getLogger("tlc_orchestrator")

WORKER_FUNCTION_NAME = os.environ.get("WORKER_FUNCTION_NAME", "")
INVOKE_MODE = os.environ.get("INVOKE_MODE", "async").lower()

DEFAULTS = {
    "datasets": [
        "yellow_tripdata",
        "green_tripdata",
        "fhv_tripdata",
        "fhvhv_tripdata",
    ],
    "year": 2023,
    "months": [1, 2, 3, 4, 5],
    "overwrite": True,
}

_lambda = boto3.client("lambda")


def build_tasks(event: dict) -> list[dict]:
    datasets = event.get("datasets", DEFAULTS["datasets"])
    year = event.get("year", DEFAULTS["year"])
    months = event.get("months", DEFAULTS["months"])
    overwrite = event.get("overwrite", DEFAULTS["overwrite"])

    return [
        {"dataset": ds, "year": year, "month": m, "overwrite": overwrite}
        for ds in datasets
        for m in months
    ]


def invoke_worker(payload: dict, sync: bool) -> dict:
    invocation_type = "RequestResponse" if sync else "Event"
    resp = _lambda.invoke(
        FunctionName=WORKER_FUNCTION_NAME,
        InvocationType=invocation_type,
        Payload=json.dumps(payload).encode("utf-8"),
    )
    out: dict[str, Any] = {
        "task": payload,
        "status_code": resp["StatusCode"],
    }
    if sync:
        body = resp["Payload"].read()
        try:
            out["response"] = json.loads(body)
        except json.JSONDecodeError:
            out["response_raw"] = body.decode("utf-8", errors="replace")
    return out


def lambda_handler(event: dict, context: Any) -> dict:  # noqa: ARG001
    tasks = build_tasks(event)
    log.info(f"Generated {len(tasks)} tasks (mode={INVOKE_MODE})")

    if INVOKE_MODE == "list_only":
        return {"tasks": tasks, "count": len(tasks)}

    if not WORKER_FUNCTION_NAME:
        raise RuntimeError("WORKER_FUNCTION_NAME not configured")

    sync = INVOKE_MODE == "sync"
    results = []
    for t in tasks:
        try:
            r = invoke_worker(t, sync=sync)
            results.append(r)
            log.info(f"Dispatched: {t['dataset']} {t['year']}-{t['month']:02d}")
        except Exception as e:  # noqa: BLE001
            log.exception(f"Failed to dispatch {t}: {e}")
            results.append({"task": t, "error": str(e)})

    return {
        "dispatched": len(results),
        "mode": INVOKE_MODE,
        "results": results if sync else [r["task"] for r in results],
    }
