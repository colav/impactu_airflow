"""Open Library monthly snapshot capture DAG.

Workflow
--------
1. ``resolve_snapshot``   — pin ``latest`` to a dated snapshot and its MD5s; skip the
   run when that snapshot is already loaded.
2. ``download_file``      — one task per dump; resumable and verified against the MD5.
3. ``load_collection``    — one task per dump; upserts by ``key`` from a line checkpoint.
4. ``purge_and_cleanup``  — after every collection passes its safety checks, delete
   records absent from the snapshot and remove old snapshots from disk.

Scheduled on day 5 because Open Library publishes a few days after the month ends.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Param

from config.notifications import completion_callbacks

_DUMP_TYPES = ("editions", "works", "authors", "redirects")
_DEFAULT_SNAPSHOT_BASE = "/storage/raw_data/openlibrary"


def _extractor(params: dict[str, Any], collection_name: str) -> Any:
    from extract.openlibrary.openlibrary_extractor import OpenLibraryExtractor

    hook = MongoHook(params["mongo_conn_id"])
    return OpenLibraryExtractor(
        mongodb_uri=hook.uri,
        db_name=params["db_name"],
        collection_name=collection_name,
        client=hook.get_conn(),
        chunk_size=params["chunk_size"],
    )


def _manifest(context: dict[str, Any]) -> dict[str, Any]:
    manifest: dict[str, Any] = context["ti"].xcom_pull(task_ids="resolve_snapshot")
    return manifest


def resolve_snapshot(**context: Any) -> dict[str, Any]:
    """Return the manifest of the latest snapshot, or skip the run if already loaded."""
    from extract.openlibrary.openlibrary_extractor import (
        SNAPSHOT_CHECKPOINT,
        resolve_latest_snapshot,
    )

    params = context["params"]
    manifest = resolve_latest_snapshot(_DUMP_TYPES)
    extractor = _extractor(params, _DUMP_TYPES[0])
    try:
        if params["force"]:
            extractor.reset_load_state()
        elif extractor.get_checkpoint(SNAPSHOT_CHECKPOINT) == manifest["snapshot_date"]:
            raise AirflowSkipException(f"Snapshot {manifest['snapshot_date']} is already loaded")
    finally:
        extractor.close()
    return manifest


def download_file(dump_type: str, **context: Any) -> str:
    """Download and verify one dump file of the pinned snapshot."""
    from extract.openlibrary.openlibrary_extractor import download_snapshot_file

    manifest = _manifest(context)
    target_dir = Path(context["params"]["snapshot_base_dir"]) / manifest["snapshot_date"]
    return download_snapshot_file(manifest["files"][dump_type], target_dir)


def load_collection(dump_type: str, **context: Any) -> dict[str, Any]:
    """Load one verified dump file into its MongoDB collection."""
    params = context["params"]
    manifest = _manifest(context)
    snapshot_date = manifest["snapshot_date"]
    file_path = (
        Path(params["snapshot_base_dir"]) / snapshot_date / manifest["files"][dump_type]["name"]
    )
    extractor = _extractor(params, dump_type)
    try:
        result: dict[str, Any] = extractor.run(snapshot_date, str(file_path))
        return result
    finally:
        extractor.close()


def purge_and_cleanup(**context: Any) -> dict[str, Any]:
    """Purge records absent from the snapshot, mark it loaded and trim the disk."""
    from extract.openlibrary.openlibrary_extractor import (
        SNAPSHOT_CHECKPOINT,
        remove_old_snapshots,
    )

    params = context["params"]
    snapshot_date = _manifest(context)["snapshot_date"]
    extractors = [_extractor(params, dump_type) for dump_type in _DUMP_TYPES]
    try:
        for extractor in extractors:
            extractor.check_ready_to_purge(
                snapshot_date, params["max_error_ratio"], params["max_purge_ratio"]
            )
        purged = {item.collection_name: item.purge_stale(snapshot_date) for item in extractors}
        extractors[0].save_checkpoint(SNAPSHOT_CHECKPOINT, snapshot_date)
    finally:
        for extractor in extractors:
            extractor.close()

    removed = remove_old_snapshots(params["snapshot_base_dir"], params["keep_snapshots"])
    return {"snapshot_date": snapshot_date, "purged": purged, "removed_snapshots": removed}


with DAG(
    dag_id="openlibrary_capture",
    default_args={
        "owner": "impactu",
        "retries": 2,
        "retry_delay": timedelta(hours=1),
    },
    description="Download the Open Library monthly dumps and mirror them into MongoDB",
    params={
        "mongo_conn_id": Param(
            "mongodb_default", type="string", description="Airflow MongoDB connection ID"
        ),
        "db_name": Param("openlibrary", type="string", description="Target MongoDB database"),
        "snapshot_base_dir": Param(
            _DEFAULT_SNAPSHOT_BASE,
            type="string",
            description="Directory holding one sub-directory per snapshot",
        ),
        "chunk_size": Param(
            2000, type="integer", minimum=1, description="Documents per bulk_write batch"
        ),
        "keep_snapshots": Param(
            2, type="integer", minimum=1, description="Snapshots to retain on disk"
        ),
        "max_error_ratio": Param(
            0.001,
            type="number",
            minimum=0,
            maximum=1,
            description="Largest share of unstorable rows that still allows the purge",
        ),
        "max_purge_ratio": Param(
            0.1,
            type="number",
            minimum=0,
            maximum=1,
            description="Largest share of a collection the purge may delete",
        ),
        "force": Param(
            False,
            type="boolean",
            description="Reload from the first line even if the snapshot is already loaded",
        ),
    },
    start_date=datetime(2026, 1, 1),
    schedule="0 3 5 * *",
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=["capture", "openlibrary"],
    **completion_callbacks(),
) as dag:
    resolve = PythonOperator(task_id="resolve_snapshot", python_callable=resolve_snapshot)

    downloads = PythonOperator.partial(
        task_id="download_file", python_callable=download_file
    ).expand(op_args=[[dump_type] for dump_type in _DUMP_TYPES])

    loads = PythonOperator.partial(
        task_id="load_collection", python_callable=load_collection
    ).expand(op_args=[[dump_type] for dump_type in _DUMP_TYPES])

    finish = PythonOperator(task_id="purge_and_cleanup", python_callable=purge_and_cleanup)

    resolve >> downloads >> loads >> finish
