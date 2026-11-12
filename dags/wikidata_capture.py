"""Refresh the Wikidata entities referenced by ROR organizations."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Param

from config.notifications import completion_callbacks


def run_wikidata_capture(**context: Any) -> dict[str, Any]:
    from extract.wikidata.wikidata_extractor import WikidataExtractor

    params = context["params"]
    hook = MongoHook(mongo_conn_id=params["mongo_conn_id"])
    extractor = WikidataExtractor(
        mongodb_uri=hook.uri,
        db_name=params["db_name"],
        collection_name=params["collection_name"],
        client=hook.get_conn(),
        ror_db_name=params["ror_db_name"],
        ror_collection_name=params["ror_collection_name"],
        request_delay=float(params["request_delay"]),
    )
    try:
        return extractor.run(batch_size=int(params["batch_size"]), force=bool(params["force"]))
    finally:
        extractor.close()


default_args = {
    "owner": "impactu",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(hours=1),
}


with DAG(
    "wikidata_capture",
    default_args=default_args,
    description=(
        "Refresh wikidata.data with the entities referenced by ROR organizations "
        "(wbgetentities, 50 ids per request)"
    ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["capture", "wikidata"],
    params={
        "mongo_conn_id": Param("mongodb_default", type="string", description="Mongo connection id"),
        "db_name": Param("wikidata", type="string", description="Database of Wikidata entities"),
        "collection_name": Param("data", type="string", description="Entities collection"),
        "ror_db_name": Param("ror", type="string", description="Database of the ROR staging"),
        "ror_collection_name": Param(
            "ror_stage", type="string", description="ROR collection that lists the QIDs"
        ),
        "batch_size": Param(
            50, type="integer", minimum=1, maximum=50, description="Ids per API request"
        ),
        "request_delay": Param(
            0.5, type="number", minimum=0, description="Seconds between API requests"
        ),
        "force": Param(
            False, type="boolean", description="Ignore the checkpoint and start from the first QID"
        ),
    },
    **completion_callbacks(),
) as dag:
    PythonOperator(task_id="wikidata_capture", python_callable=run_wikidata_capture)
