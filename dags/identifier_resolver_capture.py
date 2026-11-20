"""Resolve the DOIs and ORCID iDs Kahi queued for its identifier disputes."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Param

from config.notifications import completion_callbacks


def run_identifier_resolver_capture(**context: Any) -> dict[str, Any]:
    from extract.identifier_resolver.identifier_resolver_extractor import (
        IdentifierResolverExtractor,
    )

    params = context["params"]
    hook = MongoHook(mongo_conn_id=params["mongo_conn_id"])
    extractor = IdentifierResolverExtractor(
        mongodb_uri=hook.uri,
        db_name=params["db_name"],
        client=hook.get_conn(),
        request_delay=float(params["request_delay"]),
        mailto=params["mailto"],
    )
    try:
        return extractor.run(
            kinds=list(params["kinds"]),
            limit=int(params["limit"]),
            refresh_not_found=bool(params["refresh_not_found"]),
        )
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
    "identifier_resolver_capture",
    default_args=default_args,
    description=(
        "Resolve the pending DOIs (Crossref, DataCite, doi.org) and ORCID iDs "
        "queued in impactu_identifier_resolver"
    ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["capture", "identifier_resolver"],
    params={
        "mongo_conn_id": Param("mongodb_default", type="string", description="Mongo connection id"),
        "db_name": Param(
            "impactu_identifier_resolver", type="string", description="Resolver database"
        ),
        "kinds": Param(
            ["doi", "orcid"],
            type="array",
            items={"type": "string", "enum": ["doi", "orcid"]},
            description="Queues to resolve",
        ),
        "limit": Param(
            0, type="integer", minimum=0, description="Maximum identifiers per kind (0 = all)"
        ),
        "request_delay": Param(
            0.2, type="number", minimum=0, description="Seconds between identifiers"
        ),
        "mailto": Param(
            "", type="string", description="Contact for the Crossref polite pool (optional)"
        ),
        "refresh_not_found": Param(
            False, type="boolean", description="Ask again for identifiers that did not exist"
        ),
    },
    **completion_callbacks(),
) as dag:
    PythonOperator(
        task_id="identifier_resolver_capture", python_callable=run_identifier_resolver_capture
    )
