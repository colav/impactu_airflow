"""Load OpenAlex works from MongoDB into Elasticsearch using mohan.Similarity.

Workflow
--------
1. ``load_works`` — reads ``openalex.works`` from MongoDB, parses titles
   (MathML / HTML), builds documents and bulk-indexes them into Elasticsearch
   via ``mohan.Similarity``.  The index is recreated from scratch on each run.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Param

with DAG(
    dag_id="load_elasticsearch_openalex",
    default_args={
        "owner": "impactu",
        "retries": 2,
        "retry_delay": timedelta(hours=1),
    },
    params={
        "mongo_conn_id": Param(
            "mongodb_default",
            type="string",
            description="Airflow MongoDB connection ID",
        ),
        "db_name": Param(
            "openalex",
            type="string",
            description="MongoDB database name",
        ),
        "collection_name": Param(
            "works",
            type="string",
            description="MongoDB collection to read works from",
        ),
        "es_host": Param(
            "http://localhost:9200",
            type="string",
            description="Elasticsearch host URI (e.g. http://localhost:9200)",
        ),
        "es_user": Param(
            "elastic",
            type="string",
            description="Elasticsearch username",
        ),
        "es_password": Param(
            "colav",
            type="string",
            description="Elasticsearch password",
        ),
        "es_index": Param(
            "openalex_index",
            type="string",
            description="Elasticsearch index name",
        ),
        "bulk_size": Param(
            1000,
            type="integer",
            description="Number of documents per bulk insert",
        ),
    },
    schedule=None,
    catchup=False,
    tags=["openalex", "load", "elasticsearch"],
) as dag:

    def load_works(**context: Any) -> None:
        """Read works from MongoDB and index them into Elasticsearch."""
        from airflow.providers.mongo.hooks.mongo import MongoHook
        from kahi_impactu_utils.String import parse_html, parse_mathml
        from mohan.Similarity import Similarity

        params = context["params"]
        es_index = params["es_index"]
        bulk_size = params["bulk_size"]

        # ------------------------------------------------------------------ #
        # Elasticsearch connection via mohan.Similarity                        #
        # ------------------------------------------------------------------ #
        s = Similarity(
            es_index,
            es_uri=params["es_host"],
            es_auth=(params["es_user"], params["es_password"]),
        )
        s.delete_index(es_index)

        # ------------------------------------------------------------------ #
        # MongoDB connection via MongoHook                                     #
        # ------------------------------------------------------------------ #
        hook = MongoHook(params["mongo_conn_id"])
        client = hook.get_conn()
        collection = client[params["db_name"]][params["collection_name"]]

        cursor = collection.find(
            {"title": {"$exists": True}},
            {
                "title": 1,
                "primary_location.source": 1,
                "publication_year": 1,
                "biblio": 1,
                "authorships": 1,
                "_id": 1,
            },
        )

        es_entries: list[dict] = []
        counter = 0
        count_nones = 0

        for doc in cursor:
            if doc.get("title") is None:
                count_nones += 1
                continue

            title = parse_mathml(doc["title"])
            title = parse_html(title)

            primary_location = doc.get("primary_location") or {}
            source_info = primary_location.get("source") or {}
            source_name = source_info.get("display_name", "") if source_info else ""

            biblio = doc.get("biblio") or {}
            authors = [
                a["author"]["display_name"]
                for a in doc.get("authorships", [])
                if "display_name" in a.get("author", {})
            ]

            work = {
                "title": title,
                "source": source_name,
                "year": doc.get("publication_year", ""),
                "volume": biblio.get("volume", ""),
                "issue": biblio.get("issue", ""),
                "first_page": biblio.get("first_page", ""),
                "last_page": biblio.get("last_page", ""),
                "authors": authors,
            }

            es_entries.append(
                {
                    "_index": es_index,
                    "_id": str(doc["_id"]),
                    "_source": work,
                }
            )

            if len(es_entries) >= bulk_size:
                s.insert_bulk(es_entries)
                es_entries = []

            counter += 1
            if counter % 1000 == 0:
                context["ti"].log.info("Progress: %d works indexed", counter)

        if es_entries:
            s.insert_bulk(es_entries)

        context["ti"].log.info(
            "Done. Total indexed: %d | Skipped (no title): %d", counter, count_nones
        )

    load_works_task = PythonOperator(
        task_id="load_works",
        python_callable=load_works,
    )
