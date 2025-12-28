"""ScimagoJR data extraction DAG."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook

from extract.scimagojr.scimagojr_extractor import ScimagoJRExtractor

default_args = {
    "owner": "impactu",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(hours=1),
}


def run_extraction_by_year(year: int, **kwargs: dict) -> None:
    """
    Extract ScimagoJR data for a specific year.

    Parameters
    ----------
    year : int
        Year to extract data for
    **kwargs : dict
        Airflow context and parameters

    Notes
    -----
    Uses MongoHook to get connection from Airflow connections.
    """
    # Get params from DAG
    force_redownload = kwargs["params"].get("force_redownload", True)
    chunk_size = kwargs["params"].get("chunk_size", 1000)

    # Use MongoHook to get the connection
    hook = MongoHook(mongo_conn_id="mongodb_default")
    client = hook.get_conn()
    db_name = hook.connection.schema or "impactu"

    extractor = ScimagoJRExtractor("", db_name, client=client)
    try:
        extractor.process_year(year, force_redownload=force_redownload, chunk_size=chunk_size)
    finally:
        extractor.close()


with DAG(
    "extract_scimagojr",
    default_args=default_args,
    description="Extract data from ScimagoJR and load into MongoDB",
    schedule="@monthly",
    catchup=False,
    tags=["extract", "scimagojr"],
    params={
        "force_redownload": Param(
            True,
            type="boolean",
            description="Force data download even if already in cache or database",
        ),
        "chunk_size": Param(
            1000, type="integer", description="Number of records to insert in each bulk operation"
        ),
    },
) as dag:
    years = list(range(1999, datetime.now().year + 1))

    extract_task = PythonOperator.partial(
        task_id="extract_and_load_scimagojr",
        python_callable=run_extraction_by_year,
    ).expand(op_kwargs=[{"year": year} for year in years])
