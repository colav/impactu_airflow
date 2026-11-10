"""Resolve, download and reload the latest ROR release on every DAG run."""

from __future__ import annotations

import ast
import hashlib
import io
import json
import logging
import os
import re
import tempfile
import zipfile
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import requests
from airflow import DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.standard.operators.python import PythonOperator
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config.notifications import completion_callbacks

log = logging.getLogger(__name__)
ROR_CONCEPT_ID = 6347574
ROR_CONCEPT_DOI = f"10.5281/zenodo.{ROR_CONCEPT_ID}"
GITHUB_REPOSITORY = "https://api.github.com/repos/ror-community/ror-data"
ARCHIVE_PATTERN = re.compile(r"(v\d+\.\d+(?:\.\d+)?)-(\d{4}-\d{2}-\d{2})-ror-data\.zip")


def _session() -> requests.Session:
    session = requests.Session()
    session.headers["User-Agent"] = (
        "ImpactU-ROR-Downloader/1.0 (https://github.com/colav/impactu_airflow)"
    )
    retries = Retry(
        total=3,
        connect=0,
        read=0,
        other=0,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    return session


def _get_json(session, url):
    with session.get(url, timeout=(10, 30)) as response:
        response.raise_for_status()
        return response.json()


def _version(value):
    value = str(value)
    if not re.fullmatch(r"v?\d+\.\d+(?:\.\d+)?", value):
        raise ValueError(f"Invalid ROR release version: {value}")
    return "v" + value.removeprefix("v")


def _archive_date(filename, version):
    match = ARCHIVE_PATTERN.fullmatch(filename)
    if not match or match[1] != _version(version):
        raise ValueError(f"ROR archive {filename} does not match version {version}")
    datetime.strptime(match[2], "%Y-%m-%d")
    return match[2]


def _github_file(session, version, filename=None):
    """Select the exact release, pin its commit, and retain its Git blob checksum."""
    commit = _get_json(session, f"{GITHUB_REPOSITORY}/commits/main")
    commit_id = commit["sha"]
    tree_id = commit["commit"]["tree"]["sha"]
    if not all(re.fullmatch(r"[0-9a-f]{40}", x) for x in (commit_id, tree_id)):
        raise ValueError("Invalid ROR GitHub revision")
    tree = _get_json(session, f"{GITHUB_REPOSITORY}/git/trees/{tree_id}")
    if tree.get("truncated"):
        raise ValueError("Incomplete ROR GitHub file listing")
    candidates = []
    for entry in tree["tree"]:
        match = ARCHIVE_PATTERN.fullmatch(entry["path"])
        if (
            entry["type"] == "blob"
            and match
            and match[1] == _version(version)
            and (filename is None or entry["path"] == filename)
        ):
            candidates.append(entry)
    if len(candidates) != 1:
        raise ValueError(f"Expected exactly one GitHub archive for ROR {version}")
    entry = candidates[0]
    if not re.fullmatch(r"[0-9a-f]{40}", entry["sha"]):
        raise ValueError("Invalid ROR GitHub file checksum")
    return {
        "key": entry["path"],
        "size": entry["size"],
        "git_blob_sha1": entry["sha"],
        "github_commit": commit_id,
        "links": {
            "self": f"https://raw.githubusercontent.com/ror-community/ror-data/"
            f"{commit_id}/{entry['path']}"
        },
    }


def _datacite_release(session):
    """Resolve the current version through the DOI authority when Zenodo is down."""
    concept = _get_json(session, f"https://api.datacite.org/dois/{ROR_CONCEPT_DOI}")["data"][
        "attributes"
    ]
    version = _version(concept["version"])
    candidates = set()
    for relation in concept.get("relatedIdentifiers", []):
        match = re.fullmatch(r"10\.5281/zenodo\.(\d+)", relation["relatedIdentifier"])
        if relation["relationType"] == "HasVersion" and match:
            candidates.add(int(match[1]))
    # IDs only prioritize lookup; version and parent DOI must both match.
    for record_id in sorted(candidates, reverse=True):
        data = _get_json(session, f"https://api.datacite.org/dois/10.5281/zenodo.{record_id}")[
            "data"
        ]["attributes"]
        if str(data.get("version", "")).removeprefix("v") != version.removeprefix("v"):
            continue
        if not any(
            r["relationType"] == "IsVersionOf" and r["relatedIdentifier"] == ROR_CONCEPT_DOI
            for r in data.get("relatedIdentifiers", [])
        ):
            continue
        file = _github_file(session, version)
        publication_date = _archive_date(file["key"], version)
        issued = [d["date"][:10] for d in data.get("dates", []) if d["dateType"] == "Issued"]
        if issued and publication_date not in issued:
            raise ValueError("ROR DOI and GitHub archive publication dates disagree")
        return {
            "record_id": record_id,
            "version": version,
            "publication_date": publication_date,
            "resolved_via": "datacite+github",
            "file": file,
        }
    raise ValueError(f"Cannot verify the Zenodo record for the current ROR version {version}")


def resolve_ror_release() -> dict[str, Any]:
    with _session() as session:
        try:
            record = _get_json(session, f"https://zenodo.org/api/records/{ROR_CONCEPT_ID}")
        except requests.RequestException as exc:
            log.warning(
                "Zenodo unavailable (%s); resolving the current release through DataCite", exc
            )
            release: dict[str, Any] = _datacite_release(session)
        else:
            parent = str(record.get("conceptrecid", ""))
            if parent != str(ROR_CONCEPT_ID) and record.get("conceptdoi") != ROR_CONCEPT_DOI:
                raise ValueError("Zenodo response is not a version of the ROR dataset")
            version = _version(record["metadata"]["version"])
            files = [
                f
                for f in record.get("files", [])
                if (match := ARCHIVE_PATTERN.fullmatch(f["key"])) and match[1] == version
            ]
            if len(files) != 1 or not files[0].get("checksum"):
                raise ValueError(f"Expected one checksummed archive for ROR {version}")
            date = _archive_date(files[0]["key"], version)
            if record["metadata"]["publication_date"][:10] != date:
                raise ValueError("ROR metadata and archive publication dates disagree")
            release = {
                "record_id": int(record["id"]),
                "version": version,
                "publication_date": date,
                "resolved_via": "zenodo",
                "file": files[0],
            }
    if release["record_id"] == ROR_CONCEPT_ID:
        raise ValueError("ROR concept record did not resolve to a version-specific record")
    log.info(
        "Resolved ROR %s, Zenodo record %s, archive %s via %s",
        release["version"],
        release["record_id"],
        release["file"]["key"],
        release["resolved_via"],
    )
    return release


def _mapping(value):
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            value = ast.literal_eval(value)
    if not isinstance(value, dict):
        raise ValueError("Expected ROR metadata from the preceding task")
    return value


def _compute_file_checksum(path, algorithm="sha256"):
    digest = hashlib.new(algorithm)
    with open(path, "rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _git_blob_checksum(path):
    digest = hashlib.sha1(f"blob {os.path.getsize(path)}\0".encode())
    with open(path, "rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _matches_ror_file(path, entry):
    if os.path.getsize(path) != int(entry["size"]):
        return False
    checksum = entry.get("checksum")
    if checksum:
        algorithm, separator, expected = checksum.partition(":")
        if not separator:
            algorithm, expected = "sha256", checksum
        if _compute_file_checksum(path, algorithm) != expected:
            return False
    if entry.get("git_blob_sha1") and _git_blob_checksum(path) != entry["git_blob_sha1"]:
        return False
    return bool(checksum or entry.get("git_blob_sha1"))


def _download_file(session, url, destination, entry):
    temporary = None
    try:
        with session.get(url, stream=True, timeout=(10, 120)) as response:
            response.raise_for_status()
            with tempfile.NamedTemporaryFile(
                dir=os.path.dirname(destination), delete=False
            ) as file:
                temporary = file.name
                for chunk in response.iter_content(1024 * 1024):
                    file.write(chunk)
        if not _matches_ror_file(temporary, entry):
            raise ValueError(f"ROR file size or checksum mismatch: {entry['key']}")
        sha256 = _compute_file_checksum(temporary)
        os.replace(temporary, destination)
        return sha256
    finally:
        if temporary and os.path.exists(temporary):
            os.unlink(temporary)


def download_ror_to_path(release: dict | str) -> dict[str, Any]:
    resolved: dict[str, Any] = _mapping(release)
    entry = resolved["file"]
    version = _version(resolved["version"])
    if _archive_date(entry["key"], version) != resolved["publication_date"]:
        raise ValueError("Resolved ROR release and archive publication dates disagree")
    directory = os.path.join(
        os.environ.get("AIRFLOW_HOME", os.getcwd()),
        "cache",
        "ror_import",
        str(resolved["record_id"]),
    )
    os.makedirs(directory, exist_ok=True)
    destination = os.path.join(directory, entry["key"])
    url = entry["links"].get("download") or entry["links"]["self"]
    with _session() as session:
        try:
            sha256 = _download_file(session, url, destination, entry)
        except requests.RequestException as exc:
            if entry.get("git_blob_sha1"):
                raise
            log.warning("Zenodo download unavailable (%s); finding the same release on GitHub", exc)
            fallback = _github_file(session, version, entry["key"])
            if int(fallback["size"]) != int(entry["size"]):
                raise ValueError("Zenodo and GitHub ROR archive sizes disagree") from exc
            entry = {**entry, **fallback, "checksum": entry["checksum"]}
            url = entry["links"]["self"]
            sha256 = _download_file(session, url, destination, entry)
    result = {
        key: resolved[key] for key in ("record_id", "version", "publication_date", "resolved_via")
    }
    result.update(
        file_path=destination,
        sha256=sha256,
        downloaded_from=url,
        github_commit=entry.get("github_commit"),
        checksum=entry.get("checksum"),
        git_blob_sha1=entry.get("git_blob_sha1"),
        downloaded=True,
    )
    log.info(
        "Downloaded ROR %s record %s to %s; SHA-256 %s",
        version,
        resolved["record_id"],
        destination,
        sha256,
    )
    return result


def _copy_db(client, source, destination, collections):
    for name in collections:
        batch = []
        for document in client[source][name].find({}):
            batch.append(document)
            if len(batch) >= 1000:
                client[destination][name].insert_many(batch)
                batch.clear()
        if batch:
            client[destination][name].insert_many(batch)


def load_ror_from_path(
    file_info: dict | str,
    mongo_conn_id="mongodb_default",
    db_name=None,
    collection_name="ror_stage",
) -> int:
    info = _mapping(file_info)
    path = info["file_path"]
    version = _version(info["version"])
    if _archive_date(os.path.basename(path), version) != info["publication_date"]:
        raise ValueError("Downloaded ROR metadata and archive publication dates disagree")
    if os.path.basename(os.path.dirname(path)) != str(info["record_id"]):
        raise ValueError("ROR archive directory does not match the resolved record")
    if _compute_file_checksum(path) != info["sha256"]:
        raise ValueError("ROR archive changed between download and load")
    expected_member = os.path.basename(path).removesuffix(".zip") + ".json"
    with zipfile.ZipFile(path) as archive:
        members = [name for name in archive.namelist() if os.path.basename(name) == expected_member]
        if len(members) != 1:
            raise ValueError(f"Expected one JSON member named {expected_member}")
        with archive.open(members[0]) as source:
            records = json.load(io.TextIOWrapper(source, encoding="utf-8"))
    if not isinstance(records, list) or not records:
        raise ValueError("ROR dataset must be a nonempty list of records")
    identifiers = [r.get("id") if isinstance(r, dict) else None for r in records]
    if not all(
        isinstance(identifier, str) and re.fullmatch(r"https://ror\.org/[0-9a-z]{9}", identifier)
        for identifier in identifiers
    ) or len(set(identifiers)) != len(records):
        raise ValueError("ROR dataset contains missing, invalid or duplicate identifiers")

    hook = MongoHook(mongo_conn_id=mongo_conn_id)
    client = hook.get_conn()
    db_name = db_name or getattr(getattr(hook, "connection", None), "schema", None)
    if not db_name:
        raise ValueError("MongoDB database not provided")
    database = client[db_name]
    prefix = f"_{collection_name}_loading_"
    existing = [name for name in database.list_collection_names() if not name.startswith(prefix)]
    token = uuid4().hex
    staging = database[prefix + token]
    archive_name = None
    try:
        staging.create_index("id", unique=True)
        for offset in range(0, len(records), 1000):
            staging.insert_many(records[offset : offset + 1000])
        if staging.count_documents({}) != len(records):
            raise RuntimeError("Incomplete ROR staging collection")
        if existing:
            archive_name = f"{db_name[:30]}_{datetime.now(UTC):%Y%m%dT%H%M%S%fZ}_{token[:8]}"
            _copy_db(client, db_name, archive_name, existing)
        staging.rename(collection_name, dropTarget=True)
    finally:
        staging.drop()
    metadata = {
        **info,
        "loaded_at": datetime.now(UTC),
        "count": len(records),
        "archive_database": archive_name,
        "collection": collection_name,
    }
    database.ror_import_metadata.replace_one(
        {"_id": "current"}, {"_id": "current", **metadata}, upsert=True
    )
    log.info(
        "Loaded %d ROR records; version %s; Zenodo record %s; archive %s",
        len(records),
        version,
        info["record_id"],
        archive_name,
    )
    return len(records)


with DAG(
    dag_id="ror_capture",
    default_args={"owner": "impactu", "depends_on_past": False, "retries": 1, "retry_delay": 300},
    description="Resolve, download and reload the latest ROR release on every run",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
    tags=["ror", "import"],
    params={"mongo_conn_id": "mongodb_default", "mongo_db": "ror"},
    is_paused_upon_creation=True,
    **completion_callbacks(),
) as dag:
    resolve_task = PythonOperator(task_id="resolve_ror", python_callable=resolve_ror_release)
    download_task = PythonOperator(
        task_id="download_ror",
        python_callable=download_ror_to_path,
        op_kwargs={"release": "{{ ti.xcom_pull(task_ids='resolve_ror') }}"},
    )
    load_task = PythonOperator(
        task_id="load_ror",
        python_callable=load_ror_from_path,
        op_kwargs={
            "file_info": "{{ ti.xcom_pull(task_ids='download_ror') }}",
            "mongo_conn_id": "{{ params.mongo_conn_id }}",
            "db_name": "{{ params.mongo_db }}",
            "collection_name": "ror_stage",
        },
    )
    resolve_task >> download_task >> load_task
