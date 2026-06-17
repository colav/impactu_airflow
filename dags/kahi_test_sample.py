from __future__ import annotations

import hashlib
import json
import re
import shutil
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, TypeAlias, cast

import pandas as pd
import yaml
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Param

try:
    from airflow.providers.mongo.hooks import mongo as mongo_hook_module
except ModuleNotFoundError:
    MongoHook: Any = None
else:
    MongoHook = cast(Any, mongo_hook_module.MongoHook)

PLUGIN_PARAM_MAP: list[tuple[str, str]] = [
    ("run_ror_affiliations", "ror_affiliations"),
    ("run_openalex_affiliations", "openalex_affiliations"),
    ("run_staff_affiliations", "staff_affiliations"),
    ("run_scienti_affiliations", "scienti_affiliations"),
    ("run_minciencias_opendata_affiliations", "minciencias_opendata_affiliations"),
    ("run_staff_person", "staff_person"),
    ("run_scienti_person", "scienti_person"),
    ("run_minciencias_opendata_person", "minciencias_opendata_person"),
    ("run_openalex_person", "openalex_person"),
    ("run_orcid_person", "orcid_person"),
    ("run_scholar_person", "scholar_person"),
    ("run_unicity_person", "unicity_person"),
    ("run_openalex_works_doi", "openalex_works/doi"),
    ("run_scienti_works_doi", "scienti_works/doi"),
    ("run_ciarp_works_doi", "ciarp_works/doi"),
    ("run_scholar_works_doi", "scholar_works/doi"),
    ("run_openalex_works", "openalex_works"),
    ("run_scienti_works", "scienti_works"),
    ("run_ciarp_works", "ciarp_works"),
    ("run_scholar_works", "scholar_works"),
    ("run_minciencias_opendata_works", "minciencias_opendata_works"),
]

Identifiers: TypeAlias = dict[str, list[str] | str]
MongoClientType: TypeAlias = Any

PLUGIN_ORDER = [plugin for _, plugin in PLUGIN_PARAM_MAP]
WORK_PLUGINS = {
    "openalex_works/doi",
    "scienti_works/doi",
    "ciarp_works/doi",
    "scholar_works/doi",
    "minciencias_opendata_works",
    "openalex_works",
    "scienti_works",
    "ciarp_works",
    "scholar_works",
}
LOCAL_FILE_PLUGINS = {
    "staff_affiliations",
    "staff_person",
    "ciarp_works/doi",
    "ciarp_works",
}
SCIENTI_PLUGINS = {"scienti_affiliations", "scienti_person", "scienti_works/doi", "scienti_works"}
NON_DOI_WORK_PLUGINS = {
    "openalex_works",
    "scienti_works",
    "ciarp_works",
    "scholar_works",
    "minciencias_opendata_works",
}
OPENALEX_SAMPLE_PLUGINS = {
    "openalex_affiliations",
    "openalex_person",
    "openalex_works/doi",
    "openalex_works",
}
ROR_SAMPLE_PLUGINS = {"ror_affiliations"}
SCIENTI_SAMPLE_PLUGINS = {
    "scienti_affiliations",
    "scienti_person",
    "scienti_works/doi",
    "scienti_works",
}
MINCIENCIAS_SAMPLE_PLUGINS = {
    "minciencias_opendata_affiliations",
    "minciencias_opendata_person",
    "minciencias_opendata_works",
}
SCHOLAR_SAMPLE_PLUGINS = {"scholar_person", "scholar_works/doi", "scholar_works"}
ORCID_SAMPLE_PLUGINS = {"orcid_person"}
STAFF_SAMPLE_PLUGINS = {"staff_affiliations", "staff_person"}
CIARP_SAMPLE_PLUGINS = {"ciarp_works/doi", "ciarp_works"}
SAFE_DROP_PREFIXES = (
    "kahi_sample_test",
    "kahi_test",
    "kahi_samples_log",
    "openalex_sample",
    "minciencias_sample",
    "orcid_sample",
    "scienti_sample",
    "scholar_sample",
    "ror_sample",
)
DANGEROUS_DATABASE_NAMES = {"admin", "config", "local", "openalex", "dam", "ror", "orcid"}

AFFILIATION_RULES: dict[str, dict[str, Any]] = {
    "https://ror.org/03bp5hc83": {
        "name": "Universidad de Antioquia",
        "staff_patterns": ["formato_talento_humano_udea_*.xlsx"],
        "ciarp_patterns": ["formato_CIARP_UDEA_*.xlsx"],
        "scienti_db": "scienti_udea_2024",
    },
    "https://ror.org/00jb9vg53": {
        "name": "Universidad del Valle",
        "staff_patterns": ["formato_talento_humano_univalle_*.xlsx"],
        "ciarp_patterns": ["formato_CIARP_UNIVALLE_*.xlsx"],
        "scienti_db": "scienti_univalle_2024",
    },
    "https://ror.org/059yx9a68": {
        "name": "Universidad Nacional de Colombia",
        "staff_patterns": ["formato_talento_humano_unal_*.xlsx"],
        "ciarp_patterns": ["formato_CIARP_UNAL_*.xlsx"],
        "scienti_db": "",
    },
    "https://ror.org/05tkb8v92": {
        "name": "Universidad Autonoma Latinoamericana",
        "staff_patterns": ["formato_talento_humano_unaula_*.xlsx"],
        "ciarp_patterns": [],
        "scienti_db": "scienti_unaula_2024",
    },
    "https://ror.org/02xtwpk10": {
        "name": "Universidad Externado de Colombia",
        "staff_patterns": ["formato_talento_humano_uec_*.xlsx"],
        "ciarp_patterns": [],
        "scienti_db": "scienti_uec_2024",
    },
}

DEFAULT_SAMPLE_DBS = {
    "openalex": "openalex_sample",
    "minciencias": "minciencias_sample",
    "orcid": "orcid_sample",
    "scienti": "scienti_sample",
    "scholar": "scholar_sample",
    "ror": "ror_sample",
}


def _merged_params(kwargs: dict[str, Any]) -> dict[str, Any]:
    params = dict(kwargs.get("params", {}))
    dag_run = kwargs.get("dag_run")
    conf = getattr(dag_run, "conf", None)
    if conf:
        for key, value in conf.items():
            if value is not None:
                params[key] = value
    return params


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on", "y"}
    return bool(value)


def _to_list(value: Any) -> list[str]:
    if value is None:
        return []
    tokens = value if isinstance(value, list) else re.split(r"[\n,;]+", str(value))
    return [str(token).strip() for token in tokens if str(token).strip()]


def _id_list(identifiers: Identifiers, key: str) -> list[str]:
    value = identifiers.get(key, [])
    if isinstance(value, list):
        return value
    return [value] if value else []


def _id_text(identifiers: Identifiers, key: str) -> str:
    value = identifiers.get(key, "")
    return value if isinstance(value, str) else ""


def _resolve_pymongo():
    try:
        from pymongo import MongoClient, ReplaceOne
    except ModuleNotFoundError as exc:
        raise ValueError(
            "pymongo is not installed in this Airflow image. "
            "Install dependency `pymongo` (or use an image that includes it) to run kahi_test_sample."
        ) from exc
    return MongoClient, ReplaceOne


def _resolve_mongo_uri(mongo_uri_override: str, mongo_conn_id: str) -> str:
    uri = (mongo_uri_override or "").strip()
    if uri:
        return uri
    return _mongo_hook_uri(mongo_conn_id)


def _mongo_hook_uri(mongo_conn_id: str) -> str:
    if MongoHook is None:
        raise ValueError(
            "Mongo provider is not available in this Airflow image. "
            "Set mongo_uri_override (e.g., mongodb://host:27017)."
        )
    return str(cast(Any, MongoHook)(mongo_conn_id=mongo_conn_id).get_uri())


def _parse_thresholds(value: Any) -> list[int]:
    tokens = _to_list(value)
    if not tokens:
        return [65, 90, 95]
    if len(tokens) != 3:
        raise ValueError("minciencias_works_thresholds must contain exactly 3 values.")
    try:
        return [int(token) for token in tokens]
    except ValueError as exc:
        raise ValueError("minciencias_works_thresholds must be integers.") from exc


def _normalize_ror(value: str) -> str:
    value = value.strip()
    if not value:
        return ""
    if value.startswith("https://ror.org/"):
        return value
    if value.startswith("http://ror.org/"):
        return "https://" + value[len("http://") :]
    if value.startswith("ror.org/"):
        return "https://" + value
    return f"https://ror.org/{value}"


def _normalize_orcid(value: str) -> str:
    value = value.strip()
    if not value:
        return ""
    if value.startswith("https://orcid.org/"):
        return value
    if value.startswith("http://orcid.org/"):
        return "https://" + value[len("http://") :]
    if value.startswith("orcid.org/"):
        return "https://" + value
    return f"https://orcid.org/{value}"


def _normalize_doi(value: str) -> str:
    value = value.strip()
    if not value:
        return ""
    value = re.sub(r"^https?://(dx\.)?doi\.org/", "", value, flags=re.IGNORECASE)
    value = value.strip().lower()
    if not value:
        return ""
    return f"https://doi.org/{value}"


def _doi_variants(values: list[str]) -> list[str]:
    variants: set[str] = set()
    for value in values:
        normalized = _normalize_doi(value)
        if not normalized:
            continue
        bare = normalized.removeprefix("https://doi.org/")
        variants.add(normalized)
        variants.add(bare)
    return list(variants)


def _normalize_openalex_id(value: str, entity_prefix: str) -> str:
    value = value.strip()
    if not value:
        return ""
    if value.startswith("https://openalex.org/"):
        return value
    if value.startswith("http://openalex.org/"):
        return "https://" + value[len("http://") :]
    prefix = entity_prefix.upper()
    if value.upper().startswith(prefix):
        return f"https://openalex.org/{value.upper()}"
    if value.isdigit():
        return f"https://openalex.org/{prefix}{value}"
    return value


def _normalize_scholar_user(value: str) -> str:
    value = value.strip()
    if not value:
        return ""
    if "scholar.google." in value and "user=" in value:
        match = re.search(r"[?&]user=([^&]+)", value)
        if match:
            return match.group(1)
    return value


def _extract_ciarp_cedula(ciarp_id: str) -> str:
    ciarp_id = ciarp_id.strip()
    if not ciarp_id:
        return ""
    match = re.match(r"^\d+-(\d+)-\d+$", ciarp_id)
    if not match:
        return ""
    return match.group(1)


def _normalize_title(value: str) -> str:
    text = (value or "").strip().lower()
    text = text.replace('"', " ").replace("'", " ")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _safe_name(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_.-]+", "_", value)


def _pick_latest_file(base_dir: Path, patterns: list[str]) -> str:
    matches: list[Path] = []
    for pattern in patterns:
        matches.extend(path for path in base_dir.glob(pattern) if path.is_file())
    matches = [path for path in matches if "_muestra" not in path.name.lower()]
    if not matches:
        return ""
    matches.sort(key=lambda path: path.stat().st_mtime, reverse=True)
    return str(matches[0])


def _resolve_affiliation_context(
    params: dict[str, Any], selected_plugins: list[str]
) -> dict[str, Any]:
    affiliation_ror = _normalize_ror(str(params.get("affiliation_ror", "")))
    if not affiliation_ror:
        raise ValueError("Missing required param: affiliation_ror")

    data_root = Path(
        str(params.get("data_root_dir", "/home/fvergara/projects/colav/data"))
    ).expanduser()
    staff_dir = data_root / "STAFF"
    ciarp_dir = data_root / "CIARP"

    rule = AFFILIATION_RULES.get(affiliation_ror)
    if not rule:
        supported = ", ".join(sorted(AFFILIATION_RULES.keys()))
        raise ValueError(
            f"Unsupported affiliation_ror '{affiliation_ror}'. Supported values: {supported}"
        )

    selected_set = set(selected_plugins)
    needs_staff_file = bool(selected_set & STAFF_SAMPLE_PLUGINS)
    needs_ciarp_file = bool(selected_set & CIARP_SAMPLE_PLUGINS)

    staff_file = str(params.get("staff_source_file", "")).strip()
    ciarp_file = str(params.get("ciarp_source_file", "")).strip()
    if needs_staff_file and not staff_file:
        if not staff_dir.exists():
            raise FileNotFoundError(f"STAFF directory not found: {staff_dir}")
        staff_file = _pick_latest_file(staff_dir, rule.get("staff_patterns", []))
    if needs_ciarp_file and not ciarp_file:
        if not ciarp_dir.exists():
            raise FileNotFoundError(f"CIARP directory not found: {ciarp_dir}")
        ciarp_file = _pick_latest_file(ciarp_dir, rule.get("ciarp_patterns", []))

    scienti_db = str(params.get("scienti_source_db_override", "")).strip() or str(
        rule.get("scienti_db", "")
    )

    return {
        "affiliation_ror": affiliation_ror,
        "affiliation_name": rule.get("name", ""),
        "staff_source_file": staff_file,
        "ciarp_source_file": ciarp_file,
        "scienti_source_db": scienti_db,
        "data_root": str(data_root),
    }


def _select_plugins(params: dict[str, Any]) -> list[str]:
    selected = {
        plugin for flag, plugin in PLUGIN_PARAM_MAP if _coerce_bool(params.get(flag, False))
    }

    dependency_pairs = [
        ("openalex_affiliations", "ror_affiliations"),
        ("scienti_affiliations", "ror_affiliations"),
        ("minciencias_opendata_affiliations", "ror_affiliations"),
        ("openalex_works/doi", "openalex_affiliations"),
        ("openalex_works", "openalex_affiliations"),
        ("scienti_works/doi", "scienti_affiliations"),
        ("scienti_works", "scienti_affiliations"),
        ("minciencias_opendata_works", "minciencias_opendata_affiliations"),
        ("openalex_works", "openalex_works/doi"),
        ("scienti_works", "scienti_works/doi"),
        ("ciarp_works", "ciarp_works/doi"),
        ("scholar_works", "scholar_works/doi"),
    ]
    changed = True
    while changed:
        changed = False
        for parent, required in dependency_pairs:
            if parent in selected and required not in selected:
                selected.add(required)
                changed = True

    return [plugin for plugin in PLUGIN_ORDER if plugin in selected]


def _build_identifiers(params: dict[str, Any]) -> Identifiers:
    author_openalex_ids = [
        _normalize_openalex_id(item, "A") for item in _to_list(params.get("author_openalex", ""))
    ]
    work_openalex_ids = [
        _normalize_openalex_id(item, "W") for item in _to_list(params.get("work_openalex", ""))
    ]
    orcids = [_normalize_orcid(item) for item in _to_list(params.get("author_orcid", ""))]
    dois = [_normalize_doi(item) for item in _to_list(params.get("work_doi", ""))]
    scholar_users = [
        _normalize_scholar_user(item) for item in _to_list(params.get("author_scholar", ""))
    ]
    ciarp_ids = _to_list(params.get("work_ciarp_id", ""))
    work_scienti_cod_producto = _to_list(params.get("work_scienti_cod_producto", ""))
    work_scienti_cod_rh = _to_list(params.get("work_scienti_cod_rh", ""))

    derived_dam_product_ids = []
    if work_scienti_cod_rh and work_scienti_cod_producto:
        derived_dam_product_ids = [
            f"ART-{cod_rh}-{cod_producto}"
            for cod_rh in work_scienti_cod_rh
            for cod_producto in work_scienti_cod_producto
        ]

    author_cod_rh = _to_list(params.get("author_cod_rh", ""))
    author_dam_ids = list(set(_to_list(params.get("author_dam_id", "")) + author_cod_rh))

    return {
        "author_cedula": _to_list(params.get("author_cedula", "")),
        "author_cod_rh": author_cod_rh,
        "author_dam_id": author_dam_ids,
        "author_orcid": [item for item in orcids if item],
        "author_openalex": [item for item in author_openalex_ids if item],
        "author_scholar": [item for item in scholar_users if item],
        "work_doi": [item for item in dois if item],
        "work_openalex": [item for item in work_openalex_ids if item],
        "work_scienti_cod_producto": work_scienti_cod_producto,
        "work_scienti_cod_rh": work_scienti_cod_rh,
        "work_dam_id_producto": list(
            set(_to_list(params.get("work_dam_id_producto", "")) + derived_dam_product_ids)
        ),
        "work_ciarp_id": ciarp_ids,
        "work_ciarp_cedula": [
            ced for ced in (_extract_ciarp_cedula(item) for item in ciarp_ids) if ced
        ],
        "work_scholar_cid": _to_list(params.get("work_scholar_cid", "")),
        "work_title": str(params.get("work_title", "")).strip(),
        "work_author_cedula": _to_list(params.get("work_author_cedula", "")),
    }


def _assign_missing_ids(documents: list[dict[str, Any]]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for doc in documents:
        if "_id" not in doc:
            fingerprint = hashlib.sha1(
                json.dumps(doc, sort_keys=True, default=str).encode("utf-8")
            ).hexdigest()
            doc["_id"] = fingerprint
        output.append(doc)
    return output


def _upsert_documents(collection, documents: list[dict[str, Any]]) -> int:
    if not documents:
        return 0
    docs = _assign_missing_ids(documents)
    _, replace_one_cls = _resolve_pymongo()
    operations = [replace_one_cls({"_id": doc["_id"]}, doc, upsert=True) for doc in docs]
    collection.bulk_write(operations, ordered=False)
    return len(docs)


def _ensure_collection(database, collection_name: str) -> None:
    if collection_name not in database.list_collection_names():
        database.create_collection(collection_name)


def _create_staff_sample(staff_source_file: str, output_file: Path, cedulas: list[str]) -> int:
    if not staff_source_file:
        return 0
    frame = pd.read_excel(staff_source_file, dtype=str).fillna("")
    filtered = frame
    if cedulas:
        filtered = frame[frame["identificación"].astype(str).isin(set(cedulas))].copy()
    filtered.to_excel(output_file, index=False)
    return int(len(filtered))


def _create_ciarp_sample(
    ciarp_source_file: str,
    output_file: Path,
    target_type: str,
    identifiers: Identifiers,
) -> int:
    if not ciarp_source_file:
        return 0

    frame = pd.read_excel(ciarp_source_file, dtype=str).fillna("")
    filtered = frame

    cedulas = set(
        _id_list(identifiers, "author_cedula")
        + _id_list(identifiers, "work_author_cedula")
        + _id_list(identifiers, "work_ciarp_cedula")
    )
    doi_set = set(_id_list(identifiers, "work_doi"))
    normalized_titles = {_normalize_title(_id_text(identifiers, "work_title"))} - {""}

    if target_type == "author" and cedulas:
        filtered = frame[frame["identificación"].astype(str).isin(cedulas)].copy()
    elif target_type == "work":
        mask = pd.Series([True] * len(frame))
        if doi_set:
            doi_values = frame["doi"].astype(str).map(_normalize_doi)
            mask = mask & doi_values.isin(doi_set)
        if normalized_titles:
            title_values = frame["título"].astype(str).map(_normalize_title)
            mask = mask & title_values.isin(normalized_titles)
        if cedulas:
            mask = mask & frame["identificación"].astype(str).isin(cedulas)
        filtered = frame[mask].copy()

    filtered.to_excel(output_file, index=False)
    return int(len(filtered))


def _build_openalex_sample(
    client,
    source_db_name: str,
    sample_db_name: str,
    target_type: str,
    affiliation_ror: str,
    identifiers: Identifiers,
    max_records: int,
) -> dict[str, Any]:
    source_db = client[source_db_name]
    sample_db = client[sample_db_name]

    related_collection_names = ["funders", "publishers", "sources"]
    for collection_name in ["works", "authors", "institutions", *related_collection_names]:
        sample_db.drop_collection(collection_name)

    works_collection = source_db["works"]
    source_collections = set(source_db.list_collection_names())
    has_authors = "authors" in source_collections
    has_institutions = "institutions" in source_collections
    has_related_collections = {
        collection_name: collection_name in source_collections
        for collection_name in related_collection_names
    }

    works: list[dict[str, Any]] = []
    authors: list[dict[str, Any]] = []
    institutions: list[dict[str, Any]] = []
    related_ids: dict[str, set[str]] = {
        collection_name: set() for collection_name in related_collection_names
    }
    work_dois = _id_list(identifiers, "work_doi")

    if target_type == "work":
        work_openalex_ids = _id_list(identifiers, "work_openalex")
        seen_work_ids: set[Any] = set()
        if work_openalex_ids:
            for work in works_collection.find({"id": {"$in": work_openalex_ids}}).limit(
                max_records
            ):
                seen_work_ids.add(work.get("_id"))
                works.append(work)
        if work_dois:
            remaining = max(max_records - len(works), 0)
            if remaining:
                for work in works_collection.find({"doi": {"$in": work_dois}}).limit(remaining):
                    if work.get("_id") not in seen_work_ids:
                        seen_work_ids.add(work.get("_id"))
                        works.append(work)
        if not works and work_dois:
            works = list(works_collection.find({"ids.doi": {"$in": work_dois}}).limit(max_records))
    else:
        author_ids = set(_id_list(identifiers, "author_openalex"))
        author_orcids = _id_list(identifiers, "author_orcid")
        if has_authors and author_orcids:
            orcid_docs = list(
                source_db["authors"].find({"ids.orcid": {"$in": author_orcids}}, {"id": 1})
            )
            for doc in orcid_docs:
                if doc.get("id"):
                    author_ids.add(str(doc["id"]))

        if author_ids:
            works = list(
                works_collection.find({"authorships.author.id": {"$in": list(author_ids)}}).limit(
                    max_records
                )
            )
        elif work_dois:
            works = list(works_collection.find({"doi": {"$in": work_dois}}).limit(max_records))

    author_ids_from_works: set[str] = set(_id_list(identifiers, "author_openalex"))
    institution_ids_from_works: set[str] = set()

    for work in works:
        for authorship in work.get("authorships", []):
            author_id = (authorship.get("author") or {}).get("id")
            if author_id:
                author_ids_from_works.add(str(author_id))
            for institution in authorship.get("institutions", []):
                institution_id = institution.get("id")
                if institution_id:
                    institution_ids_from_works.add(str(institution_id))
        for location_key in ["primary_location", "best_oa_location"]:
            location = work.get(location_key) or {}
            source_id = (location.get("source") or {}).get("id")
            if source_id:
                related_ids["sources"].add(str(source_id))
        for location in work.get("locations", []) or []:
            source_id = ((location or {}).get("source") or {}).get("id")
            if source_id:
                related_ids["sources"].add(str(source_id))
        for grant in work.get("grants", []) or []:
            funder_id = (grant or {}).get("funder")
            if funder_id:
                related_ids["funders"].add(str(funder_id))

    if has_authors and author_ids_from_works:
        authors = list(source_db["authors"].find({"id": {"$in": list(author_ids_from_works)}}))

    if has_institutions:
        institution_clauses: list[dict[str, Any]] = [
            {"ids.ror": affiliation_ror},
            {"ror": affiliation_ror},
        ]
        if institution_ids_from_works:
            institution_clauses.append({"id": {"$in": list(institution_ids_from_works)}})
        institutions = list(source_db["institutions"].find({"$or": institution_clauses}))

    works_count = _upsert_documents(sample_db["works"], works)
    authors_count = _upsert_documents(sample_db["authors"], authors)
    institutions_count = _upsert_documents(sample_db["institutions"], institutions)
    related_counts: dict[str, int] = {}
    for collection_name, ids in related_ids.items():
        if has_related_collections[collection_name] and ids:
            documents = list(source_db[collection_name].find({"id": {"$in": list(ids)}}))
            related_counts[collection_name] = _upsert_documents(
                sample_db[collection_name], documents
            )
        else:
            related_counts[collection_name] = 0

    return {
        "works": works_count,
        "authors": authors_count,
        "institutions": institutions_count,
        **related_counts,
        "source_has_authors": has_authors,
        "source_has_institutions": has_institutions,
        "source_has_related_collections": has_related_collections,
    }


def _build_scienti_sample(
    client,
    source_db_name: str,
    source_collection_name: str,
    sample_db_name: str,
    target_type: str,
    identifiers: Identifiers,
    max_records: int,
) -> int:
    source_db = client[source_db_name]
    sample_db = client[sample_db_name]
    sample_db.drop_collection("product")

    if source_collection_name not in source_db.list_collection_names():
        raise ValueError(
            f"Collection '{source_collection_name}' not found in database '{source_db_name}'"
        )

    clauses: list[dict[str, Any]] = []
    if target_type == "author":
        author_ids = _id_list(identifiers, "author_cod_rh")
        if author_ids:
            clauses.append({"COD_RH": {"$in": author_ids}})
    else:
        product_ids = _id_list(identifiers, "work_scienti_cod_producto")
        work_cod_rh = _id_list(identifiers, "work_scienti_cod_rh")
        if product_ids and work_cod_rh:
            clauses.append(
                {
                    "$and": [
                        {"COD_PRODUCTO": {"$in": product_ids}},
                        {"COD_RH": {"$in": work_cod_rh}},
                    ]
                }
            )
        elif product_ids:
            clauses.append({"COD_PRODUCTO": {"$in": product_ids}})
        elif work_cod_rh:
            clauses.append({"COD_RH": {"$in": work_cod_rh}})

    if not clauses:
        return 0

    query: dict[str, Any] = {"$or": clauses} if len(clauses) > 1 else clauses[0]
    documents = list(source_db[source_collection_name].find(query).limit(max_records))
    return _upsert_documents(sample_db["product"], documents)


def _build_scholar_sample(
    client,
    source_db_name: str,
    sample_db_name: str,
    target_type: str,
    identifiers: Identifiers,
    max_records: int,
) -> int:
    source_db = client[source_db_name]
    sample_db = client[sample_db_name]
    sample_db.drop_collection("stage")

    source_collection = source_db["stage"]
    documents: list[dict[str, Any]] = []

    if target_type == "author":
        author_scholar_ids = _id_list(identifiers, "author_scholar")
        if author_scholar_ids:
            query: dict[str, Any] = {
                "$expr": {
                    "$gt": [
                        {
                            "$size": {
                                "$setIntersection": [
                                    author_scholar_ids,
                                    {
                                        "$map": {
                                            "input": {
                                                "$objectToArray": {"$ifNull": ["$profiles", {}]}
                                            },
                                            "as": "pair",
                                            "in": "$$pair.v",
                                        }
                                    },
                                ]
                            }
                        },
                        0,
                    ]
                }
            }
            documents = list(source_collection.find(query).limit(max_records))
    else:
        clauses: list[dict[str, Any]] = []
        scholar_cids = _id_list(identifiers, "work_scholar_cid")
        work_dois = _id_list(identifiers, "work_doi")
        if scholar_cids:
            clauses.append({"cid": {"$in": scholar_cids}})
        if work_dois:
            clauses.append({"doi": {"$in": _doi_variants(work_dois)}})
        if clauses:
            query = {"$or": clauses} if len(clauses) > 1 else clauses[0]
            documents = list(source_collection.find(query).limit(max_records))

    return _upsert_documents(sample_db["stage"], documents)


def _build_orcid_sample(
    client,
    source_db_name: str,
    sample_db_name: str,
    identifiers: Identifiers,
) -> int:
    source_db = client[source_db_name]
    sample_db = client[sample_db_name]
    sample_db.drop_collection("summaries")

    orcids = _id_list(identifiers, "author_orcid")
    if not orcids:
        return 0

    query = {"record:record.common:orcid-identifier.common:uri": {"$in": orcids}}
    documents = list(source_db["summaries"].find(query))
    return _upsert_documents(sample_db["summaries"], documents)


def _build_ror_sample(
    client,
    source_db_name: str,
    source_collection_name: str,
    sample_db_name: str,
    affiliation_ror: str,
) -> int:
    source_db = client[source_db_name]
    sample_db = client[sample_db_name]
    sample_db.drop_collection(source_collection_name)

    if source_collection_name not in source_db.list_collection_names():
        raise ValueError(
            f"Collection '{source_collection_name}' not found in database '{source_db_name}'"
        )

    documents = list(source_db[source_collection_name].find({"id": affiliation_ror}))
    documents = [_normalize_ror_document_for_kahi(document) for document in documents]
    return _upsert_documents(sample_db[source_collection_name], documents)


def _normalize_ror_document_for_kahi(document: dict[str, Any]) -> dict[str, Any]:
    if "name" in document and "aliases" in document and "acronyms" in document:
        return document

    names = document.get("names", [])

    def _name_values(name_type: str) -> list[str]:
        values = []
        for item in names:
            if name_type in item.get("types", []) and item.get("value"):
                values.append(str(item["value"]))
        return values

    label_names = _name_values("ror_display") or _name_values("label")
    document["name"] = document.get("name") or (label_names[0] if label_names else "")
    document["aliases"] = document.get("aliases", _name_values("alias"))
    document["acronyms"] = document.get("acronyms", _name_values("acronym"))

    locations = document.get("locations", [])
    addresses = []
    for location in locations:
        details = location.get("geonames_details", {})
        addresses.append(
            {
                "lat": details.get("lat"),
                "lng": details.get("lng"),
                "postcode": "",
                "state": details.get("country_subdivision_name", ""),
                "city": details.get("name", ""),
            }
        )
    document["addresses"] = document.get("addresses", addresses or [{}])
    first_details = locations[0].get("geonames_details", {}) if locations else {}
    document["country"] = document.get(
        "country",
        {
            "country_name": first_details.get("country_name", ""),
            "country_code": first_details.get("country_code", ""),
        },
    )

    links = document.get("links", [])
    if links and isinstance(links[0], dict):
        document["links"] = [
            str(link["value"])
            for link in links
            if link.get("type") == "website" and link.get("value")
        ]
        wikipedia_links = [
            str(link["value"])
            for link in links
            if link.get("type") == "wikipedia" and link.get("value")
        ]
        document["wikipedia_url"] = wikipedia_links[0] if wikipedia_links else ""
    else:
        document["wikipedia_url"] = document.get("wikipedia_url", "")

    external_ids = document.get("external_ids", {})
    if isinstance(external_ids, list):
        document["external_ids"] = {
            str(item["type"]): {"all": item.get("all", []), "preferred": item.get("preferred")}
            for item in external_ids
            if item.get("type")
        }

    return document


def _build_minciencias_sample(
    client,
    source_db_name: str,
    sample_db_name: str,
    target_type: str,
    identifiers: Identifiers,
    max_records: int,
) -> dict[str, int]:
    source_db = client[source_db_name]
    sample_db = client[sample_db_name]

    required_collections = [
        "cvlac_data",
        "cvlac_stage",
        "cvlac_stage_private",
        "cvlac_stage_raw",
        "gruplac_production_data",
        "gruplac_groups_data",
    ]

    for collection_name in required_collections:
        sample_db.drop_collection(collection_name)

    person_ids = set(_id_list(identifiers, "author_dam_id"))
    product_ids = set(_id_list(identifiers, "work_dam_id_producto"))

    production_query = {}
    if target_type == "author" and person_ids:
        production_query = {"id_persona_pd": {"$in": list(person_ids)}}
    elif target_type == "work" and product_ids:
        production_query = {"id_producto_pd": {"$in": list(product_ids)}}

    production_docs = []
    if production_query:
        production_docs = list(
            source_db["gruplac_production_data"].find(production_query).limit(max_records)
        )

    for doc in production_docs:
        if doc.get("id_persona_pd"):
            person_ids.add(str(doc["id_persona_pd"]))

    groups = {
        str(doc.get("cod_grupo_gr"))
        for doc in production_docs
        if doc.get("cod_grupo_gr") is not None and str(doc.get("cod_grupo_gr")).strip()
    }

    cvlac_data_docs = []
    cvlac_stage_docs = []
    cvlac_private_docs = []
    groups_docs = []

    if person_ids:
        person_query = {"id_persona_pr": {"$in": list(person_ids)}}
        cvlac_data_docs = list(source_db["cvlac_data"].find(person_query).limit(max_records))
        cvlac_stage_docs = list(source_db["cvlac_stage"].find(person_query).limit(max_records))
        cvlac_private_docs = list(
            source_db["cvlac_stage_private"].find(person_query).limit(max_records)
        )

    if groups:
        groups_docs = list(
            source_db["gruplac_groups_data"].find({"cod_grupo_gr": {"$in": list(groups)}})
        )

    counts = {
        "gruplac_production_data": _upsert_documents(
            sample_db["gruplac_production_data"], production_docs
        ),
        "cvlac_data": _upsert_documents(sample_db["cvlac_data"], cvlac_data_docs),
        "cvlac_stage": _upsert_documents(sample_db["cvlac_stage"], cvlac_stage_docs),
        "cvlac_stage_private": _upsert_documents(
            sample_db["cvlac_stage_private"], cvlac_private_docs
        ),
        "gruplac_groups_data": _upsert_documents(sample_db["gruplac_groups_data"], groups_docs),
    }

    _ensure_collection(sample_db, "cvlac_stage_raw")
    counts["cvlac_stage_raw"] = sample_db["cvlac_stage_raw"].count_documents({})
    return counts


def prepare_context(**kwargs: Any) -> dict[str, Any]:
    params = _merged_params(kwargs)
    selected_plugins = _select_plugins(params)
    if not selected_plugins:
        raise ValueError("No plugins selected. Enable at least one run_* plugin flag.")

    target_type = str(params.get("target_type", "author")).strip().lower()
    if target_type not in {"author", "work"}:
        raise ValueError("target_type must be one of: author, work")

    identifiers = _build_identifiers(params)
    affiliation_context = _resolve_affiliation_context(params, selected_plugins)

    person_id_values = (
        _id_list(identifiers, "author_cedula")
        + _id_list(identifiers, "author_cod_rh")
        + _id_list(identifiers, "author_dam_id")
        + _id_list(identifiers, "author_orcid")
        + _id_list(identifiers, "author_openalex")
        + _id_list(identifiers, "author_scholar")
    )
    work_id_values = (
        _id_list(identifiers, "work_doi")
        + _id_list(identifiers, "work_openalex")
        + _id_list(identifiers, "work_scienti_cod_producto")
        + _id_list(identifiers, "work_dam_id_producto")
        + _id_list(identifiers, "work_ciarp_id")
        + _id_list(identifiers, "work_scholar_cid")
    )
    work_title = _id_text(identifiers, "work_title")
    if work_title:
        work_id_values.append(work_title)

    if target_type == "author" and not person_id_values:
        raise ValueError("For target_type=author, at least one author identifier is required.")
    if target_type == "work" and not work_id_values:
        raise ValueError("For target_type=work, at least one work identifier is required.")

    selected_set = set(selected_plugins)
    has_author_cedula = bool(_id_list(identifiers, "author_cedula"))
    has_work_author_cedula = bool(_id_list(identifiers, "work_author_cedula"))
    has_staff_cedula = (
        has_author_cedula
        or has_work_author_cedula
        or bool(_id_list(identifiers, "work_ciarp_cedula"))
    )
    has_author_cod_rh = bool(_id_list(identifiers, "author_cod_rh"))
    has_author_dam_id = bool(_id_list(identifiers, "author_dam_id"))
    has_author_openalex = bool(_id_list(identifiers, "author_openalex"))
    has_author_orcid = bool(_id_list(identifiers, "author_orcid"))
    has_author_scholar = bool(_id_list(identifiers, "author_scholar"))
    has_work_openalex = bool(_id_list(identifiers, "work_openalex"))
    has_work_doi = bool(_id_list(identifiers, "work_doi"))
    has_work_scienti_pair = bool(
        _id_list(identifiers, "work_scienti_cod_rh")
        and _id_list(identifiers, "work_scienti_cod_producto")
    )
    has_work_scholar_cid = bool(_id_list(identifiers, "work_scholar_cid"))
    has_work_ciarp_id = bool(_id_list(identifiers, "work_ciarp_id"))
    has_work_ciarp_selector = bool(
        has_work_ciarp_id
        or has_work_doi
        or has_work_author_cedula
        or _id_text(identifiers, "work_title")
        or _id_list(identifiers, "work_ciarp_cedula")
    )
    has_work_dam_id = bool(_id_list(identifiers, "work_dam_id_producto"))

    if selected_set & LOCAL_FILE_PLUGINS:
        if {"staff_affiliations", "staff_person"} & selected_set and not affiliation_context.get(
            "staff_source_file"
        ):
            raise ValueError(
                "No STAFF file resolved for affiliation. Provide staff_source_file or adjust affiliation."
            )
        if {"ciarp_works/doi", "ciarp_works"} & selected_set and not affiliation_context.get(
            "ciarp_source_file"
        ):
            raise ValueError(
                "No CIARP file resolved for affiliation. Provide ciarp_source_file or adjust affiliation."
            )

    if selected_set & SCIENTI_PLUGINS and not affiliation_context.get("scienti_source_db"):
        raise ValueError(
            "The selected affiliation has no configured Scienti database. "
            "Set scienti_source_db_override or disable Scienti plugins."
        )

    def _require(plugin_name: str, condition: bool, message: str) -> None:
        if plugin_name in selected_set and not condition:
            raise ValueError(f"{plugin_name} requires {message}.")

    _require(
        "ror_affiliations", bool(affiliation_context.get("affiliation_ror")), "affiliation_ror"
    )
    _require(
        "openalex_affiliations", bool(affiliation_context.get("affiliation_ror")), "affiliation_ror"
    )
    _require("staff_affiliations", has_staff_cedula, "author_cedula or work_author_cedula")
    _require(
        "scienti_affiliations",
        has_author_cod_rh or has_work_scienti_pair,
        "COD_RH or (COD_RH + COD_PRODUCTO)",
    )
    _require(
        "minciencias_opendata_affiliations",
        has_author_dam_id or has_work_dam_id,
        "author_dam_id/COD_RH or work_dam_id_producto",
    )
    _require("staff_person", has_staff_cedula, "author_cedula or work_author_cedula")
    _require("scienti_person", has_author_cod_rh, "author_cod_rh")
    _require("minciencias_opendata_person", has_author_dam_id, "author_dam_id or COD_RH")
    _require("openalex_person", has_author_openalex, "author_openalex")
    _require("orcid_person", has_author_orcid, "author_orcid")
    _require("scholar_person", has_author_scholar, "author_scholar (profile id or URL)")
    _require("openalex_works/doi", has_work_doi or has_work_openalex, "work_doi or work_openalex")
    _require(
        "scienti_works/doi",
        has_work_doi or has_work_scienti_pair,
        "work_doi or (COD_RH + COD_PRODUCTO)",
    )
    _require(
        "ciarp_works/doi",
        has_work_ciarp_selector,
        "work_doi, work_ciarp_id, work_author_cedula or work_title",
    )
    _require(
        "scholar_works/doi", has_work_doi or has_work_scholar_cid, "work_doi or work_scholar_cid"
    )
    _require("minciencias_opendata_works", has_work_dam_id, "work_dam_id_producto")
    _require("openalex_works", has_work_openalex, "work_openalex")
    _require(
        "scienti_works", has_work_scienti_pair, "work_scienti_cod_rh + work_scienti_cod_producto"
    )
    _require(
        "ciarp_works",
        has_work_ciarp_id or has_work_author_cedula,
        "work_ciarp_id or work_author_cedula",
    )
    _require("scholar_works", has_work_scholar_cid, "work_scholar_cid")

    if "unicity_person" in selected_set:
        person_plugins = {
            "staff_person",
            "scienti_person",
            "minciencias_opendata_person",
            "openalex_person",
            "orcid_person",
            "scholar_person",
        }
        if not (selected_set & person_plugins):
            raise ValueError(
                "unicity_person requires at least one upstream person plugin selected."
            )

    if selected_set & NON_DOI_WORK_PLUGINS and (
        not str(params.get("es_url", "")).strip() or not str(params.get("es_index", "")).strip()
    ):
        raise ValueError(
            "Non-doi work plugins require Elasticsearch. Set es_url and es_index or disable non-doi plugins."
        )

    source_db_names = {
        "openalex": str(params.get("openalex_source_db", "openalex")).strip(),
        "orcid": str(params.get("orcid_source_db", "orcid")).strip(),
        "dam": str(params.get("dam_source_db", "dam")).strip(),
        "scholar": str(params.get("scholar_source_db", "scholar_colombia_2024")).strip(),
        "ror": str(params.get("ror_source_db", "ror")).strip(),
        "ror_collection": str(params.get("ror_source_collection", "ror_stage")).strip(),
        "scienti_collection": str(params.get("scienti_source_collection", "product")).strip(),
    }

    sample_dbs = {
        "openalex": str(params.get("openalex_sample_db", DEFAULT_SAMPLE_DBS["openalex"])).strip(),
        "minciencias": str(
            params.get("minciencias_sample_db", DEFAULT_SAMPLE_DBS["minciencias"])
        ).strip(),
        "orcid": str(params.get("orcid_sample_db", DEFAULT_SAMPLE_DBS["orcid"])).strip(),
        "scienti": str(params.get("scienti_sample_db", DEFAULT_SAMPLE_DBS["scienti"])).strip(),
        "scholar": str(params.get("scholar_sample_db", DEFAULT_SAMPLE_DBS["scholar"])).strip(),
        "ror": str(params.get("ror_sample_db", DEFAULT_SAMPLE_DBS["ror"])).strip(),
    }

    target_db = str(params.get("target_db", "kahi_sample_test")).strip()
    log_db = str(params.get("log_db", "kahi_samples_log")).strip()
    allow_drop_databases = _coerce_bool(params.get("allow_drop_databases", True))
    cleanup_sample_databases = _coerce_bool(params.get("cleanup_sample_databases", True))

    artifacts_dir = Path(
        str(params.get("artifacts_dir", "/tmp/impactu_airflow_cache/kahi_test_sample"))
    ).expanduser()
    mongo_conn_id = str(params.get("mongo_conn_id", "mongodb_default")).strip()
    mongo_uri = _resolve_mongo_uri(
        str(params.get("mongo_uri_override", "")).strip(),
        mongo_conn_id,
    )

    context = {
        "target_type": target_type,
        "selected_plugins": selected_plugins,
        "affiliation": affiliation_context,
        "identifiers": identifiers,
        "mongo_conn_id": mongo_conn_id,
        "mongo_uri_override": mongo_uri,
        "source_db_names": source_db_names,
        "sample_dbs": sample_dbs,
        "target_db": target_db,
        "log_db": log_db,
        "artifacts_dir": str(artifacts_dir),
        "sample_max_records": int(params.get("sample_max_records", 5000)),
        "num_jobs": int(params.get("num_jobs", 1)),
        "verbose": int(params.get("verbose", 3)),
        "minciencias_works_insert_all": _coerce_bool(
            params.get("minciencias_works_insert_all", False)
        ),
        "minciencias_works_thresholds": _parse_thresholds(
            params.get("minciencias_works_thresholds", "65,90,95")
        ),
        "es_url": str(params.get("es_url", "")).strip(),
        "es_index": str(params.get("es_index", "")).strip(),
        "es_user": str(params.get("es_user", "")).strip(),
        "es_password": str(params.get("es_password", "")).strip(),
        "kahi_run_cmd": str(params.get("kahi_run_cmd", "kahi_run")).strip(),
        "allow_drop_databases": allow_drop_databases,
        "cleanup_sample_databases": cleanup_sample_databases,
        "drop_databases": [
            target_db,
            log_db,
            sample_dbs["openalex"],
            sample_dbs["minciencias"],
            sample_dbs["orcid"],
            sample_dbs["scienti"],
            sample_dbs["scholar"],
            sample_dbs["ror"],
        ],
    }
    return context


def _required_sample_groups(selected_plugins: list[str]) -> set[str]:
    selected = set(selected_plugins)
    groups: set[str] = set()
    if selected & OPENALEX_SAMPLE_PLUGINS:
        groups.add("openalex")
    if selected & ROR_SAMPLE_PLUGINS:
        groups.add("ror")
    if selected & SCIENTI_SAMPLE_PLUGINS:
        groups.add("scienti")
    if selected & MINCIENCIAS_SAMPLE_PLUGINS:
        groups.add("minciencias")
    if selected & SCHOLAR_SAMPLE_PLUGINS:
        groups.add("scholar")
    if selected & ORCID_SAMPLE_PLUGINS:
        groups.add("orcid")
    if selected & STAFF_SAMPLE_PLUGINS:
        groups.add("staff")
    if selected & CIARP_SAMPLE_PLUGINS:
        groups.add("ciarp")
    return groups


def _validate_safe_database_drop_names(database_names: list[str]) -> None:
    unsafe_names = []
    for name in database_names:
        normalized = str(name).strip()
        if (
            not normalized
            or normalized in DANGEROUS_DATABASE_NAMES
            or not normalized.startswith(SAFE_DROP_PREFIXES)
        ):
            unsafe_names.append(normalized or "<empty>")
    if unsafe_names:
        raise ValueError(
            "Refusing to drop unsafe database name(s): "
            f"{', '.join(unsafe_names)}. Use test/sample database prefixes."
        )


def _initial_drop_databases(context: dict[str, Any]) -> list[str]:
    return list(dict.fromkeys(context["drop_databases"]))


def _sample_drop_databases(context: dict[str, Any]) -> list[str]:
    return list(dict.fromkeys(context["sample_dbs"].values()))


def build_samples(**kwargs: Any) -> dict[str, Any]:
    ti = kwargs["ti"]
    context = ti.xcom_pull(task_ids="prepare_context")
    if not context:
        raise ValueError("Missing context from prepare_context task")

    artifacts_dir = Path(context["artifacts_dir"])
    artifacts_dir.mkdir(parents=True, exist_ok=True)

    run_id = _safe_name(str(kwargs.get("run_id", "manual_run")))

    staff_sample_path = artifacts_dir / f"staff_{run_id}.xlsx"
    ciarp_sample_path = artifacts_dir / f"ciarp_{run_id}.xlsx"

    sample_groups = _required_sample_groups(context["selected_plugins"])
    needs_mongo_client = bool(sample_groups - {"staff", "ciarp"}) or context["allow_drop_databases"]

    client = None
    if needs_mongo_client:
        mongo_client_cls, _ = _resolve_pymongo()
        client = mongo_client_cls(context["mongo_uri_override"])

    if context["allow_drop_databases"]:
        if client is None:
            raise ValueError("Mongo client is required to drop sample databases.")
        drop_databases = _initial_drop_databases(context)
        _validate_safe_database_drop_names(drop_databases)
        for db_name in drop_databases:
            client.drop_database(db_name)

    sample_counts: dict[str, Any] = {}

    affiliation = context["affiliation"]
    identifiers = context["identifiers"]
    source_db_names = context["source_db_names"]
    sample_dbs = context["sample_dbs"]

    staff_rows = 0
    if "staff" in sample_groups:
        staff_rows = _create_staff_sample(
            affiliation.get("staff_source_file", ""),
            staff_sample_path,
            list(
                set(
                    _id_list(identifiers, "author_cedula")
                    + _id_list(identifiers, "work_author_cedula")
                )
            ),
        )

    ciarp_rows = 0
    if "ciarp" in sample_groups:
        ciarp_rows = _create_ciarp_sample(
            affiliation.get("ciarp_source_file", ""),
            ciarp_sample_path,
            context["target_type"],
            identifiers,
        )

    sample_counts["staff_sample_rows"] = staff_rows
    sample_counts["ciarp_sample_rows"] = ciarp_rows

    if "openalex" in sample_groups:
        sample_counts["openalex"] = _build_openalex_sample(
            client=client,
            source_db_name=source_db_names["openalex"],
            sample_db_name=sample_dbs["openalex"],
            target_type=context["target_type"],
            affiliation_ror=affiliation["affiliation_ror"],
            identifiers=identifiers,
            max_records=context["sample_max_records"],
        )

    if "ror" in sample_groups:
        sample_counts["ror"] = _build_ror_sample(
            client=client,
            source_db_name=source_db_names["ror"],
            source_collection_name=source_db_names["ror_collection"],
            sample_db_name=sample_dbs["ror"],
            affiliation_ror=affiliation["affiliation_ror"],
        )

    if "scienti" in sample_groups and affiliation.get("scienti_source_db"):
        scienti_count = _build_scienti_sample(
            client=client,
            source_db_name=affiliation["scienti_source_db"],
            source_collection_name=source_db_names["scienti_collection"],
            sample_db_name=sample_dbs["scienti"],
            target_type=context["target_type"],
            identifiers=identifiers,
            max_records=context["sample_max_records"],
        )
        sample_counts["scienti"] = scienti_count

    if "scholar" in sample_groups:
        sample_counts["scholar"] = _build_scholar_sample(
            client=client,
            source_db_name=source_db_names["scholar"],
            sample_db_name=sample_dbs["scholar"],
            target_type=context["target_type"],
            identifiers=identifiers,
            max_records=context["sample_max_records"],
        )

    if "orcid" in sample_groups:
        sample_counts["orcid"] = _build_orcid_sample(
            client=client,
            source_db_name=source_db_names["orcid"],
            sample_db_name=sample_dbs["orcid"],
            identifiers=identifiers,
        )

    if "minciencias" in sample_groups:
        sample_counts["minciencias"] = _build_minciencias_sample(
            client=client,
            source_db_name=source_db_names["dam"],
            sample_db_name=sample_dbs["minciencias"],
            target_type=context["target_type"],
            identifiers=identifiers,
            max_records=context["sample_max_records"],
        )

    sample_result = {
        "sample_counts": sample_counts,
        "staff_sample_path": str(staff_sample_path),
        "ciarp_sample_path": str(ciarp_sample_path),
    }
    return sample_result


def _add_es_config(plugin_config: dict[str, Any], context: dict[str, Any]) -> None:
    if context["es_url"] and context["es_index"]:
        plugin_config["es_index"] = context["es_index"]
        plugin_config["es_url"] = context["es_url"]
        plugin_config["es_user"] = context["es_user"]
        plugin_config["es_password"] = context["es_password"]


def build_workflow(**kwargs: Any) -> dict[str, Any]:
    ti = kwargs["ti"]
    context = ti.xcom_pull(task_ids="prepare_context")
    sample_result = ti.xcom_pull(task_ids="build_samples")

    if not context or not sample_result:
        raise ValueError("Missing context/sample data from previous tasks")

    sample_dbs = context["sample_dbs"]
    affiliation = context["affiliation"]
    source_db_names = context["source_db_names"]

    workflow: dict[str, Any] = {}
    selected = set(context["selected_plugins"])

    if "ror_affiliations" in selected:
        workflow["ror_affiliations"] = {
            "database_url": context["mongo_uri_override"]
            or _mongo_hook_uri(context["mongo_conn_id"]),
            "database_name": sample_dbs["ror"],
            "collection_name": source_db_names["ror_collection"],
            "num_jobs": context["num_jobs"],
            "verbose": context["verbose"],
        }

    if "openalex_affiliations" in selected:
        workflow["openalex_affiliations"] = {
            "database_url": context["mongo_uri_override"]
            or _mongo_hook_uri(context["mongo_conn_id"]),
            "database_name": sample_dbs["openalex"],
            "collection_name": "institutions",
            "num_jobs": context["num_jobs"],
            "verbose": context["verbose"],
        }

    if "staff_affiliations" in selected:
        workflow["staff_affiliations"] = {
            "databases": [
                {
                    "institution_id": affiliation["affiliation_ror"],
                    "file_path": sample_result["staff_sample_path"],
                }
            ],
            "verbose": context["verbose"],
        }

    if "scienti_affiliations" in selected:
        workflow["scienti_affiliations"] = {
            "databases": [
                {
                    "database_url": context["mongo_uri_override"]
                    or _mongo_hook_uri(context["mongo_conn_id"]),
                    "database_name": sample_dbs["scienti"],
                    "collection_name": "product",
                }
            ],
            "verbose": context["verbose"],
        }

    if "minciencias_opendata_affiliations" in selected:
        workflow["minciencias_opendata_affiliations"] = {
            "database_url": context["mongo_uri_override"]
            or _mongo_hook_uri(context["mongo_conn_id"]),
            "database_name": sample_dbs["minciencias"],
            "collection_name": "gruplac_groups_data",
            "num_jobs": context["num_jobs"],
            "verbose": context["verbose"],
        }

    if "staff_person" in selected:
        entry: dict[str, Any] = {
            "institution_id": affiliation["affiliation_ror"],
            "staff_file_path": sample_result["staff_sample_path"],
        }
        ciarp_sample_path = sample_result.get("ciarp_sample_path", "")
        if ciarp_sample_path and Path(ciarp_sample_path).exists():
            entry["ciarp_file_path"] = ciarp_sample_path
        workflow["staff_person"] = {"databases": [entry], "verbose": context["verbose"]}

    if "scienti_person" in selected:
        workflow["scienti_person"] = {
            "databases": [
                {
                    "database_url": context["mongo_uri_override"]
                    or _mongo_hook_uri(context["mongo_conn_id"]),
                    "database_name": sample_dbs["scienti"],
                    "collection_name": "product",
                }
            ],
            "verbose": context["verbose"],
        }

    if "minciencias_opendata_person" in selected:
        workflow["minciencias_opendata_person"] = {
            "database_url": context["mongo_uri_override"]
            or _mongo_hook_uri(context["mongo_conn_id"]),
            "database_name": sample_dbs["minciencias"],
            "researchers": "cvlac_data",
            "cvlac": "cvlac_stage",
            "groups_production": "gruplac_production_data",
            "private_profiles": "cvlac_stage_private",
            "cvlac_html_profiles": "cvlac_stage_raw",
            "num_jobs": context["num_jobs"],
            "verbose": context["verbose"],
        }

    if "openalex_person" in selected:
        workflow["openalex_person"] = {
            "database_url": context["mongo_uri_override"]
            or _mongo_hook_uri(context["mongo_conn_id"]),
            "database_name": sample_dbs["openalex"],
            "collection_name": "authors",
            "collection_name_works": "works",
            "num_jobs": context["num_jobs"],
            "verbose": context["verbose"],
        }

    if "orcid_person" in selected:
        workflow["orcid_person"] = {
            "database_url": context["mongo_uri_override"]
            or _mongo_hook_uri(context["mongo_conn_id"]),
            "database_name": sample_dbs["orcid"],
            "collection_name": "summaries",
            "num_jobs": context["num_jobs"],
            "verbose": context["verbose"],
        }

    if "scholar_person" in selected:
        workflow["scholar_person"] = {
            "database_url": context["mongo_uri_override"]
            or _mongo_hook_uri(context["mongo_conn_id"]),
            "database_name": sample_dbs["scholar"],
            "collection_name": "stage",
            "num_jobs": context["num_jobs"],
            "verbose": context["verbose"],
        }

    if "unicity_person" in selected:
        workflow["unicity_person"] = {
            "collection_name": "person",
            "max_authors_threshold": 20,
            "num_jobs": 1,
            "verbose": context["verbose"],
            "task": ["scienti", "orcid", "scholar", "scopus", "researchgate", "doi"],
        }

    if "openalex_works/doi" in selected:
        plugin_config = {
            "database_url": context["mongo_uri_override"]
            or _mongo_hook_uri(context["mongo_conn_id"]),
            "database_name": sample_dbs["openalex"],
            "collection_name": "works",
            "num_jobs": context["num_jobs"],
            "verbose": context["verbose"],
        }
        _add_es_config(plugin_config, context)
        workflow["openalex_works/doi"] = plugin_config

    if "scienti_works/doi" in selected:
        plugin_config = {
            "databases": [
                {
                    "database_url": context["mongo_uri_override"]
                    or _mongo_hook_uri(context["mongo_conn_id"]),
                    "database_name": sample_dbs["scienti"],
                    "collection_name": "product",
                }
            ],
            "num_jobs": context["num_jobs"],
            "verbose": context["verbose"],
        }
        _add_es_config(plugin_config, context)
        workflow["scienti_works/doi"] = plugin_config

    if "ciarp_works/doi" in selected:
        workflow["ciarp_works/doi"] = {
            "databases": [
                {
                    "institution_id": affiliation["affiliation_ror"],
                    "file_path": sample_result["ciarp_sample_path"],
                }
            ],
            "num_jobs": 1,
            "verbose": context["verbose"],
        }

    if "scholar_works/doi" in selected:
        plugin_config = {
            "database_url": context["mongo_uri_override"]
            or _mongo_hook_uri(context["mongo_conn_id"]),
            "database_name": sample_dbs["scholar"],
            "collection_name": "stage",
            "num_jobs": context["num_jobs"],
            "verbose": context["verbose"],
        }
        _add_es_config(plugin_config, context)
        workflow["scholar_works/doi"] = plugin_config

    if "minciencias_opendata_works" in selected:
        plugin_config = {
            "database_url": context["mongo_uri_override"]
            or _mongo_hook_uri(context["mongo_conn_id"]),
            "database_name": sample_dbs["minciencias"],
            "collection_name": "gruplac_production_data",
            "insert_all": context["minciencias_works_insert_all"],
            "thresholds": context["minciencias_works_thresholds"],
            "num_jobs": context["num_jobs"],
            "verbose": context["verbose"],
        }
        _add_es_config(plugin_config, context)
        workflow["minciencias_opendata_works"] = plugin_config

    if "openalex_works" in selected:
        plugin_config = {
            "database_url": context["mongo_uri_override"]
            or _mongo_hook_uri(context["mongo_conn_id"]),
            "database_name": sample_dbs["openalex"],
            "collection_name": "works",
            "num_jobs": context["num_jobs"],
            "verbose": context["verbose"],
            "backend": "threading",
        }
        _add_es_config(plugin_config, context)
        workflow["openalex_works"] = plugin_config

    if "scienti_works" in selected:
        plugin_config = {
            "databases": [
                {
                    "database_url": context["mongo_uri_override"]
                    or _mongo_hook_uri(context["mongo_conn_id"]),
                    "database_name": sample_dbs["scienti"],
                    "collection_name": "product",
                }
            ],
            "num_jobs": context["num_jobs"],
            "verbose": context["verbose"],
        }
        _add_es_config(plugin_config, context)
        workflow["scienti_works"] = plugin_config

    if "ciarp_works" in selected:
        plugin_config = {
            "databases": [
                {
                    "institution_id": affiliation["affiliation_ror"],
                    "file_path": sample_result["ciarp_sample_path"],
                }
            ],
            "num_jobs": 1,
            "verbose": context["verbose"],
        }
        _add_es_config(plugin_config, context)
        workflow["ciarp_works"] = plugin_config

    if "scholar_works" in selected:
        plugin_config = {
            "database_url": context["mongo_uri_override"]
            or _mongo_hook_uri(context["mongo_conn_id"]),
            "database_name": sample_dbs["scholar"],
            "collection_name": "stage",
            "num_jobs": context["num_jobs"],
            "verbose": context["verbose"],
        }
        _add_es_config(plugin_config, context)
        workflow["scholar_works"] = plugin_config

    ordered_workflow = {name: workflow[name] for name in PLUGIN_ORDER if name in workflow}

    mongo_uri = context["mongo_uri_override"] or _mongo_hook_uri(context["mongo_conn_id"])
    workflow_config = {
        "config": {
            "database_url": mongo_uri,
            "database_name": context["target_db"],
            "log_database": context["log_db"],
            "log_collection": "log",
            "profile": False,
        },
        "workflow": ordered_workflow,
    }

    artifacts_dir = Path(context["artifacts_dir"])
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    run_id = _safe_name(str(kwargs.get("run_id", "manual_run")))
    workflow_path = artifacts_dir / f"kahi_test_sample_{run_id}.yml"

    with workflow_path.open("w", encoding="utf-8") as handler:
        yaml.safe_dump(workflow_config, handler, sort_keys=False, allow_unicode=False)

    return {
        "workflow_path": str(workflow_path),
        "workflow_plugins": list(ordered_workflow.keys()),
    }


def run_kahi_test_sample(**kwargs: Any) -> dict[str, Any]:
    ti = kwargs["ti"]
    context = ti.xcom_pull(task_ids="prepare_context")
    workflow_data = ti.xcom_pull(task_ids="build_workflow")

    if not context or not workflow_data:
        raise ValueError("Missing context/workflow from previous tasks")

    workflow_path = workflow_data["workflow_path"]
    kahi_run_cmd = context["kahi_run_cmd"]

    executable = shutil.which(kahi_run_cmd)
    if executable is None:
        raise RuntimeError(f"Command not found in PATH: {kahi_run_cmd}")

    process = subprocess.run(
        [executable, "--workflow", workflow_path],
        check=True,
        capture_output=True,
        text=True,
    )

    stdout_tail = "\n".join(process.stdout.splitlines()[-50:])
    stderr_tail = "\n".join(process.stderr.splitlines()[-50:])

    return {
        "workflow_path": workflow_path,
        "plugins": workflow_data["workflow_plugins"],
        "stdout_tail": stdout_tail,
        "stderr_tail": stderr_tail,
    }


def summarize_run(**kwargs: Any) -> None:
    ti = kwargs["ti"]
    context = ti.xcom_pull(task_ids="prepare_context")
    sample_data = ti.xcom_pull(task_ids="build_samples")
    workflow_data = ti.xcom_pull(task_ids="build_workflow")
    run_data = ti.xcom_pull(task_ids="run_kahi_test_sample")

    print(
        json.dumps(
            {
                "target_db": context["target_db"],
                "target_type": context["target_type"],
                "affiliation": context["affiliation"],
                "selected_plugins": context["selected_plugins"],
                "sample_counts": sample_data["sample_counts"],
                "workflow_path": workflow_data["workflow_path"],
                "workflow_plugins": workflow_data["workflow_plugins"],
                "run_stdout_tail": run_data["stdout_tail"],
                "run_stderr_tail": run_data["stderr_tail"],
            },
            ensure_ascii=True,
            indent=2,
            default=str,
        )
    )


def cleanup_sample_databases(**kwargs: Any) -> dict[str, Any]:
    ti = kwargs["ti"]
    context = ti.xcom_pull(task_ids="prepare_context")
    if not context:
        print("No prepare_context XCom found; skipping sample database cleanup.")
        return {"cleanup_sample_databases": False, "dropped_databases": []}

    if not context.get("cleanup_sample_databases", True):
        print("cleanup_sample_databases is disabled; leaving sample databases in place.")
        return {"cleanup_sample_databases": False, "dropped_databases": []}

    mongo_client_cls, _ = _resolve_pymongo()
    client = mongo_client_cls(context["mongo_uri_override"])
    drop_databases = _sample_drop_databases(context)
    _validate_safe_database_drop_names(drop_databases)
    for db_name in drop_databases:
        client.drop_database(db_name)

    result = {"cleanup_sample_databases": True, "dropped_databases": drop_databases}
    print(json.dumps(result, ensure_ascii=True, indent=2, default=str))
    return result


def _plugin_params_defaults() -> dict[str, Param]:
    result: dict[str, Param] = {}
    for param_name, _ in PLUGIN_PARAM_MAP:
        result[param_name] = Param(
            False,
            type="boolean",
            description=f"Enable plugin execution for {param_name.replace('run_', '')}",
        )
    return result


default_args = {
    "owner": "impactu",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="kahi_test_sample",
    default_args=default_args,
    description="Build sample databases and run selected Kahi plugins for forensic reconstruction",
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
    tags=["kahi", "forensics", "sample"],
    params={
        "target_type": Param(
            "author",
            type="string",
            enum=["author", "work"],
            description="Reconstruction target type",
        ),
        "affiliation_ror": Param(
            "",
            type="string",
            description="Affiliation ROR (e.g., https://ror.org/03bp5hc83)",
        ),
        "author_cedula": Param(
            "", type="string", description="Author national id (comma-separated)"
        ),
        "author_cod_rh": Param("", type="string", description="Author COD_RH (comma-separated)"),
        "author_dam_id": Param("", type="string", description="Author DAM id_persona_pr"),
        "author_orcid": Param("", type="string", description="Author ORCID URL/id"),
        "author_openalex": Param("", type="string", description="Author OpenAlex id/url"),
        "author_scholar": Param(
            "", type="string", description="Author Scholar profile id or full profile URL"
        ),
        "work_doi": Param("", type="string", description="Work DOI (comma-separated)"),
        "work_openalex": Param("", type="string", description="Work OpenAlex id/url"),
        "work_scienti_cod_producto": Param(
            "", type="string", description="Work Scienti COD_PRODUCTO (comma-separated)"
        ),
        "work_scienti_cod_rh": Param("", type="string", description="Work Scienti COD_RH"),
        "work_dam_id_producto": Param("", type="string", description="Work DAM id_producto_pd"),
        "work_ciarp_id": Param(
            "",
            type="string",
            description="Work CIARP external id(s), e.g. 111017-0000559695-1771787918",
        ),
        "work_scholar_cid": Param("", type="string", description="Work Scholar CID"),
        "work_title": Param(
            "", type="string", description="Work title (for CIARP exact-title filtering)"
        ),
        "work_author_cedula": Param(
            "",
            type="string",
            description="Author cédula linked to work (for STAFF/CIARP filtering)",
        ),
        "data_root_dir": Param(
            "/home/fvergara/projects/colav/data",
            type="string",
            description="Base directory containing STAFF and CIARP folders",
        ),
        "staff_source_file": Param(
            "",
            type="string",
            description="Optional explicit STAFF source file path (overrides affiliation mapping)",
        ),
        "ciarp_source_file": Param(
            "",
            type="string",
            description="Optional explicit CIARP source file path (overrides affiliation mapping)",
        ),
        "artifacts_dir": Param(
            "/tmp/impactu_airflow_cache/kahi_test_sample",
            type="string",
            description="Directory for generated sample files and workflow artifacts",
        ),
        "mongo_conn_id": Param(
            "mongodb_default", type="string", description="Airflow Mongo connection id"
        ),
        "mongo_uri_override": Param(
            "",
            type="string",
            description="Optional Mongo URI override used in generated Kahi workflow",
        ),
        "openalex_source_db": Param(
            "openalex", type="string", description="OpenAlex source database"
        ),
        "scholar_source_db": Param(
            "scholar_colombia_2024", type="string", description="Scholar source database"
        ),
        "orcid_source_db": Param("orcid", type="string", description="ORCID source database"),
        "dam_source_db": Param("dam", type="string", description="Minciencias/DAM source database"),
        "ror_source_db": Param("ror", type="string", description="ROR source database"),
        "ror_source_collection": Param(
            "ror_stage", type="string", description="ROR source collection name"
        ),
        "scienti_source_collection": Param(
            "product", type="string", description="Scienti source collection name"
        ),
        "scienti_source_db_override": Param(
            "",
            type="string",
            description="Override Scienti source database resolved from affiliation",
        ),
        "target_db": Param(
            "kahi_sample_test", type="string", description="Output Kahi target database"
        ),
        "log_db": Param("kahi_samples_log", type="string", description="Kahi log database"),
        "openalex_sample_db": Param("openalex_sample", type="string"),
        "minciencias_sample_db": Param("minciencias_sample", type="string"),
        "orcid_sample_db": Param("orcid_sample", type="string"),
        "scienti_sample_db": Param("scienti_sample", type="string"),
        "scholar_sample_db": Param("scholar_sample", type="string"),
        "ror_sample_db": Param("ror_sample", type="string"),
        "sample_max_records": Param(
            5000,
            type="integer",
            description="Maximum number of records copied per source collection",
        ),
        "num_jobs": Param(1, type="integer", description="num_jobs passed to Kahi plugins"),
        "verbose": Param(3, type="integer", description="verbose passed to Kahi plugins"),
        "minciencias_works_insert_all": Param(
            False,
            type="boolean",
            description="Insert unmatched minciencias works as new documents",
        ),
        "minciencias_works_thresholds": Param(
            "65,90,95",
            type="string",
            description="Three comma-separated thresholds for minciencias_opendata_works",
        ),
        "es_url": Param("", type="string", description="Elasticsearch URL for non-doi plugins"),
        "es_index": Param("", type="string", description="Elasticsearch index for non-doi plugins"),
        "es_user": Param("", type="string", description="Elasticsearch user"),
        "es_password": Param("", type="string", description="Elasticsearch password"),
        "kahi_run_cmd": Param(
            "kahi_run", type="string", description="kahi_run executable path/name"
        ),
        "allow_drop_databases": Param(
            True,
            type="boolean",
            description="Safely drop target, log, and sample databases before rebuilding samples",
        ),
        "cleanup_sample_databases": Param(
            True,
            type="boolean",
            description="Drop temporary sample databases after the Kahi run finishes",
        ),
        **_plugin_params_defaults(),
    },
) as dag:
    prepare_context_task = PythonOperator(
        task_id="prepare_context",
        python_callable=prepare_context,
    )

    build_samples_task = PythonOperator(
        task_id="build_samples",
        python_callable=build_samples,
    )

    build_workflow_task = PythonOperator(
        task_id="build_workflow",
        python_callable=build_workflow,
    )

    run_kahi_task = PythonOperator(
        task_id="run_kahi_test_sample",
        python_callable=run_kahi_test_sample,
    )

    summarize_task = PythonOperator(
        task_id="summarize_run",
        python_callable=summarize_run,
    )

    cleanup_task = PythonOperator(
        task_id="cleanup_sample_databases",
        python_callable=cleanup_sample_databases,
        trigger_rule="all_done",
    )

    (
        prepare_context_task
        >> build_samples_task
        >> build_workflow_task
        >> run_kahi_task
        >> summarize_task
    )
    run_kahi_task >> cleanup_task
    summarize_task >> cleanup_task
