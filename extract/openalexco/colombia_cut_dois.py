"""Colombia DOI-based cut: collects DOIs from multiple sources and upserts matching works."""

from __future__ import annotations

import re

import pandas as pd
from bs4 import BeautifulSoup  # noqa: F401 — kept for upstream compat
from joblib import Parallel, delayed
from kahi_impactu_utils.Utils import doi_processor
from pandas import isna
from pymongo import MongoClient

# ---------------------------------------------------------------------------
# Source databases — edit these to match your environment
# ---------------------------------------------------------------------------

# Google Scholar
DB_GS = "scholar_colombia_2024"
COL_GS = "data"

# Scienti
DBS_SCI = [
    "scienti_udea_2024",
    "scienti_uec_2024",
    "scienti_unaula_2024",
    "scienti_univalle_2024",
]
COL_SCI = "product"

# Scopus
DB_SC = "scopus_colombia"
COL_SC = "stage"

# WoS
DB_WOS = "wos_colombia"
COL_WOS = "stage"

# DSpace
DB_DSPACE = "oxomoc_colombia"
DSPACE_PIPELINE = [
    {
        "$project": {
            "doi": {
                "$filter": {
                    "input": {
                        "$cond": [
                            {"$isArray": "$OAI-PMH.GetRecord.record.metadata.dim:dim.dim:field"},
                            "$OAI-PMH.GetRecord.record.metadata.dim:dim.dim:field",
                            ["$OAI-PMH.GetRecord.record.metadata.dim:dim.dim:field"],
                        ]
                    },
                    "as": "field",
                    "cond": {
                        "$and": [
                            {"$eq": ["$$field.@element", "identifier"]},
                            {"$eq": ["$$field.@qualifier", "doi"]},
                        ]
                    },
                }
            },
            "_id": 0,
        }
    }
]

# CIARP files
CIARP_FILES: list[str] = ["/storage/kahi_data/kahi_data/staff/formato_CIARP_UDEA_2024_11.xlsx"]

# DAM
DB_DAM = "yuku_2025_2"
COL_DAM = "cvlac_stage_raw"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _extract_doi_candidates_from_html(html: str) -> list[str]:
    candidate_pattern = r"10\.\d{3,}/[^\s\"'<]+"
    return re.findall(candidate_pattern, html, flags=re.IGNORECASE)


def _extract_valid_dois(html: str) -> list[str]:
    candidates = _extract_doi_candidates_from_html(html)
    valid: set[str] = set()
    for cand in candidates:
        doi_url = doi_processor(cand)
        if doi_url:
            valid.add(doi_url)
    return list(valid)


def _process_doi(client: MongoClient, doi: str, db_in: str, db_out: str) -> None:
    work = client[db_in]["works"].find_one({"doi": doi})
    if work:
        found = client[db_out]["works"].count_documents({"id": work["id"]})
        if found == 0:
            client[db_out]["works"].insert_one(work)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def colombia_cut_dois(
    db_in: str,
    db_out: str,
    jobs: int = 72,
    backend: str = "threading",
    client: MongoClient | None = None,
) -> None:
    """Collect DOIs from all configured sources and upsert matching works."""
    c: MongoClient = client if client is not None else MongoClient()
    dois: list[str] = []

    # Google Scholar
    data = list(c[DB_GS][COL_GS].find({"doi": {"$ne": "", "$exists": 1}}, {"doi": 1, "_id": 0}))
    for doc in data:
        try:
            dois.append(doc["doi"])
        except KeyError:
            print(doc)

    # Scienti
    for db in DBS_SCI:
        data = list(
            c[db][COL_SCI].find(
                {"TXT_DOI": {"$ne": None, "$exists": 1}},
                {"TXT_DOI": 1, "_id": 0},
            )
        )
        for doc in data:
            if doc.get("TXT_DOI"):
                dois.append(doc["TXT_DOI"])

    # Scopus
    data = list(c[DB_SC][COL_SC].find({"DOI": {"$ne": None, "$exists": 1}}, {"DOI": 1, "_id": 0}))
    for doc in data:
        if not isna(doc.get("DOI", float("nan"))):
            dois.append(doc["DOI"])

    # WoS
    data = list(c[DB_WOS][COL_WOS].find({"DI": {"$ne": ""}}, {"DI": 1, "_id": 0}))
    for doc in data:
        dois.append(doc["DI"])

    # DSpace
    dspace_db = c[DB_DSPACE]
    collections = dspace_db.list_collection_names(filter={"name": {"$regex": r"^dspace.*records$"}})
    for collection in collections:
        print(f"INFO: processing {collection}")
        cursor = dspace_db[collection].aggregate(DSPACE_PIPELINE)
        for doc in cursor:
            if doc.get("doi") is None:
                continue
            for raw_doi in doc["doi"]:
                if raw_doi and "#text" in raw_doi:
                    dois.append(raw_doi["#text"])

    # CIARP
    for ciarp_file in CIARP_FILES:
        df = pd.read_excel(ciarp_file)
        dois.extend(df["doi"].dropna().values.tolist())

    # DAM
    cursor = c[DB_DAM][COL_DAM].find()
    raw = Parallel(n_jobs=-1, verbose=10)(
        delayed(_extract_valid_dois)(item["html"]) for item in cursor
    )
    dam_dois = [d for sub in raw for d in sub if d]
    dois.extend(list(set(dam_dois)))

    # Remove already-inserted DOIs
    already = [
        doc["doi"] for doc in c[db_out]["works"].find({"doi": {"$ne": None}}, {"doi": 1, "_id": 0})
    ]
    already_set = set(already)

    processed: list[str] = []
    for raw_doi in dois:
        if raw_doi is not None:
            pdoi = doi_processor(raw_doi)
            if pdoi and pdoi not in already_set:
                processed.append(pdoi)
    processed = list(set(processed))
    print(f"INFO: dois found = {len(processed)}")

    Parallel(n_jobs=jobs, backend=backend, verbose=10, batch_size=4)(
        delayed(_process_doi)(c, doi, db_in, db_out) for doi in processed
    )
