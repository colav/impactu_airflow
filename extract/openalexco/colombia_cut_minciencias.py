"""Colombia cut via Minciencias products using Elasticsearch title similarity.

Reads the latest full ``minciencias_works_vXX_YYYYMMDD`` collection (DAM
pipeline) and, for every product **without DOI** (those are handled by the DOI
cut) of a bibliographic type, searches its title in the OpenAlex ES index.
Matching OpenAlex works are copied into the Colombia cut.
"""

from __future__ import annotations

import time
from typing import Any

from bson.objectid import ObjectId
from joblib import Parallel, delayed
from mohan.Similarity import Similarity
from pymongo import MongoClient
from pymongo.collection import Collection
from thefuzz import fuzz, process
from unidecode import unidecode

from extract.openalexco.minciencias_source import MINCIENCIAS_DB, latest_minciencias_collection

# Similarity thresholds. Authors are compared with ``token_set_ratio``: it is
# word-order independent ("Perez Rave Jorge Ivan" vs "Jorge Ivan Perez Rave"
# scores 100, ``partial_ratio`` only 65) and rejects shared-surname-only pairs.
AUTHOR_THD = 85
PAPER_THD_LOW = 90
PAPER_THD_HIGH = 94

# Plausibility filter applied after the score thresholds (validated on random
# samples: removes ~90% of false positives without losing true matches).
# Without author match: generic/short titles, length mismatch or distant years.
NO_AUTHOR_MIN_WORDS = 5
NO_AUTHOR_MIN_LEN_RATIO = 0.75
NO_AUTHOR_MAX_YEAR_GAP = 4
# With author match: only reject titles where one is a fragment of the other.
AUTHOR_MIN_LEN_RATIO = 0.4

# ImpactU product types (``types[source="impactu"]``) searched by title —
# bibliographic products that OpenAlex may index.
IMPACTU_TYPES = [
    "Artículo de revista",
    "Capítulo de libro",
    "Libro",
    "Tesis de pregrado",
    "Tesis de posgrado",
    "Publicaciones editoriales no especializadas",
    "Informe",
    "Documento de trabajo",
]

_PIPELINE: list[dict[str, Any]] = [
    {
        "$match": {
            "$or": [{"doi": {"$exists": False}}, {"doi": {"$in": [None, ""]}}],
            "types": {"$elemMatch": {"source": "impactu", "type": {"$in": IMPACTU_TYPES}}},
        }
    },
    {
        "$project": {
            "_id": 0,
            "title": {"$arrayElemAt": ["$titles.title", 0]},
            "author": {"$arrayElemAt": ["$authors.full_name", 0]},
            "year": "$year_published",
        }
    },
]


def _str_normalize(word: str) -> str:
    return str(unidecode(word)).lower().strip().replace(".", "")


def _plausible_match(
    title_work: str, oa_title: str, author_found: bool, year: Any, oa_year: Any
) -> bool:
    """Reject title matches that pass the fuzzy threshold but are likely wrong.

    ``WRatio`` scores high when one title is contained in the other, e.g.
    "Aves sin nido o el deseo del Estado-nación" vs "Aves sin nido", and short
    generic titles ("Ictericia") match unrelated works without author support.
    """
    t1, t2 = _str_normalize(title_work), _str_normalize(oa_title)
    len_ratio = min(len(t1), len(t2)) / max(len(t1), len(t2), 1)
    if author_found:
        return len_ratio >= AUTHOR_MIN_LEN_RATIO
    if len(t1.split()) < NO_AUTHOR_MIN_WORDS or len_ratio < NO_AUTHOR_MIN_LEN_RATIO:
        return False
    try:
        return abs(int(year) - int(oa_year)) <= NO_AUTHOR_MAX_YEAR_GAP
    except (TypeError, ValueError):
        return True  # unknown year: do not reject on it


def _check_work(
    openalex_in: Collection,
    openalex_out: Collection,
    title_work: str,
    authors: list[str],
    response: dict,
    dry_run: bool = False,
    year: Any = None,
) -> int:
    author_found = False
    if authors and authors[0] != "":
        _authors = [_str_normalize(a) for a in response["_source"]["authors"]]
        scores = process.extract(_str_normalize(authors[0]), _authors, scorer=fuzz.token_set_ratio)
        for score in scores:
            if score[1] >= AUTHOR_THD:
                author_found = True
                break

    if response["_source"]["title"]:
        score = fuzz.WRatio(
            _str_normalize(title_work), _str_normalize(response["_source"]["title"])
        )
        threshold = PAPER_THD_LOW if author_found else PAPER_THD_HIGH
        if score >= threshold:
            if not _plausible_match(
                title_work,
                response["_source"]["title"],
                author_found,
                year,
                response["_source"].get("year"),
            ):
                return 0
            oid = ObjectId(response["_id"])
            if dry_run:
                return 1
            if openalex_out["works"].find_one({"_id": oid}) is None:
                oa_work = openalex_in["works"].find_one({"_id": oid})
                try:
                    if oa_work is not None:
                        openalex_out["works"].insert_one(oa_work)
                except Exception as e:
                    print(e)
                    print("Parallel duplicate insert — not a problem, continuing.")
            return 1
    else:
        print(response)
    return 0


def _process_one(
    openalex_in: Collection,
    openalex_out: Collection,
    s: Similarity,
    work: dict,
    dry_run: bool = False,
) -> int:
    title_work = work.get("title") or ""
    authors: list[str] = [work["author"]] if work.get("author") else []
    if not authors and len(title_work) < 30:
        return 0
    if title_work:
        responses = s.search_work(
            title=title_work,
            source="",
            year="0",
            authors=authors,
            volume="",
            issue="",
            page_start="",
            page_end="",
            use_es_thold=True,
            es_thold=0,
            hits=20,
        )
        if responses:
            for resp in responses:
                if _check_work(
                    openalex_in, openalex_out, title_work, authors, resp, dry_run, work.get("year")
                ):
                    return 1
    return 0


def load_minciencias_candidates(
    client: MongoClient, minciencias_db: str = MINCIENCIAS_DB, limit: int | None = None
) -> tuple[str, list[dict]]:
    """Return ``(collection_name, [{"title", "author"}, ...])`` for the similarity search."""
    coll_name = latest_minciencias_collection(client[minciencias_db], filtered=False)
    pipeline = list(_PIPELINE)
    if limit:
        pipeline.append({"$limit": limit})
    data = list(client[minciencias_db][coll_name].aggregate(pipeline, allowDiskUse=True))
    return coll_name, data


def colombia_cut_minciencias(
    db_in: str,
    db_out: str,
    es_index: str,
    jobs: int = 20,
    backend: str = "threading",
    client: MongoClient | None = None,
    es_uri: str = "http://localhost:9200",
    es_auth: tuple[str, str] = ("elastic", "colav"),
    minciencias_db: str = MINCIENCIAS_DB,
    limit: int | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Insert Minciencias products into the Colombia cut via ES similarity.

    ``limit`` and ``dry_run`` (no inserts) are meant for sampling the match rate.
    """
    c: MongoClient = client if client is not None else MongoClient()
    t0 = time.time()
    coll_name, data = load_minciencias_candidates(c, minciencias_db, limit)
    print(f"INFO: Minciencias candidates from {minciencias_db}.{coll_name} = {len(data)}")

    openalex_in = c[db_in]
    openalex_out = c[db_out]
    s = Similarity(es_index, es_uri=es_uri, es_auth=es_auth)
    # mohan's client pools ~10 connections; with ``jobs`` threads the pool runs
    # dry (EmptyPoolError), so give every thread its own connection.
    from elasticsearch import Elasticsearch

    s.es = Elasticsearch(
        es_uri,
        basic_auth=es_auth,
        request_timeout=120,
        retry_on_timeout=True,
        max_retries=5,
        connections_per_node=jobs,
    )

    results = Parallel(n_jobs=jobs, backend="threading", verbose=10, batch_size=4)(
        delayed(_process_one)(openalex_in, openalex_out, s, work, dry_run) for work in data
    )
    stats = {
        "collection": f"{minciencias_db}.{coll_name}",
        "candidates": len(data),
        "matched": int(sum(results)),
        "dry_run": dry_run,
        "elapsed_seconds": round(time.time() - t0, 1),
    }
    print(f"INFO: Minciencias similarity cut {stats}")
    return stats
