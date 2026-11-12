"""Refresh the Wikidata entities referenced by ROR organizations."""

from __future__ import annotations

import logging
import re
import time
from collections.abc import Iterable
from typing import Any

import requests
from pymongo import UpdateOne
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from extract.base_extractor import BaseExtractor

log = logging.getLogger(__name__)

API_URL = "https://www.wikidata.org/w/api.php"
ENTITIES_CHECKPOINT = "wikidata_entities"
MAX_IDS_PER_REQUEST = 50
# Properties that identify an entity in the dump and in wbgetentities alike.
ENTITY_FIELDS = (
    "pageid",
    "ns",
    "title",
    "lastrevid",
    "modified",
    "type",
    "id",
    "labels",
    "descriptions",
    "aliases",
    "claims",
    "sitelinks",
)
_QID = re.compile(r"Q\d+")


def _session() -> requests.Session:
    session = requests.Session()
    session.headers["User-Agent"] = (
        "ImpactU-Wikidata-Capture/1.0 (https://github.com/colav/impactu_airflow)"
    )
    retries = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        respect_retry_after_header=True,
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    return session


def _qid_sort_key(qid: str) -> int:
    return int(qid[1:])


def entity_ids_from_ror(documents: Iterable[dict[str, Any]]) -> list[str]:
    """
    Collect the Wikidata ids declared in ROR v2 ``external_ids``.

    Parameters
    ----------
    documents : Iterable[dict]
        ROR organization records (only ``external_ids`` is read).

    Returns
    -------
    list[str]
        Unique QIDs sorted by their numeric part.
    """
    found: set[str] = set()
    for document in documents:
        for external in document.get("external_ids") or []:
            if external.get("type") != "wikidata":
                continue
            values = list(external.get("all") or [])
            if external.get("preferred"):
                values.append(external["preferred"])
            for value in values:
                match = _QID.fullmatch(str(value).strip().upper())
                if match:
                    found.add(match[0])
    return sorted(found, key=_qid_sort_key)


class WikidataExtractor(BaseExtractor):
    """
    Keep ``wikidata.data`` current for the entities ROR points to.

    The collection was first loaded from a full JSON dump; this extractor
    refreshes only the entities referenced by ROR organizations through
    ``wbgetentities`` (50 ids per request), storing each entity exactly as the
    API returns it, which is the same shape the dump uses.

    Parameters
    ----------
    mongodb_uri : str
        MongoDB connection URI
    db_name : str
        Database that holds the Wikidata entities (default: "wikidata")
    collection_name : str, optional
        Collection of entities (default: "data")
    client : pymongo.MongoClient, optional
        Existing MongoDB client instance
    ror_db_name : str, optional
        Database with the ROR staging collection (default: "ror")
    ror_collection_name : str, optional
        ROR staging collection (default: "ror_stage")
    redirects_collection_name : str, optional
        Collection of ``{"id": requested QID, "redirect": target QID}`` for the
        ROR ids that Wikidata merged into another item (default: "redirects")
    request_delay : float, optional
        Seconds to wait between API requests (default: 0.5)
    session : requests.Session, optional
        HTTP session (tests inject a fake one)
    """

    def __init__(
        self,
        mongodb_uri: str,
        db_name: str = "wikidata",
        collection_name: str = "data",
        client: Any = None,
        ror_db_name: str = "ror",
        ror_collection_name: str = "ror_stage",
        redirects_collection_name: str = "redirects",
        request_delay: float = 0.5,
        session: requests.Session | None = None,
    ):
        super().__init__(mongodb_uri, db_name, collection_name, client=client)
        self.ror_collection = self.client[ror_db_name][ror_collection_name]
        self.redirects = self.db[redirects_collection_name]
        self.request_delay = request_delay
        self.session = session or _session()
        self.create_indexes()

    def create_indexes(self) -> None:
        """Index ``id``; the pre-existing non-unique index of the dump load is kept."""
        self.redirects.create_index([("id", 1)], unique=True, name="id_1", background=True)
        if "id_1" in self.collection.index_information():
            return
        self.collection.create_index([("id", 1)], unique=True, name="id_1", background=True)

    def entity_ids(self) -> list[str]:
        """Return the QIDs referenced by the ROR staging collection."""
        cursor = self.ror_collection.find(
            {"external_ids.type": "wikidata"}, {"external_ids": 1, "_id": 0}
        )
        ids = entity_ids_from_ror(cursor)
        if not ids:
            raise RuntimeError(
                f"No Wikidata ids found in {self.ror_collection.database.name}."
                f"{self.ror_collection.name}"
            )
        return ids

    def fetch_entities(self, ids: list[str]) -> dict[str, Any]:
        """
        Request a batch of entities from ``wbgetentities``.

        Returns
        -------
        dict
            ``entities`` (list of full entity documents), ``missing`` (ids that
            do not exist) and ``redirects`` (mapping requested id -> target id).
        """
        if len(ids) > MAX_IDS_PER_REQUEST:
            raise ValueError(f"wbgetentities accepts at most {MAX_IDS_PER_REQUEST} ids")
        params: dict[str, str] = {
            "action": "wbgetentities",
            "ids": "|".join(ids),
            "format": "json",
            "maxlag": "5",
        }
        with self.session.get(API_URL, params=params, timeout=(10, 60)) as response:
            response.raise_for_status()
            payload = response.json()
        if "error" in payload:
            raise RuntimeError(f"wbgetentities error: {payload['error']}")
        entities: list[dict[str, Any]] = []
        missing: list[str] = []
        redirects: dict[str, str] = {}
        for requested, entity in payload.get("entities", {}).items():
            if "missing" in entity:
                missing.append(requested)
                continue
            if entity.get("id") != requested:
                redirects[requested] = entity["id"]
            entities.append({k: entity[k] for k in ENTITY_FIELDS if k in entity})
        return {"entities": entities, "missing": missing, "redirects": redirects}

    def store_entities(self, entities: list[dict[str, Any]]) -> int:
        """Upsert entities by ``id``."""
        ops = [UpdateOne({"id": e["id"]}, {"$set": e}, upsert=True) for e in entities]
        if ops:
            self.collection.bulk_write(ops, ordered=False)
        return len(ops)

    def store_redirects(self, redirects: dict[str, str]) -> int:
        """Upsert ``{"id": requested, "redirect": target}`` so Kahi can follow merged items."""
        ops = [
            UpdateOne({"id": requested}, {"$set": {"redirect": target}}, upsert=True)
            for requested, target in redirects.items()
        ]
        if ops:
            self.redirects.bulk_write(ops, ordered=False)
        return len(ops)

    def run(
        self, batch_size: int = MAX_IDS_PER_REQUEST, force: bool = False, **kwargs: Any
    ) -> dict[str, Any]:
        """
        Refresh every ROR-referenced entity, resuming after the last stored QID.

        Parameters
        ----------
        batch_size : int, optional
            Ids per request, at most 50 (default: 50)
        force : bool, optional
            Ignore the checkpoint and start from the first QID (default: False)

        Returns
        -------
        dict
            Counts of requested, stored, missing and redirected entities.
        """
        batch_size = max(1, min(int(batch_size), MAX_IDS_PER_REQUEST))
        ids = self.entity_ids()
        checkpoint = None if force else self.get_checkpoint(ENTITIES_CHECKPOINT)
        pending = ids
        if checkpoint:
            last = _qid_sort_key(str(checkpoint))
            pending = [qid for qid in ids if _qid_sort_key(qid) > last]
            self.logger.info(
                f"Resuming after {checkpoint}: {len(pending)} of {len(ids)} ids pending"
            )
        stats = {
            "total_ids": len(ids),
            "requested": 0,
            "stored": 0,
            "missing": 0,
            "redirected": 0,
            "requests": 0,
        }
        for start in range(0, len(pending), batch_size):
            batch = pending[start : start + batch_size]
            result = self.fetch_entities(batch)
            stats["requests"] += 1
            stats["requested"] += len(batch)
            stats["stored"] += self.store_entities(result["entities"])
            stats["missing"] += len(result["missing"])
            stats["redirected"] += self.store_redirects(result["redirects"])
            for requested, target in result["redirects"].items():
                self.logger.warning(f"{requested} redirects to {target}")
            self.save_checkpoint(ENTITIES_CHECKPOINT, batch[-1])
            if stats["requests"] % 100 == 0:
                self.logger.info(
                    f"{stats['requested']}/{len(pending)} ids requested, "
                    f"{stats['stored']} stored, {stats['missing']} missing"
                )
            if self.request_delay and start + batch_size < len(pending):
                time.sleep(self.request_delay)
        self.save_checkpoint(ENTITIES_CHECKPOINT, None)
        self.logger.info(f"Wikidata refresh finished: {stats}")
        return stats
