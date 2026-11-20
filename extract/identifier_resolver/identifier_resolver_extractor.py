"""Resolve the queued DOIs and ORCID iDs against their public registries."""

from __future__ import annotations

import argparse
import json
import os
import re
import time
from collections import Counter
from datetime import datetime
from typing import Any
from urllib.parse import quote

import requests
from pymongo import UpdateOne
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from extract.base_extractor import UTC, BaseExtractor

VERSION = "0.1.0"
DOI_RA_URL = "https://doi.org/doiRA/{}"
DOI_URL = "https://doi.org/{}"
CROSSREF_URL = "https://api.crossref.org/works/{}"
DATACITE_URL = "https://api.datacite.org/dois/{}"
ORCID_URL = "https://pub.orcid.org/v3.0/{}/person"
CSL_JSON = "application/vnd.citationstyles.csl+json"

KINDS = ("doi", "orcid")
# Statuses the capture takes from the queue. "ok" and "not_found" are final.
QUEUE_STATUSES = ("pending", "error")
MAX_AUTHORS = 50
MAX_CONSECUTIVE_ERRORS = 20
_DOI = re.compile(r"10\.\d{4,9}/\S+")
_ORCID = re.compile(r"\d{4}-\d{4}-\d{4}-\d{3}[\dX]")


def _session(mailto: str = "") -> requests.Session:
    session = requests.Session()
    contact = f"; mailto:{mailto}" if mailto else ""
    session.headers["User-Agent"] = (
        f"ImpactU-Identifier-Resolver/{VERSION} (https://github.com/colav/impactu_airflow{contact})"
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


def _strings(value: Any) -> list[str]:
    """Return the non-empty strings of a string or list of strings."""
    values = value if isinstance(value, list) else [value]
    return [item.strip() for item in values if isinstance(item, str) and item.strip()]


def _year(value: Any) -> int | None:
    try:
        return int(value["date-parts"][0][0])
    except (KeyError, IndexError, TypeError, ValueError):
        return None


def parse_crossref(message: dict[str, Any]) -> dict[str, Any]:
    """
    Keep the Crossref fields that tell what a DOI identifies.

    Parameters
    ----------
    message : dict
        ``message`` of a Crossref ``/works/{doi}`` response.

    Returns
    -------
    dict
        ``type``, ``titles`` (title, original title and subtitle, unchanged),
        ``container_titles``, ``issn``, ``isbn``, ``year`` and ``authors``.
    """
    authors = [
        " ".join(_strings([author.get("given"), author.get("family")])) or author.get("name", "")
        for author in message.get("author") or []
    ]
    return {
        "type": message.get("type"),
        "titles": _strings(message.get("title")) + _strings(message.get("original-title")),
        "subtitles": _strings(message.get("subtitle")),
        "container_titles": _strings(message.get("container-title")),
        "issn": _strings(message.get("ISSN")),
        "isbn": _strings(message.get("ISBN")),
        "year": _year(message.get("issued")) or _year(message.get("published")),
        "authors": [name for name in authors if name][:MAX_AUTHORS],
        "author_count": len(authors),
    }


def parse_datacite(attributes: dict[str, Any]) -> dict[str, Any]:
    """Same fields as :func:`parse_crossref` from DataCite ``data.attributes``."""
    types = attributes.get("types") or {}
    creators = _strings([creator.get("name") for creator in attributes.get("creators") or []])
    year = attributes.get("publicationYear")
    return {
        "type": types.get("resourceTypeGeneral"),
        "titles": _strings([title.get("title") for title in attributes.get("titles") or []]),
        "subtitles": [],
        "container_titles": _strings((attributes.get("container") or {}).get("title")),
        "issn": [],
        "isbn": [],
        "year": int(str(year)) if str(year or "").isdigit() else None,
        "authors": creators[:MAX_AUTHORS],
        "author_count": len(creators),
    }


def parse_csl(item: dict[str, Any]) -> dict[str, Any]:
    """Same fields from the CSL JSON doi.org returns for the other agencies."""
    authors = [
        " ".join(_strings([author.get("given"), author.get("family")]))
        or str(author.get("literal") or "")
        for author in item.get("author") or []
    ]
    return {
        "type": item.get("type"),
        "titles": _strings(item.get("title")),
        "subtitles": [],
        "container_titles": _strings(item.get("container-title")),
        "issn": _strings(item.get("ISSN")),
        "isbn": _strings(item.get("ISBN")),
        "year": _year(item.get("issued")),
        "authors": [name for name in authors if name][:MAX_AUTHORS],
        "author_count": len(authors),
    }


def parse_orcid_person(person: dict[str, Any]) -> dict[str, Any]:
    """Public names of an ORCID record (``/person`` response)."""
    name = person.get("name") or {}

    def value(key: str) -> str | None:
        return (name.get(key) or {}).get("value")

    other = (person.get("other-names") or {}).get("other-name") or []
    return {
        "given_names": value("given-names"),
        "family_name": value("family-name"),
        "credit_name": value("credit-name"),
        "other_names": _strings([item.get("content") for item in other]),
    }


class IdentifierResolverExtractor(BaseExtractor):
    """
    Resolve the identifiers queued in ``impactu_identifier_resolver``.

    Kahi queues the identifiers it needs to settle a dispute (a DOI shared by
    several works, an ORCID iD shared by several people) as
    ``{"_id": <identifier>, "status": "pending"}`` in the ``doi`` and ``orcid``
    collections. This extractor asks the public registries what each one
    identifies and stores the answer in the same document, so Kahi never needs
    the network.

    The queue is the checkpoint: a resolved document becomes ``ok`` or
    ``not_found`` and is never requested again; a failed one stays ``error``
    and is retried by the next run. Running it twice requests nothing new.

    Parameters
    ----------
    mongodb_uri : str
        MongoDB connection URI
    db_name : str, optional
        Resolver database (default: "impactu_identifier_resolver")
    client : pymongo.MongoClient, optional
        Existing MongoDB client instance
    request_delay : float, optional
        Seconds to wait between identifiers (default: 0.2)
    mailto : str, optional
        Contact added to the User-Agent for the Crossref polite pool
    session : requests.Session, optional
        HTTP session (tests inject a fake one)
    """

    def __init__(
        self,
        mongodb_uri: str,
        db_name: str = "impactu_identifier_resolver",
        client: Any = None,
        request_delay: float = 0.2,
        mailto: str = "",
        session: requests.Session | None = None,
    ):
        super().__init__(mongodb_uri, db_name, "doi", client=client)
        self.collections = {kind: self.db[kind] for kind in KINDS}
        self.runs = self.db["runs"]
        self.request_delay = request_delay
        self.session = session or _session(mailto)
        self.create_indexes()

    def create_indexes(self) -> None:
        """Index the queue status; ``_id`` (the identifier) is the natural key."""
        for collection in self.collections.values():
            collection.create_index([("status", 1)], background=True)
        self.runs.create_index([("started_at", 1)], background=True)

    def _get(self, url: str, accept: str = "application/json") -> requests.Response:
        return self.session.get(url, headers={"Accept": accept}, timeout=(10, 60))

    def resolve_doi(self, doi: str) -> dict[str, Any]:
        """
        Ask Crossref, then the DOI registration agency, what ``doi`` identifies.

        Returns
        -------
        dict
            ``status`` ("ok" or "not_found"), ``agency``, ``source`` and, when
            found, the metadata fields of :func:`parse_crossref`.
        """
        if not _DOI.fullmatch(doi):
            return {"status": "not_found", "agency": None, "detail": "Invalid DOI"}
        path = quote(doi, safe="/")
        response = self._get(CROSSREF_URL.format(path))
        if response.status_code == 200:
            metadata = parse_crossref(response.json()["message"])
            return {"status": "ok", "agency": "Crossref", "source": "crossref", **metadata}
        if response.status_code != 404:
            response.raise_for_status()

        response = self._get(DOI_RA_URL.format(path))
        response.raise_for_status()
        answer = (response.json() or [{}])[0]
        agency = answer.get("RA")
        if not agency:
            return {"status": "not_found", "agency": None, "detail": answer.get("status")}
        if agency == "DataCite":
            response = self._get(DATACITE_URL.format(path))
            if response.status_code == 200:
                metadata = parse_datacite(response.json()["data"]["attributes"])
                return {"status": "ok", "agency": agency, "source": "datacite", **metadata}
            if response.status_code != 404:
                response.raise_for_status()
        else:
            # doi.org redirects to the agency's own server, which may not answer at all.
            # The DOI is registered either way, so that is not a reason to ask again.
            try:
                response = self._get(DOI_URL.format(path), accept=CSL_JSON)
                item = response.json() if response.status_code == 200 else None
            except (requests.RequestException, ValueError) as error:
                unavailable = f"metadata unavailable: {type(error).__name__}"
                return {**self._registered(agency), "detail": unavailable}
            if isinstance(item, dict):
                metadata = parse_csl(item)
                return {"status": "ok", "agency": agency, "source": "doi.org", **metadata}
        return self._registered(agency)

    @staticmethod
    def _registered(agency: str) -> dict[str, Any]:
        """A DOI its agency registered but publishes no metadata for."""
        return {"status": "ok", "agency": agency, "source": None, "type": None, "titles": []}

    def resolve_orcid(self, orcid: str) -> dict[str, Any]:
        """Return the public names of an ORCID iD."""
        if not _ORCID.fullmatch(orcid):
            return {"status": "not_found", "detail": "Invalid ORCID iD"}
        response = self._get(ORCID_URL.format(orcid))
        if response.status_code in (404, 409):
            return {"status": "not_found", "detail": f"HTTP {response.status_code}"}
        response.raise_for_status()
        return {"status": "ok", **parse_orcid_person(response.json())}

    def run(
        self,
        kinds: tuple[str, ...] | list[str] = KINDS,
        limit: int = 0,
        batch_size: int = 50,
        refresh_not_found: bool = False,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """
        Resolve every queued identifier.

        Parameters
        ----------
        kinds : sequence of str, optional
            Queues to process: "doi", "orcid" (default: both)
        limit : int, optional
            Maximum identifiers per kind; 0 means no limit (default: 0)
        batch_size : int, optional
            Identifiers written per ``bulk_write`` (default: 50)
        refresh_not_found : bool, optional
            Also ask again for the identifiers that did not exist (default: False)

        Returns
        -------
        dict
            Per kind: identifiers requested and the count of each final status.
        """
        unknown = sorted(set(kinds) - set(KINDS))
        if unknown:
            raise ValueError(f"Unknown identifier kinds: {unknown}")
        statuses = list(QUEUE_STATUSES) + (["not_found"] if refresh_not_found else [])
        started = datetime.now(UTC)
        run_id = started.strftime("%Y%m%d_%H%M%S")
        stats: dict[str, Any] = {}
        failure: str | None = None
        try:
            for kind in kinds:
                stats[kind] = self._resolve_queue(kind, statuses, int(limit), int(batch_size))
        except Exception as error:
            failure = f"{type(error).__name__}: {error}"
            raise
        finally:
            summary = {
                "started_at": started,
                "finished_at": datetime.now(UTC),
                "status": "FAILED" if failure else "COMPLETED",
                "error": failure,
                "resolver_version": VERSION,
                "kinds": list(kinds),
                "limit": int(limit),
                "refresh_not_found": bool(refresh_not_found),
                "stats": stats,
            }
            self.runs.update_one({"_id": run_id}, {"$set": summary}, upsert=True)
        self.logger.info(f"Identifier resolver finished: {stats}")
        return stats

    def _resolve_queue(
        self, kind: str, statuses: list[str], limit: int, batch_size: int
    ) -> dict[str, int]:
        collection = self.collections[kind]
        resolve = self.resolve_doi if kind == "doi" else self.resolve_orcid
        cursor = collection.find({"status": {"$in": statuses}}, {"_id": 1, "attempts": 1})
        queue = list(cursor.sort("_id", 1).limit(limit) if limit else cursor.sort("_id", 1))
        counts: Counter[str] = Counter()
        operations: list[UpdateOne] = []
        consecutive_errors = 0
        self.logger.info(f"{kind}: {len(queue)} identifiers queued")
        for position, queued in enumerate(queue, start=1):
            identifier = str(queued["_id"])
            try:
                result = resolve(identifier)
                consecutive_errors = 0
            except (requests.RequestException, ValueError, KeyError) as error:
                result = {"status": "error", "error": f"{type(error).__name__}: {error}"[:500]}
                consecutive_errors += 1
            counts[result["status"]] += 1
            result.update(
                retrieved_at=datetime.now(UTC),
                resolver_version=VERSION,
                attempts=int(queued.get("attempts") or 0) + 1,
            )
            update: dict[str, Any] = {"$set": result}
            if result["status"] != "error":
                update["$unset"] = {"error": ""}
            operations.append(UpdateOne({"_id": queued["_id"]}, update, upsert=True))
            abort = consecutive_errors >= MAX_CONSECUTIVE_ERRORS
            if len(operations) >= batch_size or position == len(queue) or abort:
                collection.bulk_write(operations, ordered=False)
                operations = []
                self.logger.info(f"{kind}: {position}/{len(queue)} {dict(counts)}")
            if abort:
                raise RuntimeError(
                    f"{kind}: {consecutive_errors} consecutive errors, last: {result['error']}"
                )
            if self.request_delay and position < len(queue):
                time.sleep(self.request_delay)
        return {"requested": len(queue), **dict(counts)}


def main() -> int:
    """Run the resolver from the command line (the DAG is the scheduled entry point)."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mongodb-uri", default=os.environ.get("MONGODB_URI", "localhost:27017"))
    parser.add_argument("--database", default="impactu_identifier_resolver")
    parser.add_argument("--kinds", nargs="+", choices=KINDS, default=list(KINDS))
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--request-delay", type=float, default=0.2)
    parser.add_argument("--mailto", default=os.environ.get("CROSSREF_MAILTO", ""))
    parser.add_argument("--refresh-not-found", action="store_true")
    args = parser.parse_args()
    extractor = IdentifierResolverExtractor(
        args.mongodb_uri, args.database, request_delay=args.request_delay, mailto=args.mailto
    )
    try:
        stats = extractor.run(
            kinds=args.kinds, limit=args.limit, refresh_not_found=args.refresh_not_found
        )
    finally:
        extractor.close()
    print(json.dumps(stats, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    raise SystemExit(main())
