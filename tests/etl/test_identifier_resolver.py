from typing import Any, cast
from unittest.mock import MagicMock

import mongomock
import pytest
import requests
from mongomock.collection import BulkOperationBuilder

from extract.identifier_resolver.identifier_resolver_extractor import (
    IdentifierResolverExtractor,
    parse_crossref,
    parse_orcid_person,
)

# Workaround for mongomock bug with pymongo 4.x bulk_write
# https://github.com/mongomock/mongomock/issues/812
_builder_cls = cast(Any, BulkOperationBuilder)
if hasattr(_builder_cls, "add_update"):
    original_add_update = _builder_cls.add_update

    def patched_add_update(self, filter, doc, upsert=False, multi=False, **kwargs):
        kwargs.pop("sort", None)
        return original_add_update(self, filter, doc, upsert, multi, **kwargs)

    _builder_cls.add_update = patched_add_update

JOURNAL = {
    "type": "journal",
    "title": ["Saber, Ciencia y Libertad"],
    "ISSN": ["1794-7154", "2382-3240"],
}
ARTICLE = {
    "type": "journal-article",
    "title": ["Medicina, corazón y música de acordeón"],
    "original-title": ["Medicine, the heart and accordion music"],
    "container-title": ["Revista Colombiana de Cardiología"],
    "issued": {"date-parts": [[2014, 5]]},
    "author": [{"given": "Ana", "family": "Pérez"}, {"name": "Grupo Vallenato"}],
}


class FakeSession:
    """Answers by URL from a dict of ``url -> (status, json)``; records every request."""

    def __init__(self, answers: dict[str, tuple[int, Any]], failing: tuple[str, ...] = ()):
        self.answers = answers
        self.failing = failing
        self.requests: list[str] = []

    def get(self, url, headers=None, timeout=None):
        self.requests.append(url)
        if any(fragment in url for fragment in self.failing):
            raise requests.ConnectionError("service down")
        status, payload = self.answers.get(url, (404, None))
        response = MagicMock()
        response.status_code = status
        response.json.return_value = payload
        if status >= 400:
            response.raise_for_status.side_effect = requests.HTTPError(f"HTTP {status}")
        return response


def crossref(doi: str, message: dict[str, Any]) -> dict[str, tuple[int, Any]]:
    return {f"https://api.crossref.org/works/{doi}": (200, {"message": message})}


def agency(doi: str, name: str | None) -> dict[str, tuple[int, Any]]:
    answer = {"DOI": doi, "RA": name} if name else {"DOI": doi, "status": "DOI does not exist"}
    return {f"https://doi.org/doiRA/{doi}": (200, [answer])}


def build(answers: dict[str, tuple[int, Any]], failing: tuple[str, ...] = ()):
    client: Any = mongomock.MongoClient()
    session = cast(Any, FakeSession(answers, failing))
    extractor = IdentifierResolverExtractor(
        "mongodb://localhost", client=client, request_delay=0, session=session
    )
    return extractor, client["impactu_identifier_resolver"], session


def queue(collection, *identifiers: str) -> None:
    for identifier in identifiers:
        collection.update_one(
            {"_id": identifier}, {"$setOnInsert": {"status": "pending"}}, upsert=True
        )


def test_parse_crossref_keeps_every_title_and_author():
    parsed = parse_crossref(ARTICLE)
    assert parsed["type"] == "journal-article"
    assert parsed["titles"] == [
        "Medicina, corazón y música de acordeón",
        "Medicine, the heart and accordion music",
    ]
    assert parsed["year"] == 2014
    assert parsed["authors"] == ["Ana Pérez", "Grupo Vallenato"]
    assert parsed["author_count"] == 2


def test_parse_orcid_person_reads_public_names():
    person = {
        "name": {
            "given-names": {"value": "Ryan"},
            "family-name": {"value": "Atkin"},
            "credit-name": None,
        },
        "other-names": {"other-name": [{"content": "R. J. Atkin"}]},
    }
    assert parse_orcid_person(person) == {
        "given_names": "Ryan",
        "family_name": "Atkin",
        "credit_name": None,
        "other_names": ["R. J. Atkin"],
    }


def test_run_resolves_the_queue_and_requests_nothing_the_second_time():
    answers = {
        **crossref("10.18041/2382-3240", JOURNAL),
        **agency("10.4067/s0718", None),
        **agency("10.5281/zenodo.1", "DataCite"),
        "https://api.datacite.org/dois/10.5281/zenodo.1": (
            200,
            {
                "data": {
                    "attributes": {
                        "types": {"resourceTypeGeneral": "Dataset"},
                        "titles": [{"title": "Datos de prueba"}],
                        "creators": [{"name": "Pérez, Ana"}],
                        "publicationYear": 2020,
                    }
                }
            },
        ),
    }
    extractor, db, session = build(answers)
    queue(db.doi, "10.18041/2382-3240", "10.4067/s0718", "10.5281/zenodo.1")

    stats = extractor.run(kinds=["doi"])

    assert stats == {"doi": {"requested": 3, "ok": 2, "not_found": 1}}
    journal = db.doi.find_one({"_id": "10.18041/2382-3240"})
    assert journal["status"] == "ok" and journal["type"] == "journal"
    assert journal["agency"] == "Crossref" and journal["issn"] == ["1794-7154", "2382-3240"]
    truncated = db.doi.find_one({"_id": "10.4067/s0718"})
    assert truncated["status"] == "not_found" and truncated["detail"] == "DOI does not exist"
    dataset = db.doi.find_one({"_id": "10.5281/zenodo.1"})
    assert dataset["source"] == "datacite" and dataset["type"] == "Dataset"
    assert dataset["titles"] == ["Datos de prueba"] and dataset["year"] == 2020
    assert db.runs.find_one()["status"] == "COMPLETED"

    before = len(session.requests)
    assert extractor.run(kinds=["doi"]) == {"doi": {"requested": 0}}
    assert len(session.requests) == before


def test_other_agencies_use_doi_org_content_negotiation():
    answers = {
        **agency("10.3233/abc", "mEDRA"),
        "https://doi.org/10.3233/abc": (200, {"type": "article-journal", "title": "Un título"}),
    }
    extractor, db, _ = build(answers)
    queue(db.doi, "10.3233/abc")

    extractor.run(kinds=["doi"])

    resolved = db.doi.find_one({"_id": "10.3233/abc"})
    assert resolved["agency"] == "mEDRA" and resolved["source"] == "doi.org"
    assert resolved["type"] == "article-journal" and resolved["titles"] == ["Un título"]


def test_failed_requests_stay_in_the_queue_and_are_retried():
    answers = crossref("10.1234/ok", JOURNAL)
    extractor, db, _ = build(answers, failing=("10.1234/down",))
    queue(db.doi, "10.1234/down", "10.1234/ok")

    assert extractor.run(kinds=["doi"]) == {"doi": {"requested": 2, "error": 1, "ok": 1}}
    failed = db.doi.find_one({"_id": "10.1234/down"})
    assert failed["status"] == "error" and "service down" in failed["error"]

    recovered, db2, _ = build({**answers, **crossref("10.1234/down", ARTICLE)})
    db2.doi.insert_one(failed)
    assert recovered.run(kinds=["doi"]) == {"doi": {"requested": 1, "ok": 1}}
    again = db2.doi.find_one({"_id": "10.1234/down"})
    assert again["status"] == "ok" and again["attempts"] == 2 and "error" not in again


def test_invalid_identifiers_are_closed_without_requests():
    extractor, db, session = build({})
    queue(db.doi, "no-es-un-doi")
    queue(db.orcid, "1234")

    stats = extractor.run()

    assert stats == {
        "doi": {"requested": 1, "not_found": 1},
        "orcid": {"requested": 1, "not_found": 1},
    }
    assert session.requests == []
    assert db.doi.find_one()["detail"] == "Invalid DOI"


def test_orcid_names_are_stored():
    person = {"name": {"given-names": {"value": "Ryan"}, "family-name": {"value": "Atkin"}}}
    answers = {"https://pub.orcid.org/v3.0/0000-0002-1972-1006/person": (200, person)}
    extractor, db, _ = build(answers)
    queue(db.orcid, "0000-0002-1972-1006", "0000-0000-0000-0000")

    stats = extractor.run(kinds=["orcid"])

    assert stats == {"orcid": {"requested": 2, "not_found": 1, "ok": 1}}
    resolved = db.orcid.find_one({"_id": "0000-0002-1972-1006"})
    assert resolved["given_names"] == "Ryan" and resolved["family_name"] == "Atkin"


def test_refresh_not_found_asks_again():
    extractor, db, session = build(crossref("10.1234/late", JOURNAL))
    db.doi.insert_one({"_id": "10.1234/late", "status": "not_found", "attempts": 1})

    assert extractor.run(kinds=["doi"]) == {"doi": {"requested": 0}}
    assert extractor.run(kinds=["doi"], refresh_not_found=True) == {
        "doi": {"requested": 1, "ok": 1}
    }
    assert db.doi.find_one()["status"] == "ok"


def test_consecutive_errors_abort_the_run(monkeypatch):
    module = "extract.identifier_resolver.identifier_resolver_extractor"
    monkeypatch.setattr(f"{module}.MAX_CONSECUTIVE_ERRORS", 3)
    extractor, db, _ = build({}, failing=("crossref",))
    queue(db.doi, *[f"10.1234/x{i}" for i in range(6)])

    with pytest.raises(RuntimeError, match="3 consecutive errors"):
        extractor.run(kinds=["doi"])

    assert db.doi.count_documents({"status": "error"}) == 3
    assert db.doi.count_documents({"status": "pending"}) == 3
    assert db.runs.find_one()["status"] == "FAILED"


def test_unknown_kind_is_rejected():
    extractor, _, _ = build({})
    with pytest.raises(ValueError, match="Unknown identifier kinds"):
        extractor.run(kinds=["isbn"])
