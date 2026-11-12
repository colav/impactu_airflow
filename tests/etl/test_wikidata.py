from typing import Any, cast
from unittest.mock import MagicMock, patch

import mongomock
import pytest
from mongomock.collection import BulkOperationBuilder

from extract.wikidata.wikidata_extractor import (
    ENTITIES_CHECKPOINT,
    WikidataExtractor,
    entity_ids_from_ror,
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


def ror(ror_id: str, *qids: str) -> dict[str, Any]:
    return {
        "id": f"https://ror.org/{ror_id}",
        "external_ids": [
            {"type": "grid", "all": ["grid.1"], "preferred": "grid.1"},
            {"type": "wikidata", "all": list(qids), "preferred": None},
        ],
    }


def entity(qid: str, label: str = "Universidad") -> dict[str, Any]:
    return {
        "pageid": 1,
        "ns": 0,
        "title": qid,
        "lastrevid": 10,
        "modified": "2026-11-01T00:00:00Z",
        "type": "item",
        "id": qid,
        "labels": {"es": {"language": "es", "value": label}},
        "descriptions": {},
        "aliases": {},
        "claims": {"P31": [{"mainsnak": {"datavalue": {"value": {"id": "Q3918"}}}}]},
        "sitelinks": {},
    }


class FakeSession:
    """Answers wbgetentities from a dict of entities; records every request."""

    def __init__(self, entities: dict[str, Any], redirects: dict[str, str] | None = None):
        self.entities = entities
        self.redirects = redirects or {}
        self.requests: list[list[str]] = []

    def get(self, url, params=None, timeout=None):
        ids = params["ids"].split("|")
        self.requests.append(ids)
        answer: dict[str, Any] = {}
        for qid in ids:
            target = self.redirects.get(qid, qid)
            answer[qid] = self.entities.get(target, {"id": qid, "missing": ""})
        response = MagicMock()
        response.json.return_value = {"entities": answer, "success": 1}
        response.__enter__.return_value = response
        response.__exit__.return_value = False
        return response


@pytest.fixture
def mock_mongo():
    client: Any = mongomock.MongoClient()
    client["ror"]["ror_stage"].insert_many(
        [ror("aaa", "Q3"), ror("bbb", "Q1", "q2"), ror("ccc", "Q3"), {"id": "no-ids"}]
    )
    return client


def make_extractor(mock_mongo, session) -> WikidataExtractor:
    return WikidataExtractor(
        mongodb_uri="mongodb://localhost:27017",
        client=mock_mongo,
        request_delay=0,
        session=session,
    )


def test_extractor_initialization(mock_mongo):
    extractor = make_extractor(mock_mongo, FakeSession({}))
    assert extractor.db_name == "wikidata"
    assert extractor.collection_name == "data"
    assert "id_1" in extractor.collection.index_information()


def test_existing_id_index_is_kept(mock_mongo):
    collection = mock_mongo["wikidata"]["data"]
    collection.create_index("id")
    extractor = make_extractor(mock_mongo, FakeSession({}))
    assert extractor.collection.index_information()["id_1"].get("unique") is not True


def test_entity_ids_from_ror_are_unique_and_sorted():
    documents = [ror("a", "Q10"), ror("b", "Q2", "Q10"), ror("c", " q3 "), ror("d", "bad")]
    assert entity_ids_from_ror(documents) == ["Q2", "Q3", "Q10"]


def test_entity_ids_require_ror_data(mock_mongo):
    mock_mongo["ror"]["ror_stage"].delete_many({})
    extractor = make_extractor(mock_mongo, FakeSession({}))
    with pytest.raises(RuntimeError):
        extractor.run()


def test_run_inserts_documents(mock_mongo):
    session = FakeSession({"Q1": entity("Q1"), "Q2": entity("Q2"), "Q3": entity("Q3")})
    extractor = make_extractor(mock_mongo, session)
    stats = extractor.run(batch_size=2)
    assert stats == {
        "total_ids": 3,
        "requested": 3,
        "stored": 3,
        "missing": 0,
        "redirected": 0,
        "requests": 2,
    }
    assert session.requests == [["Q1", "Q2"], ["Q3"]]
    stored = extractor.collection.find_one({"id": "Q2"}, {"_id": 0})
    assert stored == entity("Q2")


def test_missing_and_redirected_entities(mock_mongo):
    session = FakeSession({"Q1": entity("Q1"), "Q9": entity("Q9")}, redirects={"Q3": "Q9"})
    extractor = make_extractor(mock_mongo, session)
    stats = extractor.run()
    assert stats["stored"] == 2
    assert stats["missing"] == 1
    assert stats["redirected"] == 1
    assert extractor.collection.count_documents({}) == 2
    assert extractor.collection.find_one({"id": "Q9"}) is not None
    assert extractor.collection.find_one({"id": "Q3"}) is None


def test_checkpoint_is_saved_per_batch_and_cleared(mock_mongo):
    entities = {"Q1": entity("Q1"), "Q2": entity("Q2"), "Q3": entity("Q3")}
    extractor = make_extractor(mock_mongo, FakeSession(entities))
    saved: list[Any] = []
    original = extractor.save_checkpoint

    def spy(source_id, value):
        saved.append(value)
        original(source_id, value)

    with patch.object(extractor, "save_checkpoint", side_effect=spy):
        extractor.run(batch_size=2)
    assert saved == ["Q2", "Q3", None]
    assert extractor.get_checkpoint(ENTITIES_CHECKPOINT) is None


def test_checkpoint_resume(mock_mongo):
    entities = {"Q1": entity("Q1"), "Q2": entity("Q2"), "Q3": entity("Q3")}
    session = FakeSession(entities)
    extractor = make_extractor(mock_mongo, session)
    extractor.save_checkpoint(ENTITIES_CHECKPOINT, "Q2")
    stats = extractor.run()
    assert session.requests == [["Q3"]]
    assert stats["requested"] == 1
    assert stats["total_ids"] == 3


def test_force_ignores_checkpoint(mock_mongo):
    entities = {"Q1": entity("Q1"), "Q2": entity("Q2"), "Q3": entity("Q3")}
    session = FakeSession(entities)
    extractor = make_extractor(mock_mongo, session)
    extractor.save_checkpoint(ENTITIES_CHECKPOINT, "Q2")
    extractor.run(force=True)
    assert session.requests == [["Q1", "Q2", "Q3"]]


def test_run_idempotent(mock_mongo):
    entities = {"Q1": entity("Q1"), "Q2": entity("Q2"), "Q3": entity("Q3")}
    extractor = make_extractor(mock_mongo, FakeSession(entities))
    extractor.run()
    extractor.run()
    assert extractor.collection.count_documents({}) == 3


def test_run_updates_existing_dump_document(mock_mongo):
    old = entity("Q1", label="Nombre viejo")
    old["lastrevid"] = 1
    mock_mongo["wikidata"]["data"].insert_one(old)
    extractor = make_extractor(mock_mongo, FakeSession({"Q1": entity("Q1", "Nombre nuevo")}))
    extractor.run()
    stored = extractor.collection.find_one({"id": "Q1"})
    assert stored["labels"]["es"]["value"] == "Nombre nuevo"
    assert stored["lastrevid"] == 10
    assert extractor.collection.count_documents({"id": "Q1"}) == 1


def test_fetch_entities_rejects_large_batches(mock_mongo):
    extractor = make_extractor(mock_mongo, FakeSession({}))
    with pytest.raises(ValueError):
        extractor.fetch_entities([f"Q{i}" for i in range(51)])


def test_fetch_entities_raises_api_errors(mock_mongo):
    session = MagicMock()
    response = MagicMock()
    response.json.return_value = {"error": {"code": "maxlag", "info": "lagged"}}
    response.__enter__.return_value = response
    response.__exit__.return_value = False
    session.get.return_value = response
    extractor = make_extractor(mock_mongo, session)
    with pytest.raises(RuntimeError):
        extractor.fetch_entities(["Q1"])
