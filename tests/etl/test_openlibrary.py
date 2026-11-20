import gzip
import hashlib
import json
from pathlib import Path
from typing import Any, cast
from unittest.mock import patch

import mongomock
import pytest
import requests
from airflow.exceptions import AirflowSkipException
from bson.errors import InvalidDocument
from mongomock.collection import BulkOperationBuilder

from dags import openlibrary_capture as capture
from extract.openlibrary.openlibrary_extractor import (
    SNAPSHOT_CHECKPOINT,
    OpenLibraryExtractor,
    download_snapshot_file,
    remove_old_snapshots,
    resolve_latest_snapshot,
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

SNAPSHOT = "2026-10-31"
NEXT_SNAPSHOT = "2026-11-30"
CHECKPOINT_ID = "openlibrary_load_editions"


class FakeResponse:
    def __init__(
        self,
        status_code: int = 200,
        chunks: tuple[bytes, ...] = (),
        error: Exception | None = None,
        headers: dict[str, str] | None = None,
        payload: Any = None,
    ) -> None:
        self.status_code = status_code
        self.chunks = chunks
        self.error = error
        self.headers = headers or {}
        self.payload = payload

    def __enter__(self) -> "FakeResponse":
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(response=cast(Any, self))

    def iter_content(self, chunk_size: int):
        yield from self.chunks
        if self.error:
            raise self.error

    def json(self) -> Any:
        return self.payload


class FakeSession:
    def __init__(self, *responses: FakeResponse, head: FakeResponse | None = None) -> None:
        self.responses = list(responses)
        self.head_response = head
        self.request_headers: list[dict[str, str]] = []

    def head(self, url: str, **kwargs: Any) -> FakeResponse:
        assert self.head_response is not None
        return self.head_response

    def get(self, url: str, **kwargs: Any) -> FakeResponse:
        self.request_headers.append(kwargs.get("headers") or {})
        return self.responses.pop(0)


def _row(key: str, **fields: Any) -> str:
    record = {"key": key, "type": {"key": "/type/edition"}, **fields}
    return f"/type/edition\t{key}\t1\t2026-01-01T00:00:00\t{json.dumps(record)}\n"


def _dump(path: Path, *rows: str) -> str:
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        handle.writelines(rows)
    return str(path)


def _entry(body: bytes) -> dict[str, Any]:
    return {
        "name": "ol_dump_authors_2026-10-31.txt.gz",
        "url": "https://example.test/file",
        "size": len(body),
        "md5": hashlib.md5(body, usedforsecurity=False).hexdigest(),
    }


@pytest.fixture
def mock_mongo():
    client: Any = mongomock.MongoClient()
    return client


@pytest.fixture
def extractor(mock_mongo):
    return OpenLibraryExtractor(
        mongodb_uri="mongodb://localhost:27017",
        db_name="test_db",
        collection_name="editions",
        client=mock_mongo,
        chunk_size=1,
    )


def test_extractor_initialization(extractor):
    assert extractor.db_name == "test_db"
    assert extractor.collection_name == "editions"
    assert extractor.collection.index_information()["key_1"]["unique"] is True


def test_run_inserts_documents(extractor, tmp_path):
    path = _dump(tmp_path / "d.gz", _row("/books/OL1M", isbn_13=["9780107716882"]))

    extractor.run(SNAPSHOT, path)

    doc = extractor.collection.find_one({"key": "/books/OL1M"})
    assert doc["snapshot_date"] == SNAPSHOT
    assert doc["record"]["isbn_13"] == ["9780107716882"]


def test_checkpoint_is_saved(extractor, tmp_path):
    path = _dump(tmp_path / "d.gz", _row("/books/OL1M"), _row("/books/OL2M"))

    extractor.run(SNAPSHOT, path)

    state = extractor.get_checkpoint(CHECKPOINT_ID)
    assert (state["lines"], state["written"], state["errors"]) == (2, 2, 0)
    assert state["done"] is True


def test_checkpoint_resume(extractor, tmp_path):
    path = _dump(tmp_path / "d.gz", _row("/books/OL1M"), _row("/books/OL2M"))
    extractor.save_checkpoint(
        CHECKPOINT_ID,
        {"snapshot_date": SNAPSHOT, "lines": 1, "written": 1, "errors": 0, "done": False},
    )

    extractor.run(SNAPSHOT, path)

    assert [doc["key"] for doc in extractor.collection.find()] == ["/books/OL2M"]
    assert extractor.get_checkpoint(CHECKPOINT_ID)["written"] == 2


def test_checkpoint_of_previous_snapshot_is_ignored(extractor, tmp_path):
    path = _dump(tmp_path / "d.gz", _row("/books/OL1M"))
    extractor.save_checkpoint(
        CHECKPOINT_ID,
        {"snapshot_date": SNAPSHOT, "lines": 99, "written": 99, "errors": 0, "done": True},
    )

    extractor.run(NEXT_SNAPSHOT, path)

    assert extractor.collection.count_documents({"snapshot_date": NEXT_SNAPSHOT}) == 1


def test_run_idempotent(extractor, tmp_path):
    path = _dump(tmp_path / "d.gz", _row("/books/OL1M"), _row("/books/OL2M"))

    extractor.run(SNAPSHOT, path)
    extractor.reset_load_state()
    extractor.run(SNAPSHOT, path)

    assert extractor.collection.count_documents({}) == 2


def test_field_removed_upstream_disappears(extractor, tmp_path):
    first = _dump(tmp_path / "a.gz", _row("/books/OL1M", isbn_13=["9780000000000"], title="T"))
    second = _dump(tmp_path / "b.gz", _row("/books/OL1M", title="T"))

    extractor.run(SNAPSHOT, first)
    extractor.run(NEXT_SNAPSHOT, second)

    assert "isbn_13" not in extractor.collection.find_one({"key": "/books/OL1M"})["record"]


def test_malformed_rows_are_counted_and_skipped(extractor, tmp_path):
    path = _dump(
        tmp_path / "d.gz",
        "too\tfew\tcolumns\n",
        "/type/edition\t/books/OL9M\t1\t2026\t{not json}\n",
        _row("/books/OL1M"),
    )

    state = extractor.run(SNAPSHOT, path)

    assert (state["errors"], state["written"]) == (2, 1)
    assert extractor.collection.count_documents({}) == 1


def test_unstorable_record_does_not_lose_its_batch(mock_mongo, tmp_path):
    batched = OpenLibraryExtractor("", "test_db", "editions", client=mock_mongo, chunk_size=3)
    path = _dump(tmp_path / "d.gz", _row("/books/OL1M"), _row("/books/BAD"), _row("/books/OL2M"))
    real_bulk_write = batched.collection.bulk_write

    def reject_bad(ops: list[Any], ordered: bool = False) -> Any:
        if any(op._filter["key"] == "/books/BAD" for op in ops):
            raise InvalidDocument("cannot encode")
        return real_bulk_write(ops, ordered=ordered)

    with patch.object(batched.collection, "bulk_write", side_effect=reject_bad):
        state = batched.run(SNAPSHOT, path)

    assert (state["errors"], state["written"]) == (1, 2)
    assert batched.collection.count_documents({}) == 2


def test_purge_removes_only_stale_documents(extractor, tmp_path):
    first = _dump(tmp_path / "a.gz", *[_row(f"/books/OL{i}M") for i in range(20)])
    second = _dump(tmp_path / "b.gz", *[_row(f"/books/OL{i}M") for i in range(19)])
    extractor.run(SNAPSHOT, first)
    extractor.run(NEXT_SNAPSHOT, second)

    extractor.check_ready_to_purge(NEXT_SNAPSHOT, 0.001, 0.1)

    assert extractor.purge_stale(NEXT_SNAPSHOT) == 1
    assert extractor.collection.find_one({"key": "/books/OL19M"}) is None
    assert extractor.collection.count_documents({}) == 19


def test_purge_blocked_when_load_is_incomplete(extractor):
    with pytest.raises(RuntimeError, match="not complete"):
        extractor.check_ready_to_purge(SNAPSHOT, 0.001, 0.1)


def test_purge_blocked_by_error_ratio(extractor, tmp_path):
    path = _dump(tmp_path / "d.gz", "bad row\n", _row("/books/OL1M"))
    extractor.run(SNAPSHOT, path)

    with pytest.raises(RuntimeError, match="error ratio"):
        extractor.check_ready_to_purge(SNAPSHOT, 0.001, 0.1)


def test_purge_blocked_when_snapshot_shrinks_too_much(extractor, tmp_path):
    first = _dump(tmp_path / "a.gz", *[_row(f"/books/OL{i}M") for i in range(10)])
    second = _dump(tmp_path / "b.gz", _row("/books/OL0M"))
    extractor.run(SNAPSHOT, first)
    extractor.run(NEXT_SNAPSHOT, second)

    with pytest.raises(RuntimeError, match="would delete 9 of 10"):
        extractor.check_ready_to_purge(NEXT_SNAPSHOT, 0.001, 0.1)


def _resolve_session(files: list[dict[str, Any]]) -> FakeSession:
    location = f"https://archive.org/download/ol_dump_{SNAPSHOT}/ol_dump_{SNAPSHOT}.txt.gz"
    return FakeSession(
        FakeResponse(payload={"result": files}),
        head=FakeResponse(status_code=302, headers={"Location": location}),
    )


def test_resolve_pins_dated_urls_and_checksums():
    name = f"ol_dump_authors_{SNAPSHOT}.txt.gz"
    session = _resolve_session([{"name": name, "size": "10", "md5": "abc"}])

    manifest = resolve_latest_snapshot(["authors"], cast(Any, session))

    assert manifest["snapshot_date"] == SNAPSHOT
    assert manifest["files"]["authors"] == {
        "name": name,
        "url": f"https://archive.org/download/ol_dump_{SNAPSHOT}/{name}",
        "size": 10,
        "md5": "abc",
    }


def test_resolve_fails_when_a_file_is_missing():
    with pytest.raises(ValueError, match="no size and checksum"):
        resolve_latest_snapshot(["authors"], cast(Any, _resolve_session([])))


def test_download_verifies_and_leaves_no_partial(tmp_path):
    body = b"0123456789"
    session = FakeSession(FakeResponse(chunks=(body,)))

    result = download_snapshot_file(_entry(body), tmp_path / "snap", cast(Any, session))

    assert Path(result).read_bytes() == body
    assert not list((tmp_path / "snap").glob("*.part"))


def test_download_resumes_after_interruption(tmp_path):
    body = b"0123456789"
    session = FakeSession(
        FakeResponse(chunks=(body[:4],), error=requests.exceptions.ChunkedEncodingError()),
        FakeResponse(status_code=206, chunks=(body[4:],)),
    )

    result = download_snapshot_file(_entry(body), tmp_path, cast(Any, session), backoff_seconds=0)

    assert Path(result).read_bytes() == body
    assert session.request_headers[1]["Range"] == "bytes=4-"


def test_download_restarts_when_server_ignores_range(tmp_path):
    body = b"0123456789"
    entry = _entry(body)
    (tmp_path / f"{entry['name']}.part").write_bytes(b"XXXX")
    session = FakeSession(FakeResponse(status_code=200, chunks=(body,)))

    result = download_snapshot_file(entry, tmp_path, cast(Any, session))

    assert Path(result).read_bytes() == body


def test_download_skips_verified_file(tmp_path):
    body = b"0123456789"
    entry = _entry(body)
    (tmp_path / entry["name"]).write_bytes(body)
    session = FakeSession()

    download_snapshot_file(entry, tmp_path, cast(Any, session))

    assert session.request_headers == []


def test_download_discards_file_with_bad_checksum(tmp_path):
    body = b"0123456789"
    entry = {**_entry(body), "md5": "wrong"}
    session = FakeSession(FakeResponse(chunks=(body,)))

    with pytest.raises(ValueError, match="checksum mismatch"):
        download_snapshot_file(entry, tmp_path, cast(Any, session))

    assert list(tmp_path.iterdir()) == []


def test_download_does_not_retry_permanent_errors(tmp_path):
    session = FakeSession(FakeResponse(status_code=404))

    with pytest.raises(requests.HTTPError):
        download_snapshot_file(_entry(b"x"), tmp_path, cast(Any, session), backoff_seconds=0)

    assert len(session.request_headers) == 1


def test_download_gives_up_when_stalled(tmp_path):
    responses = [FakeResponse(status_code=503) for _ in range(2)]
    session = FakeSession(*responses)

    with pytest.raises(RuntimeError, match="no progress"):
        download_snapshot_file(
            _entry(b"x"), tmp_path, cast(Any, session), max_stalled_attempts=2, backoff_seconds=0
        )


def test_remove_old_snapshots_only_touches_date_directories(tmp_path):
    for name in ("2026-08-31", "2026-09-30", "2026-10-31", "keep_me"):
        (tmp_path / name).mkdir()

    removed = remove_old_snapshots(tmp_path, keep=2)

    assert removed == [str(tmp_path / "2026-08-31")]
    assert sorted(p.name for p in tmp_path.iterdir()) == ["2026-09-30", "2026-10-31", "keep_me"]


def test_dag_structure():
    assert capture.dag.schedule == "0 3 5 * *"
    assert capture.dag.catchup is False
    assert capture.dag.is_paused_upon_creation is True
    resolve = capture.dag.get_task("resolve_snapshot")
    assert resolve.downstream_task_ids == {"download_file"}
    assert capture.dag.get_task("load_collection").downstream_task_ids == {"purge_and_cleanup"}


@pytest.fixture
def dag_context(mock_mongo):
    params = {
        "mongo_conn_id": "mongodb_default",
        "db_name": "test_db",
        "chunk_size": 10,
        "force": False,
    }
    with patch("dags.openlibrary_capture.MongoHook") as hook_cls:
        hook_cls.return_value.uri = "mongodb://localhost:27017"
        hook_cls.return_value.get_conn.return_value = mock_mongo
        yield {"params": params}


def _set_loaded(mock_mongo: Any, snapshot_date: str) -> None:
    mock_mongo["test_db"]["etl_checkpoints"].insert_one(
        {"_id": SNAPSHOT_CHECKPOINT, "last_value": snapshot_date}
    )


@patch("extract.openlibrary.openlibrary_extractor.resolve_latest_snapshot")
def test_resolve_task_skips_loaded_snapshot(mock_resolve, dag_context, mock_mongo):
    mock_resolve.return_value = {"snapshot_date": SNAPSHOT, "files": {}}
    _set_loaded(mock_mongo, SNAPSHOT)

    with pytest.raises(AirflowSkipException):
        capture.resolve_snapshot(**dag_context)


@patch("extract.openlibrary.openlibrary_extractor.resolve_latest_snapshot")
def test_resolve_task_continues_with_new_snapshot(mock_resolve, dag_context, mock_mongo):
    mock_resolve.return_value = {"snapshot_date": NEXT_SNAPSHOT, "files": {}}
    _set_loaded(mock_mongo, SNAPSHOT)

    assert capture.resolve_snapshot(**dag_context)["snapshot_date"] == NEXT_SNAPSHOT


@patch("extract.openlibrary.openlibrary_extractor.resolve_latest_snapshot")
def test_resolve_task_force_resets_load_progress(mock_resolve, dag_context, mock_mongo):
    mock_resolve.return_value = {"snapshot_date": SNAPSHOT, "files": {}}
    _set_loaded(mock_mongo, SNAPSHOT)
    checkpoints = mock_mongo["test_db"]["etl_checkpoints"]
    checkpoints.insert_one({"_id": CHECKPOINT_ID, "last_value": {"done": True}})
    dag_context["params"]["force"] = True

    capture.resolve_snapshot(**dag_context)

    assert checkpoints.find_one({"_id": CHECKPOINT_ID}) is None
    assert checkpoints.find_one({"_id": SNAPSHOT_CHECKPOINT}) is not None
