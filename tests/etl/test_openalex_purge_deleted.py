"""Tests for purge_deleted_works."""

from __future__ import annotations

import gzip
from typing import Any, cast

import mongomock
from mongomock.collection import BulkOperationBuilder

from extract.openalex.purge_deleted_works import purge

# Mongomock / pymongo 4.x compatibility patch (pymongo passes ``sort`` to add_replace)
_cls = cast(Any, BulkOperationBuilder)
if hasattr(_cls, "add_replace"):
    _orig_add_replace = _cls.add_replace

    def _patched_add_replace(self, f, d, upsert=False, **kw):
        kw.pop("sort", None)
        return _orig_add_replace(self, f, d, upsert, **kw)

    _cls.add_replace = _patched_add_replace


def _setup(tmp_path):
    col: Any = mongomock.MongoClient()["openalex"]["works"]
    col.insert_many(
        [
            {"id": "https://openalex.org/W1", "updated_date": "2026-03-01T00:00:00"},
            {"id": "https://openalex.org/W2", "updated_date": "2026-09-01T00:00:00"},
            {"id": "https://openalex.org/W3", "updated_date": "2026-03-01T00:00:00"},
        ]
    )
    csv_path = tmp_path / "deleted_ids.csv.gz"
    with gzip.open(csv_path, "wt") as fh:
        fh.write("work_id,deleted_date\n")
        fh.write("https://openalex.org/W1,2026-08-14\n")  # stale -> removed
        fh.write("https://openalex.org/W2,2026-08-14\n")  # re-published later -> kept
        fh.write("https://openalex.org/W9,2026-08-14\n")  # not in Mongo
    return col, str(csv_path)


def test_dry_run_counts_without_deleting(tmp_path):
    col, csv_path = _setup(tmp_path)
    stats = purge(col, csv_path, apply=False, batch_size=2)
    assert stats == {"ids_read": 3, "matched": 1, "archived": 0, "deleted": 0}
    assert col.count_documents({}) == 3


def test_apply_deletes_only_stale(tmp_path):
    col, csv_path = _setup(tmp_path)
    stats = purge(col, csv_path, apply=True, batch_size=2)
    assert stats["deleted"] == 1
    assert {d["id"] for d in col.find()} == {
        "https://openalex.org/W2",
        "https://openalex.org/W3",
    }


def test_apply_with_archive_is_restorable_and_idempotent(tmp_path):
    col, csv_path = _setup(tmp_path)
    archive = col.database["works_purged"]
    original = col.find_one({"id": "https://openalex.org/W1"})

    stats = purge(col, csv_path, apply=True, batch_size=2, archive=archive)
    assert stats["archived"] == 1 and stats["deleted"] == 1
    assert archive.find_one({"id": "https://openalex.org/W1"}) == original

    # Re-run: nothing left to match, archive unchanged
    stats = purge(col, csv_path, apply=True, batch_size=2, archive=archive)
    assert stats["deleted"] == 0
    assert archive.count_documents({}) == 1

    # Restore
    col.insert_many(list(archive.find()))
    assert col.count_documents({}) == 3
