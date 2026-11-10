"""Remove from MongoDB the works that OpenAlex lists as deleted.

The snapshot upsert never deletes documents, so works removed upstream stay in
``<db>.works`` forever. OpenAlex publishes them in
``data/jsonl/works/deleted_ids.csv.gz`` (``work_id,deleted_date``).

A document is only removed when its ``updated_date`` is older than the
``deleted_date`` — a work re-published after the deletion is kept.

Dry-run by default (counts only); pass ``--apply`` to delete. With
``--archive-collection`` each document is copied there (same ``_id``) before
it is deleted, so the purge can be undone. Re-running is safe: archived
copies are replaced, already-deleted works no longer match.

Usage::

    python -m extract.openalex.purge_deleted_works \
        --csv /storage/raw_data/openalex/openalex-snapshot/data/jsonl/works/deleted_ids.csv.gz \
        [--mongodb-uri mongodb://localhost:27017/] [--db openalex] \
        [--apply [--archive-collection works_purged_YYYYMMDD]]
"""

from __future__ import annotations

import argparse
import csv
import gzip
import logging
import time
from collections import defaultdict

import pymongo
from pymongo import ReplaceOne

logger = logging.getLogger("openalex.purge_deleted_works")

BATCH_SIZE = 10_000


def purge(
    collection: pymongo.collection.Collection,
    csv_path: str,
    apply: bool = False,
    batch_size: int = BATCH_SIZE,
    archive: pymongo.collection.Collection | None = None,
) -> dict[str, int]:
    """Count (or delete) stale works listed in ``deleted_ids.csv.gz``.

    Parameters
    ----------
    collection : pymongo.collection.Collection
        The ``works`` collection (needs the unique ``id`` index).
    csv_path : str
        Path to ``deleted_ids.csv.gz``.
    apply : bool
        Delete matching documents; otherwise only count them.
    batch_size : int
        IDs per query.
    archive : pymongo.collection.Collection, optional
        When given (and ``apply``), matching documents are copied here before
        being deleted.

    Returns
    -------
    dict
        ``{"ids_read": int, "matched": int, "archived": int, "deleted": int}``
    """
    stats = {"ids_read": 0, "matched": 0, "archived": 0, "deleted": 0}
    pending: dict[str, list[str]] = defaultdict(list)
    n_pending = 0
    t0 = time.time()

    def _flush() -> None:
        nonlocal n_pending
        for deleted_date, ids in pending.items():
            query = {"id": {"$in": ids}, "updated_date": {"$lt": deleted_date}}
            if apply:
                docs = list(collection.find(query))
                if not docs:
                    continue
                stats["matched"] += len(docs)
                if archive is not None:
                    archive.bulk_write(
                        [ReplaceOne({"_id": d["_id"]}, d, upsert=True) for d in docs],
                        ordered=False,
                    )
                    stats["archived"] += len(docs)
                # Delete exactly the documents read (and archived), not a re-run of the query
                ids_to_delete = [d["_id"] for d in docs]
                stats["deleted"] += collection.delete_many(
                    {"_id": {"$in": ids_to_delete}}
                ).deleted_count
            else:
                stats["matched"] += collection.count_documents(query)
        pending.clear()
        n_pending = 0

    with gzip.open(csv_path, "rt", encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            pending[row["deleted_date"]].append(row["work_id"])
            n_pending += 1
            stats["ids_read"] += 1
            if n_pending >= batch_size:
                _flush()
                if stats["ids_read"] % (batch_size * 100) == 0:
                    logger.info("%s (%.0fs)", stats, time.time() - t0)
    _flush()
    logger.info("Done %s mode=%s (%.0fs)", stats, "apply" if apply else "dry-run", time.time() - t0)
    return stats


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--csv", required=True)
    parser.add_argument("--mongodb-uri", default="mongodb://localhost:27017/")
    parser.add_argument("--db", default="openalex")
    parser.add_argument("--apply", action="store_true", help="Delete (default: dry-run)")
    parser.add_argument(
        "--archive-collection",
        default="",
        help="With --apply: copy each document to this collection before deleting it",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    client: pymongo.MongoClient = pymongo.MongoClient(args.mongodb_uri)
    db = client[args.db]
    archive = db[args.archive_collection] if args.archive_collection else None
    if archive is not None:
        archive.create_index([("id", 1)], unique=True, background=True)
    purge(db["works"], args.csv, apply=args.apply, archive=archive)


if __name__ == "__main__":
    main()
