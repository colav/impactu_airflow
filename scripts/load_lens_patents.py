"""Load a Lens patent export into MongoDB (``lens.patents`` by default).

The input is the JSON produced from the Lens patent API (full records), e.g.
``patents_lens_residence_CO.json.gz``::

    {"source": ..., "retrieved": "2026-11-11", "counts": {...},
     "patents": {"<lens_id>": {"lens": {...}, "residence": {...},
                               "cvlac_patent_ids": [...], "linked_by": ...}}}

Each patent becomes one document ``{_id: lens_id, lens, residence,
cvlac_patent_ids, linked_by}``; the Lens record is stored untouched so
Kahi_lens_patents can apply its own matching rules. No matching happens here.

The load is idempotent: documents are upserted by lens_id and the ones absent
from the export are removed, so loading the same file twice leaves the
collection identical. A summary of every load is kept in ``<collection>_loads``.

Usage::

    MONGODB_URI=mongodb://localhost:27017 \\
    python scripts/load_lens_patents.py /path/patents_lens_residence_CO.json.gz

A future Lens capture DAG should write the same document shape.
"""

import argparse
import gzip
import hashlib
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

from pymongo import ASCENDING, MongoClient, UpdateOne

BATCH_SIZE = 1000


def read_export(path: Path) -> dict[str, Any]:
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8") as handle:
        data: dict[str, Any] = json.load(handle)
    patents = data.get("patents")
    if not isinstance(patents, dict) or not patents:
        raise ValueError("export has no 'patents' mapping")
    return data


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_documents(data: dict[str, Any]) -> list[dict[str, Any]]:
    documents = []
    for lens_id, item in data["patents"].items():
        record = item.get("lens") or {}
        if record.get("lens_id") != lens_id:
            raise ValueError(f"lens_id mismatch for key {lens_id}")
        documents.append(
            {
                "_id": lens_id,
                "lens": record,
                "residence": item.get("residence"),
                "cvlac_patent_ids": item.get("cvlac_patent_ids") or [],
                "linked_by": item.get("linked_by"),
            }
        )
    return documents


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("path", type=Path, help="Lens export (.json or .json.gz)")
    parser.add_argument("--mongodb-uri", default=os.environ.get("MONGODB_URI", "localhost:27017"))
    parser.add_argument("--database", default="lens")
    parser.add_argument("--collection", default="patents")
    parser.add_argument("--dry-run", action="store_true", help="read and validate only")
    args = parser.parse_args()

    data = read_export(args.path)
    load_id = time.strftime("%Y%m%d_%H%M%S")
    documents = build_documents(data)
    with_abstract = sum(1 for doc in documents if doc["lens"].get("abstract"))
    summary = {
        "_id": load_id,
        "file": args.path.name,
        "sha256": sha256(args.path),
        "source": data.get("source"),
        "retrieved": data.get("retrieved"),
        "export_counts": data.get("counts"),
        "documents": len(documents),
        "with_abstract": with_abstract,
        "loaded_at": int(time.time()),
    }
    print(
        json.dumps({k: v for k, v in summary.items() if k != "export_counts"}, ensure_ascii=False)
    )
    if args.dry_run:
        return 0

    client: MongoClient[dict[str, Any]] = MongoClient(args.mongodb_uri)
    db = client[args.database]
    collection = db[args.collection]
    collection.create_index([("lens.jurisdiction", ASCENDING), ("lens.doc_number", ASCENDING)])
    upserted = modified = 0
    for start in range(0, len(documents), BATCH_SIZE):
        operations = [
            UpdateOne({"_id": doc["_id"]}, {"$set": doc}, upsert=True)
            for doc in documents[start : start + BATCH_SIZE]
        ]
        result = collection.bulk_write(operations, ordered=False)
        upserted += result.upserted_count
        modified += result.modified_count
    removed = collection.delete_many({"_id": {"$nin": [doc["_id"] for doc in documents]}})
    if collection.count_documents({}) != len(documents):
        raise RuntimeError("collection count differs from the export after the load")
    summary.update(upserted=upserted, modified=modified, removed=removed.deleted_count)
    db[f"{args.collection}_loads"].update_one({"_id": load_id}, {"$set": summary}, upsert=True)
    print(
        f"LOADED {len(documents)} -> {args.database}.{args.collection} "
        f"(new {upserted}, modified {modified}, removed {removed.deleted_count})"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
