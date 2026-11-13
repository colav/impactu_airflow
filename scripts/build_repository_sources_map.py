"""Candidate equivalences between dspace repositories (oxomoc_colombia) and
OpenAlex repository sources (openalexco), by handle prefix and homepage host.

Run it when a repository is added to ``config/oxomoc_colombia_config.py``:
review ``repository_sources_candidates.csv`` (written next to this script) and
copy the confirmed OpenAlex id into the endpoint's ``openalex_source`` so the
capture DAG persists it in the ``*_identity`` document that Kahi reads.
Read-only against MongoDB (``MONGODB_URI``, default localhost:27017)."""

import csv
import os
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from pymongo import MongoClient

AGGREGATORS = re.compile(r"la referencia|americanae|redcol|core\b|base\b|oatd|openaire", re.I)
FAKE_PREFIXES = {"123456789", "001", "1", "0"}  # DSpace defaults, shared by many installs
MIN_HANDLES, MIN_SHARE = 10, 0.5
client: MongoClient[Any] = MongoClient(os.environ.get("MONGODB_URI", "localhost:27017"))
oxo, oa = client["oxomoc_colombia"], client["openalexco"]

# 1. OpenAlex: (source, handle prefix) counts and source metadata
pairs: Counter[tuple[str, str]] = Counter()
sources: dict[str, str] = {}
pipeline: list[dict[str, Any]] = [
    {"$unwind": "$locations"},
    {
        "$match": {
            "locations.source.type": "repository",
            "locations.landing_page_url": {"$regex": r"hdl\.handle\.net/[0-9.]+/"},
        }
    },
    {
        "$project": {
            "sid": "$locations.source.id",
            "name": "$locations.source.display_name",
            "url": "$locations.landing_page_url",
        }
    },
]
for row in oa["works"].aggregate(pipeline, allowDiskUse=True):
    prefix = row["url"].split("hdl.handle.net/", 1)[1].split("/", 1)[0]
    pairs[(row["sid"], prefix)] += 1
    sources[row["sid"]] = row["name"]
by_prefix: dict[str, list[tuple[int, str]]] = defaultdict(list)
total_by_source: Counter[str] = Counter()
for (sid, prefix), n in pairs.items():
    by_prefix[prefix].append((n, sid))
    total_by_source[sid] += n
homepage_hosts: dict[str, list[tuple[str, str]]] = {}
for s in oa["sources"].find(
    {"type": "repository", "homepage_url": {"$nin": [None, ""]}},
    {"id": 1, "display_name": 1, "homepage_url": 1},
):
    host = urlparse(s["homepage_url"]).netloc.lower().replace("www.", "")
    if host:
        homepage_hosts.setdefault(host, []).append((s["id"], s["display_name"]))
        sources.setdefault(s["id"], s["display_name"])

# 2. dspace repositories
rows = []
for name in sorted(
    c for c in oxo.list_collection_names() if c.startswith("dspace_") and c.endswith("_records")
):
    key = name[: -len("_records")]
    identity = oxo[name[: -len("records")] + "identity"].find_one({}) or {}
    base_host = urlparse(identity.get("base_url") or "").netloc.lower().replace("www.", "")
    prefixes: Counter[str] = Counter()
    for doc in oxo[name].find({}, {"_id": 1}).limit(2000):
        m = re.search(r":([0-9.]+)/[^/]+$", str(doc["_id"]))
        if m:
            prefixes[m.group(1)] += 1
    prefix = prefixes.most_common(1)[0][0] if prefixes else ""
    fake = prefix in FAKE_PREFIXES
    cands = [] if fake else sorted(by_prefix.get(prefix, []), reverse=True)
    best = next(
        ((n, sid) for n, sid in cands if not AGGREGATORS.search(sources.get(sid, ""))), None
    )
    share = round(best[0] / total_by_source[best[1]], 2) if best else 0.0
    home = homepage_hosts.get(base_host, [])
    if best and (best[0] < MIN_HANDLES or share < MIN_SHARE):
        confidence = "weak"
    elif best:
        confidence = "handle"
    elif home:
        confidence = "homepage"
    else:
        confidence = ""
    rows.append(
        {
            "confidence": confidence,
            "dspace": key,
            "repository_name": identity.get("repository_name", ""),
            "ror": identity.get("ror", ""),
            "base_host": base_host,
            "records": oxo[name].estimated_document_count(),
            "handle_prefix": prefix + (" (default)" if fake else ""),
            "openalex_source": best[1] if best else "",
            "openalex_name": sources.get(best[1], "") if best else "",
            "handles_matched": best[0] if best else 0,
            "share_of_source": share,
            "other_candidates": "; ".join(
                f"{sid} ({n})" for n, sid in cands if not best or sid != best[1]
            )[:300],
            "homepage_match": "; ".join(f"{sid} {nm}" for sid, nm in home)[:300],
            "decision": "",
        }
    )
out = Path(__file__).with_name("repository_sources_candidates.csv")
with out.open("w", newline="") as fh:
    w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
    w.writeheader()
    w.writerows(rows)
print(
    "repositorios:",
    len(rows),
    " confianza:",
    dict(Counter(r["confidence"] or "none" for r in rows)),
)
print("escrito:", out)
