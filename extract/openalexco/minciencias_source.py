"""Locate the latest Minciencias works collections produced by the DAM pipeline.

The DAM pipeline publishes versioned snapshots in the ``dam`` database:

* ``minciencias_works_vXX_YYYYMMDD``          — every consolidated product.
* ``minciencias_works_vXX_YYYYMMDD_filtered`` — only products with identifiers
  (DOI, ISSN, ISBN, …).

The latest snapshot is chosen by date (``YYYYMMDD``) and, on ties, by version.
"""

from __future__ import annotations

import re

from pymongo.database import Database

MINCIENCIAS_DB = "dam"

_FULL_RE = re.compile(r"^minciencias_works_v(\d+)_(\d{8})$")
_FILTERED_RE = re.compile(r"^minciencias_works_v(\d+)_(\d{8})_filtered$")


def latest_minciencias_collection(db: Database, filtered: bool) -> str:
    """Return the name of the most recent Minciencias works collection in ``db``.

    Parameters
    ----------
    db : pymongo.database.Database
        Database holding the DAM outputs (usually ``dam``).
    filtered : bool
        ``True`` for ``..._filtered`` (products with identifiers), ``False`` for
        the full collection.

    Returns
    -------
    str
        Collection name, e.g. ``minciencias_works_v05_20261017_filtered``.

    Raises
    ------
    RuntimeError
        If no collection matches — the cut must not silently run without
        Minciencias data.
    """
    pattern = _FILTERED_RE if filtered else _FULL_RE
    candidates: list[tuple[str, int, str]] = []
    for name in db.list_collection_names():
        match = pattern.match(name)
        if match:
            candidates.append((match.group(2), int(match.group(1)), name))
    if not candidates:
        kind = "filtered" if filtered else "full"
        raise RuntimeError(
            f"No {kind} Minciencias works collection ({pattern.pattern}) found in "
            f"database '{db.name}'."
        )
    return max(candidates)[2]
