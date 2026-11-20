"""Download Open Library monthly dumps and mirror them into MongoDB."""

from __future__ import annotations

import gzip
import hashlib
import json
import logging
import os
import re
import shutil
import time
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import requests
from bson.errors import InvalidDocument
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from extract.base_extractor import BaseExtractor

log = logging.getLogger(__name__)

LATEST_DUMP_URL = "https://openlibrary.org/data/ol_dump_latest.txt.gz"
ARCHIVE_BASE_URL = "https://archive.org"
SNAPSHOT_CHECKPOINT = "openlibrary_snapshot"

_LOAD_CHECKPOINT_PREFIX = "openlibrary_load_"
_SNAPSHOT_DATE = re.compile(r"ol_dump_(\d{4}-\d{2}-\d{2})")
_SNAPSHOT_DIR = re.compile(r"\d{4}-\d{2}-\d{2}")
_DOWNLOAD_CHUNK_BYTES = 8 * 1024 * 1024
_PROGRESS_LOG_BYTES = 1024**3
_PROGRESS_LOG_LINES = 1_000_000


def _session() -> requests.Session:
    session = requests.Session()
    session.headers["User-Agent"] = (
        "ImpactU-OpenLibrary-Downloader/1.0 (https://github.com/colav/impactu_airflow)"
    )
    retries = Retry(
        total=3,
        read=0,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "HEAD"),
        raise_on_status=False,
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    return session


def resolve_latest_snapshot(
    dump_types: Iterable[str], session: requests.Session | None = None
) -> dict[str, Any]:
    """
    Pin the current ``latest`` dump to a dated snapshot and its checksums.

    ``latest`` moves when Open Library publishes, so every later step must use the
    dated URLs returned here to avoid mixing two snapshots in one run.

    Parameters
    ----------
    dump_types : iterable of str
        Dump files to resolve, e.g. ``editions`` or ``authors``.
    session : requests.Session, optional
        HTTP session to use.

    Returns
    -------
    dict
        ``snapshot_date`` and ``files``, mapping each dump type to its ``name``,
        ``url``, ``size`` and ``md5``.
    """
    http = session or _session()
    response = http.head(LATEST_DUMP_URL, allow_redirects=False, timeout=(10, 30))
    response.raise_for_status()
    location = response.headers.get("Location", "")
    match = _SNAPSHOT_DATE.search(location)
    if not match:
        raise ValueError(f"Cannot read a snapshot date from redirect: {location!r}")

    snapshot_date = match[1]
    identifier = f"ol_dump_{snapshot_date}"
    metadata = http.get(f"{ARCHIVE_BASE_URL}/metadata/{identifier}/files", timeout=(10, 60))
    metadata.raise_for_status()
    published = {item["name"]: item for item in metadata.json().get("result", [])}

    files: dict[str, dict[str, Any]] = {}
    for dump_type in dump_types:
        name = f"ol_dump_{dump_type}_{snapshot_date}.txt.gz"
        item = published.get(name, {})
        if not item.get("md5") or not item.get("size"):
            raise ValueError(f"{identifier} has no size and checksum for {name}")
        files[dump_type] = {
            "name": name,
            "url": f"{ARCHIVE_BASE_URL}/download/{identifier}/{name}",
            "size": int(item["size"]),
            "md5": item["md5"],
        }
    return {"snapshot_date": snapshot_date, "files": files}


def _size_on_disk(path: Path) -> int:
    return path.stat().st_size if path.exists() else 0


def _is_verified(path: Path, entry: dict[str, Any]) -> bool:
    if _size_on_disk(path) != entry["size"]:
        return False
    digest = hashlib.md5(usedforsecurity=False)
    with path.open("rb") as handle:
        while chunk := handle.read(_DOWNLOAD_CHUNK_BYTES):
            digest.update(chunk)
    return bool(digest.hexdigest() == entry["md5"])


def _is_transient(exc: requests.RequestException) -> bool:
    status = exc.response.status_code if exc.response is not None else None
    return status is None or status == 429 or status >= 500


def _stream_to_partial(http: requests.Session, entry: dict[str, Any], partial: Path) -> None:
    offset = _size_on_disk(partial)
    # Without identity, transparent compression would make byte offsets meaningless.
    headers = {"Accept-Encoding": "identity"}
    if offset:
        headers["Range"] = f"bytes={offset}-"

    with http.get(entry["url"], headers=headers, stream=True, timeout=(30, 120)) as response:
        response.raise_for_status()
        # A 200 reply to a Range request means the server is resending the whole file.
        resumed = offset and response.status_code == 206
        received = offset if resumed else 0
        next_log = received + _PROGRESS_LOG_BYTES
        log.info("Downloading %s from byte %d of %d", entry["name"], received, entry["size"])
        with partial.open("ab" if resumed else "wb") as handle:
            for chunk in response.iter_content(chunk_size=_DOWNLOAD_CHUNK_BYTES):
                handle.write(chunk)
                received += len(chunk)
                if received >= next_log:
                    log.info("%s: %d of %d bytes", entry["name"], received, entry["size"])
                    next_log += _PROGRESS_LOG_BYTES


def download_snapshot_file(
    entry: dict[str, Any],
    target_dir: str | Path,
    session: requests.Session | None = None,
    max_stalled_attempts: int = 8,
    backoff_seconds: float = 60,
) -> str:
    """
    Download one dump file, resuming interrupted transfers and verifying its MD5.

    The file only takes its final name after the checksum matches, so a file with
    that name is always complete.

    Parameters
    ----------
    entry : dict
        File description produced by :func:`resolve_latest_snapshot`.
    target_dir : str or Path
        Directory of this snapshot.
    session : requests.Session, optional
        HTTP session to use.
    max_stalled_attempts : int
        Consecutive attempts without new bytes before giving up.
    backoff_seconds : float
        Pause after a transient network failure.

    Returns
    -------
    str
        Path of the verified file.
    """
    directory = Path(target_dir)
    directory.mkdir(parents=True, exist_ok=True)
    final = directory / entry["name"]
    if final.exists():
        if _is_verified(final, entry):
            log.info("%s is already downloaded and verified", entry["name"])
            return str(final)
        log.warning("%s failed verification; downloading it again", entry["name"])
        final.unlink()

    partial = final.with_name(f"{final.name}.part")
    if _size_on_disk(partial) > entry["size"]:
        partial.unlink()

    http = session or _session()
    stalled = 0
    while (offset := _size_on_disk(partial)) < entry["size"]:
        failure: requests.RequestException | None = None
        try:
            _stream_to_partial(http, entry, partial)
        except requests.RequestException as exc:
            if not _is_transient(exc):
                raise
            failure = exc

        stalled = 0 if _size_on_disk(partial) > offset else stalled + 1
        if stalled >= max_stalled_attempts:
            raise RuntimeError(
                f"{entry['name']}: no progress after {stalled} attempts"
            ) from failure
        if failure is not None:
            log.warning("%s: transfer interrupted (%s); resuming", entry["name"], failure)
            time.sleep(backoff_seconds)

    if not _is_verified(partial, entry):
        partial.unlink()
        raise ValueError(f"{entry['name']}: checksum mismatch; partial file discarded")
    os.replace(partial, final)
    return str(final)


def remove_old_snapshots(base_dir: str | Path, keep: int) -> list[str]:
    """
    Delete all but the newest ``keep`` date-named snapshot directories.

    Parameters
    ----------
    base_dir : str or Path
        Directory holding one sub-directory per snapshot.
    keep : int
        Number of snapshots to retain; must be positive.

    Returns
    -------
    list of str
        Paths of the removed directories.
    """
    if keep < 1:
        raise ValueError("keep must be at least 1")
    snapshots = sorted(
        (p for p in Path(base_dir).iterdir() if p.is_dir() and _SNAPSHOT_DIR.fullmatch(p.name)),
        key=lambda p: p.name,
    )
    removed = []
    for directory in snapshots[:-keep]:
        shutil.rmtree(directory)
        removed.append(str(directory))
    return removed


def _parse_line(raw: bytes) -> tuple[str, dict[str, Any]] | None:
    """Return ``(key, record)`` for a dump row, or ``None`` when it is malformed."""
    columns = raw.rstrip(b"\r\n").split(b"\t", 4)
    if len(columns) != 5:
        return None
    try:
        key = columns[1].decode("utf-8")
        record = json.loads(columns[4])
    except ValueError:
        return None
    if not key or not isinstance(record, dict):
        return None
    return key, record


class OpenLibraryExtractor(BaseExtractor):
    """
    Mirror one Open Library dump file into a MongoDB collection.

    Documents have the shape ``{key, snapshot_date, record}``. The source record is
    nested so that ``$set`` replaces it whole: a field removed upstream, such as a
    wrong ISBN, also disappears here instead of surviving the upsert.

    Parameters
    ----------
    mongodb_uri : str
        MongoDB connection URI.
    db_name : str
        Database name.
    collection_name : str
        Target collection, named after the dump type.
    client : pymongo.MongoClient, optional
        Existing MongoDB client.
    chunk_size : int
        Documents per ``bulk_write`` batch.
    """

    def __init__(
        self,
        mongodb_uri: str,
        db_name: str,
        collection_name: str,
        client: Any = None,
        chunk_size: int = 2000,
    ) -> None:
        super().__init__(mongodb_uri, db_name, collection_name, client=client)
        self.chunk_size = chunk_size
        self.create_indexes()

    def create_indexes(self) -> None:
        """Create the unique index on the Open Library ``key``."""
        self.collection.create_index([("key", 1)], unique=True, background=True)

    @property
    def _checkpoint_id(self) -> str:
        return f"{_LOAD_CHECKPOINT_PREFIX}{self.collection_name}"

    def _load_state(self, snapshot_date: str) -> dict[str, Any]:
        state: dict[str, Any] | None = self.get_checkpoint(self._checkpoint_id)
        if state and state.get("snapshot_date") == snapshot_date:
            return state
        return {
            "snapshot_date": snapshot_date,
            "lines": 0,
            "written": 0,
            "errors": 0,
            "done": False,
        }

    def reset_load_state(self) -> None:
        """Forget load progress for every collection so the next run reloads them."""
        self.checkpoints.delete_many({"_id": {"$regex": f"^{_LOAD_CHECKPOINT_PREFIX}"}})

    def _flush(self, ops: list[UpdateOne], state: dict[str, Any]) -> None:
        if not ops:
            return
        try:
            self.collection.bulk_write(ops, ordered=False)
            state["written"] += len(ops)
        except BulkWriteError as exc:
            failed = len(exc.details.get("writeErrors", []))
            state["written"] += len(ops) - failed
            state["errors"] += failed
            self.logger.warning("%s: %d writes rejected", self.collection_name, failed)
        except (InvalidDocument, OverflowError) as exc:
            # BSON encoding happens client-side, so one bad record aborts its whole batch.
            if len(ops) == 1:
                state["errors"] += 1
                self.logger.warning("%s: unstorable record: %s", self.collection_name, exc)
                return
            for op in ops:
                self._flush([op], state)

    def run(self, snapshot_date: str, file_path: str) -> dict[str, Any]:
        """
        Upsert a dump file, resuming after the last checkpointed line.

        Parameters
        ----------
        snapshot_date : str
            Snapshot the file belongs to.
        file_path : str
            Path of the verified ``.txt.gz`` dump.

        Returns
        -------
        dict
            Load state with ``lines``, ``written``, ``errors`` and ``done``.
        """
        state = self._load_state(snapshot_date)
        if state["done"]:
            self.logger.info("%s: snapshot already loaded", self.collection_name)
            return state

        ops: list[UpdateOne] = []
        last_line = state["lines"]
        with gzip.open(file_path, "rb") as handle:
            for last_line, raw in enumerate(handle, start=1):
                if last_line <= state["lines"]:
                    continue
                parsed = _parse_line(raw)
                if parsed is None:
                    state["errors"] += 1
                else:
                    key, record = parsed
                    document = {"key": key, "snapshot_date": snapshot_date, "record": record}
                    ops.append(UpdateOne({"key": key}, {"$set": document}, upsert=True))
                if len(ops) >= self.chunk_size:
                    self._flush(ops, state)
                    ops.clear()
                    state["lines"] = last_line
                    self.save_checkpoint(self._checkpoint_id, state)
                if last_line % _PROGRESS_LOG_LINES == 0:
                    self.logger.info("%s: %d lines read", self.collection_name, last_line)

        self._flush(ops, state)
        state["lines"] = max(state["lines"], last_line)
        state["done"] = True
        self.save_checkpoint(self._checkpoint_id, state)
        self.logger.info("%s: load finished: %s", self.collection_name, state)
        return state

    def check_ready_to_purge(
        self, snapshot_date: str, max_error_ratio: float, max_purge_ratio: float
    ) -> None:
        """
        Raise unless this collection can safely drop records from older snapshots.

        Run this on every collection before purging any of them.

        Parameters
        ----------
        snapshot_date : str
            Snapshot that was just loaded.
        max_error_ratio : float
            Largest accepted share of rows that could not be stored.
        max_purge_ratio : float
            Largest accepted share of the collection to delete.
        """
        name = self.collection_name
        state = self._load_state(snapshot_date)
        if not state["done"] or not state["lines"]:
            raise RuntimeError(f"{name}: load of snapshot {snapshot_date} is not complete")

        error_ratio = state["errors"] / state["lines"]
        if error_ratio > max_error_ratio:
            raise RuntimeError(f"{name}: error ratio {error_ratio:.4%} exceeds the limit")

        total = self.collection.estimated_document_count()
        stale = max(total - state["written"], 0)
        if total and stale / total > max_purge_ratio:
            raise RuntimeError(f"{name}: purge would delete {stale} of {total} documents")

    def purge_stale(self, snapshot_date: str) -> int:
        """
        Delete documents that are absent from the given snapshot.

        Parameters
        ----------
        snapshot_date : str
            Snapshot that was just loaded.

        Returns
        -------
        int
            Number of deleted documents.
        """
        result = self.collection.delete_many({"snapshot_date": {"$ne": snapshot_date}})
        return int(result.deleted_count)
