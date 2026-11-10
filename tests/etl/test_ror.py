import hashlib
import io
import json
import os
import tempfile
import unittest
import zipfile
from pathlib import Path
from typing import Any, cast
from unittest.mock import MagicMock, patch

import requests

from dags.ror_capture import (
    ROR_CONCEPT_DOI,
    ROR_CONCEPT_ID,
    dag,
    download_ror_to_path,
    load_ror_from_path,
    resolve_ror_release,
)


def response(value, status=200):
    result = requests.Response()
    result.status_code = status
    result._content = value if isinstance(value, bytes) else json.dumps(value).encode()
    result._content_consumed = True  # type: ignore[attr-defined]
    return result


class RorFixture(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.home = Path(temporary.name)
        environment = patch.dict(os.environ, {"AIRFLOW_HOME": str(self.home)})
        environment.start()
        self.addCleanup(environment.stop)
        self.records = [{"id": "https://ror.org/02yr3f298", "names": []}]
        self.filename = "v2.14-2026-10-06-ror-data.zip"
        self.payload = self.archive(self.records)
        self.entry = {
            "key": self.filename,
            "size": len(self.payload),
            "checksum": "md5:" + hashlib.md5(self.payload).hexdigest(),
            "links": {"self": "https://zenodo.org/test-file"},
        }
        self.release = {
            "record_id": 200,
            "version": "v2.14",
            "publication_date": "2026-10-06",
            "resolved_via": "zenodo",
            "file": self.entry,
        }
        self.zenodo: dict[str, Any] = {
            "id": 200,
            "conceptrecid": ROR_CONCEPT_ID,
            "metadata": {"version": "v2.14", "publication_date": "2026-10-06"},
            "files": [self.entry],
        }
        self.destination = self.home / "cache" / "ror_import" / "200" / self.filename
        self.session = requests.Session()
        self.addCleanup(self.session.close)
        session = patch("dags.ror_capture._session", return_value=self.session)
        session.start()
        self.addCleanup(session.stop)
        get = patch.object(self.session, "get")
        self.get = get.start()
        self.addCleanup(get.stop)
        self.commit = {"sha": "a" * 40, "commit": {"tree": {"sha": "b" * 40}}}
        self.blob_sha = hashlib.sha1(
            f"blob {len(self.payload)}\0".encode() + self.payload
        ).hexdigest()
        self.tree = {
            "tree": [
                {
                    "path": self.filename,
                    "type": "blob",
                    "size": len(self.payload),
                    "sha": self.blob_sha,
                }
            ]
        }
        self.concept = {
            "data": {
                "attributes": {
                    "version": "v2.14",
                    "relatedIdentifiers": [
                        {"relationType": "HasVersion", "relatedIdentifier": "10.5281/zenodo.200"},
                        {"relationType": "HasVersion", "relatedIdentifier": "10.5281/zenodo.201"},
                    ],
                }
            }
        }
        self.datacite = {
            "data": {
                "attributes": {
                    "version": "v2.14",
                    "dates": [{"dateType": "Issued", "date": "2026-10-06"}],
                    "relatedIdentifiers": [
                        {"relationType": "IsVersionOf", "relatedIdentifier": ROR_CONCEPT_DOI}
                    ],
                }
            }
        }

    def archive(self, records, member="v2.14-2026-10-06-ror-data.json"):
        output = io.BytesIO()
        with zipfile.ZipFile(output, "w") as archive:
            archive.writestr(member, json.dumps(records))
        return output.getvalue()

    def write_archive(self, payload=None):
        self.destination.parent.mkdir(parents=True, exist_ok=True)
        self.destination.write_bytes(self.payload if payload is None else payload)
        return {
            "file_path": str(self.destination),
            "record_id": 200,
            "version": "v2.14",
            "publication_date": "2026-10-06",
            "resolved_via": "zenodo",
            "sha256": hashlib.sha256(self.destination.read_bytes()).hexdigest(),
            "downloaded": True,
        }


class TestRorResolution(RorFixture):
    def test_concept_resolves_actual_record_on_each_call(self):
        later = {
            **self.zenodo,
            "id": 201,
            "metadata": {"version": "v2.15", "publication_date": "2026-11-01"},
            "files": [{**self.entry, "key": "v2.15-2026-11-01-ror-data.zip"}],
        }
        self.get.side_effect = [response(self.zenodo), response(later)]
        self.assertEqual(resolve_ror_release()["record_id"], 200)
        self.assertEqual(resolve_ror_release()["version"], "v2.15")
        self.assertTrue(
            all(call.args[0].endswith(f"/{ROR_CONCEPT_ID}") for call in self.get.call_args_list)
        )

    def test_wrong_dataset_is_rejected(self):
        self.get.return_value = response({**self.zenodo, "conceptrecid": 123})
        with self.assertRaisesRegex(ValueError, "not a version"):
            resolve_ror_release()

    def test_archive_version_must_match_metadata(self):
        self.zenodo["files"][0] = {**self.entry, "key": "v2.1-2026-01-15-ror-data.zip"}
        self.get.return_value = response(self.zenodo)
        with self.assertRaises(ValueError):
            resolve_ror_release()

    def test_datacite_checks_version_not_largest_record_number(self):
        older = {"data": {"attributes": {"version": "v2.13"}}}
        self.get.side_effect = [
            requests.exceptions.SSLError("EOF"),
            response(self.concept),
            response(older),
            response(self.datacite),
            response(self.commit),
            response(self.tree),
        ]
        result = resolve_ror_release()
        self.assertEqual(result["record_id"], 200)
        self.assertEqual(result["version"], "v2.14")
        self.assertIn("a" * 40, result["file"]["links"]["self"])
        self.assertEqual(result["file"]["git_blob_sha1"], self.blob_sha)

    def test_datacite_wrong_parent_is_not_accepted(self):
        self.concept["data"]["attributes"]["relatedIdentifiers"] = [
            {"relationType": "HasVersion", "relatedIdentifier": "10.5281/zenodo.200"}
        ]
        self.datacite["data"]["attributes"]["relatedIdentifiers"] = []
        self.get.side_effect = [
            requests.ConnectionError("EOF"),
            response(self.concept),
            response(self.datacite),
        ]
        with self.assertRaisesRegex(ValueError, "Cannot verify"):
            resolve_ror_release()

    def test_github_does_not_choose_another_version(self):
        self.concept["data"]["attributes"]["relatedIdentifiers"] = [
            {"relationType": "HasVersion", "relatedIdentifier": "10.5281/zenodo.200"}
        ]
        self.tree["tree"][0]["path"] = "v2.1-2026-01-15-ror-data.zip"
        self.get.side_effect = [
            requests.Timeout("EOF"),
            response(self.concept),
            response(self.datacite),
            response(self.commit),
            response(self.tree),
        ]
        with self.assertRaisesRegex(ValueError, "exactly one"):
            resolve_ror_release()

    def test_dag_runs_resolve_before_download_and_load(self):
        self.assertEqual(dag.get_task("download_ror").upstream_task_ids, {"resolve_ror"})
        self.assertEqual(dag.get_task("load_ror").upstream_task_ids, {"download_ror"})
        self.assertNotIn("record_id", cast(Any, dag.get_task("download_ror")).op_kwargs)
        self.assertEqual(dag.max_active_runs, 1)


class TestRorDownload(RorFixture):
    def test_valid_existing_cache_is_still_downloaded_again(self):
        self.write_archive()
        self.get.return_value = response(self.payload)
        result = download_ror_to_path(self.release)
        self.get.assert_called_once()
        self.assertTrue(result["downloaded"])
        self.assertEqual(result["version"], self.release["version"])
        self.assertEqual(result["sha256"], hashlib.sha256(self.payload).hexdigest())
        self.assertTrue(self.session.verify)

    def test_zenodo_failure_uses_exact_github_release(self):
        self.get.side_effect = [
            requests.ConnectionError("reset"),
            response(self.commit),
            response(self.tree),
            response(self.payload),
        ]
        result = download_ror_to_path(self.release)
        self.assertIn("a" * 40, result["downloaded_from"])
        self.assertEqual(result["record_id"], 200)
        self.assertEqual(self.destination.read_bytes(), self.payload)

    def test_corrupt_download_preserves_previous_file(self):
        self.write_archive()
        self.get.return_value = response(b"X" * len(self.payload))
        with self.assertRaisesRegex(ValueError, "checksum mismatch"):
            download_ror_to_path(self.release)
        self.assertEqual(self.destination.read_bytes(), self.payload)
        self.assertEqual(list(self.destination.parent.iterdir()), [self.destination])

    def test_github_blob_checksum_is_checked(self):
        file = {**self.entry, "git_blob_sha1": "0" * 40}
        self.get.return_value = response(self.payload)
        with self.assertRaisesRegex(ValueError, "checksum mismatch"):
            download_ror_to_path({**self.release, "file": file})
        self.assertFalse(self.destination.exists())

    def test_xcom_string_is_supported(self):
        self.get.return_value = response(self.payload)
        self.assertEqual(download_ror_to_path(str(self.release))["record_id"], 200)


class TestRorLoad(RorFixture):
    def test_duplicate_identifiers_fail_before_mongo_access(self):
        info = self.write_archive(self.archive(self.records * 2))
        with (
            patch("dags.ror_capture.MongoHook") as hook,
            self.assertRaisesRegex(ValueError, "duplicate"),
        ):
            load_ror_from_path(info, db_name="test")
        hook.assert_not_called()

    def test_changed_archive_fails_before_mongo_access(self):
        info = self.write_archive()
        self.destination.write_bytes(b"changed")
        with (
            patch("dags.ror_capture.MongoHook") as hook,
            self.assertRaisesRegex(ValueError, "changed"),
        ):
            load_ror_from_path(info, db_name="test")
        hook.assert_not_called()

    def test_wrong_version_fails_before_mongo_access(self):
        info = {**self.write_archive(), "version": "v2.1"}
        with patch("dags.ror_capture.MongoHook") as hook, self.assertRaises(ValueError):
            load_ror_from_path(info, db_name="test")
        hook.assert_not_called()

    def test_missing_xcom_does_not_guess_an_old_cached_file(self):
        self.write_archive()
        with patch("dags.ror_capture.MongoHook") as hook, self.assertRaises(ValueError):
            load_ror_from_path(cast(Any, None), db_name="test")
        hook.assert_not_called()

    def test_empty_archive_is_rejected(self):
        info = self.write_archive(self.archive([]))
        with (
            patch("dags.ror_capture.MongoHook") as hook,
            self.assertRaisesRegex(ValueError, "nonempty"),
        ):
            load_ror_from_path(info, db_name="test")
        hook.assert_not_called()

    def test_failed_staging_does_not_replace_current_collection(self):
        info = self.write_archive()
        client = MagicMock()
        staging = client["test"].__getitem__.return_value
        staging.insert_many.side_effect = RuntimeError("insert failed")
        with (
            patch("dags.ror_capture.MongoHook") as hook,
            patch("dags.ror_capture._copy_db") as copy,
        ):
            hook.return_value.get_conn.return_value = client
            with self.assertRaisesRegex(RuntimeError, "insert failed"):
                load_ror_from_path(info, db_name="test")
        staging.rename.assert_not_called()
        client.drop_database.assert_not_called()
        copy.assert_not_called()

    def test_reload_updates_metadata_and_uses_unique_archive_names(self):
        info = {**self.write_archive(), "is_new": False}
        client = MagicMock()
        database = client["test"]
        database.list_collection_names.return_value = ["ror_stage"]
        staging = database.__getitem__.return_value
        staging.count_documents.return_value = 1
        with (
            patch("dags.ror_capture.MongoHook") as hook,
            patch("dags.ror_capture._copy_db") as copy,
        ):
            hook.return_value.get_conn.return_value = client
            self.assertEqual(load_ror_from_path(info, db_name="test"), 1)
            self.assertEqual(load_ror_from_path(info, db_name="test"), 1)
        archives = [call.args[2] for call in copy.call_args_list]
        self.assertEqual(len(set(archives)), 2)
        metadata = database.ror_import_metadata.replace_one.call_args.args[1]
        self.assertEqual(metadata["version"], "v2.14")
        self.assertEqual(metadata["record_id"], 200)
        self.assertEqual(metadata["count"], 1)
        client.drop_database.assert_not_called()
        staging.rename.assert_called_with("ror_stage", dropTarget=True)
