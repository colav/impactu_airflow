# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Apache Airflow 3.x ETL platform (Python 3.12+) for bibliographic/scientometric data. It
orchestrates **capture → transform → load** for many scientific sources (DOAJ, ROR, ScimagoJR,
CIARP, Staff, DSpace, Publindex, DAM, OpenAlex, OpenAlexCo, ORCID, Wikidata, Scienti, Horus,
Minciencias). Raw and normalized data live in **MongoDB**; the query/search layer is
**Elasticsearch**. Normalization is done with [Kahi](https://github.com/colav/Kahi).

Base infrastructure (the Airflow runtime itself) lives in a separate **Chia** repo — this repo is
DAG logic + ETL code only. CI/CD details are in [README_DEVOPS.md](README_DEVOPS.md).

## Commands

```bash
# Local Airflow (uses scripts/start_airflow_local.sh; defaults to airflow standalone, port 8080)
make start-airflow
AIRFLOW_PORT=8090 ./scripts/start_airflow_local.sh

# Quality gates (mirror CI: code-quality + test jobs)
ruff format .
ruff check .
mypy .
pytest

# Single test file / single test
pytest tests/etl/test_scimagojr.py
pytest tests/etl/test_scimagojr.py::test_name

# DAG integrity (the must-pass gate — loads every DAG via DagBag and fails on import errors)
pytest tests/etl/test_dag_integrity.py

# Dev setup
pip install -r requirements.txt -r requirements-dev.txt
pre-commit install
```

Ruff: line-length 100, rules `E,W,F,I,N,UP,B,C4,SIM` (E501 ignored — formatter handles length).
Type hints required on public methods; NumPy-style docstrings. Conventional commit messages
(`feat:`, `fix:`, `docs:`, `test:`, `refactor:`, `chore:`).

## Architecture

The capture flow is a three-layer stack:

```
BaseExtractor (extract/base_extractor.py)
  └── {Source}Extractor (extract/{source}/{source}_extractor.py)
        └── {source}_capture DAG (dags/{source}_capture.py)
              └── MongoDB via MongoHook("mongo_default")
```

- **`extract/base_extractor.py`** — abstract base. Owns the MongoDB client/collection, the
  checkpoint API (`get_checkpoint(source_id)` / `save_checkpoint(source_id, value)`, stored in the
  `etl_checkpoints` collection), `self.logger`, and `dump_collection_if_exists()`. Subclasses
  implement `run()`.
- **`extract/{source}/`** — per-source extractor subclasses (and some standalone modules still run
  outside DAGs).
- **`dags/`** — Airflow DAG definitions. **`transform/`** — Kahi normalization. **`load/`** —
  loaders to `mongodb/` and `elasticsearch/`. **`deploys/`** / **`backups/`** — service deploy and
  backup DAGs.

**Checkpoints + idempotency are the core design constraint.** Long-running captures must resume
from the last saved checkpoint and must never create duplicates — re-running a DAG over the same
data must leave MongoDB in an identical state. See [REQUIREMENTS.md](REQUIREMENTS.md).

### DAG naming convention

| Type | Format | Example |
|---|---|---|
| Capture | `{source}_capture` | `openalex_capture` |
| Transform | `transform_{entity}` | `transform_sources` |
| Load | `load_{db}_{env}` | `load_elasticsearch_production` |
| Deploy | `deploy_{service}_{env}` | `deploy_mongodb_production` |
| Backup | `backup_{db}_{name}` | `backup_mongodb_kahi` |
| Tests | `tests_{service}` | `tests_kahi` |

## Mandatory patterns

**Extractor** — call `self.create_indexes()` in `__init__`; create the **unique index on the
natural key before any write**. Page from the checkpoint, then `save_checkpoint` after the batch.

```python
class MyExtractor(BaseExtractor):
    def __init__(self, mongodb_uri, db_name, collection_name="my_col", client=None):
        super().__init__(mongodb_uri, db_name, collection_name, client=client)
        self.create_indexes()

    def create_indexes(self):
        self.collection.create_index([("id", 1)], unique=True, background=True)
```

**Upsert, never insert** — idempotency rule. Always
`UpdateOne(filter, {"$set": doc}, upsert=True)` + `bulk_write(ops, ordered=False)`; never
`insert_many`.

**DAGs:**
- Import extractors/Kahi **inside task functions**, never at module top level (avoids parse-time
  side effects that break DagBag loading).
- Parallelize with `PythonOperator.expand()` — never a dynamic `for` loop building tasks.
- `default_args`: `owner="impactu"`, `retries=2`, `retry_delay=timedelta(hours=1)`.
- `catchup=False` on capture DAGs.
- All runtime config via `Param()` — no hardcoded values. Connect with `MongoHook(conn_id)`, never
  a raw `MongoClient` with a hardcoded URI.
- Transform DAGs: read raw collections → normalize with Kahi → write to `kahi.{entity}`.

## Testing

pytest + mongomock. Mock all external deps (MongoDB, APIs). Tests that exercise `bulk_write`
upserts need the pymongo-4.x `BulkOperationBuilder` compatibility patch — see
[tests/etl/test_scimagojr.py](tests/etl/test_scimagojr.py) for the canonical version to copy.

## Git policy

**Do not run `git add` / `commit` / `push` (or any history-changing git command) without an
explicit, unambiguous request for that exact action.** Prepare patches, diffs, and suggested
commit commands instead, and let the user run them. (Carried over from
[.github/copilot-instructions.md](.github/copilot-instructions.md); the user may override per task.)

## Contributing / CI notes

PRs run `code-quality` (ruff lint + format check + mypy) and `test` (DAG integrity + changed-file
tests) — these run on forks without secrets. Validation against the live dev Airflow
(`dev.airflow.colav.co`) requires a maintainer to add the `validate-on-dev` label, because it needs
`AIRFLOW_API_TOKEN` which forks can't access. Details in [CONTRIBUTING.md](CONTRIBUTING.md).
