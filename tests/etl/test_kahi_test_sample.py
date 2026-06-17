import importlib
import sys
import types
from pathlib import Path

import pytest
import yaml


class _DummyDAG:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _DummyPythonOperator:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def __rshift__(self, other):
        return other


class _DummyParam:
    def __init__(self, default, **kwargs):
        self.default = default
        self.kwargs = kwargs


class _DummyMongoHook:
    def __init__(self, mongo_conn_id="mongodb_default"):
        self.mongo_conn_id = mongo_conn_id

    def get_uri(self):
        return "mongodb://localhost:27017"


def _install_airflow_stubs() -> None:
    airflow = types.ModuleType("airflow")
    airflow.DAG = _DummyDAG

    providers = types.ModuleType("airflow.providers")
    mongo_pkg = types.ModuleType("airflow.providers.mongo")
    mongo_hooks_pkg = types.ModuleType("airflow.providers.mongo.hooks")
    mongo_module = types.ModuleType("airflow.providers.mongo.hooks.mongo")
    mongo_module.MongoHook = _DummyMongoHook

    standard_pkg = types.ModuleType("airflow.providers.standard")
    operators_pkg = types.ModuleType("airflow.providers.standard.operators")
    python_module = types.ModuleType("airflow.providers.standard.operators.python")
    python_module.PythonOperator = _DummyPythonOperator

    sdk_module = types.ModuleType("airflow.sdk")
    sdk_module.Param = _DummyParam

    sys.modules["airflow"] = airflow
    sys.modules["airflow.providers"] = providers
    sys.modules["airflow.providers.mongo"] = mongo_pkg
    sys.modules["airflow.providers.mongo.hooks"] = mongo_hooks_pkg
    sys.modules["airflow.providers.mongo.hooks.mongo"] = mongo_module
    sys.modules["airflow.providers.standard"] = standard_pkg
    sys.modules["airflow.providers.standard.operators"] = operators_pkg
    sys.modules["airflow.providers.standard.operators.python"] = python_module
    sys.modules["airflow.sdk"] = sdk_module


def _load_module():
    module_name = "dags.kahi_test_sample"
    if module_name in sys.modules:
        del sys.modules[module_name]
    try:
        return importlib.import_module(module_name)
    except ModuleNotFoundError as exc:
        if exc.name and exc.name.startswith("airflow"):
            _install_airflow_stubs()
            return importlib.import_module(module_name)
        raise


@pytest.fixture(scope="module")
def kahi_module():
    return _load_module()


@pytest.fixture
def data_root(tmp_path):
    staff_dir = tmp_path / "STAFF"
    ciarp_dir = tmp_path / "CIARP"
    staff_dir.mkdir(parents=True)
    ciarp_dir.mkdir(parents=True)

    (staff_dir / "formato_talento_humano_udea_2024_11.xlsx").write_text("dummy", encoding="utf-8")
    (ciarp_dir / "formato_CIARP_UDEA_2024_12.xlsx").write_text("dummy", encoding="utf-8")
    return tmp_path


def _base_params(data_root: Path):
    return {
        "target_type": "author",
        "affiliation_ror": "https://ror.org/03bp5hc83",
        "data_root_dir": str(data_root),
        "mongo_uri_override": "mongodb://localhost:27017",
    }


def test_prepare_context_staff_person_requires_cedula(kahi_module, data_root):
    params = _base_params(data_root)
    params.update(
        {
            "run_staff_person": True,
            # Satisface validacion global de target_type=author, pero no la especifica de staff_person.
            "author_openalex": "https://openalex.org/A5089674963",
        }
    )

    with pytest.raises(ValueError, match="staff_person requires"):
        kahi_module.prepare_context(params=params)


def test_prepare_context_accepts_scholar_profile_url(kahi_module, data_root):
    params = _base_params(data_root)
    params.update(
        {
            "run_scholar_person": True,
            "author_scholar": "https://scholar.google.com/citations?user=EXHDEOsAAAAJ",
        }
    )

    context = kahi_module.prepare_context(params=params)
    assert "scholar_person" in context["selected_plugins"]
    assert context["identifiers"]["author_scholar"] == ["EXHDEOsAAAAJ"]


def test_prepare_context_scienti_works_requires_pair(kahi_module, data_root):
    params = _base_params(data_root)
    params.update(
        {
            "target_type": "work",
            "run_scienti_works": True,
            "work_scienti_cod_producto": "26",
            "es_url": "http://localhost:9200",
            "es_index": "kahi_es_test",
        }
    )

    # scienti_works activa dependencias automaticas de afiliaciones y DOI.
    with pytest.raises(ValueError, match="scienti_affiliations requires"):
        kahi_module.prepare_context(params=params)


def test_prepare_context_derives_minciencias_work_id_from_scienti_pair(kahi_module, data_root):
    params = _base_params(data_root)
    params.update(
        {
            "target_type": "work",
            "run_minciencias_opendata_works": True,
            "work_scienti_cod_rh": "0000026921",
            "work_scienti_cod_producto": "26",
            "es_url": "http://localhost:9200",
            "es_index": "kahi_es_test",
        }
    )

    context = kahi_module.prepare_context(params=params)
    assert "minciencias_opendata_works" in context["selected_plugins"]
    assert "ART-0000026921-26" in context["identifiers"]["work_dam_id_producto"]


def test_prepare_context_openalex_does_not_require_local_file_dirs(kahi_module, tmp_path):
    params = _base_params(tmp_path)
    params.update(
        {
            "run_openalex_person": True,
            "author_openalex": "https://openalex.org/A5089674963",
        }
    )

    context = kahi_module.prepare_context(params=params)

    assert "openalex_person" in context["selected_plugins"]
    assert context["affiliation"]["staff_source_file"] == ""
    assert context["affiliation"]["ciarp_source_file"] == ""


def test_prepare_context_ciarp_id_extracts_cedula(kahi_module, data_root):
    params = _base_params(data_root)
    params.update(
        {
            "target_type": "work",
            "run_ciarp_works": True,
            "work_ciarp_id": "111017-0000559695-1771787918",
            "es_url": "http://localhost:9200",
            "es_index": "kahi_es_test",
        }
    )

    context = kahi_module.prepare_context(params=params)
    assert "ciarp_works" in context["selected_plugins"]
    assert context["identifiers"]["work_ciarp_cedula"] == ["0000559695"]


class _TI:
    def __init__(self, values):
        self.values = values

    def xcom_pull(self, task_ids):
        return self.values[task_ids]


class _FakeMongoClient:
    def __init__(self, uri):
        self.uri = uri
        self.dropped = []

    def drop_database(self, db_name):
        self.dropped.append(db_name)


def test_prepare_context_auto_adds_work_affiliation_dependencies(kahi_module, data_root):
    params = _base_params(data_root)
    params.update(
        {
            "target_type": "work",
            "run_scienti_works": True,
            "work_scienti_cod_rh": "0000177733",
            "work_scienti_cod_producto": "95",
            "es_url": "http://localhost:9200",
            "es_index": "kahi_es_test",
        }
    )

    context = kahi_module.prepare_context(params=params)

    assert context["selected_plugins"][:2] == ["ror_affiliations", "scienti_affiliations"]
    assert "scienti_works/doi" in context["selected_plugins"]
    assert "scienti_works" in context["selected_plugins"]


def test_work_plugins_follow_reference_workflow_order(kahi_module, data_root):
    params = _base_params(data_root)
    params.update(
        {
            "target_type": "work",
            "run_openalex_works": True,
            "run_scienti_works": True,
            "run_ciarp_works": True,
            "run_scholar_works": True,
            "run_minciencias_opendata_works": True,
            "work_doi": "https://doi.org/10.1088/1475-7516/2013/04/044",
            "work_openalex": "https://openalex.org/W2047997675",
            "work_scienti_cod_rh": "0000177733",
            "work_scienti_cod_producto": "95",
            "work_dam_id_producto": "ART-0000177733-95",
            "work_ciarp_id": "5256-98554575-1756237221",
            "work_scholar_cid": "rEu5Cxt-1psJ",
            "es_url": "http://localhost:9200",
            "es_index": "kahi_es_test",
        }
    )

    context = kahi_module.prepare_context(params=params)

    assert context["selected_plugins"] == [
        "ror_affiliations",
        "openalex_affiliations",
        "scienti_affiliations",
        "minciencias_opendata_affiliations",
        "openalex_works/doi",
        "scienti_works/doi",
        "ciarp_works/doi",
        "scholar_works/doi",
        "openalex_works",
        "scienti_works",
        "ciarp_works",
        "scholar_works",
        "minciencias_opendata_works",
    ]


def test_unicity_person_is_ordered_before_work_plugins(kahi_module, data_root):
    params = _base_params(data_root)
    params.update(
        {
            "run_scholar_person": True,
            "run_unicity_person": True,
            "run_openalex_works_doi": True,
            "author_scholar": "https://scholar.google.com/citations?user=EXHDEOsAAAAJ",
            "work_doi": "https://doi.org/10.1088/1475-7516/2013/04/044",
        }
    )

    context = kahi_module.prepare_context(params=params)

    assert context["selected_plugins"] == [
        "ror_affiliations",
        "openalex_affiliations",
        "scholar_person",
        "unicity_person",
        "openalex_works/doi",
    ]


def test_doi_variants_include_url_and_bare_doi(kahi_module):
    assert set(kahi_module._doi_variants(["https://doi.org/10.1234/ABC"])) == {
        "https://doi.org/10.1234/abc",
        "10.1234/abc",
    }


def test_build_samples_drops_all_managed_databases_by_default(kahi_module, tmp_path, monkeypatch):
    params = _base_params(tmp_path)
    params.update(
        {
            "run_openalex_person": True,
            "author_openalex": "https://openalex.org/A5089674963",
            "artifacts_dir": str(tmp_path / "artifacts"),
        }
    )
    context = kahi_module.prepare_context(params=params)
    fake_clients = []

    def fake_client_factory(uri):
        client = _FakeMongoClient(uri)
        fake_clients.append(client)
        return client

    monkeypatch.setattr(kahi_module, "_resolve_pymongo", lambda: (fake_client_factory, object))
    monkeypatch.setattr(
        kahi_module,
        "_build_openalex_sample",
        lambda **kwargs: {
            "works": 1,
            "authors": 1,
            "institutions": 1,
            "sources": 1,
            "funders": 0,
            "publishers": 0,
        },
    )

    def fail_unselected_builder(**kwargs):
        raise AssertionError("unselected sample builder should not be called")

    monkeypatch.setattr(kahi_module, "_build_ror_sample", fail_unselected_builder)
    monkeypatch.setattr(kahi_module, "_build_scienti_sample", fail_unselected_builder)
    monkeypatch.setattr(kahi_module, "_build_scholar_sample", fail_unselected_builder)
    monkeypatch.setattr(kahi_module, "_build_orcid_sample", fail_unselected_builder)
    monkeypatch.setattr(kahi_module, "_build_minciencias_sample", fail_unselected_builder)

    result = kahi_module.build_samples(ti=_TI({"prepare_context": context}), run_id="unit_test")

    assert result["sample_counts"]["openalex"] == {
        "works": 1,
        "authors": 1,
        "institutions": 1,
        "sources": 1,
        "funders": 0,
        "publishers": 0,
    }
    assert set(result["sample_counts"]) == {
        "staff_sample_rows",
        "ciarp_sample_rows",
        "openalex",
    }
    assert fake_clients
    assert fake_clients[0].dropped == [
        "kahi_sample_test",
        "kahi_samples_log",
        "openalex_sample",
        "minciencias_sample",
        "orcid_sample",
        "scienti_sample",
        "scholar_sample",
        "ror_sample",
    ]


def test_cleanup_sample_databases_leaves_target_and_log(kahi_module, tmp_path, monkeypatch):
    params = _base_params(tmp_path)
    params.update(
        {
            "run_openalex_person": True,
            "author_openalex": "https://openalex.org/A5089674963",
        }
    )
    context = kahi_module.prepare_context(params=params)
    fake_clients = []

    def fake_client_factory(uri):
        client = _FakeMongoClient(uri)
        fake_clients.append(client)
        return client

    monkeypatch.setattr(kahi_module, "_resolve_pymongo", lambda: (fake_client_factory, object))

    result = kahi_module.cleanup_sample_databases(ti=_TI({"prepare_context": context}))

    assert result["cleanup_sample_databases"] is True
    assert fake_clients[0].dropped == [
        "openalex_sample",
        "minciencias_sample",
        "orcid_sample",
        "scienti_sample",
        "scholar_sample",
        "ror_sample",
    ]


def test_build_samples_rejects_unsafe_database_drop_names(kahi_module, tmp_path, monkeypatch):
    params = _base_params(tmp_path)
    params.update(
        {
            "run_openalex_person": True,
            "author_openalex": "https://openalex.org/A5089674963",
            "openalex_sample_db": "openalex",
            "allow_drop_databases": True,
            "artifacts_dir": str(tmp_path / "artifacts"),
        }
    )
    context = kahi_module.prepare_context(params=params)
    monkeypatch.setattr(kahi_module, "_resolve_pymongo", lambda: (_FakeMongoClient, object))

    with pytest.raises(ValueError, match="Refusing to drop unsafe database"):
        kahi_module.build_samples(ti=_TI({"prepare_context": context}), run_id="unit_test")


def test_build_workflow_includes_minciencias_opendata_works(kahi_module, data_root, tmp_path):
    params = _base_params(data_root)
    params.update(
        {
            "target_type": "work",
            "run_minciencias_opendata_works": True,
            "work_dam_id_producto": "ART-0000026921-26",
            "es_url": "http://localhost:9200",
            "es_index": "kahi_es_test",
            "minciencias_works_insert_all": True,
            "minciencias_works_thresholds": "70,92,97",
            "artifacts_dir": str(tmp_path / "artifacts"),
        }
    )
    context = kahi_module.prepare_context(params=params)

    result = kahi_module.build_workflow(
        ti=_TI(
            {
                "prepare_context": context,
                "build_samples": {
                    "staff_sample_path": str(tmp_path / "staff_sample.xlsx"),
                    "ciarp_sample_path": str(tmp_path / "ciarp_sample.xlsx"),
                    "sample_counts": {},
                },
            }
        ),
        run_id="unit_test",
    )
    workflow_path = Path(result["workflow_path"])
    assert workflow_path.exists()

    payload = yaml.safe_load(workflow_path.read_text(encoding="utf-8"))
    works_cfg = payload["workflow"]["minciencias_opendata_works"]

    assert works_cfg["database_name"] == context["sample_dbs"]["minciencias"]
    assert works_cfg["collection_name"] == "gruplac_production_data"
    assert works_cfg["insert_all"] is True
    assert works_cfg["thresholds"] == [70, 92, 97]
    assert works_cfg["es_url"] == "http://localhost:9200"
    assert works_cfg["es_index"] == "kahi_es_test"
