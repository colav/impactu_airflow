import sys
from pathlib import Path
from typing import Any

import mongomock

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from config.oxomoc_colombia_config import endpoints  # noqa: E402
from dags.dspace_capture import persist_repository_identity, persist_repository_ror  # noqa: E402


def test_persist_repository_ror_enriches_identity_document():
    client: Any = mongomock.MongoClient()
    client.oxomoc_colombia.dspace_fucsalud_identity.insert_one(
        {"base_url": "https://repositorio.fucsalud.edu.co/oai/request"}
    )

    matched = persist_repository_ror(
        client,
        "oxomoc_colombia",
        "dspace_fucsalud",
        "https://ror.org/02yr3f298",
    )

    identity = client.oxomoc_colombia.dspace_fucsalud_identity.find_one()
    assert matched == 1
    assert identity["ror"] == "https://ror.org/02yr3f298"


def test_persist_repository_ror_does_not_create_missing_identity():
    client: Any = mongomock.MongoClient()

    matched = persist_repository_ror(
        client, "oxomoc_colombia", "dspace_missing", "https://ror.org/example"
    )

    assert matched == 0
    assert client.oxomoc_colombia.dspace_missing_identity.count_documents({}) == 0


def test_corrected_repository_rors_are_stable():
    expected = {
        "dspace_uniandes": "https://ror.org/02mhbdp94",
        "dspace_pedagogica": "https://ror.org/04cmc9894",
        "dspace_udes": "https://ror.org/04n6qsf08",
        "dspace_uac": "https://ror.org/05gqfsf87",
        "dspace_usta": "https://ror.org/01x628269",
        "dspace_lasalle": "https://ror.org/0474gxy81",
        "dspace_univalle": "https://ror.org/00jb9vg53",
        "dspace_ibero": "https://ror.org/04331z002",
        "dspace_uniajc": "https://ror.org/030v7ct83",
        "dspace_universidadean": "https://ror.org/00tncsy16",
        "dspace_minciencias": "https://ror.org/03fd5ne08",
        "dspace_uajs": "https://ror.org/03ven1671",
        "dspace_icanh": "https://ror.org/04nagxm86",
    }

    assert {name: endpoints[name]["ror"] for name in expected} == expected
    assert "dspace_upn" not in endpoints
    for name in ("dspace_ccb", "dspace_car", "dspace_artesaniasdecolombia", "dspace_cinde"):
        assert endpoints[name]["ror"] == ""


def test_persist_repository_identity_stores_ror_and_openalex_source():
    client: Any = mongomock.MongoClient()
    client.oxomoc_colombia.dspace_areandina_identity.insert_one(
        {"base_url": "https://digitk.areandina.edu.co"}
    )

    matched = persist_repository_identity(
        client,
        "oxomoc_colombia",
        "dspace_areandina",
        {
            "ror": "https://ror.org/01hb6tn62",
            "openalex_source": "https://openalex.org/S7407055425",
            "url": "x",
        },
    )

    identity = client.oxomoc_colombia.dspace_areandina_identity.find_one()
    assert matched == 1
    assert identity["ror"] == "https://ror.org/01hb6tn62"
    assert identity["openalex_source"] == "https://openalex.org/S7407055425"
    assert "url" not in identity


def test_configured_openalex_sources_are_openalex_source_ids():
    configured = {
        name: cfg["openalex_source"]
        for name, cfg in endpoints.items()
        if cfg.get("openalex_source")
    }
    assert len(configured) >= 68
    assert all(value.startswith("https://openalex.org/S") for value in configured.values())
    assert configured["dspace_areandina"] == "https://openalex.org/S7407055425"
