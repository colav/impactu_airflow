"""Tests for the Minciencias sources of the OpenAlexCO cut (steps 3 and 4)."""

from __future__ import annotations

import mongomock
import pytest

from extract.openalexco.minciencias_source import latest_minciencias_collection


@pytest.fixture
def client():
    return mongomock.MongoClient()


def _create(db, *names: str) -> None:
    for name in names:
        db[name].insert_one({"_": 1})


def test_latest_picks_newest_date_then_version(client):
    db = client["dam"]
    _create(
        db,
        "minciencias_works_v05_20261017",
        "minciencias_works_v05_20261017_filtered",
        "minciencias_works_v04_20260901",
        "minciencias_works_v06_20261017",  # same date, higher version
        "minciencias_works_v09_20250101",  # higher version, older date
        "minciencias_works_v05_20261017_filtered_backup",  # not the pattern
        "cvlac_stage_raw",
    )
    assert latest_minciencias_collection(db, filtered=False) == "minciencias_works_v06_20261017"
    assert (
        latest_minciencias_collection(db, filtered=True)
        == "minciencias_works_v05_20261017_filtered"
    )


def test_latest_raises_when_missing(client):
    db = client["dam"]
    _create(db, "minciencias_works_v05_20261017")
    with pytest.raises(RuntimeError, match="filtered"):
        latest_minciencias_collection(db, filtered=True)


def test_dois_from_latest_filtered(client):
    from extract.openalexco.colombia_cut_dois import _dois_from_minciencias

    db = client["dam"]
    db["minciencias_works_v04_20260901_filtered"].insert_one(
        {"external_ids": [{"source": "doi", "id": "https://doi.org/10.1/old"}]}
    )
    db["minciencias_works_v05_20261017_filtered"].insert_many(
        [
            {
                "external_ids": [
                    {"source": "doi", "id": "https://doi.org/10.1/a"},
                    {"source": "doi", "id": "https://doi.org/10.1/b"},
                    {"source": "issn", "id": "1234-5678"},
                ]
            },
            {"external_ids": [{"source": "doi", "id": "https://doi.org/10.1/a"}]},
            {"external_ids": [{"source": "minciencias", "id": "ART-1"}]},
        ]
    )
    assert sorted(_dois_from_minciencias(client, "dam")) == [
        "https://doi.org/10.1/a",
        "https://doi.org/10.1/b",
    ]


def test_similarity_candidates_without_doi_and_bibliographic_types(client):
    pytest.importorskip("mohan")
    from extract.openalexco.colombia_cut_minciencias import load_minciencias_candidates

    def work(title, doi=None, impactu_type="Artículo de revista", author="Ana Pérez"):
        doc = {
            "titles": [{"title": title, "source": "minciencias"}],
            "types": [{"source": "impactu", "type": impactu_type}],
            "authors": [{"full_name": author}],
        }
        if doi is not None:
            doc["doi"] = doi
        return doc

    client["dam"]["minciencias_works_v05_20261017"].insert_many(
        [
            work("no doi field"),
            work("empty doi", doi=""),
            work("has doi", doi="https://doi.org/10.1/x"),
            work("software", impactu_type="Código fuente"),
        ]
    )
    name, data = load_minciencias_candidates(client, "dam")
    assert name == "minciencias_works_v05_20261017"
    assert sorted(d["title"] for d in data) == ["empty doi", "no doi field"]
    assert data[0]["author"] == "Ana Pérez"


@pytest.mark.parametrize(
    ("m_title", "oa_title", "author", "year", "oa_year", "expected"),
    [
        # true matches from the precision sample
        (
            "La Tierra Que Atardece. Ensayos Sobre La Modernidad",
            "la tierra que atardece ensayos sobre la modernidad",
            True,
            1998,
            2019,
            True,
        ),
        (
            "Medidor vestible de temperatura: una aplicación a la telemedicina",
            "medidor vestible de temperatura: una aplicacion a la telemedicina",
            False,
            2014,
            2015,
            True,
        ),
        (
            "Dynamic capacity of adaptation",
            "capacidad dinamica de adaptacion - dynamic capacity of adaptation",
            True,
            2018,
            2018,
            True,
        ),
        (
            "Proposal for a VoD Service Supported in a Context-Based Architecture",
            "proposal for a vod service supported in a context-based architecture",
            False,
            None,
            2018,
            True,
        ),
        # false positives from the precision sample
        ("Ictericia", "ictericia", False, 1991, 2024, False),
        ("Enfermedad de Parkinson", "enfermedad de parkinson", False, 2005, 2023, False),
        ("Aves sin nido o el deseo del Estado-nación", "aves sin nido", True, 2015, 1982, False),
        ("El poeta y la tradición de ruptura", "tradicion de la ruptura", False, 2006, 2021, False),
        (
            "Diseño estratégico para la transformación organizacional en tiempos de crisis",
            "diseno estrategico para la transformacion organizacional en tiempos de crisis",
            False,
            2016,
            2025,
            False,
        ),
    ],
)
def test_plausible_match(m_title, oa_title, author, year, oa_year, expected):
    pytest.importorskip("mohan")
    from extract.openalexco.colombia_cut_minciencias import _plausible_match

    assert _plausible_match(m_title, oa_title, author, year, oa_year) is expected
