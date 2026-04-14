"""Colombia cut via Minciencias (Gruplac/CvLAC) data using Elasticsearch similarity."""

from __future__ import annotations

from bson.objectid import ObjectId
from joblib import Parallel, delayed
from mohan.Similarity import Similarity
from pymongo import MongoClient
from pymongo.collection import Collection
from thefuzz import fuzz, process
from unidecode import unidecode

# ---------------------------------------------------------------------------
# Source databases — edit to match your environment
# ---------------------------------------------------------------------------
YUKU_DB = "yuku"
YUKU_GRCOL = "gruplac_production_data"
YUKU_CVCOL = "cvlac_stage"

# Similarity thresholds
AUTHOR_THD = 65
PAPER_THD_LOW = 90
PAPER_THD_HIGH = 94

BIBLIO = [
    "Publicaciones editoriales no especializadas",
    "Notas científica",
    "Informe Final de Investigación",
    "Capítulos de libro de investigación",
    "Libros de investigación",
    "Artículos de investigación",
    "Libros de Formación",
    "Libros",
    "Tesis de doctorado",
    "Capítulos de libro",
    "Documento de trabajo",
    "Tesis de pregrado",
    "Informe técnico final",
    "Artículos",
    "Edicion",
    "Manuales y Guías Especializadas",
    "Boletín divulgativo de resultado de investigación",
    "Libros de Divulgación de investigación y/o Compilación de Divulgación",
    "Tesis de maestria",
    "Generación de contenido impresa",
]

_PIPELINE = [
    {"$match": {"nme_producto_pd": {"$exists": True}}},
    {"$match": {"nme_tipologia_pd": {"$in": BIBLIO}}},
    {"$group": {"_id": "$id_producto_pd", "originalDoc": {"$first": "$$ROOT"}}},
    {"$replaceRoot": {"newRoot": "$originalDoc"}},
]

_PIPELINE_ZEROS = [
    {"$match": {"nme_producto_pd": {"$exists": True}}},
    {"$match": {"nme_tipologia_pd": {"$in": BIBLIO}}},
    {"$match": {"id_producto_pd": "0000000000"}},
]


def _str_normalize(word: str) -> str:
    return str(unidecode(word)).lower().strip().replace(".", "")


def _check_work(
    openalex_in: Collection,
    openalex_out: Collection,
    title_work: str,
    authors: list[str],
    response: dict,
) -> int:
    author_found = False
    if authors and authors[0] != "":
        _authors = [_str_normalize(a) for a in response["_source"]["authors"]]
        scores = process.extract(_str_normalize(authors[0]), _authors, scorer=fuzz.partial_ratio)
        for score in scores:
            if score[1] >= AUTHOR_THD:
                author_found = True
                break

    if response["_source"]["title"]:
        score = fuzz.WRatio(
            _str_normalize(title_work), _str_normalize(response["_source"]["title"])
        )
        threshold = PAPER_THD_LOW if author_found else PAPER_THD_HIGH
        if score >= threshold:
            oid = ObjectId(response["_id"])
            if openalex_out["works"].find_one({"_id": oid}) is None:
                oa_work = openalex_in["works"].find_one({"_id": oid})
                try:
                    if oa_work is not None:
                        openalex_out["works"].insert_one(oa_work)
                except Exception as e:
                    print(e)
                    print("Parallel duplicate insert — not a problem, continuing.")
            return 1
    else:
        print(response)
    return 0


def _process_one(
    openalex_in: Collection,
    openalex_out: Collection,
    cv_col: Collection,
    s: Similarity,
    work: dict,
) -> int:
    title_work = work.get("nme_producto_pd", "")
    authors: list[str] = []
    if work.get("id_persona_pd"):
        author = cv_col.find_one(
            {"id_persona_pr": work["id_persona_pd"]},
            {"datos_generales.Nombre": 1},
        )
        if author:
            authors.append(author["datos_generales"]["Nombre"])
    if not authors and len(title_work) < 30:
        return 0
    if title_work:
        responses = s.search_work(
            title=title_work,
            source="",
            year="0",
            authors=authors,
            volume="",
            issue="",
            page_start="",
            page_end="",
            use_es_thold=True,
            es_thold=0,
            hits=20,
        )
        if responses:
            for resp in responses:
                if _check_work(openalex_in, openalex_out, title_work, authors, resp):
                    return 1
    return 0


def colombia_cut_minciencias(
    db_in: str,
    db_out: str,
    es_index: str,
    jobs: int = 20,
    backend: str = "threading",
    client: MongoClient | None = None,
    es_uri: str = "http://localhost:9200",
    es_auth: tuple[str, str] = ("elastic", "colav"),
) -> None:
    """Insert works from Gruplac/CvLAC into the Colombia cut via ES similarity."""
    c: MongoClient = client if client is not None else MongoClient()
    gr_col = c[YUKU_DB][YUKU_GRCOL]
    data = list(gr_col.aggregate(_PIPELINE, allowDiskUse=True))
    data.extend(list(gr_col.aggregate(_PIPELINE_ZEROS, allowDiskUse=True)))

    openalex_in = c[db_in]
    openalex_out = c[db_out]
    cv_col = c[YUKU_DB][YUKU_CVCOL]
    s = Similarity(es_index, es_uri=es_uri, es_auth=es_auth)

    Parallel(n_jobs=jobs, backend="threading", verbose=10, batch_size=4)(
        delayed(_process_one)(openalex_in, openalex_out, cv_col, s, work) for work in data
    )
