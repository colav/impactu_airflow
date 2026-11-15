# Identifier resolver

`impactu_identifier_resolver` tells Kahi what a disputed identifier really
identifies, so the Kahi plugin that settles the disputes never needs the network.

A dispute is an identifier that Kahi found on more than one entity: a DOI on
several works (a journal DOI copied to its articles, a truncated DOI, the DOI of
another paper) or an ORCID iD on several people.

## The queue

Kahi (`identifier_disputes/collect`) queues each identifier once:

```json
{"_id": "10.18041/2382-3240", "status": "pending", "requested_at": "..."}
```

- collection `doi`: `_id` is the bare DOI in lower case (no `https://doi.org/`);
- collection `orcid`: `_id` is the ORCID iD (`0000-0002-1972-1006`).

The `identifier_resolver_capture` DAG takes every document with status `pending`
or `error` and writes the answer into the same document:

| status | meaning | asked again |
|---|---|---|
| `pending` | queued, not requested yet | yes |
| `ok` | resolved | no |
| `not_found` | the identifier does not exist (or is malformed) | only with `refresh_not_found` |
| `error` | the request failed (timeout, 429/5xx after retries) | yes, on the next run |

The queue is the checkpoint: a run that stops halfway leaves the rest `pending`,
and running it again with nothing queued makes no request. After 20 consecutive
errors the run stops, so an outage does not mark the whole queue as `error`.
Every run leaves a summary in `runs`.

## DOI

1. Crossref `/works/{doi}`. Most DOIs end here.
2. On 404, `doi.org/doiRA/{doi}` names the registration agency or says the DOI
   does not exist (`status: not_found`, `detail: "DOI does not exist"`).
3. DataCite DOIs are read from the DataCite API; the other agencies through
   doi.org content negotiation (CSL JSON). That request ends at the agency's own
   server: when it publishes no metadata or does not answer, the DOI is closed
   as registered without metadata (`status: ok`, `source: null`, `type: null`,
   `detail: "metadata unavailable: ..."`) instead of being retried forever.

```json
{
  "_id": "10.18041/2382-3240",
  "status": "ok",
  "agency": "Crossref",
  "source": "crossref",
  "type": "journal",
  "titles": ["Saber, Ciencia y Libertad"],
  "subtitles": [],
  "container_titles": [],
  "issn": ["1794-7154", "2382-3240"],
  "isbn": [],
  "year": null,
  "authors": [],
  "author_count": 0,
  "attempts": 1,
  "retrieved_at": "...",
  "resolver_version": "0.1.0"
}
```

`type` is the registry's own value (`journal`, `journal-issue`, `journal-article`,
`book`, DataCite's `resourceTypeGeneral`, ...). `titles` keeps the title and the
original title exactly as registered, markup included; `authors` keeps at most
50 names and `author_count` the real number.

## ORCID

`pub.orcid.org/v3.0/{orcid}/person` (public data, no token): `given_names`,
`family_name`, `credit_name` and `other_names`.

## Running it

The DAG is the scheduled entry point. Next to a local Kahi run:

```bash
MONGODB_URI=mongodb://localhost:27017 \
python -m extract.identifier_resolver.identifier_resolver_extractor --kinds doi orcid
```

`--mailto` (or `CROSSREF_MAILTO`) adds a contact to the User-Agent for the
Crossref polite pool. About 0.35 s per DOI: 12 000 DOIs take a bit over an hour.
