# Wikidata

`wikidata.data` holds Wikidata entities in the shape of the JSON dump
(`id`, `type`, `labels`, `descriptions`, `aliases`, `claims`, `sitelinks`,
`pageid`, `ns`, `title`, `lastrevid`, `modified`). The first load came from a
full dump (see <https://github.com/colav-playground/wikidata_laod>).

Kahi (`Kahi_wikidata_affiliations`) only reads the entities that ROR links to an
organization, so the `wikidata_capture` DAG refreshes just those instead of
reloading the dump:

1. collects the QIDs from `ror.ror_stage` (`external_ids[type=wikidata]`);
2. requests them from `wbgetentities` in batches of 50, with a delay between
   requests, `maxlag=5` and retries on 429/5xx;
3. upserts each entity by `id`, unchanged from the API response.

Checkpoint `wikidata_entities` stores the last QID processed, so a failed run
resumes where it stopped; it is cleared when the run finishes. Missing entities
are counted. When Wikidata merged a ROR item into another one, the entity is
stored under the target id and `wikidata.redirects` gets
`{"id": <ROR QID>, "redirect": <target QID>}` so `Kahi_wikidata_affiliations`
can follow it without calling the API.
