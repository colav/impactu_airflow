
## Repository metadata persisted in `*_identity`

The capture DAG copies `ror` and `openalex_source` from each endpoint in
`config/oxomoc_colombia_config.py` into the repository's `*_identity`
document. Kahi (`Kahi_dspace_works`) reads both from there: the ROR gives the
institution of the repository and the OpenAlex source id lets it reuse the
`sources` document OpenAlex works already point to. To find the OpenAlex
source of a new repository run `scripts/build_repository_sources_map.py` and
review `repository_sources_candidates.csv` before writing the id in the config.
