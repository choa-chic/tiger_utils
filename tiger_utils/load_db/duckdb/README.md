# duckdb TIGER/Line Shapefile Importer

## Usage
### Import downloaded shapefiles into DuckDB database:
`python -m tiger_utils.load_db.duckdb.importer`

`python -m tiger_utils.load_db.duckdb.importer all ./tiger_data/2025 --db geocoder.duckdb --state 13`

### Consolidate into single tables for each kind of geomtry
```sh
python -m tiger_utils.load_db.duckdb.consolidator
# Drop source tables after consolidation (saves space)
python -m tiger_utils.load_db.duckdb.consolidator --drop-source
```
