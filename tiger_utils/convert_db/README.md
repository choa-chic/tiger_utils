# Conversion between database formats

## DuckDB to DeGAUSS
```sh 
python -m tiger_utils.convert_db.duckdb_to_degauss \
    --source database/geocoder.duckdb \
    --target database/degauss.duckdb \
    --state 13
```