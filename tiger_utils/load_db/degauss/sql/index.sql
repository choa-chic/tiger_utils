-- Create indexes for all the relevant ways each table is queried
-- Referenced from DeGAUSS-org/geocoder implementation
PRAGMA temp_store=MEMORY;
PRAGMA journal_mode=MEMORY;
PRAGMA synchronous=OFF;
PRAGMA cache_size=500000;
PRAGMA count_changes=0;

CREATE INDEX IF NOT EXISTS place_city_phone_state_idx ON place (city_phone, state);
CREATE INDEX IF NOT EXISTS place_zip_priority_idx ON place (zip, priority);
CREATE INDEX IF NOT EXISTS feature_street_phone_zip_idx ON feature (street_phone, zip);
CREATE INDEX IF NOT EXISTS feature_edge_fid_idx ON feature_edge (fid);
CREATE INDEX IF NOT EXISTS range_tlid_idx ON range (tlid);
