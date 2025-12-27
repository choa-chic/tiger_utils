-- Initialize the database tables
-- Referenced from DeGAUSS-org/geocoder implementation
-- 
-- 'place' contains the gazetteer of place names
CREATE TABLE IF NOT EXISTS place(
    zip CHAR(5),
    city VARCHAR(100),
    state CHAR(2),
    city_phone VARCHAR(5),
    lat NUMERIC(9,6),
    lon NUMERIC(9,6),
    status CHAR(1),
    fips_class CHAR(2),
    fips_place CHAR(7),
    fips_county CHAR(5),
    priority CHAR(1)
);

-- 'edge' stores the line geometries and their IDs
CREATE TABLE IF NOT EXISTS edge (
    tlid INTEGER PRIMARY KEY,
    geometry BLOB
);

-- 'feature' stores the name(s) and ZIP(s) of each edge
CREATE TABLE IF NOT EXISTS feature (
    fid INTEGER PRIMARY KEY,
    street VARCHAR(100),
    street_phone VARCHAR(5),
    paflag BOOLEAN,
    zip CHAR(5)
);

-- 'feature_edge' links each edge to a feature
CREATE TABLE IF NOT EXISTS feature_edge (
    fid INTEGER,
    tlid INTEGER
);

-- 'range' stores the address range(s) for each edge
CREATE TABLE IF NOT EXISTS range (
    tlid INTEGER,
    fromhn INTEGER,
    tohn INTEGER,
    prenum VARCHAR(12),
    zip CHAR(5),
    side CHAR(1)
);
