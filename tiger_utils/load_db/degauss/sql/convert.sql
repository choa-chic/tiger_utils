-- Transform temporary tables into final tables
-- Referenced from DeGAUSS-org/geocoder implementation
-- 
-- Note: This version is adapted for SQLite without custom C extensions
-- The metaphone() and compress_wkb_line() functions from the original are replaced
-- with simpler alternatives or omitted where not critical for basic functionality

BEGIN;

-- Start by indexing the temporary tables created from the input data
CREATE INDEX IF NOT EXISTS featnames_tlid ON tiger_featnames (tlid);
CREATE INDEX IF NOT EXISTS addr_tlid ON tiger_addr (tlid);
CREATE INDEX IF NOT EXISTS edges_tlid ON tiger_edges (tlid);

-- Generate a summary table matching each edge to one or more ZIPs
-- for those edges that are streets and have a name
CREATE TEMPORARY TABLE linezip AS
    SELECT DISTINCT tlid, zip FROM (
        SELECT tlid, zip FROM tiger_addr a
        UNION
        SELECT tlid, zipr AS zip FROM tiger_edges e
           WHERE e.mtfcc LIKE 'S%' AND zipr <> "" AND zipr IS NOT NULL
        UNION
        SELECT tlid, zipl AS zip FROM tiger_edges e
           WHERE e.mtfcc LIKE 'S%' AND zipl <> "" AND zipl IS NOT NULL
    ) AS whatever;

CREATE INDEX linezip_tlid ON linezip (tlid);

-- Generate features from the featnames table for each desired edge
-- Note: In the original, metaphone(name,5) was used to compute phonetic hash
-- For simplicity, we use substr(lower(name),1,5) as a basic approximation
CREATE TEMPORARY TABLE feature_bin (
    fid INTEGER PRIMARY KEY AUTOINCREMENT,
    street VARCHAR(100),
    street_phone VARCHAR(5),
    paflag BOOLEAN,
    zip CHAR(5)
);

INSERT OR IGNORE INTO sqlite_sequence (name, seq) VALUES ('feature_bin', 0);
UPDATE sqlite_sequence
    SET seq = (SELECT COALESCE(MAX(fid), 0) FROM feature)
    WHERE name = "feature_bin";

INSERT INTO feature_bin
    SELECT DISTINCT NULL, fullname, substr(lower(name), 1, 5), paflag, zip
        FROM linezip l, tiger_featnames f
        WHERE l.tlid = f.tlid AND name <> "" AND name IS NOT NULL;

CREATE INDEX feature_bin_idx ON feature_bin (street, zip);

INSERT INTO feature_edge
    SELECT DISTINCT fid, f.tlid
        FROM linezip l, tiger_featnames f, feature_bin b
        WHERE l.tlid = f.tlid AND l.zip = b.zip
          AND f.fullname = b.street AND f.paflag = b.paflag;

INSERT INTO feature
    SELECT * FROM feature_bin;

-- Generate edges from the edges table for each desired edge
-- Note: compress_wkb_line() from original is omitted; we store WKB as-is
INSERT OR IGNORE INTO edge
    SELECT l.tlid, the_geom FROM
        (SELECT DISTINCT tlid FROM linezip) AS l, tiger_edges e
        WHERE l.tlid = e.tlid AND fullname <> "" AND fullname IS NOT NULL;

-- Generate all ranges from the addr table
-- Note: digit_suffix() and nondigit_prefix() functions from original are replaced
-- with simpler CAST operations and SUBSTR for basic numeric extraction
INSERT INTO range
    SELECT 
        tlid,
        -- Extract numeric part: try to CAST, if fails use NULL
        CAST(
            CASE 
                WHEN fromhn GLOB '*[0-9]*' THEN 
                    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                        fromhn, 'A', ''), 'B', ''), 'C', ''), 'D', ''), 'E', ''), 'F', ''), 'G', ''), 'H', ''), 'I', ''), 'J', '')
                ELSE NULL
            END AS INTEGER
        ) as fromhn_int,
        CAST(
            CASE 
                WHEN tohn GLOB '*[0-9]*' THEN 
                    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                        tohn, 'A', ''), 'B', ''), 'C', ''), 'D', ''), 'E', ''), 'F', ''), 'G', ''), 'H', ''), 'I', ''), 'J', '')
                ELSE NULL
            END AS INTEGER
        ) as tohn_int,
        -- Extract non-numeric prefix: basic implementation
        CASE 
            WHEN fromhn GLOB '[0-9]*' THEN ''
            ELSE SUBSTR(fromhn, 1, 1)
        END as prenum,
        zip,
        side
    FROM tiger_addr;

END;

-- Clean up temporary tables
DROP TABLE IF EXISTS feature_bin;
DROP TABLE IF EXISTS linezip;
DROP TABLE IF EXISTS tiger_addr;
DROP TABLE IF EXISTS tiger_featnames;
DROP TABLE IF EXISTS tiger_edges;
