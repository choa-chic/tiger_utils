"""
shp_to_temp_tables.py
Loads TIGER/Line shapefiles into temporary tables for transformation.
Referenced from DeGAUSS-org/geocoder implementation.
"""

import sqlite3
from pathlib import Path
import fiona
from shapely.geometry import shape


def load_edges_to_temp(shp_path: str, db_path: str) -> None:
    """
    Load edges shapefile into tiger_edges temporary table.
    """
    with fiona.open(shp_path) as src:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        
        for feat in src:
            props = feat['properties']
            geom = shape(feat['geometry'])
            wkb = geom.wkb if feat['geometry'] else None
            
            cur.execute('''
                INSERT INTO tiger_edges (
                    statefp, countyfp, tlid, tfidl, tfidr, mtfcc, fullname, smid,
                    lfromadd, ltoadd, rfromadd, rtoadd, zipl, zipr,
                    featcat, hydroflg, railflg, roadflg, olfflg, passflg,
                    divroad, exttyp, ttyp, deckedroad, artpath, persist,
                    gcseflg, offsetl, offsetr, tnidf, tnidt, the_geom
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                props.get('STATEFP'), props.get('COUNTYFP'), props.get('TLID'),
                props.get('TFIDL'), props.get('TFIDR'), props.get('MTFCC'),
                props.get('FULLNAME'), props.get('SMID'),
                props.get('LFROMADD'), props.get('LTOADD'),
                props.get('RFROMADD'), props.get('RTOADD'),
                props.get('ZIPL'), props.get('ZIPR'),
                props.get('FEATCAT'), props.get('HYDROFLG'), props.get('RAILFLG'),
                props.get('ROADFLG'), props.get('OLFFLG'), props.get('PASSFLG'),
                props.get('DIVROAD'), props.get('EXTTYP'), props.get('TTYP'),
                props.get('DECKEDROAD'), props.get('ARTPATH'), props.get('PERSIST'),
                props.get('GCSEFLG'), props.get('OFFSETL'), props.get('OFFSETR'),
                props.get('TNIDF'), props.get('TNIDT'), wkb
            ))
        
        conn.commit()
        conn.close()
    print(f"Loaded {shp_path} into tiger_edges temporary table")


def load_featnames_to_temp(dbf_path: str, db_path: str) -> None:
    """
    Load featnames DBF into tiger_featnames temporary table.
    """
    with fiona.open(dbf_path) as src:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        
        for feat in src:
            props = feat['properties']
            
            cur.execute('''
                INSERT INTO tiger_featnames (
                    tlid, fullname, name, predirabrv, pretypabrv, prequalabr,
                    sufdirabrv, suftypabrv, sufqualabr, predir, pretyp, prequal,
                    sufdir, suftyp, sufqual, linearid, mtfcc, paflag
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                props.get('TLID'), props.get('FULLNAME'), props.get('NAME'),
                props.get('PREDIRABRV'), props.get('PRETYPABRV'), props.get('PREQUALABR'),
                props.get('SUFDIRABRV'), props.get('SUFTYPABRV'), props.get('SUFQUALABR'),
                props.get('PREDIR'), props.get('PRETYP'), props.get('PREQUAL'),
                props.get('SUFDIR'), props.get('SUFTYP'), props.get('SUFQUAL'),
                props.get('LINEARID'), props.get('MTFCC'), props.get('PAFLAG')
            ))
        
        conn.commit()
        conn.close()
    print(f"Loaded {dbf_path} into tiger_featnames temporary table")


def load_addr_to_temp(dbf_path: str, db_path: str) -> None:
    """
    Load addr DBF into tiger_addr temporary table.
    """
    with fiona.open(dbf_path) as src:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        
        for feat in src:
            props = feat['properties']
            
            cur.execute('''
                INSERT INTO tiger_addr (
                    tlid, fromhn, tohn, side, zip, plus4, fromtyp, totyp,
                    fromarmid, toarmid, arid, mtfcc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                props.get('TLID'), props.get('FROMHN'), props.get('TOHN'),
                props.get('SIDE'), props.get('ZIP'), props.get('PLUS4'),
                props.get('FROMTYP'), props.get('TOTYP'),
                props.get('FROMARMID'), props.get('TOARMID'),
                props.get('ARID'), props.get('MTFCC')
            ))
        
        conn.commit()
        conn.close()
    print(f"Loaded {dbf_path} into tiger_addr temporary table")


def load_tiger_files_to_temp(shp_dir: str, db_path: str, state: str = None) -> None:
    """
    Load all TIGER files from directory into temporary tables.
    Follows DeGAUSS-org/geocoder import_tiger pattern.
    """
    import re
    from pathlib import Path
    
    shp_dir = Path(shp_dir)
    
    # Find and load edges files
    for shp_file in shp_dir.rglob("*_edges.shp"):
        name = shp_file.name
        if state:
            if not re.search(r"tl_\d{4}_(0?%s)[0-9]{3}_" % re.escape(state), name):
                continue
        load_edges_to_temp(str(shp_file), db_path)
    
    # Find and load featnames files
    for dbf_file in shp_dir.rglob("*_featnames.dbf"):
        name = dbf_file.name
        if state:
            if not re.search(r"tl_\d{4}_(0?%s)[0-9]{3}_" % re.escape(state), name):
                continue
        load_featnames_to_temp(str(dbf_file), db_path)
    
    # Find and load addr files
    for dbf_file in shp_dir.rglob("*_addr.dbf"):
        name = dbf_file.name
        if state:
            if not re.search(r"tl_\d{4}_(0?%s)[0-9]{3}_" % re.escape(state), name):
                continue
        load_addr_to_temp(str(dbf_file), db_path)
