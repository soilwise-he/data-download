# ----------------------------------------------------------------------------
#  Copyright (C) 2026 ISRIC - World Soil Information
#  Produced in the Scope of the SoilWISE Project
#  SoilWISE is funded by the European Union’s Horizon Europe research and 
#  innovation programme under grant agreement No 101056973.
# ----------------------------------------------------------------------------

from cProfile import label
import datetime

from rdflib import Graph, Namespace, BNode, URIRef, Literal
from rdflib.namespace import RDF, RDFS, XSD, SKOS, DCTERMS
from dateutil.parser import parse as dtparse
import struct
from app.utils.graph import (
    first_value, node_to_uri, label_for, types_text_for
)

from contextlib import contextmanager

@contextmanager
def get_cursor(conn):
    cur = conn.cursor()
    try:
        yield cur
    finally:
        cur.close()

# Namespaces
SOSA = Namespace("http://www.w3.org/ns/sosa/")
GEO  = Namespace("http://www.opengis.net/ont/geosparql#")
WGS  = Namespace("http://www.w3.org/2003/01/geo/wgs84_pos#")
QUDT = Namespace("http://qudt.org/schema/qudt/")


def dbinit(conn):
    # Create an in-memory SQLite database
    # initialise with various tables, also geopackage related
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE observation (
        observation_uri TEXT PRIMARY KEY,
        result_id INTEGER,
        phenomenon_time TEXT,
        procedure_id INTEGER,
        property_id INTEGER,
        foi_id INTEGER,
        FOREIGN KEY(result_id) REFERENCES result(id)
    )
    """)

    cur.execute("""
    CREATE TABLE result (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        result_uri TEXT,
        value REAL,
        unit_of_measure_id TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE procedure (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uri TEXT,
        label TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE unitofmeasure (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uri TEXT,
        label TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE property (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uri TEXT,
        label TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE feature_of_interest (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uri TEXT,
        label TEXT,
        type TEXT,
        geom BLOB NOT NULL
    )
    """)

    cur.execute("""
CREATE TABLE gpkg_spatial_ref_sys (
    srs_name TEXT NOT NULL,
    srs_id INTEGER NOT NULL PRIMARY KEY,
    organization TEXT NOT NULL,
    organization_coordsys_id INTEGER NOT NULL,
    definition TEXT NOT NULL,
    description TEXT
)""")

    cur.execute("""
CREATE TABLE gpkg_contents (
    table_name TEXT NOT NULL PRIMARY KEY,
    data_type TEXT NOT NULL,
    identifier TEXT UNIQUE,
    description TEXT DEFAULT '',
    last_change DATETIME NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    min_x DOUBLE, min_y DOUBLE,
    max_x DOUBLE, max_y DOUBLE,
    srs_id INTEGER,
    FOREIGN KEY (srs_id) REFERENCES gpkg_spatial_ref_sys(srs_id)
)""")

    cur.execute("""
CREATE TABLE gpkg_geometry_columns (
    table_name TEXT NOT NULL,
    column_name TEXT NOT NULL,
    geometry_type_name TEXT NOT NULL,
    srs_id INTEGER NOT NULL,
    z TINYINT NOT NULL,
    m TINYINT NOT NULL,
    PRIMARY KEY (table_name, column_name),
    FOREIGN KEY (srs_id) REFERENCES gpkg_spatial_ref_sys(srs_id)
)""")
    cur.execute("""
INSERT INTO gpkg_spatial_ref_sys (
    srs_name, srs_id, organization, organization_coordsys_id, definition, description
) VALUES (
    'WGS 84 geodetic',
    4326,
    'EPSG',
    4326,
    'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563]]]',
    'longitude/latitude coordinates in decimal degrees'
)""")

    cur.execute("""
INSERT INTO gpkg_contents (
    table_name,
    data_type,
    identifier,
    description,
    srs_id
) VALUES (
    'feature_of_interest',
    'features',
    'feature_of_interest',
    'Feature of interest points',
    4326
)""")

    cur.execute("""
INSERT INTO gpkg_geometry_columns (
    table_name,
    column_name,
    geometry_type_name,
    srs_id,
    z,
    m
) VALUES (
    'feature_of_interest',
    'geom',
    'POINT',
    4326,
    0,
    0
)""")



def to_gpkg_geom(point, srs_id=4326):
    # geom_blob = to_gpkg_geom(Point(5.0, 52.0))
    wkb = point.wkb
    magic = b"GP" 
    version = b"\x00"
    flags = b"\x01"
    srs = struct.pack("<I", srs_id)
    header = magic + version + flags + srs
    return header + wkb

# get sqlite for rdf
def rdf2rdb(g, conn): 

    dbinit(conn)
    # iterate over explicit observation subjects
    obs_count = 0
    for obs in g.subjects(RDF.type, SOSA.Observation):
        obs_count += 1
        obs_uri = node_to_uri(obs)
        # Phenomenon time (literal)
        phen_time = None
        phen_time2 = first_value(g, obs, SOSA.phenomenonTime) or first_value(g, obs, SOSA.phenomenon_time) or first_value(g, obs, SOSA.resultTime)
        if phen_time2:
            try:    
                dt = dtparse(phen_time2)
                phen_time = dt.timestamp()
            except Exception as e:
                print('Warning: Can not date parse',phen_time2,e)
        # result text (simple literal)
        qual_value_text = first_value(g, obs, SOSA.hasSimpleResult)
        # qualitative value node (may be blank node or URI)
        qual_node = first_value(g, obs, SOSA.result) or first_value(g, obs, SOSA.hasResult)
        qual_uri = None
        qual_label = None
        qual_value_text = None
        if qual_node is not None:
            # if it's a node (URIRef/BNode), create stable id
            qual_uri = node_to_uri(qual_node) if not isinstance(qual_node, Literal) else None
            # maybe the observation also has a simple result we should preserve as value_text
            qual_value_text = first_value(
                g, qual_node, QUDT.numericValue) or first_value(
                g, qual_node, QUDT.value)                
            # unit (might be linked via qudt or as property on observation)
            unit_node = first_value(g, qual_node, QUDT.unit) or first_value(g, qual_node, QUDT.hasUnit)
            unit_uri = node_to_uri(unit_node) if unit_node is not None else None
            unit_label = label_for(g, unit_node) if unit_node is not None else None # todo: labels from qudt?
        # procedure
        proc_node = first_value(g, obs, SOSA.usedProcedure) or first_value(g, obs, SOSA.isProducedBy)
        proc_uri = node_to_uri(proc_node) if proc_node is not None else None
        proc_label = label_for(g, proc_node) if proc_node is not None else None # todo: proc labels from glosis?
        # observed property
        prop_node = first_value(g, obs, SOSA.observedProperty) or first_value(g, obs, SOSA.hasObservedProperty)
        prop_uri = node_to_uri(prop_node) if prop_node is not None else None
        prop_label = label_for(g, prop_node) if prop_node is not None else None
        # feature of interest
        foi_node = first_value(g, obs, SOSA.hasFeatureOfInterest) or first_value(g, obs, SOSA.featureOfInterest)
        foi_uri = node_to_uri(foi_node) if foi_node is not None else None
        foi_label = label_for(g, foi_node) if foi_node is not None else None
        foi_type = types_text_for(g, foi_node)
        # geometry text
        lat = first_value(g, foi_node, GEO.lat)
        lon = first_value(g, foi_node, GEO.long)
        geom = None
        if lat and lon:
            lat = float(lat)
            lon = float(lon)
            geom = to_gpkg_geom(lat, lon)
        foi_id = upsert_single_return_id(conn, "feature_of_interest", foi_uri, foi_label, 
                                            {"type":foi_type, "geom": geom})

        # Upsert referenced entities and get ids
        proc_id = upsert_single_return_id(conn, "procedure", proc_uri, proc_label)
        unit_id = upsert_single_return_id(conn, "unitofmeasure", unit_uri, unit_label)
        prop_id = upsert_single_return_id(conn, "property", prop_uri, prop_label)
        res_id = None
        if qual_value_text not in [None, '']:  # todo: value may not be integer, also check for literal value on observation
            UPSERT_RES = f"INSERT INTO result (result_uri,value,unit_of_measure_id) VALUES (?, ?, ?)"
            with get_cursor(conn) as cur:
                cur.execute(UPSERT_RES, (qual_uri, qual_value_text, unit_id))
                cur.execute(f"""
                    SELECT id FROM result WHERE result_uri = ?""", 
                    (qual_uri,))
                row = cur.fetchone()
                res_id = row[0] if row else None
        UPSERT_OBS = """
        INSERT INTO observation (observation_uri, result_id, phenomenon_time, procedure_id, property_id, foi_id)
        VALUES (?, ?, ?, ?, ?, ?)
        """
        with get_cursor(conn) as cur:
            cur.execute(UPSERT_OBS,(obs_uri, res_id, phen_time, proc_id, prop_id, foi_id))

    conn.commit()



# helpers
def db_insert(conn, table, kv):
    cls = []
    rws = []
    cvals = []
    for k,v in kv.items():
        cls.append('?')
        rws.append(k)
        cvals.append(v)
    sql_ins = f"""INSERT OR IGNORE INTO {table} (
        {','.join(rws)}
        ) VALUES ({','.join(cls)})"""
    with get_cursor(conn) as cur:
        cur.execute(sql_ins, tuple(cvals))

    # utility upsert functions returning row id
def upsert_single_return_id(conn, table, uri, label=None, extra=None):
    """
    Generic insert-or-ignore. If extra_col/extra_val provided, include in insert.
    """
    if uri is None:
        return None
    if not extra:
        extra = {}
    extra['uri'] = uri
    extra['label'] = label
    db_insert(conn, table, extra)
    with get_cursor(conn) as cur:
        cur.execute(f"SELECT id FROM {table} WHERE uri = ? or (coalesce(uri,'') <> '' and label = ?)", (uri, label))
        row = cur.fetchone()
        return row[0] if row else None
    