# ----------------------------------------------------------------------------
#  Copyright (C) 2026 ISRIC - World Soil Information
#  Produced in the Scope of the SoilWISE Project
#  SoilWISE is funded by the European Union’s Horizon Europe research and 
#  innovation programme under grant agreement No 101056973.
# ----------------------------------------------------------------------------

from fastapi import FastAPI, HTTPException, Body  
from fastapi.staticfiles import StaticFiles
from pathlib import Path
from enum import Enum
import tempfile, struct, yaml
from fastapi.responses import FileResponse, Response, JSONResponse, StreamingResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Any, Dict, Optional, Union
import httpx, json
from io import BytesIO, StringIO
import csv, os, sqlite3, sys
from rdflib import Graph, Namespace, BNode, URIRef, Literal
from rdflib.namespace import RDF, RDFS, XSD, SKOS, DCTERMS
from csvwlib import CSVWConverter
from app.utils.geopackage import (
    rdf2rdb,
)
from app.utils.table import (
    parse_excel_or_csv_from_url, safe_name_from_url, build_csv_text_from_rows, fetch_bytes, guess_type_from_samples, pick_primary_key
)
from app.utils.inspire import (
    build_inspire_gml
)
from app.utils.metadata import (
    parse_metadata
)

rootpath = os.environ.get("ROOTPATH", "")

context_map = json.loads(os.environ.get("CONTEXT_MAP", '''
    {"sosa": "http://www.w3.org/ns/sosa/", 
     "qudt": "http://qudt.org/1.1/schema/qudt#",
     "geo": "http://www.opengis.net/ont/geosparql#",
     "wgs": "http://www.w3.org/2003/01/geo/wgs84_pos#"}'''))

app = FastAPI(    
    title="Soil Observation Data Annotation with CSVW",
    description="""
API for converting soil observation data into RDF and SQLite formats.
""", root_path=rootpath)

BASE_DIR = Path(__file__).resolve().parent
app.mount(
    "/static",
    StaticFiles(directory=BASE_DIR / "static", html=True),
    name="static"
)

# DEV: permissive CORS for local testing. Lock this down in production.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

class Formats(str, Enum):
    ttl = "ttl"
    rdfxml = "rdfxml"
    json_ld = "json-ld"
    sqlite = "sqlite"
    gpkg = "gpkg"
    inspire = "inspire"


@app.get('/', response_class=HTMLResponse)
async def index_loader():
    return '<html><head><meta http-equiv="refresh" content="0; url=./docs"></head></html>'

@app.post('/import', summary="Imports column metadata from csv, mcf, json")
async def import_md(data: str | dict = "https://raw.githubusercontent.com/geopython/pygeometa/refs/heads/master/sample.mcf.yml"):
    if isinstance(data,str):
        if data.startswith('http'):
            # get file
            content = await fetch_bytes(data)
            data = content.decode("utf-8", errors="replace")
        else:
            # try to parse as JSON string
            try:
                data = json.loads(data)
            except Exception as ex:
                raise HTTPException(status_code=400, detail=f"Invalid input: {ex}")

    md = None
    csvw_tables = parse_metadata(data)

    if not csvw_tables or len(csvw_tables) == 0:
        raise HTTPException(status_code=400, detail="No tables found in metadata")

    return {
        "@context": ["https://www.w3.org/ns/csvw.jsonld", context_map],
        "tables": csvw_tables
    }



@app.post("/export", summary="Exports a CSVW project (tables and configuration) as a zip",
    description="""Accepts JSON body with { "MetadataContent": <object|json-string> }
    Produces a ZIP with:
      - one CSV file per table (try fetch table.url, else generate from tableSchema.columns headers)
      - metadata.jsonld (the metadataPayload as JSON)""")
async def export(url: str="https://raw.githubusercontent.com/soilwise-he/soil-observation-data-encodings/refs/heads/main/CSVW/examples/example3/obs.csv"):
    """
    Accepts JSON body with { "MetadataContent": <object|json-string> }
    Produces a ZIP with:
      - one CSV file per table (try fetch table.url, else generate from tableSchema.columns headers)
      - metadata.jsonld (the metadataPayload as JSON)
    """
    tables = await parse_excel_or_csv_from_url(url)
        
    # Prepare async http client
    async with httpx.AsyncClient() as client:
        zip_buf = BytesIO()
        # create zip in-memory
        import zipfile
        with zipfile.ZipFile(zip_buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
            # add a pretty-printed metadata JSON-LD file
            #try:
            #    metadata_bytes = json.dumps(metadata, ensure_ascii=False, indent=2).encode("utf-8")
            #except Exception:
            #    metadata_bytes = str(metadata).encode("utf-8")
            #zf.writestr("metadata.jsonld", metadata_bytes)

            # iterate tables and produce CSV per table
            for idx, tbl in enumerate(tables):
                # table can be a simple string or an object
                # prefer tbl['url'] if available; allow url to be list or string
                tbl_url = None
                if isinstance(tbl, str):
                    tbl_url = tbl
                elif isinstance(tbl, dict):
                    u = tbl.get("url")
                    if isinstance(u, list) and len(u) > 0:
                        tbl_url = u[0]
                    else:
                        tbl_url = u
                # fallback name
                base = f"table_{idx+1}"
                filename_hint = safe_name_from_url(str(tbl_url) if tbl_url else "", base)
                csv_filename = filename_hint
                # ensure extension .csv
                if not csv_filename.lower().endswith(".csv"):
                    csv_filename = csv_filename + ".csv"

                # Attempt to fetch the CSV at tbl_url
                got_text = None
                if tbl_url:
                    got_text = await fetch_text(client, tbl_url)

                if got_text:
                    # if the remote resource looks like CSV already, use it
                    # But if remote is JSON or other, we still include the text as .csv (user can inspect)
                    zf.writestr(csv_filename, got_text.encode("utf-8"))
                else:
                    # no remote data — try to generate CSV header from tableSchema.columns
                    headers = []
                    rows = None
                    if isinstance(tbl, dict):
                        ts = tbl.get("tableSchema") or {}
                        cols = ts.get("columns") if ts else None
                        if isinstance(cols, list):
                            # columns may be objects with titles/name; try to get 'name' or 'titles'
                            hdrs = []
                            for c in cols:
                                if isinstance(c, dict):
                                    # titles can be string or array; handle both
                                    t = c.get("name") or c.get("titles") or c.get("title")
                                    if isinstance(t, list) and len(t) > 0:
                                        hdrs.append(str(t[0]))
                                    elif t is not None:
                                        hdrs.append(str(t))
                                    else:
                                        # fallback: propertyUrl or blank
                                        hdrs.append(c.get("propertyUrl") or "")
                                else:
                                    # if column is a string
                                    hdrs.append(str(c))
                            headers = [h if h is not None else "" for h in hdrs]
                        else:
                            headers = []
                    else:
                        headers = []

                    if not headers:
                        # fallback to single column with table name
                        headers = [filename_hint.replace(".csv", "")]

                    csv_text = build_csv_text_from_rows(headers, rows)
                    zf.writestr(csv_filename, csv_text.encode("utf-8"))

        # finalize zip buffer
        zip_buf.seek(0)
        # StreamingResponse is fine for in-memory buffer
        headers = {
            "Content-Disposition": 'attachment; filename="csvw_tables.zip"'
        }
        return StreamingResponse(zip_buf, media_type="application/zip", headers=headers)

@app.post("/suggest", summary="Suggest a CSVW configuration",
    description="""Suggest a CSVW configuration or a given tableor excel""" )
async def suggest(url: str="https://raw.githubusercontent.com/soilwise-he/soil-observation-data-encodings/refs/heads/main/CSVW/examples/example3/obs.csv"):
    """
    build a CSVW metadata (JSON-LD) using tables[] with 
    guessed columns/datatype/primaryKey.
    """

    tables = await parse_excel_or_csv_from_url(url)
    # Build CSVW tables[] array
    csvw_tables = []
    all_headers = []
    for t in tables:
        headers = t.get("headers", []) or []
        rows = t.get("rows", []) or []
        # sample for types: first up to 5 rows
        sample_for_types = rows[:5]
        # sample for uniqueness for primary key: up to 200 rows
        sample_for_uniqueness = rows[:200]
        # build columns
        columns = []
        for h in headers:
            samples = [r.get(h, "") for r in sample_for_types]
            guessed = guess_type_from_samples(samples)
            datatype = None
            if guessed == "number":
                datatype = "http://www.w3.org/2001/XMLSchema#decimal"
            if guessed == "date":
                datatype = "http://www.w3.org/2001/XMLSchema#dateTime"
            prop = "schema:value"
            # small mapping for common names
            keyLower = h.lower() if isinstance(h, str) else ""
            if "name" == keyLower or keyLower.endswith("name"):
                prop = "schema:name"
            elif "email" in keyLower:
                prop = "schema:email"
            elif keyLower in ("id","identifier","uuid"):
                prop = "schema:identifier"
            columns.append({
                "titles": h,
                "name": h,
                "dcterms:description": h,
                "propertyUrl": prop,
                **({"datatype": datatype} if datatype else {})
            })
        # guess primaryKey
        pk = pick_primary_key(headers, sample_for_uniqueness)
        table = {
            "url": t.get("url"),
            "tableSchema": {
                "columns": columns
            }
        }
        if pk:
            table["tableSchema"]["primaryKey"] = pk
        csvw_tables.append(table)
        for h in headers:
            if h not in all_headers:
                all_headers.append(h)
    metadata = {
        "@context": ["https://www.w3.org/ns/csvw.jsonld", context_map],
        "tables": csvw_tables
    }
    return {"metadata": metadata, "tables": tables}

@app.post("/graph2gpkg", 
          summary="Convert a knowledge graph to a geopackage (sqlite)",
          description="""converts one or more tables to a knowledge graph, from a csvw configuration""")
async def rdf_to_gpkg(req: str="https://raw.githubusercontent.com/soilwise-he/soil-observation-data-encodings/refs/heads/main/CSVW/examples/example3/data.ttl"):
    g = Graph()
    if req.startswith("http"):
        g.parse(req)
    else:
        g.parse(data=req)  # or .jsonld
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
    file_conn = sqlite3.connect(tmp.name)
    rdf2rdb(g, file_conn)
    file_conn.close()
    return StreamingResponse(
        open(tmp.name, "rb"),
        media_type="application/vnd.sqlite3",
        headers={
            "Content-Disposition": "attachment; filename=export.db"
        }
    )


@app.post("/convert", summary="Converts one or more tables to a knowledge graph, from a csvw configuration",
    description="""converts one or more tables to a knowledge graph, from a csvw configuration""")
async def convert_to_rdf(
        context: str | dict = "https://raw.githubusercontent.com/soilwise-he/soil-observation-data-encodings/refs/heads/main/CSVW/examples/example3/obs.csv-metadata.json",
        data: Optional[List[str]] = [],
        output_format: Optional[Formats] = Formats.ttl
        ):
    """
    Convert workbook or CSV at `url` to RDF using provided CSV-W context (object or URL).
    Returns a string with the chosen serialization.
    """
    fmt = output_format or Formats.ttl

    # 0) get context
    if isinstance(context, dict):
        print('got context as dict', context)
    if isinstance(context, str) and not context.startswith("http"):  
        print('context as string', context)     
        try:
            context = json.loads(context)
        except Exception as ex:
            raise HTTPException(status_code=400, detail=f"Invalid context: {ex}")
    
    # 2) convert data to graph
    graph = CSVWConverter.to_rdf(next(iter(data), None), 
                                 context, 
                                 mode='minimal')
    
    if not isinstance(graph, Graph):
        raise HTTPException(status_code=500, detail="Parsing error")
    
    if fmt == Formats.ttl:
        return Response(content=graph.serialize(format="turtle"), 
                        media_type="text/turtle")
    if fmt == Formats.rdfxml:
        return Response(content=graph.serialize(format="xml"), 
                        media_type="application/rdf+xml")
    if fmt == Formats.json_ld:
        return Response(content=graph.serialize(format="json-ld"), 
                        media_type="application/ld+json")
    # --- BEGIN: Enhanced SQLite export with SOSA mapping ---
    if fmt == Formats.sqlite or fmt == Formats.gpkg:
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        file_conn = sqlite3.connect(tmp.name)
        rdf2rdb(graph, file_conn)
        file_conn.close()
        return StreamingResponse(
            open(tmp.name, "rb"),
            media_type="application/vnd.sqlite3",
            headers={
                "Content-Disposition": "attachment; filename=export.db"
            }
        )
    elif fmt == Formats.inspire:
        gml_bytes = None
        conn = sqlite3.connect(":memory:")
        rdf2rdb(graph, conn)
        gml_bytes = build_inspire_gml(conn)
        conn.close()
        return Response(
            content=gml_bytes,
            media_type="application/gml+xml",
            headers={"Content-Disposition": "attachment; filename=soil.gml"}
        )
    else:
        raise HTTPException(status_code=400, detail=f"Unsupported format: {fmt}")