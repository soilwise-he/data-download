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
from .convert import (
    project_as_zip,
    csvw_from_table,
    metadata_convert
)
from .utils.geopackage import (
    rdf2rdb,
)
from .utils.table import (
    parse_excel_or_csv_from_url, 
    safe_name_from_url, 
    build_csv_text_from_rows, 
    fetch_bytes, 
    guess_type_from_samples, 
    pick_primary_key
)
from .utils.inspire import (
    build_inspire_gml
)
from .utils.metadata import (
    parse_metadata
)

rootpath = os.environ.get("ROOTPATH", "")

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
    csvw = "csvw"
    mcf = "mcf"
    ttl = "ttl"
    rdfxml = "rdf-xml"
    json_ld = "json-ld"
    sqlite = "sqlite"
    gpkg = "gpkg"
    inspire = "inspire"


@app.get('/', response_class=HTMLResponse)
async def index_loader():
    return '<html><head><meta http-equiv="refresh" content="0; url=./docs"></head></html>'

@app.post('/import', summary="Imports column metadata from csv, mcf, json")
async def import_md(data: str | dict = "https://raw.githubusercontent.com/geopython/pygeometa/refs/heads/master/sample.mcf.yml"):
    
    return metadata_convert(data) 



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
        zip_buf = project_as_zip(tables, client)
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
    return csvw_from_table(url)

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
    description="""converts one or more tables to a knowledge graph or database from a csvw configuration""")
async def return_converted(
        context: Optional[str | dict] = "https://raw.githubusercontent.com/soilwise-he/soil-observation-data-encodings/refs/heads/main/CSVW/examples/example3/obs.csv-metadata.json",
        data: Optional[List[str]] = [],
        input_format: Optional[Formats] = None,
        output_format: Optional[Formats] = Formats.ttl
        ):
    
    fmt_in = input_format or Formats.csvw
    fmt = output_format or Formats.ttl

    # todo: identify if input is a context(+csv) or a graph or a geopackage request, and route accordingly. For now we assume context+csv input for RDF conversion.  
    # todo: enable to append the content to an existing database instead of always creating a new one (for sqlite/gpkg output)
    # todo: a schema parameter to specify a target schema for the graph and/or database (e.g. SOSA, schema.org, etc.) and do some mapping during conversion.

    if not data and not context:
        raise HTTPException(status_code=400, detail="No input data provided")


    # 0) get context
    if context in [None,""]:
        # skip csvw -> proceed kg
        None
    elif isinstance(context, dict):
        print('got context as dict', context)
        # assume csvw, but maybe can also be mcf-yaml? -> then needs parsing to csvw dict
    elif isinstance(context, str):  
        if context.startswith("http"):  
            # assume csvw, but maybe can also be mcf-yaml? -> then needs parsing to csvw dict
            None
        else: 
            try:
                context = json.loads(context)
            except Exception as ex:
                raise HTTPException(status_code=400, detail=f"Invalid context: {ex}")

    else:
        raise HTTPException(status_code=400, detail="Invalid context format")

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