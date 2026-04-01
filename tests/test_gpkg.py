# ----------------------------------------------------------------------------
#  Copyright (C) 2026 ISRIC - World Soil Information
#  Produced in the Scope of the SoilWISE Project
#  SoilWISE is funded by the European Union’s Horizon Europe research and 
#  innovation programme under grant agreement No 101056973.
# ----------------------------------------------------------------------------

from fastapi.responses import StreamingResponse
import sys, os, sqlite3
from app.utils.geopackage import rdf2rdb
from app.utils.inspire import build_inspire_gml
import pytest
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from rdflib import Graph
from .constants import graphdata
import app.main as main 

# export as sqlite
@pytest.mark.asyncio
async def test_export_db_has_records():
    conn = sqlite3.connect(":memory:")
    g = Graph()
    g.parse(data=graphdata, format="turtle")
    rdf2rdb(g, conn)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM observation")
    count1 = cur.fetchone()[0]
    assert count1 > 0
#    cur.execute("SELECT COUNT(*) FROM result")
#    count2 = cur.fetchone()[0]
#    assert count2 > 0
    conn.close()

@pytest.mark.asyncio
async def test_convert_graph_to_gpkg():
    rdf_to_gpkg = await main.rdf_to_gpkg(graphdata)
    assert isinstance(rdf_to_gpkg, StreamingResponse)

