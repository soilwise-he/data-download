# ----------------------------------------------------------------------------
#  Copyright (C) 2026 ISRIC - World Soil Information
#  Produced in the Scope of the SoilWISE Project
#  SoilWISE is funded by the European Union’s Horizon Europe research and 
#  innovation programme under grant agreement No 101056973.
# ----------------------------------------------------------------------------

import sys
import os, json, io, sqlite3
from app.utils.geopackage import rdf2rdb
from csvwlib import CSVWConverter
import pytest

# Add ../app to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import app.main as main 

@pytest.mark.asyncio
async def test_convert():
    foo = await main.convert_to_rdf(context='https://github.com/soilwise-he/soil-observation-data-encodings/raw/refs/heads/main/CSVW/examples/example2/leaves-of-tree-metadata-FK.jsonld',
                                    output_format="json-ld")
    assert 'https://raw.githubusercontent.com/soilwise-he/soil-observation-data-encodings/refs/heads/main/CSVW/examples/example2/trees.csv#LAT' in json.loads(foo.body)[0].keys()

# test convert as json object
@pytest.mark.asyncio
async def test_convert():
    req = json.dumps({"@context": [ "http://www.w3.org/ns/csvw"],
"tables": [{
"url": "https://raw.githubusercontent.com/soilwise-he/soil-observation-data-encodings/refs/heads/main/CSVW/examples/example2/trees.csv",
"aboutUrl": "https://soilwise.example.org/tree/{TREEID}",
"tableSchema": {
    "columns": [{
            "titles": "TREEID",
            "datatype": "string",
            "propertyUrl": "dcterms:identifier"
        }, {
            "titles": "lat",
            "propertyUrl": "schema:latitude",
            "datatype": "number"
        }, {
            "titles": "long",
            "propertyUrl": "schema:longitude",
            "datatype": "number"
        }]
    }}]})
    foo = await main.convert_to_rdf(context=req, output_format="json-ld")
    assert 'http://purl.org/dc/terms/identifier' in json.loads(foo.body)[0].keys()


# export as sqlite
@pytest.mark.asyncio
async def test_export_db_has_records():
    # Call endpoint directly

    graph = CSVWConverter.to_rdf(None, 
                                 'https://raw.githubusercontent.com/soilwise-he/soil-observation-data-encodings/refs/heads/main/CSVW/examples/example3/obs.csv-metadata.json', 
                                 mode='minimal')
    conn = sqlite3.connect(":memory:")
    rdf2rdb(graph, conn)

    # Check table contents
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM observation")
    count = cur.fetchone()[0]
    conn.close()
    assert count > 0