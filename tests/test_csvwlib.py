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
from .constants import csvw_sample

@pytest.mark.asyncio
async def test_convert():
    foo = await main.convert_to_rdf(context='https://github.com/soilwise-he/soil-observation-data-encodings/raw/refs/heads/main/CSVW/examples/example2/leaves-of-tree-metadata-FK.jsonld',
                                    output_format="json-ld")
    assert 'https://raw.githubusercontent.com/soilwise-he/soil-observation-data-encodings/refs/heads/main/CSVW/examples/example2/trees.csv#LAT' in json.loads(foo.body)[0].keys()

# test convert as json object
@pytest.mark.asyncio
async def test_convert():
    foo = await main.convert_to_rdf(context=csvw_sample, output_format="json-ld")
    assert 'http://purl.org/dc/terms/identifier' in json.loads(foo.body)[0].keys()

