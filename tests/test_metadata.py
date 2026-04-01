# ----------------------------------------------------------------------------
#  Copyright (C) 2026 ISRIC - World Soil Information
#  Produced in the Scope of the SoilWISE Project
#  SoilWISE is funded by the European Union’s Horizon Europe research and 
#  innovation programme under grant agreement No 101056973.
# ----------------------------------------------------------------------------

import sys, os, sqlite3
from app.utils.geopackage import rdf2rdb
from app.utils.metadata import parse_metadata
from app.utils.table import fetch_bytes
import pytest
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


@pytest.mark.asyncio
async def test_mcf():
    content = await fetch_bytes("https://raw.githubusercontent.com/geopython/pygeometa/refs/heads/master/sample.mcf.yml")
    data = content.decode("utf-8", errors="replace")
    foo = parse_metadata(data)
    assert len(foo) == 1   

