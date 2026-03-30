import sys
import os, json, io, sqlite3
import pytest

# Add ../app to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../app")))

import main 

@pytest.mark.asyncio
async def test_convert():
    req = main.ConvertRequest(
        context='https://github.com/soilwise-he/soil-observation-data-encodings/raw/refs/heads/main/CSVW/examples/example2/leaves-of-tree-metadata-FK.jsonld',
        output_format="json-ld"
    )
    foo = await main.convert_to_rdf(req)
    assert 'https://raw.githubusercontent.com/soilwise-he/soil-observation-data-encodings/refs/heads/main/CSVW/examples/example2/trees.csv#LAT' in json.loads(foo.body)[0].keys()

# test convert as link to csvw



# test convert as upload csvw


# export as sqlite
@pytest.mark.asyncio
async def test_export_db_has_records():
    # Call endpoint directly
    req = main.ConvertRequest(
        context='https://raw.githubusercontent.com/soilwise-he/soil-observation-data-encodings/refs/heads/main/CSVW/examples/example3/obs.csv-metadata.json',
        output_format="sqlite"
    )
    foo = await main.convert_to_rdf(req)
    
    # Extract bytes from Response
    db_bytes = io.BytesIO(foo.body)
    
    # Create a temporary SQLite connection in memory from bytes
    # Using an in-memory DB and executing SQL dump
    conn = sqlite3.connect(":memory:")
    conn.executescript(db_bytes.read().decode())  # exec the SQL dump
    
    # Check table contents
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM observation")
    count = cur.fetchone()[0]
    
    assert count > 0