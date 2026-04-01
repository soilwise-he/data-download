# ----------------------------------------------------------------------------
#  Copyright (C) 2026 ISRIC - World Soil Information
#  Produced in the Scope of the SoilWISE Project
#  SoilWISE is funded by the European Union’s Horizon Europe research and 
#  innovation programme under grant agreement No 101056973.
# ----------------------------------------------------------------------------


import sys, os, pytest
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import app.main as main
from app.utils.table import detect_header_row, parse_excel_or_csv_from_url

def test_header():
    foo = detect_header_row([['','',''],['a','b','c'],[1,2,3],[4,5,6]])
    assert foo == 1

@pytest.mark.asyncio
async def test_csv_imports():
    foo = await parse_excel_or_csv_from_url('https://raw.githubusercontent.com/soilwise-he/soil-observation-data-encodings/refs/heads/main/CSVW/examples/example3/obs.csv')
    print(foo[0].get('rows',[])[0])
    assert foo[0].get('rows',[])[0].get('SAMPLE','') == 'UAI234'