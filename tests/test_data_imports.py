# ----------------------------------------------------------------------------
#  Copyright (C) 2026 ISRIC - World Soil Information
#  Produced in the Scope of the SoilWISE Project
#  SoilWISE is funded by the European Union’s Horizon Europe research and 
#  innovation programme under grant agreement No 101056973.
# ----------------------------------------------------------------------------

import sys, os, pytest
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from csvw_api.convert import csvw_from_table
from csvw_api.utils.table import detect_header_row, parse_excel_or_csv_from_url, parse_csv
from .constants import csv_sample, csv_tab_sample, csv_noheader_sample

def test_header():
    foo = detect_header_row([['','',''],['a','b','c'],[1,2,3],[4,5,6]])
    assert foo == 1

def test_read_csv(): # todo allow tab separated as well
    headers, rows = parse_csv(csv_sample)
    assert headers == ['TREEID', 'LAT', 'LONG']
    assert rows[0]['TREEID'] == '1'
    assert rows[0]['LAT'] == '20'
    assert rows[0]['LONG'] == '10'
def test_read_tabbed_csv(): # todo allow tab separated as well
    headers, rows = parse_csv(csv_tab_sample)
    assert headers == ['TREEID', 'LAT', 'LONG']
    assert rows[0]['TREEID'] == '1'
    assert rows[0]['LAT'] == '20'
    assert rows[0]['LONG'] == '10'
def test_read_noheader_csv(): # todo allow tab separated as well
    headers, rows = parse_csv(csv_noheader_sample)
    assert headers == ['col1', 'col2', 'col3']
    assert rows[0]['col1'] == '1'
    assert rows[0]['col2'] == '20'
    assert rows[0]['col3'] == '10'

@pytest.mark.asyncio
async def test_csv_imports():
    foo = await parse_excel_or_csv_from_url('https://raw.githubusercontent.com/soilwise-he/soil-observation-data-encodings/refs/heads/main/CSVW/examples/example3/obs.csv')
    print(foo[0].get('rows',[])[0])
    assert foo[0].get('rows',[])[0].get('SAMPLE','') in ['US325','TA234','TS325','AIZ34','BS385'] # rdf may have no order


# with excel template
@pytest.mark.asyncio
async def test_excel_import():
    foo = await csvw_from_table('https://github.com/soilwise-he/soil-observation-data-encodings/raw/refs/heads/main/SimpleCSV/SoilTemplate.xlsx')
    # lsprint(foo['metadata'])
    assert len(foo['metadata']['tables'][0]['tableSchema']['columns']) > 1

