import sys
import os
import pytest

# Add ../app to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../app")))

import main

def test_header():
    foo = main.detect_header_row([['','',''],['a','b','c'],[1,2,3],[4,5,6]])
    assert foo == 1

@pytest.mark.asyncio
async def test_csv_imports():
    foo = await main.parse_excel_or_csv_from_url('https://raw.githubusercontent.com/soilwise-he/soil-observation-data-encodings/refs/heads/main/CSVW/examples/example3/obs.csv')
    print(foo[0].get('rows',[])[0])
    assert foo[0].get('rows',[])[0].get('SAMPLE','') == 'UAI234'