# ----------------------------------------------------------------------------
#  Copyright (C) 2026 ISRIC - World Soil Information
#  Produced in the Scope of the SoilWISE Project
#  SoilWISE is funded by the European Union’s Horizon Europe research and 
#  innovation programme under grant agreement No 101056973.
# ----------------------------------------------------------------------------

import json,yaml,csv
from io import StringIO


def parse_metadata(data):
    csvw_tables = []

    try:
        md = json.loads(data)
        if "resources" in md:  # datapackage resources
            for r in md['resources']:
                if "schema" in r:
                    csvw_tables.append({"url": r.get('path',''),
                            "id": r.get('name',''),
                            "tableSchema": {"columns": r['schema'].get('fields',[]) }})
        elif "fields" in md:
            csvw_tables.append({"url": md.get('path', md.get('url','')),
                            "id": md.get('name',''),
                            "tableSchema": {"columns": md['fields'] }})
        return csvw_tables
    except Exception as ex:
        print(f"Error parsing JSON metadata: {ex}")

    # yaml.loads (mcf)
    try:
        md = yaml.safe_load(data)
        if "attributes" in md.get("content_info",{}):

            mdmd = md.get('metadata',{}) or {}
            idmd = md.get('identification', {}) or {}

            id = mdmd.get('dataseturi', mdmd.get('identifier', ''))
            url = idmd.get('url', id)

            csvw_tables.append({"url": url,
                            "id": id,
                            "tableSchema": {"columns": md.get('content_info').get('attributes') }})
        print(f"Parsed YAML metadata: {csvw_tables[0]}")
        return csvw_tables

    except Exception as ex:
        print(f"Error parsing YAML metadata: {ex}")
        
    # csv.loads
    try:
        dict_reader = csv.DictReader(StringIO(data))
        md = list(dict_reader)
        if len(md) > 0:
            csvw_tables.append({"tableSchema": {"columns": md }})
        return csvw_tables    
    except Exception as ex:
        print(f"Error parsing CSV metadata: {ex}")
        

    return []