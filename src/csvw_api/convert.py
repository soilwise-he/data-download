from io import BytesIO
import json
import os
import zipfile
from .utils.table import (
    parse_excel_or_csv_from_url, 
    build_csv_text_from_rows,
    safe_name_from_url,
    fetch_text,
    pick_primary_key,
    guess_type_from_samples,
    fetch_bytes
)
from .utils.metadata import (
    parse_metadata
)

context_map = json.loads(os.environ.get("CONTEXT_MAP", '''
    {"sosa": "http://www.w3.org/ns/sosa/", 
     "qudt": "http://qudt.org/1.1/schema/qudt#",
     "geo": "http://www.opengis.net/ont/geosparql#",
     "wgs": "http://www.w3.org/2003/01/geo/wgs84_pos#"}'''))

async def csvw_from_table(url):
    """Given a URL to a CSV or Excel file, fetch it and analyze its structure to create CSVW metadata."""
    tables = await parse_excel_or_csv_from_url(url)
    # Build CSVW tables[] array
    csvw_tables = []
    csvw_metadata = []
    db_metadata = {
        'title': url
    }
    
    for t in tables:
        # identify if this is metadata or a data table
        tname = t.get("sheet_name") or t.get("url") or ""
        if tname.lower() == "metadata":
            for _r in t.get("rows",[]):
                r = list(_r.values())
                if len(r) > 1 and r[0] not in [None,''] and r[1] not in [None,'']:
                    db_metadata[r[0].lower().split(':')[0]] = r[1] 
        else:
            headers = t.get("headers", []) or []
            rows = t.get("rows", []) or []
            # sample for types: first up to 5 rows
            sample_for_types = rows[:5]
            # sample for uniqueness for primary key: up to 200 rows
            sample_for_uniqueness = rows[:200]
            # build columns
            columns = []
            for h in headers:
                samples = [r.get(h, "") for r in sample_for_types]
                guessed = guess_type_from_samples(samples)
                datatype = None
                if guessed == "number":
                    datatype = "http://www.w3.org/2001/XMLSchema#decimal"
                if guessed == "date":
                    datatype = "http://www.w3.org/2001/XMLSchema#dateTime"
                prop = "schema:value"
                # small mapping for common names
                keyLower = h.lower() if isinstance(h, str) else ""
                if "name" == keyLower or keyLower.endswith("name"):
                    prop = "schema:name"
                elif "email" in keyLower:
                    prop = "schema:email"
                elif keyLower in ("id","identifier","uuid"):
                    prop = "schema:identifier"
                columns.append({
                    "titles": h,
                    "name": h,
                    "dcterms:description": h,
                    "propertyUrl": prop,
                    **({"datatype": datatype} if datatype else {})
                })
            # guess primaryKey
            pk = pick_primary_key(headers, sample_for_uniqueness)
            table = {
                "url": t.get("url"),
                "tableSchema": {
                    "columns": columns
                },
                "rows": rows
            }
            if pk:
                table["tableSchema"]["primaryKey"] = pk

            if tname.lower().startswith("observableproperties"):
                _tbl = []
                for r in t.get("rows",[]):
                    for num in range(len(t.get("columns",[]))):
                        _tblr = {}
                        _tblr[t["columns"][num].get("titles", f"col{num+1}").lower()] = rows[num] or ""
                        _tbl.append(_tblr)
                csvw_metadata = _tbl
            else:
                table.pop('rows') # skip rows
                csvw_tables.append(table)


    # now merge the csvw_metadata to the generated metadata, preferring the csvw_metadata if it has tables (i.e. observable properties sheet is present)
    for t in csvw_tables:
        # if property is present in csvw_metadata, use that; otherwise use generated
        for c in t.get("tableSchema", {}).get("columns", []):
            title = c.get("titles")
            if title:
                # find matching property in csvw_metadata
                for _mr in csvw_metadata:
                    if title.lower() == _mr.get("column name", ""):
                        # todo: transform this into an observation
                        c.update(_mr)  # override with metadata from csvw_metadata
                                
    metadata = {
        "@context": ["https://www.w3.org/ns/csvw.jsonld", context_map],
        "tables": csvw_tables
    }
    for k in ["identification","title","abstract","creator"]:
        if k in db_metadata:
            metadata["dc:"+k] = db_metadata[k]
    return {"metadata": metadata, "tables": tables}


async def project_as_zip(tables, client):
    zip_buf = BytesIO()
    # create zip in-memory
    with zipfile.ZipFile(zip_buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        # iterate tables and produce CSV per table
        for idx, tbl in enumerate(tables):
            # table can be a simple string or an object
            # prefer tbl['url'] if available; allow url to be list or string
            tbl_url = None
            if isinstance(tbl, str):
                tbl_url = tbl
            elif isinstance(tbl, dict):
                u = tbl.get("url")
                if isinstance(u, list) and len(u) > 0:
                    tbl_url = u[0]
                else:
                    tbl_url = u
            # fallback name
            base = f"table_{idx+1}"
            filename_hint = safe_name_from_url(str(tbl_url) if tbl_url else "", base)
            csv_filename = filename_hint
            # ensure extension .csv
            if not csv_filename.lower().endswith(".csv"):
                csv_filename = csv_filename + ".csv"

            # Attempt to fetch the CSV at tbl_url
            got_text = None
            if tbl_url:
                got_text = await fetch_text(client, tbl_url)

            if got_text:
                # if the remote resource looks like CSV already, use it
                # But if remote is JSON or other, we still include the text as .csv (user can inspect)
                zf.writestr(csv_filename, got_text.encode("utf-8"))
            else:
                # no remote data — try to generate CSV header from tableSchema.columns
                headers = []
                rows = None
                if isinstance(tbl, dict):
                    ts = tbl.get("tableSchema") or {}
                    cols = ts.get("columns") if ts else None
                    if isinstance(cols, list):
                        # columns may be objects with titles/name; try to get 'name' or 'titles'
                        hdrs = []
                        for c in cols:
                            if isinstance(c, dict):
                                # titles can be string or array; handle both
                                t = c.get("name") or c.get("titles") or c.get("title")
                                if isinstance(t, list) and len(t) > 0:
                                    hdrs.append(str(t[0]))
                                elif t is not None:
                                    hdrs.append(str(t))
                                else:
                                    # fallback: propertyUrl or blank
                                    hdrs.append(c.get("propertyUrl") or "")
                            else:
                                # if column is a string
                                hdrs.append(str(c))
                        headers = [h if h is not None else "" for h in hdrs]
                    else:
                        headers = []
                else:
                    headers = []

                if not headers:
                    # fallback to single column with table name
                    headers = [filename_hint.replace(".csv", "")]

                csv_text = build_csv_text_from_rows(headers, rows)
                zf.writestr(csv_filename, csv_text.encode("utf-8"))

    # finalize zip buffer
    zip_buf.seek(0)

    return zip_buf

async def metadata_convert(data):
    if isinstance(data,str):
        if data.startswith('http'):
            # get file
            content = await fetch_bytes(data)
            data = content.decode("utf-8", errors="replace")
        else:
            # try to parse as JSON string
            try:
                data = json.loads(data)
            except Exception as ex:
                raise Exception(f"Invalid input: {ex}")

    md = None
    csvw_tables = parse_metadata(data)

    if not csvw_tables or len(csvw_tables) == 0:
        raise Exception("No tables found in metadata")
    
    return {
        "@context": ["https://www.w3.org/ns/csvw.jsonld", context_map],
        "tables": csvw_tables
    }