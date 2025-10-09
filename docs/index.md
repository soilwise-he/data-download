# soil data streamer

a component which facilitates harmonised data downloads from enriched tabular formats

those tabular formats (in repositories such as zenodo) which are enriched with a [CSV-W](https://csvw.org) context metadata, 
can be converted through this service as rdf (ttl, xml, json-ld) or Geopackage (sqlite) 

the metadata can live with the data in zenodo, or be made available within the soilwise infrastructure

```
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
```

A docker image is available

to store metadata on a database, provide postgres connection details via env variables