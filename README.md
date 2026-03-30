# Soil data streamer

A processing API which facilitates harmonised data downloads from enriched tabular formats.

The tabular formats (in repositories such as zenodo) which are enriched with a [CSV-W](https://csvw.org) context metadata, 
can be converted through this service as rdf (ttl, xml, json-ld) or Geopackage (sqlite) 

The metadata can live with the data in repositories (suc as zenodo.org), or be made available elsewhere.

## API methods

**convert**: converts one or more tables to a knowledge graph, from a csvw configuration

**suggest**: suggests a csvw configuration from one or more tables

**export**: exports the tables and csvw configuration of a project as a zipfile





## Run in a local python environment

```
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
```

## Run in docker

A docker image is available

```
docker run -p8000:8000 ghcr.io//soilwise-he/data-download:latest
```

## Storage

This service does not need data storage (users download the results directly). 
However you can enable storage of metadata on a database, provide postgres connection details via env variables

## Run tests

```
pip install -r requirements.txt
pytest test
```

## Soilwise HE project

This component has been developed in the scope of the [Soilwise-he project](https://soilwise-he.eu).
The project has received funding of the European Commission via Horizon Europe grantnr [101112838](https://cordis.europa.eu/project/id/101112838). 