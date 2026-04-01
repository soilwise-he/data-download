# Soil data streamer

A processing API which facilitates harmonised data downloads from enriched tabular formats.

The tabular formats (in repositories such as zenodo) which are enriched with a [CSV-W](https://csvw.org) context metadata, 
can be converted through this service as rdf (ttl, xml, json-ld) or Geopackage (sqlite) 

The metadata can live with the data in repositories (suc as zenodo.org), or be made available elsewhere.

## API methods

**convert**: converts one or more tables to a [SOSA](https://www.w3.org/TR/vocab-ssn/) or [schema.org](https://schema.org/Observation) based knowledge graph, a [Soil geopackage](https://github.com/ejpsoil/inspire_soil_gpkg_template) (sqlite) or [INSPIRE Soil GML](https://inspire-mif.github.io/technical-guidelines/data/so/dataspecification_so.pdf)

**graph2gpkg**: converts a [SOSA](https://www.w3.org/TR/vocab-ssn/) or [schema.org](https://schema.org/Observation) based knowledge graph to a [Soil geopackage](https://github.com/ejpsoil/inspire_soil_gpkg_template) (sqlite) or [INSPIRE Soil GML](https://inspire-mif.github.io/technical-guidelines/data/so/dataspecification_so.pdf)

**suggest**: suggests a [SOSA](https://www.w3.org/TR/vocab-ssn/) or [schema.org](https://schema.org/Observation) based csvw configuration from one or more tables

**import**: imports a metadata format and converts to csvw, supported metadata formats are [datapackage tableschema](https://datapackage.org/standard/table-schema/), [metadata control file](https://geopython.github.io/pygeometa/reference/mcf/) and [Soilwise metadata csv](#soilwise-metadata-csv)

**export**: exports the tables and csvw configuration of a project as a zipfile, which you can deposit as-is on a repository like [Zenodo](https://zenodo.org)

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
PYTHONPATH=. pytest tests
```

## Soilwise Metadata CSV

The Soilwise metadata CSV format is a CSV format, where each row represents a column in the dataset which is described by the CSV. The fields in each row are the properties of each dataset column: observed property, unit of measure, observation procedure, field type.

A Excel template is available, which can be used to easily create this format.

## Soilwise HE project

This component has been developed in the scope of the [Soilwise-he project](https://soilwise-he.eu).
The project has received funding of the European Commission via Horizon Europe grantnr [101112838](https://cordis.europa.eu/project/id/101112838). 