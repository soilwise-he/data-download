# ----------------------------------------------------------------------------
#  Copyright (C) 2026 ISRIC - World Soil Information
#  Produced in the Scope of the SoilWISE Project
#  SoilWISE is funded by the European Union’s Horizon Europe research and 
#  innovation programme under grant agreement No 101056973.
# ----------------------------------------------------------------------------


csv_sample = """TREEID,LAT,LONG
1,20,10
2,40,20
5,10,12
"""
csv_tab_sample = """TREEID\tLAT\tLONG
1\t20\t10
2\t40\t20
5\t10\t12
"""
csv_noheader_sample = """1;20;10
2;40;20
5;10;12
"""


csvw_sample = {"@context": [ "http://www.w3.org/ns/csvw"],
    "tables": [{
        "url": "https://raw.githubusercontent.com/soilwise-he/soil-observation-data-encodings/refs/heads/main/CSVW/examples/example2/trees.csv",
        "aboutUrl": "https://soilwise.example.org/tree/{TREEID}",
        "tableSchema": {"columns": [{
                "titles": "TREEID",
                "datatype": "string",
                "propertyUrl": "dcterms:identifier"
            }, {
                "titles": "lat",
                "propertyUrl": "schema:latitude",
                "datatype": "number"
            }, {
                "titles": "long",
                "propertyUrl": "schema:longitude",
                "datatype": "number"
        }]}}
    ]}

graphdata = '''
@prefix dcterms: <http://purl.org/dc/terms/> .
@prefix qudt: <http://qudt.org/1.1/schema/qudt#> .
@prefix sosa: <http://www.w3.org/ns/sosa/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
<https://soilwise.example.com/example3/AIZ34/K> a sosa:Observation ;
    sosa:hasFeatureOfInterest <https://soilwise.example.com/example3/AIZ34> ;
    sosa:hasResult <https://soilwise.example.com/example3/AIZ34/K/QV> ;
    sosa:observedProperty <http://w3id.org/glosis/model/codelists/physioChemicalPropertyCode-Potext> ;
    sosa:resultTime "2025-03-01T00:00:00+00:00"^^xsd:dateTime ;
    sosa:usedProcedure <http://w3id.org/glosis/model/procedure/exchangeableBasesProcedure-ExchBases_ph7-nh4oac-fp> .
<https://soilwise.example.com/example3/AIZ34/N> a sosa:Observation ;
    sosa:hasFeatureOfInterest <https://soilwise.example.com/example3/AIZ34> ;
    sosa:hasResult <https://soilwise.example.com/example3/AIZ34/N/QV> ;
    sosa:observedProperty <http://w3id.org/glosis/model/codelists/physioChemicalPropertyCode-Nittot> ;
    sosa:resultTime "2025-03-01T00:00:00+00:00"^^xsd:dateTime ;
    sosa:usedProcedure <http://w3id.org/glosis/model/procedure/nitrogenTotalProcedure-TotalN_dc> .
<https://soilwise.example.com/example3/AIZ34/P> a sosa:Observation ;
    sosa:hasFeatureOfInterest <https://soilwise.example.com/example3/AIZ34> ;
    sosa:hasResult <https://soilwise.example.com/example3/AIZ34/P/QV> ;
    sosa:observedProperty <http://w3id.org/glosis/model/codelists/physioChemicalPropertyCode-Phoext> ;
    sosa:resultTime "2025-03-01T00:00:00+00:00"^^xsd:dateTime ;
    sosa:usedProcedure <http://w3id.org/glosis/model/procedure/PhosphorusRetentionProcedure> .
<https://soilwise.example.com/example3/BS385/K> a sosa:Observation ;
    sosa:hasFeatureOfInterest <https://soilwise.example.com/example3/BS385> ;
    sosa:hasResult <https://soilwise.example.com/example3/BS385/K/QV> ;
    sosa:observedProperty <http://w3id.org/glosis/model/codelists/physioChemicalPropertyCode-Potext> ;
    sosa:resultTime "2025-03-01T00:00:00+00:00"^^xsd:dateTime ;
    sosa:usedProcedure <http://w3id.org/glosis/model/procedure/exchangeableBasesProcedure-ExchBases_ph7-nh4oac-fp> .
<https://soilwise.example.com/example3/BS385/N> a sosa:Observation ;
    sosa:hasFeatureOfInterest <https://soilwise.example.com/example3/BS385> ;
    sosa:hasResult <https://soilwise.example.com/example3/BS385/N/QV> ;
    sosa:observedProperty <http://w3id.org/glosis/model/codelists/physioChemicalPropertyCode-Nittot> ;
    sosa:resultTime "2025-03-01T00:00:00+00:00"^^xsd:dateTime ;
    sosa:usedProcedure <http://w3id.org/glosis/model/procedure/nitrogenTotalProcedure-TotalN_dc> .
<https://soilwise.example.com/example3/BS385/P> a sosa:Observation ;
    sosa:hasFeatureOfInterest <https://soilwise.example.com/example3/BS385> ;
    sosa:hasResult <https://soilwise.example.com/example3/BS385/P/QV> ;
    sosa:observedProperty <http://w3id.org/glosis/model/codelists/physioChemicalPropertyCode-Phoext> ;
    sosa:resultTime "2025-03-01T00:00:00+00:00"^^xsd:dateTime ;
    sosa:usedProcedure <http://w3id.org/glosis/model/procedure/PhosphorusRetentionProcedure> .
<https://soilwise.example.com/example3/TS325/K> a sosa:Observation ;
    sosa:hasFeatureOfInterest <https://soilwise.example.com/example3/TS325> ;
    sosa:hasResult <https://soilwise.example.com/example3/TS325/K/QV> ;
    sosa:observedProperty <http://w3id.org/glosis/model/codelists/physioChemicalPropertyCode-Potext> ;
    sosa:resultTime "2025-03-01T00:00:00+00:00"^^xsd:dateTime ;
    sosa:usedProcedure <http://w3id.org/glosis/model/procedure/exchangeableBasesProcedure-ExchBases_ph7-nh4oac-fp> .
<https://soilwise.example.com/example3/TS325/N> a sosa:Observation ;
    sosa:hasFeatureOfInterest <https://soilwise.example.com/example3/TS325> ;
    sosa:hasResult <https://soilwise.example.com/example3/TS325/N/QV> ;
    sosa:observedProperty <http://w3id.org/glosis/model/codelists/physioChemicalPropertyCode-Nittot> ;
    sosa:resultTime "2025-03-01T00:00:00+00:00"^^xsd:dateTime ;
    sosa:usedProcedure <http://w3id.org/glosis/model/procedure/nitrogenTotalProcedure-TotalN_dc> .
<https://soilwise.example.com/example3/TS325/P> a sosa:Observation ;
    sosa:hasFeatureOfInterest <https://soilwise.example.com/example3/TS325> ;
    sosa:hasResult <https://soilwise.example.com/example3/TS325/P/QV> ;
    sosa:observedProperty <http://w3id.org/glosis/model/codelists/physioChemicalPropertyCode-Phoext> ;
    sosa:resultTime "2025-03-01T00:00:00+00:00"^^xsd:dateTime ;
    sosa:usedProcedure <http://w3id.org/glosis/model/procedure/PhosphorusRetentionProcedure> .
<https://soilwise.example.com/example3/UAI234/K> a sosa:Observation ;
    sosa:hasFeatureOfInterest <https://soilwise.example.com/example3/UAI234> ;
    sosa:hasResult <https://soilwise.example.com/example3/UAI234/K/QV> ;
    sosa:observedProperty <http://w3id.org/glosis/model/codelists/physioChemicalPropertyCode-Potext> ;
    sosa:resultTime "2025-03-01T00:00:00+00:00"^^xsd:dateTime ;
    sosa:usedProcedure <http://w3id.org/glosis/model/procedure/exchangeableBasesProcedure-ExchBases_ph7-nh4oac-fp> .
<https://soilwise.example.com/example3/UAI234/N> a sosa:Observation ;
    sosa:hasFeatureOfInterest <https://soilwise.example.com/example3/UAI234> ;
    sosa:hasResult <https://soilwise.example.com/example3/UAI234/N/QV> ;
    sosa:observedProperty <http://w3id.org/glosis/model/codelists/physioChemicalPropertyCode-Nittot> ;
    sosa:resultTime "2025-03-01T00:00:00+00:00"^^xsd:dateTime ;
    sosa:usedProcedure <http://w3id.org/glosis/model/procedure/nitrogenTotalProcedure-TotalN_dc> .
<https://soilwise.example.com/example3/UAI234/P> a sosa:Observation ;
    sosa:hasFeatureOfInterest <https://soilwise.example.com/example3/UAI234> ;
    sosa:hasResult <https://soilwise.example.com/example3/UAI234/P/QV> ;
    sosa:observedProperty <http://w3id.org/glosis/model/codelists/physioChemicalPropertyCode-Phoext> ;
    sosa:resultTime "2025-03-01T00:00:00+00:00"^^xsd:dateTime ;
    sosa:usedProcedure <http://w3id.org/glosis/model/procedure/PhosphorusRetentionProcedure> .
<https://soilwise.example.com/example3/AIZ34/K/QV> a qudt:QuantityValue ;
    qudt:hasUnit <http://qudt.org/vocab/unit/MicroMOL-PER-KiloGM> ;
    qudt:value ".72" .
<https://soilwise.example.com/example3/AIZ34/N/QV> a qudt:QuantityValue ;
    qudt:hasUnit <http://qudt.org/vocab/unit/MicroMOL-PER-KiloGM> ;
    qudt:value ".15" .
<https://soilwise.example.com/example3/AIZ34/P/QV> a qudt:QuantityValue ;
    qudt:hasUnit <http://qudt.org/vocab/unit/MicroMOL-PER-KiloGM> ;
    qudt:value ".18" .
<https://soilwise.example.com/example3/BS385/K/QV> a qudt:QuantityValue ;
    qudt:hasUnit <http://qudt.org/vocab/unit/MicroMOL-PER-KiloGM> ;
    qudt:value ".14" .
<https://soilwise.example.com/example3/BS385/N/QV> a qudt:QuantityValue ;
    qudt:hasUnit <http://qudt.org/vocab/unit/MicroMOL-PER-KiloGM> ;
    qudt:value ".82" .
<https://soilwise.example.com/example3/BS385/P/QV> a qudt:QuantityValue ;
    qudt:hasUnit <http://qudt.org/vocab/unit/MicroMOL-PER-KiloGM> ;
    qudt:value ".22" .
<https://soilwise.example.com/example3/TS325/K/QV> a qudt:QuantityValue ;
    qudt:hasUnit <http://qudt.org/vocab/unit/MicroMOL-PER-KiloGM> ;
    qudt:value ".24" .
<https://soilwise.example.com/example3/TS325/N/QV> a qudt:QuantityValue ;
    qudt:hasUnit <http://qudt.org/vocab/unit/MicroMOL-PER-KiloGM> ;
    qudt:value ".8" .
<https://soilwise.example.com/example3/TS325/P/QV> a qudt:QuantityValue ;
    qudt:hasUnit <http://qudt.org/vocab/unit/MicroMOL-PER-KiloGM> ;
    qudt:value ".12" .
<https://soilwise.example.com/example3/UAI234/K/QV> a qudt:QuantityValue ;
    qudt:hasUnit <http://qudt.org/vocab/unit/MicroMOL-PER-KiloGM> ;
    qudt:value ".12" .
<https://soilwise.example.com/example3/UAI234/N/QV> a qudt:QuantityValue ;
    qudt:hasUnit <http://qudt.org/vocab/unit/MicroMOL-PER-KiloGM> ;
    qudt:value ".5" .
<https://soilwise.example.com/example3/UAI234/P/QV> a qudt:QuantityValue ;
    qudt:hasUnit <http://qudt.org/vocab/unit/MicroMOL-PER-KiloGM> ;
    qudt:value ".8" .
<https://soilwise.example.com/example3/AIZ34> a <http://w3id.org/glosis/model/iso28258/2013/Layer> ;
    dcterms:identifier "AIZ34" .
<https://soilwise.example.com/example3/BS385> a <http://w3id.org/glosis/model/iso28258/2013/Layer> ;
    dcterms:identifier "BS385" .
<https://soilwise.example.com/example3/TS325> a <http://w3id.org/glosis/model/iso28258/2013/Layer> ;
    dcterms:identifier "TS325" .
<https://soilwise.example.com/example3/UAI234> a <http://w3id.org/glosis/model/iso28258/2013/Layer> ;
    dcterms:identifier "UAI234" .'''