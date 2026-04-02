# ----------------------------------------------------------------------------
#  Copyright (C) 2026 ISRIC - World Soil Information
#  Produced in the Scope of the SoilWISE Project
#  SoilWISE is funded by the European Union’s Horizon Europe research and 
#  innovation programme under grant agreement No 101056973.
# ----------------------------------------------------------------------------

from lxml import etree
import struct

NSMAP = {
    "gml": "http://www.opengis.net/gml/3.2",
    "om": "http://www.opengis.net/om/2.0",
    "so": "http://inspire.ec.europa.eu/schemas/so/4.0",
    "xlink": "http://www.w3.org/1999/xlink"
}

GML = NSMAP["gml"]
OM = NSMAP["om"]
SO = NSMAP["so"]
XLINK = NSMAP["xlink"]


def safe_id(uri: str) -> str:
    return uri.replace(":", "_").replace("/", "_")


def decode_gpkg_point(blob: bytes):
    if blob is None:
        return None, None

    wkb = blob[8:]

    endian = wkb[0]
    geom_type = struct.unpack("<I", wkb[1:5])[0]

    if endian != 1 or geom_type != 1:
        return None, None

    x = struct.unpack("<d", wkb[5:13])[0]
    y = struct.unpack("<d", wkb[13:21])[0]

    return y, x  # lat, lon


def build_inspire_gml(conn):
    cur = conn.cursor()

    cur.execute("""
    SELECT
        o.observation_uri,
        o.phenomenon_time,
        o.procedure_id,
        o.property_id,
        o.foi_id,

        r.value,
        r.unit_of_measure_id,

        f.uri,
        f.label,
        f.geom

    FROM observation o
    LEFT JOIN result r ON r.id = o.result_id
    LEFT JOIN feature_of_interest f ON f.uri = o.foi_id
    """)

    rows = cur.fetchall()

    root = etree.Element(f"{{{GML}}}FeatureCollection", nsmap=NSMAP)

    for row in rows:
        (
            obs_uri, phen_time, proc_id, prop_id, foi_id,
            value, uom,
            foi_uri, foi_label, geom_blob
        ) = row

        member = etree.SubElement(root, f"{{{GML}}}featureMember")

        om_obs = etree.SubElement(member, f"{{{OM}}}OM_Observation")
        om_obs.set(f"{{{GML}}}id", safe_id(obs_uri))

        # phenomenonTime
        if phen_time:
            pt = etree.SubElement(om_obs, f"{{{OM}}}phenomenonTime")
            ti = etree.SubElement(pt, f"{{{GML}}}TimeInstant")
            tp = etree.SubElement(ti, f"{{{GML}}}timePosition")
            tp.text = phen_time

        # observedProperty
        if prop_id:
            prop = etree.SubElement(om_obs, f"{{{OM}}}observedProperty")
            prop.set(f"{{{XLINK}}}href", str(prop_id))

        # procedure
        if proc_id:
            proc = etree.SubElement(om_obs, f"{{{OM}}}procedure")
            proc.set(f"{{{XLINK}}}href", str(proc_id))

        # result
        if value is not None:
            res = etree.SubElement(om_obs, f"{{{OM}}}result")
            if uom:
                res.set("uom", str(uom))
            res.text = str(value)

        # featureOfInterest (OPTIONAL)
        if foi_uri:
            foi_el = etree.SubElement(om_obs, f"{{{OM}}}featureOfInterest")

            soil = etree.SubElement(foi_el, f"{{{SO}}}SoilBody")
            soil.set(f"{{{GML}}}id", safe_id(foi_uri))

            if foi_label:
                name = etree.SubElement(soil, f"{{{GML}}}name")
                name.text = foi_label

            lat, lon = decode_gpkg_point(geom_blob)

            if lat is not None and lon is not None:
                shape = etree.SubElement(soil, f"{{{SO}}}shape")

                point = etree.SubElement(
                    shape,
                    f"{{{GML}}}Point",
                    srsName="EPSG:4326"
                )

                pos = etree.SubElement(point, f"{{{GML}}}pos")
                pos.text = f"{lat} {lon}"

    return etree.tostring(
        root,
        pretty_print=True,
        xml_declaration=True,
        encoding="UTF-8"
    )