# ----------------------------------------------------------------------------
#  Copyright (C) 2026 ISRIC - World Soil Information
#  Produced in the Scope of the SoilWISE Project
#  SoilWISE is funded by the European Union’s Horizon Europe research and 
#  innovation programme under grant agreement No 101056973.
# ----------------------------------------------------------------------------

from rdflib import Graph, Namespace, BNode, URIRef, Literal
from rdflib.namespace import RDF, RDFS, XSD, SKOS, DCTERMS



# ---- Helpers ----
def bn_to_urn(node):
    """Create a stable URN for a Blank Node or non-URI node."""
    s = str(node)
    h = hashlib.sha1(s.encode("utf-8")).hexdigest()
    return f"urn:bn:{h}"

def node_to_uri(node):
    """Return a stable string identifier for a node (URI or BNode)."""
    if isinstance(node, URIRef):
        return str(node)
    if isinstance(node, BNode):
        return bn_to_urn(node)
    if isinstance(node, Literal):
        return str(node)
    return str(node)

def first_value(g, subject, predicate):
    """Return the first value for subject predicate or None (as python object)."""
    v = next(g.objects(subject, predicate), None)
    if v is None:
        return None
    # For rdflib LITERAL / URIRef return python value appropriately
    if isinstance(v, Literal):
        return str(v)
    return v  # keep URIRef or BNode for further lookup

def label_for(g, node):
    """Prefer rdfs:label, then skos:prefLabel if present, else the URI / bnode string."""
    if node is None:
        return None
    lab = first_value(g, node, RDFS.label) or first_value(g, node, SKOS.prefLabel) or first_value(g, node, DCTERMS.title)
    if lab:
        return str(lab)
    # fallback to node's string/uri
    return node_to_uri(node)

def get_pref_labels_from_remote(concept_uri):
    g = Graph()
    # rdflib will try to GET the resource and parse it (it guesses format)
    try:
        g.parse(concept_uri)  # remove format if server may return another RDF format
        uri = URIRef(concept_uri)
        return list(g.objects(uri, SKOS.prefLabel))
    except Exception as ex:
        print('failed get prefLabel for skos term',concept_uri,ex)

def types_text_for(g, node):
    """
    Return a text representation for rdf:type values on `node`.
    - Return comma-joined localnames of the types (e.g. 'Feature,Profile').
    - Returns None if no types found.
    """
    types = list(g.objects(node, RDF.type))
    if not types:
        return None

    # fallback: collect localnames or full URIs if localname not available
    def localname(uri):
        s = str(uri)
        if "#" in s:
            return s.split("#", 1)[1]
        if "/" in s:
            return s.rsplit("/", 1)[1]
        return s

    names = []
    for t in types:
        if isinstance(t, URIRef):
            names.append(localname(t))
        else:
            names.append(str(t))
    return ",".join(names)

