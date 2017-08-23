# Based on http://code.activestate.com/recipes/577722-xml-to-python-dictionary-and-back/
# Create python xml structures compatible with
# http://search.cpan.org/~grantm/XML-Simple-2.18/lib/XML/Simple.pm

from lxml import etree
from itertools import groupby

def xml2d(e):
    """Convert an etree into a dict structure

    @type  e: etree.Element
    @param e: the root of the tree
    @return: The dictionary representation of the XML tree
    """
    # Namespace magic
    defns = ""
    if None in e.nsmap:
        defns = "{{{0}}}".format(e.nsmap[None])

    def _xml2d(e):
        kids = {}
        for key in dict(e.attrib):
            kids["#" + key] = e.attrib[key]
        for k, g in groupby(e, lambda x: x.tag):
            if isinstance(k,str):
                g = [_xml2d(x) for x in g]

                if k.startswith(defns):
                    real_k = k.replace(defns,"")
                else:
                    real_k = k

                if real_k in kids:
                    if isinstance(kids[real_k], list):
                        kids[real_k].extend(g)
                    elif len(g) > 1:
                        g.append(kids[real_k])
                        kids[real_k] = g
                    else:
                        kids[real_k] = [kids[real_k], g[0]]
                else:
                    if len(g) == 1:
                        kids[real_k] = g[0]
                    else:
                        kids[real_k] = g

            else:
                # print(k, g)
                pass
        if not kids:
            if e.text:
                return e.text.strip()
            else:
                return ""
        return kids

    tag = e.tag
    if tag.startswith(defns):
        tag = tag.replace(defns,"")

    return {tag: _xml2d(e), '!namespaces': e.nsmap}


def d2xml(d):
    """convert dict to xml

       1. The top level d must contain a single entry i.e. the root element
       2.  Keys of the dictionary become sublements or attributes
       3.  Keys that start with # are attributes
       4.  If a value is a simple string, then the key subelement and the value is text.
       5.  if a value is dict then, then key is a subelement
       6.  if a value is list, then key is a set of sublements

       a  = { 'module' : {'tag' : [ { 'name': 'a', 'value': 'b'},
                                    { 'name': 'c', 'value': 'd'},
                                 ],
                          'gobject' : { 'name': 'g', 'type':'xx' },
                          'uri' : 'test',
                       }
           }
    >>> d2xml(a)
    <module uri="test">
       <gobject type="xx" name="g"/>
       <tag name="a" value="b"/>
       <tag name="c" value="d"/>
    </module>

    @type  d: dict
    @param d: A dictionary formatted as an XML document
    @return:  A etree Root element
    """
    def _d2xml(d, p):
        for k,v in d.items():
            if k.startswith("#"):
               p.set(k[1:],v)
            elif isinstance(v,dict):
                node = etree.SubElement(p, k)
                _d2xml(v, node)
            elif isinstance(v,list):
                for item in v:
                    node = etree.SubElement(p, k)
                    _d2xml(item, node)
            else:
                node = etree.SubElement(p, k)
                node.text = v

    # Namespace magic
    nsmap = None
    if '!namespaces' in d:
        nsmap = d['!namespaces']
        del d['!namespaces']
    k,v = d.items()[0]
    if nsmap:
        d['!namespaces'] = nsmap
    node = etree.Element(k, nsmap=nsmap)
    _d2xml(v, node)

    return node



if __name__=="__main__":

    # X = """<T uri="boo"><a n="1"/><a n="2"/><b n="3"><c x="y"/></b><d>Test</d></T>"""
    # print X
    # Y = xml2d(etree.XML(X))
    # print Y
    # Z = etree.tostring (d2xml(Y) )
    # print Z
    # assert X == Z

    from StringIO import StringIO
    x = etree.parse(StringIO("""<cap><test>
            <a>A</a>
            <!-- test tester -->
            <b>B</b>
            <!-- test3 tester -->
            <b>B</b>
            <b>B</b>
            <b>B</b>
            <!-- test2 tester -->
            <b>B</b>
            <c>C</c>
            <c>C</c>
        </test></cap>
    """))
    print(xml2d(x.getroot()))
    test2 = """<?xml version="1.0" encoding="UTF-8"?>
    <archive xmlns="http://rs.tdwg.org/dwc/text/">
        <core encoding="UTF-8" fieldsTerminatedBy="," linesTerminatedBy="\n" fieldsEnclosedBy='"' ignoreHeaderLines="1" rowType="http://rs.tdwg.org/dwc/terms/Occurrence">
            <files>
                <location>occurrence.txt</location>
            </files>
            <id index="0" />
            <!-- Occurrence fields -->
            <field index="0" term="http://rs.tdwg.org/dwc/terms/catalogNumber"/>
            <field index="1" term="http://rs.tdwg.org/dwc/terms/institutionCode"/>
            <field index="2" term="http://rs.tdwg.org/dwc/terms/collectionCode"/>
            <field index="3" term="http://rs.tdwg.org/dwc/terms/basisOfRecord"/>
            <field index="4" term="http://rs.tdwg.org/dwc/terms/occurrenceID"/>
            <field index="5" term="http://rs.tdwg.org/dwc/terms/recordNumber"/>
            <field index="6" term="http://purl.org/dc/terms/modified"/>
            <field index="7" term="http://rs.tdwg.org/dwc/terms/recordedBy"/>
            <field index="8" term="http://rs.tdwg.org/dwc/terms/fieldNumber"/>
            <field index="9" term="http://rs.tdwg.org/dwc/terms/samplingProtocol"/>
            <field index="10" term="http://rs.tdwg.org/dwc/terms/habitat"/>
            <field index="11" term="http://rs.tdwg.org/dwc/terms/eventRemarks"/>
            <field index="12" term="http://rs.tdwg.org/dwc/terms/verbatimElevation"/>
            <field index="13" term="http://rs.tdwg.org/dwc/terms/minimumElevationInMeters"/>
            <field index="14" term="http://rs.tdwg.org/dwc/terms/maximumElevationInMeters"/>
            <field index="15" term="http://rs.tdwg.org/dwc/terms/verbatimDepth"/>
            <field index="16" term="http://rs.tdwg.org/dwc/terms/minimumDepthInMeters"/>
            <field index="17" term="http://rs.tdwg.org/dwc/terms/maximumDepthInMeters"/>
            <field index="18" term="http://rs.tdwg.org/dwc/terms/minimumDistanceAboveSurfaceInMeters"/>
            <field index="19" term="http://rs.tdwg.org/dwc/terms/maximumDistanceAboveSurfaceInMeters"/>
            <field index="20" term="http://rs.tdwg.org/dwc/terms/country"/>
            <field index="21" term="http://rs.tdwg.org/dwc/terms/countryCode"/>
            <field index="22" term="http://rs.tdwg.org/dwc/terms/locality"/>
            <field index="23" term="http://rs.tdwg.org/dwc/terms/locationRemarks"/>
            <field index="24" term="http://rs.tdwg.org/dwc/terms/eventDate"/>
            <field index="25" term="http://rs.tdwg.org/dwc/terms/verbatimEventDate"/>
            <field index="26" term="http://rs.tdwg.org/dwc/terms/eventTime"/>
            <field index="27" term="http://rs.tdwg.org/dwc/terms/startDayOfYear"/>
            <field index="28" term="http://rs.tdwg.org/dwc/terms/endDayOfYear"/>
            <field index="29" term="http://rs.tdwg.org/dwc/terms/occurrenceDetails"/>
            <field index="30" term="http://rs.tdwg.org/dwc/terms/occurrenceRemarks"/>
            <field index="31" term="http://rs.tdwg.org/dwc/terms/sex"/>
            <field index="32" term="http://rs.tdwg.org/dwc/terms/verbatimCoordinates"/>
            <field index="33" term="http://rs.tdwg.org/dwc/terms/decimalLatitude"/>
            <field index="34" term="http://rs.tdwg.org/dwc/terms/decimalLongitude"/>
            <field index="35" term="http://rs.tdwg.org/dwc/terms/coordinateUncertaintyInMeters"/>
            <field index="36" term="http://rs.tdwg.org/dwc/terms/coordinatePrecision"/>
            <field index="37" term="http://rs.tdwg.org/dwc/terms/georeferenceProtocol"/>
            <field index="38" term="http://rs.tdwg.org/dwc/terms/geodeticDatum"/>
            <field index="39" term="http://rs.tdwg.org/dwc/terms/typeStatus"/>
            <field index="40" term="http://rs.tdwg.org/dwc/terms/stateProvince"/>
            <field index="41" term="http://rs.tdwg.org/dwc/terms/county"/>
            <field index="42" term="http://rs.tdwg.org/dwc/terms/municipality"/>
            <field index="43" term="http://rs.tdwg.org/dwc/terms/continent"/>
            <field index="44" term="http://rs.tdwg.org/dwc/terms/waterBody"/>
            <field index="45" term="http://rs.tdwg.org/dwc/terms/islandGroup"/>
            <field index="46" term="http://rs.tdwg.org/dwc/terms/island"/>
            <field index="47" term="http://rs.tdwg.org/dwc/terms/higherGeography"/>
            <!-- Identification fields -->
            <field index="48" term="http://rs.tdwg.org/dwc/terms/dateIdentified"/>
            <field index="49" term="http://rs.tdwg.org/dwc/terms/identifiedBy"/>
            <field index="50" term="http://rs.tdwg.org/dwc/terms/nomenclaturalCode"/>
            <field index="51" term="http://rs.tdwg.org/dwc/terms/taxonRemarks"/>
            <field index="52" term="http://rs.tdwg.org/dwc/terms/identificationQualifier"/>
            <field index="53" term="http://rs.tdwg.org/dwc/terms/identificationRemarks"/>
            <field index="54" term="http://rs.tdwg.org/dwc/terms/identificationVerificationStatus"/>
            <field index="55" term="http://rs.tdwg.org/dwc/terms/identificationReferences"/>
            <field index="56" term="http://rs.tdwg.org/dwc/terms/scientificName"/>
            <field index="57" term="http://rs.tdwg.org/dwc/terms/scientificNameAuthorship"/>
            <field index="58" term="http://rs.tdwg.org/dwc/terms/higherClassification"/>
            <field index="59" term="http://rs.tdwg.org/dwc/terms/kingdom"/>
            <field index="60" term="http://rs.tdwg.org/dwc/terms/phylum"/>
            <field index="61" term="http://rs.tdwg.org/dwc/terms/class"/>
            <field index="62" term="http://rs.tdwg.org/dwc/terms/order"/>
            <field index="63" term="http://rs.tdwg.org/dwc/terms/family"/>
            <field index="64" term="http://rs.tdwg.org/dwc/terms/genus"/>
            <field index="65" term="http://rs.tdwg.org/dwc/terms/subgenus"/>
            <field index="66" term="http://rs.tdwg.org/dwc/terms/specificEpithet"/>
            <field index="67" term="http://rs.tdwg.org/dwc/terms/infraspecificEpithet"/>
            <field index="68" term="http://rs.tdwg.org/dwc/terms/taxonRank"/>
        </core>
        <extension encoding="UTF-8" fieldsTerminatedBy="," linesTerminatedBy="\n" fieldsEnclosedBy='"' ignoreHeaderLines="1" rowType="http://rs.tdwg.org/dwc/terms/Identification">
            <files>
                <location>identification.txt</location>
            </files>
            <coreid index="0" />
            <field index="1" term="http://rs.tdwg.org/dwc/terms/dateIdentified"/>
            <field index="2" term="http://rs.tdwg.org/dwc/terms/identifiedBy"/>
            <field index="3" term="http://rs.tdwg.org/dwc/terms/nomenclaturalCode"/>
            <field index="4" term="http://rs.tdwg.org/dwc/terms/taxonRemarks"/>
            <field index="5" term="http://rs.tdwg.org/dwc/terms/identificationQualifier"/>
            <field index="6" term="http://rs.tdwg.org/dwc/terms/identificationRemarks"/>
            <field index="7" term="http://rs.tdwg.org/dwc/terms/identificationVerificationStatus"/>
            <field index="8" term="http://rs.tdwg.org/dwc/terms/identificationReferences"/>
            <field index="9" term="http://rs.tdwg.org/dwc/terms/scientificName"/>
            <field index="10" term="http://rs.tdwg.org/dwc/terms/scientificNameAuthorship"/>
            <field index="11" term="http://rs.tdwg.org/dwc/terms/higherClassification"/>
            <field index="12" term="http://rs.tdwg.org/dwc/terms/kingdom"/>
            <field index="13" term="http://rs.tdwg.org/dwc/terms/phylum"/>
            <field index="14" term="http://rs.tdwg.org/dwc/terms/class"/>
            <field index="15" term="http://rs.tdwg.org/dwc/terms/order"/>
            <field index="16" term="http://rs.tdwg.org/dwc/terms/family"/>
            <field index="17" term="http://rs.tdwg.org/dwc/terms/genus"/>
            <field index="18" term="http://rs.tdwg.org/dwc/terms/subgenus"/>
            <field index="19" term="http://rs.tdwg.org/dwc/terms/specificEpithet"/>
            <field index="20" term="http://rs.tdwg.org/dwc/terms/infraspecificEpithet"/>
            <field index="21" term="http://rs.tdwg.org/dwc/terms/taxonRank"/>
        </extension>
        <extension encoding="UTF-8" fieldsTerminatedBy="," linesTerminatedBy="\n" fieldsEnclosedBy='"' ignoreHeaderLines="1" rowType="http://rs.gbif.org/terms/1.0/Image">
            <files>
                <location>image.txt</location>
            </files>
            <coreid index="0" />
            <field index="1" term="http://purl.org/dc/terms/identifier"/>
            <field index="2" term="http://purl.org/dc/terms/description"/>
            <field index="3" term="http://purl.org/dc/terms/format"/>
            <field index="4" term="http://purl.org/dc/terms/created"/>
            <field index="5" term="http://purl.org/dc/terms/creator"/>
            <field index="6" term="http://purl.org/dc/terms/license"/>
            <field index="7" term="http://purl.org/dc/terms/rightsHolder"/>
        </extension>
    </archive>
    """
    x = etree.parse(StringIO(test2))
    print(xml2d(x.getroot()))
