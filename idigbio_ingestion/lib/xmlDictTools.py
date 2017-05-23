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
                if len(g) == 1:
                    g = g[0]

                if k.startswith(defns):
                    kids[k.replace(defns,"")] = g
                else:
                    kids[k] = g
            else:
                #print(k, g)
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
    x = etree.parse(StringIO("<test><a>A</a><!-- test --><b>B</b></test>"))
    xml2d(x.getroot())
