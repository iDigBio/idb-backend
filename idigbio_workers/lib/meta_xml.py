from fieldnames import namespaces_rev

meta = """
<archive xmlns="http://rs.tdwg.org/dwc/text/">
{0}
</archive>
"""


file_xml = """
  <{file_type} encoding="{encoding}" fieldsTerminatedBy="{field_terminator}" linesTerminatedBy="{line_termiantor}" fieldsEnclosedBy="{field_enclosure}" ignoreHeaderLines="{ignore_headers}" rowType="{row_type}">
    <files>
      <location>{filename}</location>
    </files>
    <{id_type} index="{id_index}" />
{fields}
  </{file_type}>
"""

defaults = {
    "file_type": "core",
    "row_type": "http://rs.tdwg.org/dwc/terms/Occurrence",
    "encoding": "utf-8",
    "field_terminator": ",",
    "field_enclosure": "&quot;",
    "line_termiantor": "\\n",
    "ignore_headers": 1,
    "id_type": "id",
    "id_index": 0
}


def make_meta(files):
    f_string = ""
    for f in files:
        f_string += f
    return meta.format(f_string)

def make_field(index=0,term=""):
    term_a = term.split(":")
    if term_a[0] in namespaces_rev:
        term = namespaces_rev[term_a[0]] + term_a[1]
    return "    <field index=\"{0}\" term=\"{1}\"/>\n".format(index+1,term)

def make_file_block(filename="occurence.csv",fields=[],core=True,tabs=False,t="records"):
    f_opts = {}
    f_opts.update(defaults)

    f_opts["filename"] = filename

    if t == "records":
        f_opts["row_type"] = "http://rs.tdwg.org/dwc/terms/Occurrence"
    elif t == "mediarecords":
        f_opts["row_type"] = "http://rs.tdwg.org/ac/terms/multimedia"
    elif t == "uniquelocality":
        f_opts["row_type"] = "http://rs.tdwg.org/dwc/terms/Location"
    elif t == "uniquenames":
        f_opts["row_type"] = "http://rs.tdwg.org/dwc/terms/Taxon"


    f_string = ""
    if fields is not None:
        for i,f in enumerate(fields):
            f_string += make_field(index=i,term=f)
    f_opts["fields"] = f_string

    if not core:
        f_opts["file_type"] = "extension"
        f_opts["id_type"] = "coreid"

    if tabs:
        f_opts["field_terminator"] = "\\t"
    
    return file_xml.format(**f_opts)
