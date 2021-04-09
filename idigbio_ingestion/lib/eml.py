from __future__ import print_function
from pyquery import PyQuery as pq

from idb.data_tables.rights_strings import acceptable_licenses_trans
from idb.helpers.logging import idblogger

logger = idblogger.getChild('eml')

def getElement(root,name):
    return root.find(name)

def parseEml(id, emlFilename):
    "Returns a dictionary of fields from the eml file"

    # If the target eml document is not XML, the eml object will not be created due to XMLSyntaxError or other
    # pyquery exception.  This is known to occur when a link to eml results in a 404 error page containing HTML.
    # For example:  http://xbiod.osu.edu/ipt/eml.do?r=osum-fish
    # It is possible we could trap this ahead of time by checking the raw emlText for key xml features
    # or HTML document features.

    eml = pq(filename=emlFilename, parser='xml')

    # The eml().txt() function returns an empty string instead of None if the location does not exist in the eml
    # (if there is "no text node" according to release notes https://pypi.python.org/pypi/pyquery)

    collection = {}
    collection["id"] = id

    # Future... consider replace with:
    #     collection["logo_url"] = eml.find('resourceLogoUrl').txt
    rlu = getElement(eml.root.getroot(),".//resourceLogoUrl")
    if rlu is not None:
        collection["logo_url"] = rlu.text

    collection["collection_name"] = eml("dataset > title").text()

    # Go until we find the first non-zero-length string (should be a collection description),
    collection_description_blob = ""
    for possible_collection_description in [
            'dataset > abstract > para',
            'symbiota > collection > abstract > para',
            'additionalMetadata > metadata > abstract > para',
            # Catch all... might literally catch any other desc text anywhere in the document.
            'abstract > para'
            ]:
        collection_description_blob += eml.find(possible_collection_description).text()
        if len(collection_description_blob) > 0:
            break
    collection["collection_description"] = collection_description_blob

    iwa = getElement(eml.root.getroot(),"additionalMetadata/metadata/symbiota/collection/onlineUrl")
    if iwa is not None:
        collection["institution_web_address"] = iwa.text
    elif eml("dataset distribution online url").text() is not None:
        collection["institution_web_address"] = eml("dataset distribution online url").text()

    rights_text = None

    # Look for places where the intellectualRights are located, starting at "deeper" potential node locations first.
    rights = getElement(eml.root.getroot(),"additionalMetadata/metadata/symbiota/collection/intellectualRights")
    if rights is not None:
        rights_text = rights.text
        #logger.debug('Found license in additionalMetadata: {0}'.format(rights_text))
    else:
        #logger.debug('***nothing in additionalMetadata***')
        rights_text = eml.children('dataset > intellectualRights > para > ulink > citetitle').text()
        if len(rights_text) > 0 or rights_text is None:
            #logger.debug('Found license in citetitle: {0}'.format(rights_text))
            pass
        else:
            #logger.debug('***nothing in citetitle***')

            # ALA example
            rights_text = eml.find('dataset > intellectualRights > section:last-child > para').text()

            if len(rights_text) == 0:
                rights_text = None
                rights = getElement(eml.root.getroot(),"dataset/intellectualRights")
                #logger.debug('Found license in intellectualRights: {0}'.format(rights_text))
                if rights is not None:
                    rights_para = rights.find("para")
                    if rights_para is not None:
                        rights_text = rights_para.text
                        #logger.debug('Found license in para: {0}'.format(rights_text.encode('utf-8')))
                    elif rights.text is not None:
                        #logger.debug("XXXXXXXX " + rights.text)
                        if rights.text.strip() != "":
                            rights_text = rights.text.strip()
                            #logger.debug('Rights text leftover: {0}'.format(rights_text))

    if rights_text is not None:
        if rights_text not in acceptable_licenses_trans:
            logger.debug("Unmatched data license '" + rights_text + "' in " + id)
            collection["data_rights"] = "Unknown License, assume Public Domain"
        else:
            logger.debug("Matched data license '" + rights_text + "' in " + id + " to '" + acceptable_licenses_trans[rights_text] + "'")
            collection["data_rights"] = acceptable_licenses_trans[rights_text]
    else:
        logger.debug("No data license text found in intellectualRights, using 'No license, assume Public Domain' for " + id)
        collection["data_rights"] = "No license, assume Public Domain"

    collection["contacts"] = []
    seen_emails = []
    for c in eml("creator, metadataProvider, associatedParty, contact"):
        contact = {}
        ch = pq(c)
        for cc in list(ch.children()):
            cch = list(pq(cc).children())
            if len(cch) > 0:
                for ccc in cch:
                    if ccc.tag == "individualName":
                        contact["first_name"] = ccc.text
                    elif ccc.tag == "givenName":
                        contact["first_name"] = ccc.text
                    elif ccc.tag == "surName":
                        contact["last_name"] = ccc.text
                    elif ccc.tag == "electronicMailAddress":
                        contact["email"] = ccc.text
                        if contact["email"] in seen_emails:
                            continue
                        else:
                            seen_emails.append(contact["email"])
                    elif ccc.tag == "positionName":
                        contact["role"] = ccc.text
            else:
                if cc.text != "":
                    if cc.tag == "individualName":
                        contact["first_name"] = cc.text
                    elif cc.tag == "givenName":
                        contact["first_name"] = cc.text
                    elif cc.tag == "surName":
                        contact["last_name"] = cc.text
                    elif cc.tag == "electronicMailAddress":
                        contact["email"] = cc.text
                        if contact["email"] in seen_emails:
                            continue
                        else:
                            seen_emails.append(contact["email"])
                    elif cc.tag == "positionName":
                        contact["role"] = cc.text
        if len(contact.keys()) > 0:
            collection["contacts"].append(contact)

    collection["other_guids"] = []
    for g in eml("alternateidentifier"):
        collection["other_guids"].append(g.text)

    return collection

def main():
    import sys
    import json
    print(json.dumps(parseEml("testid",sys.argv[1])))


if __name__ == '__main__':
    main()
