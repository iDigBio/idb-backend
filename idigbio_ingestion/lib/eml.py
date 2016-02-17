from pyquery import PyQuery as pq

from idb.data_tables.rights_strings import acceptable_licenses_trans

import logging
from .log import logger
logger.setLevel(logging.DEBUG)
# logger.addHandler(logging.FileHandler("update_pubs.log"))

def getElement(root,name):
    return root.find(name)

def parseEml(id, emlText):
    eml = pq(emlText)
    ### The eml().txt() function returns an empty string instead of None if the location does not exist in the eml
    ### (if there is "no text node" according to release notes https://pypi.python.org/pypi/pyquery)

    collection = {}
    collection["id"] = id

    # Future... consider replace with:
    #     collection["logo_url"] = eml.find('resourceLogoUrl').txt
    rlu = getElement(eml.root.getroot(),".//resourceLogoUrl")
    if rlu != None:
        collection["logo_url"] = rlu.text
    
    collection["collection_name"] = eml("dataset > title").text()

    # Go until we find the first non-zero-length string (should be a collection description),
    # in order of most specific selector to avoid duplicates.
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
    if iwa != None:
        collection["institution_web_address"] = iwa.text
    elif eml("dataset distribution online url").text() != None:
        collection["institution_web_address"] = eml("dataset distribution online url").text()

    rights_text = None
    # ROM license text para includes additional sub-items ulink and citetitle which breaks .find traversal,
    # so look for that item first.
    rights = getElement(eml.root.getroot(),"dataset/intellectualRights/para/ulink/citetitle")
    if rights is not None:
        rights_text = rights.text
        logger.debug('Found license in citetitle: {0}'.format(rights_text))
    else:
        rights = getElement(eml.root.getroot(),"dataset/intellectualRights")
        if rights is not None:
            rights_para = rights.find("para")
            if rights_para is not None:
                rights_text = rights_para.text
            elif rights.text is not None:
                if rights.text.strip() != "":
                    rights_text = rights.text.strip()

    if rights_text is not None:
        if rights_text not in acceptable_licenses_trans:
            logger.debug("Unmatched data license in " + id + " " + rights_text)
            collection["data_rights"] = "Unknown License, assume Public Domain"
        else:
            collection["data_rights"] = acceptable_licenses_trans[rights_text]
    else:
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

    # collection["contacts"] = [{ "name": clm.groups()[0], "email": clm.groups()[1] + "@" + clm.groups()[2]}]
    collection["other_guids"] = []
    for g in eml("alternateidentifier"):
        collection["other_guids"].append(g.text)

    return collection

def main():
    import sys
    import json
    with open(sys.argv[1],"rb") as inf:
        print json.dumps(parseEml("testid",inf.read()))


if __name__ == '__main__':
    main()
