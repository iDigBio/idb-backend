import re
import traceback

acceptable_licenses_trans = {
    "http://creativecommons.org/licenses/by-nc-sa/4.0/": "CC4 BY-NC-SA",
    "http://creativecommons.org/licenses/by-sa/4.0/": "CC4 BY-SA",
    "http://creativecommons.org/licenses/by-nc/4.0/": "CC4 BY-NC",
    "http://creativecommons.org/licenses/by/4.0/": "CC4 BY",
    "http://creativecommons.org/licenses/by-nc-sa/3.0/": "CC3 BY-NC-SA",
    "http://creativecommons.org/licenses/by-sa/3.0/": "CC3 BY-SA",
    "http://creativecommons.org/licenses/by-nc/3.0/": "CC3 BY-NC",
    "http://creativecommons.org/licenses/by/3.0/": "CC3 BY",
    "http://creativecommons.org/publicdomain/zero/1.0/": "CC0",
    "https://creativecommons.org/licenses/by-nc-sa/4.0/": "CC4 BY-NC-SA",
    "https://creativecommons.org/licenses/by-sa/4.0/": "CC4 BY-SA",
    "https://creativecommons.org/licenses/by-nc/4.0/": "CC4 BY-NC",
    "https://creativecommons.org/licenses/by/4.0/": "CC4 BY",
    "https://creativecommons.org/licenses/by-nc-sa/3.0/": "CC3 BY-NC-SA",
    "https://creativecommons.org/licenses/by-sa/3.0/": "CC3 BY-SA",
    "https://creativecommons.org/licenses/by-nc/3.0/": "CC3 BY-NC",
    "https://creativecommons.org/licenses/by/3.0/": "CC3 BY",
    "https://creativecommons.org/publicdomain/zero/1.0/": "CC0",
    "http://creativecommons.org/licenses/by-nc-sa/4.0": "CC4 BY-NC-SA",
    "http://creativecommons.org/licenses/by-sa/4.0": "CC4 BY-SA",
    "http://creativecommons.org/licenses/by-nc/4.0": "CC4 BY-NC",
    "http://creativecommons.org/licenses/by/4.0": "CC4 BY",
    "http://creativecommons.org/licenses/by-nc-sa/3.0": "CC3 BY-NC-SA",
    "http://creativecommons.org/licenses/by-sa/3.0": "CC3 BY-SA",
    "http://creativecommons.org/licenses/by-nc/3.0": "CC3 BY-NC",
    "http://creativecommons.org/licenses/by/3.0": "CC3 BY",
    "http://creativecommons.org/publicdomain/zero/1.0": "CC0",
    "https://creativecommons.org/licenses/by-nc-sa/4.0": "CC4 BY-NC-SA",
    "https://creativecommons.org/licenses/by-sa/4.0": "CC4 BY-SA",
    "https://creativecommons.org/licenses/by-nc/4.0": "CC4 BY-NC",
    "https://creativecommons.org/licenses/by/4.0": "CC4 BY",
    "https://creativecommons.org/licenses/by-nc-sa/3.0": "CC3 BY-NC-SA",
    "https://creativecommons.org/licenses/by-sa/3.0": "CC3 BY-SA",
    "https://creativecommons.org/licenses/by-nc/3.0": "CC3 BY-NC",
    "https://creativecommons.org/licenses/by/3.0": "CC3 BY",
    "https://creativecommons.org/publicdomain/zero/1.0": "CC0",
    "cc-by-nc-sa": "CC4 BY-NC-SA",
    "cc-by-nc-nd": "CC4 BY-NC-ND",
    "cc-by-sa": "CC4 BY-SA",
    "cc-by-nc": "CC4 BY-NC",
    "cc-by": "CC4 BY",
    "by-nc-sa": "CC4 BY-NC-SA",
    "by-sa": "CC4 BY-SA",
    "by-nc": "CC4 BY-NC",
    "by": "CC4 BY",
    "CC BY-NC-SA": "CC4 BY-NC-SA",
    "CC BY-SA": "CC4 BY-SA",
    "CC BY-NC": "CC4 BY-NC",
    "CC BY": "CC4 BY",
    "CC-BY-NC-SA": "CC4 BY-NC-SA",
    "CC-BY-SA": "CC4 BY-SA",
    "CC-BY-NC": "CC4 BY-NC",
    "CC-BY": "CC4 BY",
    "Creative Commons Attribution (CC-BY) 4.0": "CC4 BY",
    "CC0": "CC0",
    "CC BY-NC-SA (Attribution-NonCommercial-ShareAlike)": "CC4 BY-NC-SA",
    "CC BY-SA (Attribution-ShareAlike)": "CC4 BY-SA",
    "CC BY-NC (Attribution-NonCommercial)": "CC4 BY-NC",
    "CC BY-NC (Attribution-Non-Commercial)": "CC4 BY-NC",
    "CC BY (Attribution)": "CC4 BY",
    "Creative Commons Attribution Non Commercial (CC-BY-NC) 4.0 License" : "CC4 BY-NC",
    "Public Domain (CC0 1.0)" : "CC0",
    "CC0 1.0 (Public-domain)": "CC0",
    "Public Domain": "Public Domain",
    "Attribution-NonCommercial-ShareAlike 4.0 International": "CC4 BY-NC-SA",
    "URL: http://creativecommons.org/licenses/by-nc-sa/4.0/": "CC4 BY-NC-SA",
    "Creative Commons Attribution Non-Commercial (CC BY-NC)": "CC4 BY-NC",
    "Attribution-NonCommercial-ShareAlike CC BY-NC-SA 4.0": "CC4 BY-NC-SA",
    "This work is licensed under a Creative Commons CCZero 1.0 License http://creativecommons.org/publicdomain/zero/1.0/legalcode.": "CC0",
    "http://creativecommons.org/publicdomain/zero/1.0/ http://www.vertnet.org/resources/norms.html": "CC0",
    "http://creativecommons.org/publicdomain/zero/1.0 and http://vertnet.org/resources/norms.html": "CC0",
    "This work is licensed under a Creative Commons CCZero 1.0 License. http://creativecommons.org/publicdomain/zero/1.0/": "CC0",
    "This work is licensed under a Creative Commons CCZero 1.0 License http://creativecommons.org/publicdomain/zero/1.0/legalcode.": "CC0",
    "This work is licensed under a Creative Commons CCZero 1.0 License http://creativecommons.org/publicdomain/zero/1.0/legalcode": "CC0",
    "Tall Timbers data is governed by the Creative Commons Attribution 3.0 license (http://creativecommons.org/licenses/by/3.0/legalcode). Any use of data or images must be attributed to Tall Timbers Research Station and Land Conservancy.": "CC3 BY",
    "http://creativecommons.org/licenses/by/4.0/deed.en_US and http://biodiversity.ku.edu/research/university-kansas-biodiversity-institute-data-publication-and-use-norms": "CC4 BY",

    '<a href="http://creativecommons.org/licenses/by-nc-sa/3.0/us/" rel="license">  <img src="http://i.creativecommons.org/l/by-nc-sa/3.0/us/88x31.png" style="border-width: 0pt;" alt="Creative Commons License"/>  </a>': "CC3 BY-NC-SA",
    '<a href="http://creativecommons.org/licenses/by-nc-sa/3.0/us/" rel="license"> <img src="http://i.creativecommons.org/l/by-nc-sa/3.0/us/88x31.png" style="border-width: 0pt;" alt="Creative Commons License"/> </a>': "CC3 BY-NC-SA",
    '<a href="http://creativecommons.org/licenses/by-nc-sa/3.0/us/" rel="license"> <img src="http://i.creativecommons.org/l/by-nc-sa/3.0/us/88x31.png" style="border-width: 0pt;" alt="Creative Commons License"/></a>': "CC3 BY-NC-SA",
    '<a href="http://creativecommons.org/licenses/by-nc-sa/3.0/us/" rel="license"><img src="http://i.creativecommons.org/l/by-nc-sa/3.0/us/88x31.png" style="border-width: 0pt;" alt="Creative Commons License"/></a>': "CC3 BY-NC-SA",
    '<a rel="license" href="http://creativecommons.org/licenses/by-nc-sa/3.0/"> <img alt="Creative Commons License" style="border-width:0" src="http://i.creativecommons.org/l/by-nc-sa/3.0/88x31.png" /> </a><br />This work is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by-nc-sa/3.0/">Creative Commons Attribution-ShareAlike-Non Commercial 3.0 Unported License</a>.': "CC3 BY-NC-SA",
    '<a rel="license" href="http://creativecommons.org/licenses/by-nc-sa/3.0/"><img alt="Creative Commons License" style="border-width:0" src="http://i.creativecommons.org/l/by-nc-sa/3.0/88x31.png" /></a><br />This work is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by-nc-sa/3.0/">Creative Commons Attribution-ShareAlike-Non Commercial 3.0 Unported License</a>.': "CC3 BY-NC-SA",
    '<a rel="license" href="http://creativecommons.org/licenses/by-nc-sa/3.0/us/">  <img alt="Creative Commons License" style="border-width:0" src="http://i.creativecommons.org/l/by-nc-sa/3.0/us/88x31.png" />  </a>': "CC3 BY-NC-SA",
    '<a rel="license" href="http://creativecommons.org/licenses/by-nc-sa/3.0/us/"> <img alt="Creative Commons License" style="border-width:0" src="http://i.creativecommons.org/l/by-nc-sa/3.0/us/88x31.png" /> </a>': "CC3 BY-NC-SA",
    '<a rel="license" href="http://creativecommons.org/licenses/by-nc-sa/3.0/us/"> <img alt="Creative Commons License" style="border-width:0" src="http://i.creativecommons.org/l/by-nc-sa/3.0/us/88x31.png" /></a>': "CC3 BY-NC-SA",
    '<a rel="license" href="http://creativecommons.org/licenses/by/3.0/" title="Creative Commons Attribution 3.0 License"><img src="http://i.creativecommons.org/l/by/3.0/88x31.png" alt="License"></a>': "CC3 BY",
    '<a rel="license" href="http://creativecommons.org/licenses/by/3.0/"><img alt="Creative Commons License" style="border-width:0" src="http://i.creativecommons.org/l/by/3.0/88x31.png" /></a><br />This work is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by/3.0/">Creative Commons Attribution 3.0 Unported License</a>.': "CC3 BY",
    '<a rel="license" href="http://creativecommons.org/licenses/publicdomain/">  <img alt="Creative Commons License" style="border-width:0" src="http://i.creativecommons.org/l/publicdomain/88x31.png" />  </a>': "Public Domain",
    '<a rel="license" href="http://creativecommons.org/publicdomain/mark/1.0/"><img src="http://i.creativecommons.org/p/mark/1.0/88x31.png"     style="border-style: none;" alt="Public Domain Mark" /></a><br />This work is free of known copyright restrictions.': "Public Domain",
    '<a rel="license" href="http://creativecommons.org/publicdomain/zero/1.0/">      <img src="http://i.creativecommons.org/p/zero/1.0/88x31.png" style="border-style: none;" alt="CC0" />    </a>': "CC0",
    '<a rel="license" href="http://creativecommons.org/licenses/by-nc-sa/3.0/"> <img alt="Creative Commons License" style="border-width:0" src="http://i.creativecommons.org/l/by-nc-sa/3.0/88x31.png" /> </a><br />This work is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by-n': "CC3 BY-NC-SA",
    '<a rel="license" href="http://creativecommons.org/licenses/by-nc-sa/3.0/"><img alt="Creative Commons License" style="border-width:0" src="http://i.creativecommons.org/l/by-nc-sa/3.0/88x31.png" /></a><br />This work is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by-nc-': "CC3 BY-NC-SA",
    '<a rel="license" href="http://creativecommons.org/licenses/by/3.0/"><img alt="Creative Commons License" style="border-width:0" src="http://i.creativecommons.org/l/by/3.0/88x31.png" /></a><br />This work is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by/3.0/">Creative': "CC3 BY",

}

licenses = {
    "CC4 BY-NC-ND": {
        "rights": "BY-NC-ND",
        "licenselogourl": "https://i.creativecommons.org/l/by-nc-nd/4.0/88x31.png",
        "webstatement": "http://creativecommons.org/licenses/by-nc-nd/4.0/",
    },
    "CC4 BY-ND": {
        "rights": "BY-ND",
        "licenselogourl": "https://i.creativecommons.org/l/by-nd/4.0/88x31.png",
        "webstatement": "http://creativecommons.org/licenses/by-nd/4.0/",
    },
    "CC4 BY-NC-SA": {
        "rights": "BY-NC-SA",
        "licenselogourl": "https://i.creativecommons.org/l/by-nc-sa/4.0/88x31.png",
        "webstatement": "http://creativecommons.org/licenses/by-nc-sa/4.0/",
    },
    "CC4 BY-SA": {
        "rights": "BY-SA",
        "licenselogourl": "https://i.creativecommons.org/l/by-sa/4.0/88x31.png",
        "webstatement": "http://creativecommons.org/licenses/by-sa/4.0/",
    },
    "CC4 BY-NC": {
        "rights": "BY-NC",
        "licenselogourl": "https://i.creativecommons.org/l/by-nc/4.0/88x31.png",
        "webstatement": "http://creativecommons.org/licenses/by-nc/4.0/",
    },
    "CC4 BY": {
        "rights": "BY",
        "licenselogourl": "https://i.creativecommons.org/l/by/4.0/88x31.png",
        "webstatement": "http://creativecommons.org/licenses/by/4.0/",
    },
    "CC3 BY-NC-ND": {
        "rights": "BY-NC-ND",
        "licenselogourl": "https://i.creativecommons.org/l/by-nc-nd/4.0/88x31.png",
        "webstatement": "http://creativecommons.org/licenses/by-nc-nd/4.0/",
    },
    "CC3 BY-ND": {
        "rights": "BY-ND",
        "licenselogourl": "https://i.creativecommons.org/l/by-nd/4.0/88x31.png",
        "webstatement": "http://creativecommons.org/licenses/by-nd/4.0/",
    },
    "CC3 BY-NC-SA": {
        "rights": "BY-NC-SA",
        "licenselogourl": "http://i.creativecommons.org/l/by-nc-sa/3.0/88x31.png",
        "webstatement": "http://creativecommons.org/licenses/by-nc-sa/3.0/",
    },
    "CC3 BY-SA": {
        "rights": "BY-SA",
        "licenselogourl": "http://i.creativecommons.org/l/by-sa/3.0/88x31.png",
        "webstatement": "http://creativecommons.org/licenses/by-sa/3.0/",
    },
    "CC3 BY-NC": {
        "rights": "BY-NC",
        "licenselogourl": "http://i.creativecommons.org/l/by-nc/3.0/88x31.png",
        "webstatement": "http://creativecommons.org/licenses/by-nc/3.0/",
    },
    "CC3 BY": {
        "rights": "BY-NC-SA",
        "licenselogourl": "http://i.creativecommons.org/l/by/3.0/88x31.png",
        "webstatement": "http://creativecommons.org/licenses/by/3.0/",
    },
    "CC0": {
        "rights": "CC0",
        "licenselogourl": "http://i.creativecommons.org/p/zero/1.0/88x31.png",
        "webstatement": "http://creativecommons.org/publicdomain/zero/1.0/",
    },
    "Public Domain": {
        "rights": "Public Domain",
    }
}

manual_assignment = {
    "Attribution-NonCommercial-ShareAlike 4.0 International": "CC4 BY-NC-SA",
    "Tall Timbers data is governed by the Creative Commons Attribution 3.0 license (http://creativecommons.org/licenses/by/3.0/legalcode). Any use of data or images must be attributed to Tall Timbers Research Station and Land Conservancy.": "CC3 BY"
}

rights_order = [
    "PUBLICDOMAIN",
    "ZERO",
    "CC0",
    "BY",
    "BYSA",
    "BYNC",
    "BYND",
    "BYNCSA",
    "BYNCND",
]
rights_strings = {
    "BYNCND": "BY-NC-ND",
    "BYNCSA": "BY-NC-SA",
    "BYNC": "BY-NC",
    "BYND": "BY-ND",
    "BYSA": "BY-SA",
    "BY": "BY",
    "ZERO": "CC0",
    "CC0": "CC0",
    "PUBLICDOMAIN": "Public Domain"
}
version_strings = {
    "4.0": "CC4",
    "3.0": "CC3",
    "1.0": "",
    "": "CC4"
}


def pick_license(s, debug=False):
    if s in manual_assignment:
        return manual_assignment[s]

    rights_regex = re.compile(
        "((?:by(?:.?nc)?(?:.?sa)?(?:.?nd)?)|cc0|zero|(?:public.?domain)).?(\d\.\d)?", re.I)
    strip_special = re.compile('[^0-9a-zA-Z]+')
    picked = None
    order = -1
    for m in rights_regex.findall(s):
        try:
            r = strip_special.sub("", m[0]).upper()
            v = m[-1]
            if debug:
                print r, v, rights_order.index(r), order, picked
            if rights_order.index(r) > order:
                if r in ["CC0", "ZERO", "PUBLICDOMAIN"]:
                    picked = rights_strings[r]
                    order = rights_order.index(r)
                else:
                    picked = version_strings[v] + " " + rights_strings[r]
                    order = rights_order.index(r)
            else:
                pass
            if debug:
                print r, v, rights_order.index(r), order, picked
        except:
            traceback.print_exc()
    return picked


def get_rights_attributes(s):
    return licenses[pick_license(s)]


def main():
    for s in acceptable_licenses_trans:
        picked = pick_license(s)
        if acceptable_licenses_trans[s] != picked:
            # print s, acceptable_licenses_trans[s], picked
            print
            print s, acceptable_licenses_trans[s]
            pick_license(s, debug=True)

if __name__ == '__main__':
    main()
