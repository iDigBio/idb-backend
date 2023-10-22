# -*- coding: UTF-8 -*-
from __future__ import print_function


# http://rs.gbif.org/vocabulary/gbif/rank.xml
acceptable = {
    u"domain",
    u"kingdom",
    u"subkingdom",
    u"superphylum",
    u"phylum",
    u"subphylum",
    u"superclass",
    u"class",
    u"subclass",
    u"supercohort",
    u"cohort",
    u"subcohort",
    u"superorder",
    u"order",
    u"suborder",
    u"infraorder",
    u"superfamily",
    u"family",
    u"subfamily",
    u"tribe",
    u"subtribe",
    u"genus",
    u"subgenus",
    u"section",
    u"subsection",
    u"series",
    u"subseries",
    u"speciesAggregate",
    u"species",
    u"subspecificAggregate",
    u"subspecies",
    u"variety",
    u"subvariety",
    u"form",
    u"subform",
    u"cultivarGroup",
    u"cultivar",
    u"strain",
}



mapping = {
    u"class": u"class",
    u"classe": u"class",
    u"cultivar": u"cultivar",
    u"division": u"phylum",
    u"espècie": u"species",
    u"espécie": u"species",
    u"f.": u"form",
    u"family": u"family",
    u"famìlia": u"family",
    u"familia": u"family",
    u"fm.": u"family",
    u"fma.": u"family",
    u"form": u"form",
    u"form.": u"form",
    u"forma": u"form",
    u"genero": u"genus",
    u"genuinus": u"genus",
    u"genus": u"genus",
    u"gen.": u"genus",
    u"gínero": u"genus",
    u"hybrid fam": u"family",
    u"hybrid gen": u"genus",
    u"hybrid sp": u"species",
    u"hybrid ssp": u"subspecies",
    u"hybrid": u"species",
    u"infr.": u"subspecies",
    u"infraorder": u"suborder",
    u"infraspecificepithet": u"subspecies",
    u"kingdom": u"kingdom",
    u"nothospecies": u"species",
    u"nothosubsp.": u"subspecies",
    u"ordem": u"order",
    u"order": u"order",
    u"phylum": u"phylum",
    u"phyluym": u"phylum",
    u"phylym": u"phylum",
    u"s.": u"species",
    u"sp.": u"species",
    u"section[genus]": u"genus",
    u"sect.": u"section",
    u"specie": u"species",
    u"species": u"species",
    u"species group": u"species",
    u"specificepithet": u"species",
    u"spp.": u"subspecies",
    u"ssp": u"subspecies",
    u"ssp.": u"subspecies",
    u"sub-espècie": u"subspecies",
    u"sub-gínero": u"subgenus",
    u"subclass": u"subclass",
    u"subdivision": u"subphylum",
    u"subespècie": u"subspecies",
    u"subespécie": u"subspecies",
    u"subfamily": u"subfamily",
    u"subforma": u"subform",
    u"subgenus": u"subgenus",
    u"subkingdom": u"subkingdom",
    u"suborder": u"suborder",
    u"subordem": u"suborder",
    u"subphylum": u"subphylum",
    u"subsp": u"subspecies",
    u"subsp.": u"subspecies",
    u"subspecies": u"subspecies",
    u"subvariety": u"subvariety",
    u"superclass": u"superclass",
    u"superdivision": u"superphylum",
    u"superfamily": u"superfamily",
    u"superorder": u"superorder",
    u"tribe": u"tribe",
    u"tribo": u"tribe",
    u"var": u"variety",
    u"var.": u"variety",
    u"varietas": u"variety",
    u"variety": u"variety",
}

reject = {
    u"aberration",
    u"accipitriformes",
    u"actiniaria",
    u"aff.",
    u"affn.",
    u"agnostida",
    u"albiflora",
    u"alcyonacea",
    u"altiverruca",
    u"amazonica",
    u"amiiformes",
    u"ampelisca",
    u"ampeliscidae",
    u"amphipoda",
    u"anguilliformes",
    u"animalia",
    u"anseriformes",
    u"antipatharia",
    u"anura",
    u"apodiformes",
    u"archaeocopida",
    u"arcoida",
    u"arenaria",
    u"artiodactyla",
    u"atheriniformes",
    u"atlantapseudes",
    u"aulopiformes",
    u"baccata",
    u"benthaminum",
    u"beryciformes",
    u"bodotriidae",
    u"bonnierella",
    u"brisingida",
    u"bucerotiformes",
    u"byblis",
    u"caenogastropoda",
    u"calanoida",
    u"caprimulgiformes",
    u"captorhinida",
    u"carnivora",
    u"caryophyllales",
    u"catapaguroides",
    u"caudata",
    u"cear'a",
    u"cetacea",
    u"cete",
    u"cf.",
    u"characiformes",
    u"charadriiformes",
    u"cheirimedon",
    u"chimaeriformes",
    u"chiroptera",
    u"cimolesta",
    u"clade",
    u"cladoselachiformes",
    u"clupeiformes",
    u"coleoptera",
    u"columbiformes",
    u"condylarthra",
    u"coraciiformes",
    u"creodonta",
    u"crocodylia",
    u"cuculiformes",
    u"cv.",
    u"cyclaspis",
    u"cypriniformes",
    u"cyprinodontiformes",
    u"decapoda",
    u"dermoptera",
    u"deutella",
    u"didelphimorphia",
    u"diptera",
    u"echinoida",
    u"elegans",
    u"esociformes",
    u"falconiformes",
    u"ferruginea",
    u"forcipulatida",
    u"foscae",
    u"gadiformes",
    u"galliformes",
    u"gasterosteiformes",
    u"gaviiformes",
    u"genuina",
    u"genus or species",
    u"gorgonacea",
    u"gruiformes",
    u"hadromerida",
    u"harpacticoida",
    u"hesperornithiformes",
    u"heteronemertea",
    u"hippuritoida",
    u"holotype",
    u"humilis",
    u"hygrophila",
    u"hymenopenaeus",
    u"hyolithida",
    u"hypodigm",
    u"hypotype",
    u"ichthyodectiformes",
    u"ichthyornithiformes",
    u"incertae sedis",
    u"insectivore indet.",
    u"insectivore? indet.",
    u"ischyroceridae",
    u"isopoda",
    u"janeirensis",
    u"jemuina",
    u"lagomorpha",
    u"lepechinella",
    u"lepidoptera",
    u"leptophoxoides",
    u"leucothoe",
    u"lingulida",
    u"littorinimorpha",
    u"lobatus",
    u"lophiiformes",
    u"lutea",
    u"macrostylis",
    u"major",
    u"manihot",
    u"megaloptera",
    u"megamphopus",
    u"minor",
    u"moluanum",
    u"multituberculata",
    u"munida",
    u"musophagiformes",
    u"myctophiformes",
    u"myersius",
    u"myodocopina",
    u"myoida",
    u"mytiloida",
    u"myxiniformes",
    u"nektaspida",
    u"neogastropoda",
    u"nervosa",
    u"notopoma",
    u"notoungulata",
    u"nthmorph.",
    u"nuculoida",
    u"nudibranchia",
    u"oblongifolia",
    u"oblongifolius",
    u"obtusiloba",
    u"odonata",
    u"on",
    u"onychodontiformes",
    u"ornithischia",
    u"osmeriformes",
    u"osteoglossiformes",
    u"ovata",
    u"pachycormiformes",
    u"palaeonisciformes",
    u"passeriformes",
    u"paxillosida",
    u"pelecaniformes",
    u"pennatulacea",
    u"pentamerida",
    u"perciformes",
    u"percopsiformes",
    u"perissodactyla",
    u"petromyzontiformes",
    u"piciformes",
    u"pilosula",
    u"piriri",
    u"pl-hypotype",
    u"platyasterida",
    u"pleuronectiformes",
    u"poales",
    u"poecilosclerida",
    u"polymixiiformes",
    u"praecardioida",
    u"primates",
    u"procellariiformes",
    u"procreodi",
    u"productida",
    u"proles",
    u"prownbens",
    u"pseudericthonius",
    u"pseudharpinia",
    u"pseudischyrocerus",
    u"pterosauria",
    u"ptyctodontida",
    u"purpurea",
    u"race",
    u"radicans",
    u"rajiformes",
    u"rodentia",
    u"rosales",
    u"rugosa",
    u"salmoniformes",
    u"santalales",
    u"saurischia",
    u"sauropterygia",
    u"scleractinia",
    u"scordioides",
    u"scorpaeniformes",
    u"septentrionalis",
    u"siluriformes",
    u"solanacus",
    u"soricomorpha",
    u"sparassodonta",
    u"spinulosida",
    u"squaliformes",
    u"squamata",
    u"stolidobranchia",
    u"stomiiformes",
    u"strigiformes",
    u"stromatoporoidea",
    u"stylommatophora",
    u"subconcolor",
    u"subglabrifolia",
    u"subsect.",
    u"subtropicalis",
    u"suliformes",
    u"tabulata",
    u"temnospondyli",
    u"terebellida",
    u"thecodontia",
    u"thoracica",
    u"triqueter",
    u"undefinable",
    u"undetermined",
    u"unionoida",
    u"unnamed",
    u"valvatida",
    u"veneroida",
    u"verum",
    u"villosa",
    u"waptiida",
    u"x",
    u"zeiformes",
    u"zoanthidea",
    u"||||",
    u"|||||",
    u"||||||||",
    u"|||||||||||||||||||||||||||||||||||||",
    u"×",
}

for r in reject:
    mapping[r] = None

def main():
    new_accept_set = set()
    # new_accept_set |= acceptable

    import requests
    import json

    r = requests.get("http://search.idigbio.org/v2/summary/top/records?top_fields=[%22taxonrank%22]&count=1000")
    o = r.json()
    for k in o["taxonrank"]:
        if k not in mapping:
            print(k, o["taxonrank"][k])
        elif mapping[k] is None:
            pass
        elif mapping[k] not in acceptable:
            print(k, mapping[k])
            new_accept_set.add(mapping[k])

    if len(new_accept_set) > 0:
        print(json.dumps(list(new_accept_set), indent=4))


    
if __name__ == '__main__':
    main()