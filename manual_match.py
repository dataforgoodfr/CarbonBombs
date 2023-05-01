"""
Script that define dictionnaries objects that allows to make correspondance
between the differents database.\n
It define 3 dictionnaries :\n
manual_match_coal : Dictionnary that defined key-value pair to allow the
correspondance between Climate Bombs name into K.Kuhne paper (dictionnary key)
and Coal extraction site defined in GEM database (dictionnary value). This 
correspondance is only applicable to Coal extraction sites.\n
manual_match_gasoil: Dictionnary that defined key-value pair to allow the
correspondance between Climate Bombs name into K.Kuhne paper (dictionnary key)
and Gasoil extraction site defined in GEM database (dictionnary value). This 
correspondance is only applicable to Gasoil extraction sites.\n
manual_match_company: Dictionnary that defined key-value pair to allow the
correspondance between fossil fuel company defined into Parent company column 
of GEM database (dictionnary key) and fossil fuel company defined into 
Banking on Climate Chaos (BOCC) database\n
"""

# Dictionnary for Coal Mine only
manual_match_coal = {
    "Maritsa Coal Mines":"Troyanavo 3 Coal Mine",
    "Kaniha Coal Mine":"Gopalji Kaniha Coal Mine",
    "Kerandari BC":"Kerendari Coal Mine",
    "Bankui":"Barkui Coal Mine",
    "Mandakini B":"Mandakini–B Coal Mine",
    "Saharpur Jamarpani":"Saharpur-Jamarpani Coal Mine",
    "BIB Coal Mine":"Boneo Indobara Coal Mine",
    "Borly Coal Mines":"Molodezhny Coal Mine",
    "Listvianskaya Coal Mine":"Listvyazhnaya Coal Mine",
    "Elga Coal Mine":"Elginskiy Coal Mine",
    "Inaglinskaya-2 Mine":"Inaglinsky Coal Mine 2",
    "Bernice-Cygnus Coal Mine":"Berenice-Cygnus Coal Mine",
    "Hamilton County Mine No.1":"White Oak Coal Mine",
    "Sengwe Colliery":"Sengwa Coal Mine",
}

# Dictionnary for Gas and Oil only
manual_match_gasoil = {
    "Bu Hasa":"Bu Hasa/Shah/Asab",
    "Bab":"None",
    "Umm Shaif/Nasr":"Umm Shaif",
    "Bab (Gasco)":"None",
    "Asab":"Bu Hasa/Shah/Asab",
    "Vaca Muerta Shale":"None",
    "Goldwyer Shale":"None",
    "Gorgon LNG T1-T3":"None",
    "Velkerri Shale":"None",
    "ACG (Azeri-Chirag-Guneshli Deep Water)":"Azeri-Chirag-Guneshli (ACG) Deepwater",
    "Central Arabian Offshore":"None",
    "Santos Offshore":"None",
    "Llandovery Shale":"None",
    "Buzios (x-Franco)":"Búzios",
    "Irati Shale":"None",
    "Lula (X-Tupi)":"Tupi",
    "Parnaiba Onshore":"None",
    "Candeias Shale":"Candeias",
    "Mero (Libra NW)":"Libra",
    "Montney Play":"Northern Montney",
    "Spirit River (Notikewin, Falher, Wilrich)":"None",
    "Horizon Oil Sands Project":"None",
    "Kearl":"None",
    "Duvernay":"None",
    "Athabasca Oil Sands Project":"Athabasca Area - Scheme 9241H Oil Sands",
    "Christina Lake":"None",
    "Liard Shale":"None",
    "Syncrude Mildred Lake/Aurora":"Athabasca Area - Scheme 8573P Oil Sands",
    "Longmaxi Shale":"None",
    "Daqing":"None",
    "Cambrian/Silurian Marine Shale":"None",
    "Longmaxi Shale (Sichuan/Changyu)":"West Sichuan",
    "Tarim (CNPC)":"Tarim",
    "Southeast Uplift Onshore Heilongjiang Province":"None",
    "Oil shale China":"None",
    "Xinjiang (CNPC)":"None",
    "Central Uplift Onshore Xinjiang Uygur Autonomous Region":"None",
    "La Luna Shale":"None",
    "Tannezuft Shale":"None",
    "Hassi R'Mel (Domestic)":"Hassi R'Mel",
    "Bowland Shale":"None",
    "Kronprins Christian Offshore":"None",
    "Greater Turbot (Stabroek)":"Stabroek",
    "Greater Liza (Liza)":"None",
    "East Natuna (x-Natuna D-Alpha)":"Anoa",
    "Barail Shale":"None",
    "Rumaila North & South":"Rumaila",
    "Qurna West":"Qurna West 1",
    "Baghdad East":"East Baghdad",
    "Central Arabian Onshore":"None",
    "Qurna West-2":"Qurna West 2",
    "Halfayah":"Halfaya",
    "Nahr bin Umar":"Nahr Bin Umar",
    "Basrah Gas project":"Zubair",
    "Azadegan":"Azadegan North",
    "Ahwaz Asmari":"Ahvaz-Asmari",
    "Central Arabian Offshore":"None",
    "Agha Jari":"Aghajari",
    "Ahwaz Bangestan":"Ahvaz-Bangestan",
    "South Pars (Phases 9-10) dry gas":"South Pars (Phase 9-10)",
    "Kish Gas Project":"Kish",
    "Pars Southwest":"None",
    "South Pars (Phases 4-5) dry gas":"South Pars (Phase 4-5)",
    "Mansouri Bangestan":"Mansouri",
    "South Pars (Phases 22-24)":"South Pars (All Phases)",
    "South Pars (Phases 20-21)":"South Pars (Phase 20-21)",
    "South Pars (Phases 2-3) dry gas":"South Pars (Phase 2-3)",
    "Project Kuwait":"None",
    "Central Arabian Onshore":"None",
    "Carboniferous Shale":"None",
    "Sirte Shale":"None",
    "Eagle Ford Shale":"None",
    "Gulf Deepwater Offshore":"None",
    "Ku-Maloob-Zaap":"Ku$Maloob$Zaap",
    "Yucatan Platform Offshore":"Akal$Nohoch$Chac",
    "MZLNG Joint Development":"None",
    "Area-1 Future Phases":"Area 4",
    "Area 1 LNG (T1&T2)":"Area 1",
    "NLNG Base Projec":"None",
    "Khafji":"Khafji",
    "Lublin Basin Silurian Shale":"Lubliniec-Cieszanów",
    "North Field C LNG":"North Field",
    "North Field E":"North Field Alpha",
    "QatarGas LNG T8-T11 (NFE-East)":"Qatargas 4",
    "Central Arabian Onshore":"None",
    "Qatargas 2 LNG T4-T5":"Qatargas 2",
    "QatarGas LNG T12-T13 (NFE-South)":"Qatargas 3",
    "Rasgas 2 LNG T3-T5":"RasGas Alpha",
    "Rasgas 3 LNG T6-T7":"RasGas Alpha",
    "QatarGas 1 LNG T1-T3":"Qatargas 3",
    "Al Khaleej Gas project":"Al Khalij",
    "Bovanenkovo Zone (Yamal Megaproject)":"Bovanenkovskoye",
    "Gazprom dobycha Yamburg":"Yamburgskoye",
    "Tunguska Basin CBM":"New",
    "Urengoyskoye":"Severo-Urengoyskoye",
    "Kuznetsk Depression (Kuzbass) CBM":"None",
    "Yuganskneftegaz":"Severo-Priobskoye",
    "Eastern Gas Program":"East Tazovskoye",
    "West Siberia Offshore":"West Surgutskoye",
    "Lensky Basin CBM":"None",
    "Timan - Pechora Basin Offshore":"None",
    "Tambey Zone (Yamal Megaproject)":"Yuzhno-Tambeyskoye",
    "Timan - Pechora Basin Onshore":"None",
    "Samotlorneftegaz (TNK-BP)":"Samotlorskoye",
    "Leningradskoye (Kara Sea)":"Leningradskoye",
    "Gazprom dobycha Orenburg":"Orenburgskoye",
    "Arctic LNG 2 T1-3":"New",
    "Volga - Urals Onshore":"None",
    "Romashkino":"Romashkinskoye",
    "Rusanovskoye (Kara Sea)":"None",
    "Gazprom dobycha Nadym":"Medvezhye$Bovanenkovskoye$Yamsoveiskoye",
    "Taymyr Basin CBM":"None",
    "North Kara Sea Offshore":"None",
    "West Siberia Onshore":"None",
    "SeverEnergia Project":"Samburgskoye",
    "Ghawar Uthmaniyah":"Ghawar",
    "Khurais project":"Khurais",
    "Ghawar Haradh":"Ghawar",
    "Central Arabian Offshore":"None",
    "Ghawar Shedgum":"Ghawar",
    "Qatif project":"Qatif",
    "Manifa (redevelop)":"Manifa",
    "Ghawar Hawiyah":"Ghawar",
    "Central Arabian Onshore":"None",
    "Zuluf (CR in field)":"Zuluf",
    "Ghawar Ain Dar N":"Ghawar",
    "Safaniya YTF Concession":"Safaniya",
    "Zuluf (expansion)":"Zuluf",
    "Ghawar Ain Dar S":"Ghawar",
    "Sudair Shale":"None",
    "Harmaliyah":"None",
    "Tanf Shale":"None",
    "Yolotan (Iolotan) South":"None",
    "Yashlar Vostochnyy (East)":"None",
    "Dovletabad-Donmez":"None",
    "Tanzanian Coastal Offshore":"None",
    "Menilite Shale":"None",
    "Permian Delaware Tight":"Collie (Delaware) - OXY$Scott (Delaware) - Pitts",
    "Marcellus Shale":"Tioga County - Rockdale Marcellus",
    "Permian Midland Tight":"Midland Farms - Occidental Permian$Midland Farms Deep - Occidental Permian",
    "Haynesville/Bossier Shale":"Carthage (Haynesville Shale) - R. Lacy Services$Carthage (Haynesville Shale) - Rockcliff$Carthage (Haynesville Shale) - Sabine$Carthage (Haynesville Shale) - Shelby Boswell Operator$Carthage (Haynesville Shale) - Sheridan$Carthage (Haynesville Shale) - Tanos$Haynesville - Urban$Carthage (Haynesville Shale) - Valence$Carthage (Haynesville Shale) - Weatherly$Carthage (Haynesville Shale) - XTO$Carthage (Haynesville Shale) - Amplify$Carthage (Haynesville Shale) - BP$Carthage (Haynesville Shale) - CCI$Carthage (Haynesville Shale) - Chevron$Carthage (Haynesville Shale) - Comstock$Carthage (Haynesville Shale) - Covey Park$Carthage (Haynesville Shale) - Exco$Gladewater (Haynesville) - Fortune",
    "Utica Shale":"None",
    "Bakken Shale":"Blue Buttes - Hess Bakken$Alkali Creek - Hess Bakken$Truax - Hess Bakken$Antelope - Hess Bakken$Hawkeye - Hess Bakken$Robinson Lake - Hess Bakken$Tioga - Hess Bakken$Capa - Hess Bakken$Alger - Hess Bakken$Big Butte - Hess Bakken$Beaver Lodge - Hess Bakken$Baskin - Hess Bakken$Manitou - Hess Bakken$Dollar Joe - Hess Bakken$Ross - Hess Bakken$Banks - Hess Bakken$Little Knife - Hess Bakken$Elm Tree - Hess Bakken$Cherry Creek - Hess Bakken$Wheelock - Hess Bakken$Westberg - Hess Bakken$Big Gulch - Hess Bakken$Ellsworth - Hess Bakken$Stanley - Hess Bakken$Ray - Hess Bakken$Murphy Creek - Hess Bakken$Rainbow - Hess Bakken$Bear Creek - Hess Bakken$New Home - Hess Bakken",
    "Eagle Ford Shale":"De Witt (Eagle Ford Shale) - Burlington$De Witt (Eagle Ford Shale) - Devon$De Witt (Eagle Ford Shale) - Ensign$De Witt (Eagle Ford Shale) - Pioneer$Gates Ranch (Eagle Ford Shale) - Rosetta$De Witt (Eagle Ford Shale) - Teal$Gates Ranch (Eagle Ford Shale) - XTO",
    "DJ Basin Tight Oil":"None",
    "Western Gulf Province_Texas":"None",
    "Woodford Shale":"Ruppel (Woodford) - Bosque Texas$Oates, Sw (Woodford) - Jagged Peak",
    "PRB Tight Oil":"None",
    "Chukchi Sea Offshore":"None",
    "Meramec":"None",
    "Permian Conventional_Texas":"None",
    "North Slope Onshore":"None",
    "Anadarko Shelf_Oklahoma":"Phantom (Wolfcamp) - Anadarko$Sandbar (Bone Spring) - Anadarko$Haley (Lwr. Wolfcamp-Penn Cons.) - Anadarko$Tahiti/Cae/Tong (GC640) - Anadarko$King/Horn Mt. (MC084) - Anadarko$Lucius (KC875) - Anadarko$K2 (GC562) - Anadarko$Holstein (GC644) - Anadarko$Hopkins (GC627) - Anadarko$Heidelberg (GC859) - Anadarko$Marlin (VK915) - Anadarko$Friesian (GC599) - Anadarko",
    "Baltimore Canyon Offshore":"None",
    "Austin Chalk Tight":"Sugarkane (Austin Chalk) - BHP Billiton$Sugarkane (Austin Chalk) - Blackbrush$Sugarkane (Austin Chalk) - BPX$Sugarkane (Austin Chalk) - Burlington$Double A Wells, N (Austin Chalk) - BXP$Giddings (Austin Chalk, Gas) - Chesapeake$Lorenzo (Austin Chalk) - Chesapeake$Giddings (Austin Chalk-3) - Chesapeake$Pearsall (Austin Chalk) - CML$Sugarkane (Austin Chalk) - Devon$Lorenzo (Austin Chalk) - El Toro$Sugarkane (Austin Chalk) - Encana$Sugarkane (Austin Chalk) - EOG$Giddings (Austin Chalk, Gas) - EOG$Hawkville (Austin Chalk) - EOG$Sugarkane (Austin Chalk) - Equinor$Giddings (Austin Chalk, Gas) - Geosouthern$Sugarkane (Austin Chalk) - Gulftex$Sugarkane (Austin Chalk) - Inpex Eagle Ford$Sugarkane (Austin Chalk) - Magnolia$Sugarkane (Austin Chalk) - Marathon$Lorenzo (Austin Chalk) - Matador$Sugarkane (Austin Chalk) - Murphy$Giddings (Austin Chalk, Gas) - Ramtex$Sugarkane (Austin Chalk) - Repsol$Brookeland (Austin Chalk, 8800) - RKI$Giddings (Austin Chalk-3) - Sheridan$Lorenzo (Austin Chalk) - SM Energy$Giddings (Austin Chalk-3) - Treadstone$Pearsall (Austin Chalk) - Trinity$Giddings (Austin Chalk-3) - U. S. Operating$Giddings (Austin Chalk, Gas) - Verdun$Giddings (Austin Chalk-3) - WCS$Giddings (Austin Chalk-3) - Wildhorse$Brookeland (Austin Chalk, 8800) - Zarvona$Magnolia Springs (Austin Chalk) - Zarvona$Double A Wells, N (Austin Chalk) - Zarvona",
    "Barnett Shale":"Newark, East (Barnett Shale) - Arrowhead$Newark, East (Barnett Shale) - Bedrock$Newark, East (Barnett Shale) - Bluestone$Newark, East (Barnett Shale) - BRG Lone Star$Newark, East (Barnett Shale) - Crown Equipment$Newark, East (Barnett Shale) - Devon$Newark, East (Barnett Shale) - Eagleridge$Emma (Barnett Shale) - Elevation$Newark, East (Barnett Shale) - Endeavor$Newark, East (Barnett Shale) - Enervest$Newark, East (Barnett Shale) - EOG$Newark, East (Barnett Shale) - Faulconer, Vernon E.$Newark, East (Barnett Shale) - FDL$Newark, East (Barnett Shale) - Felderhoff Production$Newark, East (Barnett Shale) - Fuse$Newark, East (Barnett Shale) - GHA Barnett$Newark, East (Barnett Shale) - Hillwood$Newark, East (Barnett Shale) - Lakota$Newark, East (Barnett Shale) - Lime Rock$Newark, East (Barnett Shale) - Mccutchin Petroleum$Newark, East (Barnett Shale) - Merit$Newark, East (Barnett Shale) - Saddle$Newark, East (Barnett Shale) - Sage$Newark, East (Barnett Shale) - Scout$Newark, East (Barnett Shale) - TEP Barnett$Newark, East (Barnett Shale) - Texxol$Newark, East (Barnett Shale) - UPP$Newark, East (Barnett Shale) - XTO$Emma (Barnett Shale) - Zarvona$Newark, East (Barnett Shale) - 1849 Energy",
    "Beaufort Sea Offshore":"None",
    "Gulf Coast Centre Offshore":"None",
    "West Florida Offshore":"None",
    "Orinoco Joint Ventures":"Carabobo-1$Carabobo-2",
    "La Luna Shale":"None",
    "Collingham Shale":"Luiperd$Brulpadda$Paddavissie Fairway",
}

# Dictionnary for company matching between GEM (Global Energy Monitor) (key)
# and BOCC (Banking On Climate Chaos) (values)
manual_match_company_test = {
    "Thungela Resources Australia":"Thungela Resources Ltd",
    "Glencore":"Glencore PLC",
    "Integrated Energy Services Corporation":"BCE-Mach LLC",
    "China Coal":"China Coal Xinji Energy Co Ltd",
    "Anhui Xinji Coal and Electricity Group China COSCO Shipping":"Anhui Province Wanbei Coal-Electricity Group Co Ltd",
    "Zhejiang Energy Group":"Zhejiang Provincial Energy Group Co Ltd",
    "China Securities Finance Corporation":"China National Coal Group Corp (ChinaCoal)",
    "China Development Bank":"China Energy Investment Corp Ltd",
    "Zhejiang Xinhu Group Holding Company":"Zhejiang Sinhoo Co Ltd",
    "Xinjiang Energy Group":"Xinjiang Tianfu Energy Co Ltd",
    "Beijing Lirui Investment Company":"Beijing Energy Holding Co Ltd",
    "Hebei Construction & Investment Group":"Hebei Construction & Investment Group Co Ltd",
    "Shaanxi Yanchang Petroleum and Mining Company":"Shaanxi Yanchang Petroleum Group Co Ltd",
    "Anhui Xinji Coal and Electricity Group":"Anhui Province Energy Group Co Ltd",
    "Inner Mongolia Guanglian Minzu Economic Development Company":"Inner Mongolia Energy Group Co Ltd",
    "Beijing Jingneng Power Company":"Jinneng Holding Power Group Co Ltd",
    "China National Petroleum Corporation":"China National Petroleum Corporation (CNPC)",
    "Inner Mongolia Kaiyuan Shiye Group":"Inner Mongolia Energy Group Co Ltd",
    "Shenmu County State-Owned Asset Management Company":"Shenmu City State-Owned Assets Operation Co",
    "Jilin Province Coal Industry Group":"Jiangxi Province Investment Group Co Ltd",
    "Inner Mongolia Kaiyuan Shiye Group":"Inner Mongolia Energy Group Co Ltd",
    "Inner Mongolia Boyuan Holdings Group":"Inner Mongolia Energy Group Co Ltd",
    "Shanxi Boda Group":"Shaanxi Huabin Coal Industry Co Ltd",
    "State Grid Company":"State Grid Corp of China",
    "Yangcheng County Yangtai Group":"Yangcheng County Yangtai Group Industrial Co Ltd",
    "Kailuan Group":"Kailuan Group Ltd Liability Corp",
    "Gujarat State Electricity Corporation":"Gujarat State Petroleum Corp Ltd",
    "Jindal Group":"Jindal Coke Ltd",
    "Nippon Steel & Sumitomo Metal":"Nippon Steel Corp",
    "POSCO":"POSCO Holdings Inc",
    "Evraz":"Evraz PLC",
    "Euas Electricity Generation Company (Elektrik Üretim A.Ş, EÜAŞ)":"Electricity Generating PCL",
    "Petrobras":"Petróleo Brasileiro SA – Petrobras",
    "Suncor":"Suncor Energy Inc",
    "Petroleos Mexicanos":"Petróleos Mexicanos (PEMEX)",
    "Qatargas Operating Company Limited":"QatarEnergy",
    "Gazprom":"Gazprom PJSC",
    "ROSNEFTEGAZ JSC":"Rosneftegaz AO"
}

manual_match_company = {
    "Thungela Resources Australia":"Thungela Resources Ltd",
    "Glencore":"Glencore PLC",
    "China Coal":"China Coal Xinji Energy Co Ltd",
    "Anhui Xinji Coal and Electricity Group China COSCO Shipping":"Anhui Province Wanbei Coal-Electricity Group Co Ltd",
    "Zhejiang Energy Group":"Zhejiang Provincial Energy Group Co Ltd",
    "Hebei Construction & Investment Group":"Hebei Construction & Investment Group Co Ltd",
    "Shaanxi Yanchang Petroleum and Mining Company":"Shaanxi Yanchang Petroleum Group Co Ltd",
    "China National Petroleum Corporation":"China National Petroleum Corporation (CNPC)",
    "Shenmu County State-Owned Asset Management Company":"Shenmu City State-Owned Assets Operation Co",
    "State Grid Company":"State Grid Corp of China",
    "Yangcheng County Yangtai Group":"Yangcheng County Yangtai Group Industrial Co Ltd",
    "Kailuan Group":"Kailuan Group Ltd Liability Corp",
    "POSCO":"POSCO Holdings Inc",
    "Evraz":"Evraz PLC",
    "Euas Electricity Generation Company (Elektrik Üretim A.Ş, EÜAŞ)":"Electricity Generating PCL",
    "Petrobras":"Petróleo Brasileiro SA – Petrobras",
    "Suncor":"Suncor Energy Inc",
    "Petroleos Mexicanos":"Petróleos Mexicanos (PEMEX)",
    "Qatargas Operating Company Limited":"QatarEnergy",
    "Gazprom":"Gazprom PJSC",
    "ROSNEFTEGAZ JSC":"Rosneftegaz AO"
}
