## Need to use base, because virtual envs do not have C++ 14.0 or greater. Error is thrown up with pip install usaddress
## No problem adding it to base env
import pandas as pd
import usaddress, pycountry, re # type: ignore
from tqdm._tqdm_notebook import tqdm_notebook
tqdm_notebook.pandas()

def break_up_address(text, include_recipient=False, standardize_names=True, split_zip_code=True, clean_text=True, include_country=False, include_phone=False):
    ## Tag mapping puts all the pieces of the address into the same fields for usaddress package
    ## https://usaddress.readthedocs.io/en/latest/
    tag_mapping={
        'Recipient': 'Recipient',
        'AddressNumber': 'Address1',
        'AddressNumberPrefix': 'Address1',
        'AddressNumberSuffix': 'Address1',
        'StreetName': 'Address1',
        'StreetNamePreDirectional': 'Address1',
        'StreetNamePreModifier': 'Address1',
        'StreetNamePreType': 'Address1',
        'StreetNamePostDirectional': 'Address1',
        'StreetNamePostModifier': 'Address1',
        'StreetNamePostType': 'Address1',
        'CornerOf': 'Address1',
        'IntersectionSeparator': 'Address1',
        'LandmarkName': 'Address1',
        'USPSBoxGroupID': 'Address1',
        'USPSBoxGroupType': 'Address1',
        'USPSBoxID': 'Address1',
        'USPSBoxType': 'Address1',
        'BuildingName': 'Address2',
        'OccupancyType': 'Address2',
        'OccupancyIdentifier': 'Address2',
        'SubaddressIdentifier': 'Address2',
        'SubaddressType': 'Address2',
        'PlaceName': 'City',
        'StateName': 'State',
        'ZipCode': 'Zip_Code',
    }

    ## Load a dict to return with groups from mapping above
    result_dict = {
        'Address1': '',
        'Address2': '',
        'City': '',
        'State': '',
        'Zip_Code': '',
    }
    ## Add a spot for a recipient in output if this is expected
    if split_zip_code:
        result_dict['Zip_Code_4'] = ""
    ## Add a spot for a recipient in output if this is expected
    if include_recipient:
        result_dict['Recipient'] = ""
    ## Add a spot for a recipient in output if this is expected
    if include_country:
        result_dict['Country'] = ""
    ## Add a spot for a phone in output if this is expected
    if include_country:
        result_dict['Phone'] = ""

    ## If it is none, then return all none for the columns
    if text is None:
        result_dict.update((k, None) for k, v in result_dict.items())
        return result_dict
        
    ###### Clean Up ----- Get rid of wierd stuff out of the text address 
    if clean_text:
        text = re.sub('(\d{5}(\.\d+)?)', r' \1', text) ## Seperate a zip code from state (text)
        text = re.sub("\%|\#|\.", "", text)
    if include_country:
        country_name, found_text = find_countries(text)
        text = re.sub(found_text, "", text)
    if include_phone:
        phone, text = parse_phone(text)

    ## Repeated label error means the parser did not work or the address itself is most likely messed up (this one usually if error)
    ## Get the dict from the output -> hense the [0]
    text= text.strip()
    try:
        tagged_addr = usaddress.tag(text, tag_mapping=tag_mapping)[0]
    except usaddress.RepeatedLabelError as e:
        tagged_addr = {"Address1": text}

    result_dict.update((k, v.upper()) for k, v in tagged_addr.items() if k in result_dict)

    ## Do Extra Stuff
    ## Standardize Names of Streets ex: st -> street
    if standardize_names:
        result_dict['Address1'] = replace_names(result_dict['Address1'])
        result_dict['Address2'] = replace_names(result_dict['Address2'])
    ## Split zip code into two parts zip code and zip code 4
    if (split_zip_code is True) and ('Zip_Code' in result_dict.keys()):
        zip_split = split_zip(result_dict['Zip_Code'])
        result_dict.update((k, v) for k, v in zip_split.items())
    ## 
    if include_country:
        result_dict['Country'] = country_name
    if include_phone:
        result_dict['Phone'] = phone
    
    return result_dict

def make_new_address_columns(data, address_column, new_columns_prefix="", append_to_original_data=True, include_recipient=False, standardize_names=True, clean_text=True, split_zip_code=True, include_country=False, include_phone=False):
    """
    Main function in the package. Using an address column (one string with all address info available per record), break up the address into its own columns
        - data:                       data that has the address information
        - address_column:             column that has one string with all address info available per record (if list will concat cols in list to make new column)
        - new_columns_prefix:         Default is "". Adding something will make the address columns have a prefix (Example: 'Secondary_'Address1)
        - append_to_original_data:    Default is True. This will append columns back to original data. Else will return df of just address info
        - include_recipient:          Default is False. Look for recipient in address info. Will include this as a column in output. Else just ignore
        - standardize_names:          Default is True. Replaces street abbr in address1 and address2 with full name of street (Example: ave -> avenue)
        - split_zip_code:             Default is True. Splits zip code into zip and zip4. Does this by looking for a '-' and splitting on that
        - include_country:            Default is False. Will look for a country name in address and include it in the output if found. Do not need if all US addresses are expected
    """
    data = data.reset_index(drop=True).copy()
    if type(address_column) == list:
        data['Combined Address'] = data[address_column].apply(lambda row: ' '.join(row.values.astype(str)), axis=1)
        address_df =  pd.DataFrame(data['Combined Address'].progress_apply(lambda x: break_up_address(
            x, 
            include_recipient=include_recipient, 
            standardize_names=standardize_names,
            clean_text=clean_text,
            split_zip_code=split_zip_code, 
            include_country=include_country,
            include_phone=include_phone
        )).to_list())
    else:
        address_df =  pd.DataFrame(data[address_column].progress_apply(lambda x: break_up_address(
                x, 
                include_recipient=include_recipient, 
                standardize_names=standardize_names, 
                clean_text=clean_text,
                split_zip_code=split_zip_code, 
                include_country=include_country,
                include_phone=include_phone
            )).to_list())
    ## Rename columns
    rename_columns_dict = {v: new_columns_prefix+v for v in address_df.columns.to_list()}
    address_df = address_df.rename(rename_columns_dict, axis=1)

    if append_to_original_data:
        try:
            address_df = data.join(address_df)
        except ValueError:
            address_df = data.join(address_df, rsuffix='Old_')

    return address_df


def create_road_replacement_dict():
    """
    ## On 10-17-22 switched from going from abbr to full name, and now go all full names or abbrevations to official UPS abbreviation
    return {'ALLEE':'ALLEY', 'ALLY':'ALLEY', 'ALY':'ALLEY', 'ANNEX':'ANEX', 'ANNX':'ANEX', 'ANX':'ANEX', 'ARC':'ARCADE', 'AV':'AVENUE', 'AVE':'AVENUE', 'AVEN':'AVENUE', 'AVENU':'AVENUE', 'AVN':'AVENUE', 'AVNUE':'AVENUE', 'BAYOO':'BAYOU', 'BCH':'BEACH', 'BND':'BEND', 'BLF':'BLUFF', 'BLUF':'BLUFF', 'BOT':'BOTTOM', 'BTM':'BOTTOM', 'BOTTM':'BOTTOM', 'BLVD':'BOULEVARD', 'BOUL':'BOULEVARD', 'BOULV':'BOULEVARD', 'BR':'BRANCH', 'BRNCH':'BRANCH', 'BRDGE':'BRIDGE', 'BRG':'BRIDGE', 'BRK':'BROOK', 'BYP':'BYPASS', 'BYPA':'BYPASS', 'BYPAS':'BYPASS', 'BYPS':'BYPASS', 'CP':'CAMP', 'CMP':'CAMP', 'CANYN':'CANYON', 'CNYN':'CANYON', 'CPE':'CAPE', 'CAUSWA':'CAUSEWAY', 'CSWY':'CAUSEWAY', 'CEN':'CENTER', 'CENT':'CENTER', 'CENTR':'CENTER', 'CENTRE':'CENTER', 'CNTER':'CENTER', 'CNTR':'CENTER', 'CTR':'CENTER', 'CIR':'CIRCLE', 'CIRC':'CIRCLE', 'CIRCL':'CIRCLE', 'CRCL':'CIRCLE', 'CRCLE':'CIRCLE', 'CLF':'CLIFF', 'CLFS':'CLIFFS', 'CLB':'CLUB', 'COR':'CORNER', 'CORS':'CORNERS', 'CRSE':'COURSE', 'CT':'COURT', 'CTS':'COURTS', 'CV':'COVE', 'CRK':'CREEK', 'CRES':'CRESCENT', 'CRSENT':'CRESCENT', 'CRSNT':'CRESCENT', 'CRSSNG':'CROSSING', 'XING':'CROSSING', 'DL':'DALE', 'DM':'DAM', 'DIV':'DIVIDE', 'DV':'DIVIDE', 'DVD':'DIVIDE', 'DR':'DRIVE', 'DRIV':'DRIVE', 'DRV':'DRIVE', 'EST':'ESTATE', 'ESTS':'ESTATES', 'EXP':'EXPRESSWAY', 'EXPR':'EXPRESSWAY', 'EXPRESS':'EXPRESSWAY', 'EXPW':'EXPRESSWAY', 'EXPY':'EXPRESSWAY', 'EXT':'EXTENSION', 'EXTN':'EXTENSION', 'EXTNSN':'EXTENSION', 'EXTS':'EXTENSIONS', 'FLS':'FALLS', 'FRRY':'FERRY', 'FRY':'FERRY', 'FLD':'FIELD', 'FLDS':'FIELDS', 'FLT':'FLAT', 'FLTS':'FLATS', 'FRD':'FORD', 'FORESTS':'FOREST', 'FRST':'FOREST', 'FORG':'FORGE', 'FRG':'FORGE', 'FRK':'FORK', 'FRKS':'FORKS', 'FRT':'FORT', 'FT':'FORT', 'FREEWY':'FREEWAY', 'FRWAY':'FREEWAY', 'FRWY':'FREEWAY', 'FWY':'FREEWAY', 'GARDN':'GARDEN', 'GRDEN':'GARDEN', 'GRDN':'GARDEN', 'GDNS':'GARDENS', 'GRDNS':'GARDENS', 'GATEWY':'GATEWAY', 'GATWAY':'GATEWAY', 'GTWAY':'GATEWAY', 'GTWY':'GATEWAY', 'GLN':'GLEN', 'GRN':'GREEN', 'GROV':'GROVE', 'GRV':'GROVE', 'HARB':'HARBOR', 'HARBR':'HARBOR', 'HBR':'HARBOR', 'HRBOR':'HARBOR', 'HVN':'HAVEN', 'HT':'HEIGHTS', 'HTS':'HEIGHTS', 
    'HIGHWY':'HIGHWAY', 'HIWAY':'HIGHWAY', 'HIWY':'HIGHWAY', 'HWAY':'HIGHWAY', 'HWY':'HIGHWAY', 'HL':'HILL', 'HLS':'HILLS', 'HLLW':'HOLLOW', 'HOLLOWS':'HOLLOW', 'HOLW':'HOLLOW', 'HOLWS':'HOLLOW', 'INLT':'INLET', 'IS':'ISLAND', 'ISLND':'ISLAND', 'ISLNDS':'ISLANDS', 'ISS':'ISLANDS', 'ISLES':'ISLE', 'JCT':'JUNCTION', 'JCTION':'JUNCTION', 'JCTN':'JUNCTION', 'JUNCTN':'JUNCTION', 'JUNCTON':'JUNCTION', 'JCTNS':'JUNCTIONS', 'JCTS':'JUNCTIONS', 'KY':'KEY', 'KYS':'KEYS', 'KNL':'KNOLL', 'KNOL':'KNOLL', 'KNLS':'KNOLLS', 'LK':'LAKE', 'LKS':'LAKES', 'LNDG':'LANDING', 'LNDNG':'LANDING', 'LN':'LANE', 'LGT':'LIGHT', 'LF':'LOAF', 'LCK':'LOCK', 'LCKS':'LOCKS', 'LDG':'LODGE', 'LDGE':'LODGE', 'LODG':'LODGE', 'LOOPS':'LOOP', 'MNR':'MANOR', 'MNRS':'MANORS', 'MDW':'MEADOWS', 'MDWS':'MEADOWS', 'MEDOWS':'MEADOWS', 'MISSN':'MISSION', 'MSSN':'MISSION', 'MNT':'MOUNT', 'MT':'MOUNT', 'MNTAIN':'MOUNTAIN', 'MNTN':'MOUNTAIN', 'MOUNTIN':'MOUNTAIN', 'MTIN':'MOUNTAIN', 'MTN':'MOUNTAIN', 'MNTNS':'MOUNTAINS', 'NCK':'NECK', 'ORCH':'ORCHARD', 'ORCHRD':'ORCHARD', 'OVL':'OVAL', 'PRK':'PARK', 'PARKWY':'PARKWAY', 'PKWAY':'PARKWAY', 'PKWY':'PARKWAY', 'PKY':'PARKWAY', 'PKWYS':'PARKWAYS', 'PATHS':'PATH', 'PIKES':'PIKE', 'PNES':'PINES', 'PL':'PLACE', 'PLN':'PLAIN', 'PLNS':'PLAINS', 'PLZ':'PLAZA', 'PLZA':'PLAZA', 'PT':'POINT', 'PTS':'POINTS', 'PRT':'PORT', 'PRTS':'PORTS', 'PR':'PRAIRIE', 'PRR':'PRAIRIE', 'RAD':'RADIAL', 'RADIEL':'RADIAL', 'RADL':'RADIAL', 'RANCHES':'RANCH', 'RNCH':'RANCH', 'RNCHS':'RANCH', 'RPD':'RAPID', 'RPDS':'RAPIDS', 'RST':'REST', 'RDG':'RIDGE', 'RDGE':'RIDGE', 'RDGS':'RIDGES', 'RIV':'RIVER', 'RVR':'RIVER', 'RIVR':'RIVER', 'RD':'ROAD', 'RDS':'ROADS', 'SHL':'SHOAL', 'SHLS':'SHOALS', 'SHOAR':'SHORE', 'SHR':'SHORE', 'SHOARS':'SHORES', 'SHRS':'SHORES', 'SPG':'SPRING', 'SPNG':'SPRING', 'SPRNG':'SPRING', 'SPGS':'SPRINGS', 'SPNGS':'SPRINGS', 'SPRNGS':'SPRINGS', 'SQ':'SQUARE', 'SQR':'SQUARE', 'SQRE':'SQUARE', 'SQU':'SQUARE', 'SQRS':'SQUARES', 'STA':'STATION', 'STATN':'STATION', 'STN':'STATION', 'STRA':'STRAVENUE', 'STRAV':'STRAVENUE', 'STRAVEN':'STRAVENUE', 'STRAVN':'STRAVENUE', 'STRVN':'STRAVENUE', 'STRVNUE':'STRAVENUE', 'STREME':'STREAM', 'STRM':'STREAM', 
    'STRT':'STREET', 'ST':'STREET', 'STR':'STREET', 'SMT':'SUMMIT', 'SUMIT':'SUMMIT', 'SUMITT':'SUMMIT', 'TER':'TERRACE', 'TERR':'TERRACE', 'TRACES':'TRACE', 'TRCE':'TRACE', 'TRACKS':'TRACK', 'TRAK':'TRACK', 'TRK':'TRACK', 'TRKS':'TRACK', 'TRAILS':'TRAIL', 'TRL':'TRAIL', 'TRLS':'TRAIL', 'TRLR':'TRAILER', 'TRLRS':'TRAILER', 'TUNEL':'TUNNEL', 'TUNL':'TUNNEL', 'TUNLS':'TUNNEL', 'TUNNELS':'TUNNEL', 'TUNNL':'TUNNEL', 'TRNPK':'TURNPIKE', 'TURNPK':'TURNPIKE', 'UN':'UNION', 'VALLY':'VALLEY', 'VLLY':'VALLEY', 'VLY':'VALLEY', 'VLYS':'VALLEYS', 'VDCT':'VIADUCT', 'VIA':'VIADUCT', 'VIADCT':'VIADUCT', 'VW':'VIEW', 'VWS':'VIEWS', 'VILL':'VILLAGE', 'VILLAG':'VILLAGE', 'VILLG':'VILLAGE', 'VILLIAGE':'VILLAGE', 'VLG':'VILLAGE', 'VLGS':'VILLAGES', 'VL':'VILLE', 'VIS':'VISTA', 'VIST':'VISTA', 'VST':'VISTA', 'VSTA':'VISTA', 'WY':'WAY'}
    """    
    return {'ALLEE':'ALY','ALLEY':'ALY','ALLY':'ALY','ANEX':'ANX','ANNEX':'ANX','ANNX':'ANX','ARCADE':'ARC','AV':'AVE','AVEN':'AVE','AVENU':'AVE','AVENUE':'AVE','AVN':'AVE','AVNUE':'AVE','BAYOO':'BYU','BAYOU':'BYU','BEACH':'BCH','BEND':'BND','BLUF':'BLF','BLUFF':'BLF','BLUFFS':'BLFS','BOT':'BTM','BOTTM':'BTM','BOTTOM':'BTM','BOUL':'BLVD','BOULEVARD':'BLVD','BOULV':'BLVD','BRNCH':'BR','BRANCH':'BR','BRDGE':'BRG','BRIDGE':'BRG','BROOK':'BRK','BROOKS':'BRKS','BURG':'BG','BURGS':'BGS','BYPA':'BYP','BYPAS':'BYP','BYPASS':'BYP','BYPS':'BYP','CAMP':'CP','CMP':'CP','CANYN':'CYN','CANYON':'CYN','CNYN':'CYN','CAPE':'CPE','CAUSEWAY':'CSWY','CAUSWA':'CSWY','CEN':'CTR','CENT':'CTR','CENTER':'CTR','CENTR':'CTR','CENTRE':'CTR','CNTER':'CTR','CNTR':'CTR','CENTERS':'CTRS','CIRC':'CIR','CIRCL':'CIR','CIRCLE':'CIR','CRCL':'CIR','CRCLE':'CIR','CIRCLES':'CIRS','CLIFF':'CLF','CLIFFS':'CLFS','CLUB':'CLB','COMMON':'CMN','COMMONS':'CMNS','CORNER':'COR','CORNERS':'CORS','COURSE':'CRSE','COURT':'CT','COURTS':'CTS','COVE':'CV','COVES':'CVS','CREEK':'CRK','CRESCENT':'CRES','CRSENT':'CRES','CRSNT':'CRES','CREST':'CRST','CROSSING':'XING','CRSSNG':'XING','CROSSROAD':'XRD','CROSSROADS':'XRDS','CURVE':'CURV','DALE':'DL','DAM':'DM','DIV':'DV','DIVIDE':'DV','DVD':'DV','DRIV':'DR','DRIVE':'DR','DRV':'DR','DRIVES':'DRS','ESTATE':'EST','ESTATES':'ESTS','EXP':'EXPY','EXPR':'EXPY','EXPRESS':'EXPY','EXPRESSWAY':'EXPY','EXPW':'EXPY','EXTENSION':'EXT','EXTN':'EXT','EXTNSN':'EXT','FALLS':'FLS','FERRY':'FRY','FRRY':'FRY','FIELD':'FLD','FIELDS':'FLDS','FLAT':'FLT','FLATS':'FLTS','FORD':'FRD','FORDS':'FRDS','FOREST':'FRST','FORESTS':'FRST','FORG':'FRG','FORGE':'FRG','FORGES':'FRGS','FORK':'FRK','FORKS':'FRKS','FORT':'FT','FRT':'FT','FREEWAY':'FWY','FREEWY':'FWY','FRWAY':'FWY','FRWY':'FWY','GARDEN':'GDN','GARDN':'GDN','GRDEN':'GDN','GRDN':'GDN','GARDENS':'GDNS','GRDNS':'GDNS','GATEWAY':'GTWY','GATEWY':'GTWY','GATWAY':'GTWY','GTWAY':'GTWY','GLEN':'GLN','GLENS':'GLNS','GREEN':'GRN','GREENS':'GRNS','GROV':'GRV','GROVE':'GRV','GROVES':'GRVS','HARB':'HBR','HARBOR':'HBR','HARBR':'HBR','HRBOR':'HBR','HARBORS':'HBRS','HAVEN':'HVN','HT':'HTS','HIGHWAY':'HWY','HIGHWY':'HWY',
    'HIWAY':'HWY','HIWY':'HWY','HWAY':'HWY','HILL':'HL','HILLS':'HLS','HLLW':'HOLW','HOLLOW':'HOLW','HOLLOWS':'HOLW','HOLWS':'HOLW','ISLAND':'IS','ISLND':'IS','ISLANDS':'ISS','ISLNDS':'ISS','ISLES':'ISLE','JCTION':'JCT','JCTN':'JCT','JUNCTION':'JCT','JUNCTN':'JCT','JUNCTON':'JCT','JCTNS':'JCTS','JUNCTIONS':'JCTS','KEY':'KY','KEYS':'KYS','KNOL':'KNL','KNOLL':'KNL','KNOLLS':'KNLS','LAKE':'LK','LAKES':'LKS','LANDING':'LNDG','LNDNG':'LNDG','LANE':'LN','LIGHT':'LGT','LIGHTS':'LGTS','LOAF':'LF','LOCK':'LCK','LOCKS':'LCKS','LDGE':'LDG','LODG':'LDG','LODGE':'LDG','LOOPS':'LOOP','MANOR':'MNR','MANORS':'MNRS','MEADOW':'MDW','MDW':'MDWS','MEADOWS':'MDWS','MEDOWS':'MDWS','MILL':'ML','MILLS':'MLS','MISSN':'MSN','MSSN':'MSN','MOTORWAY':'MTWY','MNT':'MT','MOUNT':'MT','MNTAIN':'MTN','MNTN':'MTN','MOUNTAIN':'MTN','MOUNTIN':'MTN','MTIN':'MTN','MNTNS':'MTNS','MOUNTAINS':'MTNS','NECK':'NCK','ORCHARD':'ORCH','ORCHRD':'ORCH','OVL':'OVAL','OVERPASS':'OPAS','PRK':'PARK','PARKS':'PARK','PARKWAY':'PKWY','PARKWY':'PKWY','PKWAY':'PKWY','PKY':'PKWY','PARKWAYS':'PKWY','PKWYS':'PKWY','PASSAGE':'PSGE','PATHS':'PATH','PIKES':'PIKE','PINE':'PNE','PINES':'PNES','PLAIN':'PLN','PLAINS':'PLNS','PLAZA':'PLZ','PLZA':'PLZ','POINT':'PT','POINTS':'PTS','PORT':'PRT','PORTS':'PRTS','PRAIRIE':'PR','PRR':'PR','RAD':'RADL','RADIAL':'RADL','RADIEL':'RADL','RANCH':'RNCH','RANCHES':'RNCH','RNCHS':'RNCH','RAPID':'RPD','RAPIDS':'RPDS','REST':'RST','RDGE':'RDG','RIDGE':'RDG','RIDGES':'RDGS','RIVER':'RIV','RVR':'RIV','RIVR':'RIV','ROAD':'RD','ROADS':'RDS','ROUTE':'RTE','SHOAL':'SHL','SHOALS':'SHLS','SHOAR':'SHR','SHORE':'SHR','SHOARS':'SHRS','SHORES':'SHRS','SKYWAY':'SKWY','SPNG':'SPG','SPRING':'SPG','SPRNG':'SPG','SPNGS':'SPGS','SPRINGS':'SPGS','SPRNGS':'SPGS','SPURS':'SPUR','SQR':'SQ','SQRE':'SQ','SQU':'SQ','SQUARE':'SQ','SQRS':'SQS','SQUARES':'SQS','STATION':'STA','STATN':'STA','STN':'STA','STRAV':'STRA','STRAVEN':'STRA','STRAVENUE':'STRA','STRAVN':'STRA','STRVN':'STRA','STRVNUE':'STRA','STREAM':'STRM','STREME':'STRM','STREET':'ST','STRT':'ST','STR':'ST','STREETS':'STS','SUMIT':'SMT','SUMITT':'SMT','SUMMIT':'SMT','TERR':'TER','TERRACE':'TER','THROUGHWAY':'TRWY','TRACE':'TRCE',
    'TRACES':'TRCE','TRACK':'TRAK','TRACKS':'TRAK','TRK':'TRAK','TRKS':'TRAK','TRAFFICWAY':'TRFY','TRAIL':'TRL','TRAILS':'TRL','TRLS':'TRL','TRAILER':'TRLR','TRLRS':'TRLR','TUNEL':'TUNL','TUNLS':'TUNL','TUNNEL':'TUNL','TUNNELS':'TUNL','TUNNL':'TUNL','TRNPK':'TPKE','TURNPIKE':'TPKE','TURNPK':'TPKE','UNDERPASS':'UPAS','UNION':'UN','UNIONS':'UNS','VALLEY':'VLY','VALLY':'VLY','VLLY':'VLY','VALLEYS':'VLYS','VDCT':'VIA','VIADCT':'VIA','VIADUCT':'VIA','VIEW':'VW','VIEWS':'VWS','VILL':'VLG','VILLAG':'VLG','VILLAGE':'VLG','VILLG':'VLG','VILLIAGE':'VLG','VILLAGES':'VLGS','VILLE':'VL','VIST':'VIS','VISTA':'VIS','VST':'VIS','VSTA':'VIS','WALKS':'WALK','WY':'WAY','WELL':'WL','WELLS':'WLS',}

def replace_names(text, roads_name_dict=None):
    ## Used to replace abbr with full name to standardize addresses
    s = str(text).upper()
    ## This list was saved in a file C:\Users\Callan.Mix\OneDrive - Kroll\Documents\DataTeamScripts\Toolkit\Addresses\Data\Common_Street_Abbrevations_Official_Abbrevations.csv
    ## Originally from https://pe.usps.com/text/pub28/28apc_002.htm 
    ## We use a manual list to save time from loading from disk (changed to seperate function 7-22-22) `create_road_replacement_dict()`
    if roads_name_dict == None:
        roads_name_dict = create_road_replacement_dict()
    for k,v in roads_name_dict.items():
        s = re.sub(r"\b" + k + r"\b", v, s)
    s = re.sub(r"\.", "", s)
    return s

### Here is some custom logic to add to the countries list
class Country:
  def __init__(self, alpha_2='', alpha_3='', flag='', name='', numeric=''):
    self.alpha_2 = alpha_2
    self.alpha_3 = alpha_3
    self.flag = flag
    self.name = name
    self.numeric = numeric

for c in pycountry.countries:
    pass

## Add Countries
pycountry.countries.objects.append(Country(alpha_2='CZ', alpha_3='CZE', flag='', name='CZECH REPUBLIC', numeric='420'))
pycountry.countries.objects.append(Country(alpha_2='TW', alpha_3='TWN', flag='', name='TAIWAN', numeric='158'))

def find_countries(text):
    """
    Here we use the pycountry package to locate contries in a string. It is just a hard lookup where we go
    through a list of countries looking for its full name or abbr. We only look after the last digits in a 
    string (What we hope is the zip code)
    """
    if (text == '') or text is None:
        return ("","")
    ## Only look after zip code
    text = re.findall("\D+", text)[-1].upper()
    result_list = []
    for country in pycountry.countries:
        # It needs to be on its own (hence word break on both sides)
        if re.search(r'\b' + country.name.upper() + r'\b', text):
            result_list.append((country.name.upper(), country.name.upper()))
        elif re.search(r'\b' + country.alpha_3.upper() + r'\b', text):
            result_list.append((country.name.upper(), country.alpha_3.upper()))
    return result_list[0] if len(result_list) >= 1 else ("","")

####### Helpers For After Main Parse ###########
def parse_phone(text):
    regex_pattern = r"((\([0-9]{3}\)?|[0-9]{3}\W)[0-9]{3}\W[0-9]{4}|[0-9]{10}|\([0-9]{3}\)\W[0-9]{3}\W[0-9]{4})"
    try:
        phone = re.search(regex_pattern, text).group()
        phone = re.sub(r'[^0-9]', "", phone)
        text = re.sub(" +", " ", text)
        text = re.sub(regex_pattern, "", text)
        return phone, text
    except:
        return "", text

def keep_only_letters(text):
    ## Useful to see if city and state are just letters and therefore mostly likely valid
    return re.sub('[^a-zA-Z]+', '', text).upper()

def split_zip(zip):
    """
    If a zip has a '-' then split it out else return zip and blank
    """
    zip = ''.join(str(zip).split()).strip()
    ## If zip has extra 4 split out columns
    if "-" in zip:
        results_dict = {"Zip_Code": zip.split("-")[0], "Zip_Code_4": zip.split("-")[1]}
    elif len(zip) == 9:
        results_dict = {"Zip_Code": zip[:5], "Zip_Code_4": zip[5:]}
    elif zip.isdecimal() and len(zip) > 5:
        zip = zip.zfill(9)
        results_dict = {"Zip_Code": zip[:5], "Zip_Code_4": zip[5:]}
    else:
        results_dict = {"Zip_Code": zip.split("-")[0], "Zip_Code_4": ""}

    for key, value in results_dict.items():
        if str(value) == '0':
            results_dict[key] = ''
        if (key == 'Zip_Code') and (str(value) not in ['', '0']):
            results_dict[key] = str(value).zfill(5)
        elif (key == 'Zip_Code_4') and (str(value) not in ['', '0']):
            results_dict[key] = str(value).zfill(4)
        
    return results_dict

def split_name(text, reorder_if_comma=False, roman_nums = True):
    """
    Splits names into first, middle, last. If there are more than 3 words/names, all extras get moved to the last name spot
    """ 
    txt = str(text)
    if reorder_if_comma:
        txt =  txt if ',' not in txt else " ".join([txt.split(',')[-1], txt.split(',')[0]]).strip()
    txt = clean_name(txt)
    def combine_roman_nums(text_list):
        """
        This is for suffixes like Mike Jenson III. We want to combine the III into the previous name (Jenson in this case)
        """
        temp_list = text_list
        for i, item in enumerate(text_list):
            if all(c in 'IV' for c in item) and len(item) > 1:
                try:
                    temp_match = temp_list.pop(i)
                    temp_list[i-1] = temp_list[i-1] + ' ' + temp_match
                except:
                    pass
        return temp_list
    name_list = txt.strip().upper().split()
    if roman_nums:
        name_list = combine_roman_nums(name_list)
    if len(name_list) == 0:
        results_dict = {"First_Name":"", "Middle_Name":"", "Last_Name":""}
    elif len(name_list) == 1:
        results_dict = {"First_Name":name_list[0], "Middle_Name":"", "Last_Name":""}
    elif len(name_list) == 2:
        results_dict = {"First_Name":name_list[0], "Middle_Name":"", "Last_Name":name_list[1]}
    elif len(name_list) == 3:
        results_dict = {"First_Name":name_list[0], "Middle_Name":name_list[1],"Last_Name":name_list[2]}
    else:
        results_dict = {"First_Name":name_list[0], "Middle_Name":name_list[1], "Last_Name":" ".join(name_list[2:])}
    return results_dict

def clean_name(text, exclude_extra_characters=False):
    """
    We need to clean up names of all non alphanumeric characters. Replace them with space, trim text, and replace multiple spaces with one
    """
    regex_pattern = r'[^A-Za-z0-9\- \.\,\#\/]+' if exclude_extra_characters else r'[^A-Za-z0-9\- ]+'
    text = re.sub(regex_pattern, '', str(text))
    return clean_spaces(text)

def clean_spaces(text):
    """
    We want to get rid of leading, trailing, and extra spaces in text
    """
    text = text.strip()
    return ' '.join(text.split())

def append_new_column_to_data_with_function(data, column, function, replace_previous_column = False, new_column_names = []):
    """
    Helpful for adding new columns to a dataframe. Pretty much skip apply function
    - column: name of column on old data to apply function to
        - If this is a list, same function will be applied to all columns
    - function: function to use over the column
    - replace_previous_column: if true, overwrite the column with the new function data
    """
    
    data=data.copy()
    if replace_previous_column:
        data[column] = data[column].apply(lambda x: function(x))
        return data
    else:
        if len(new_column_names) > 0:
            result_df = pd.DataFrame(data[column].apply(lambda x: function(x)).to_list())
            data = data.reset_index(drop=True)
            result_df.columns = new_column_names
            return data.join(result_df)
        else:
            result_df = pd.DataFrame(data[column].apply(lambda x: function(x)).to_list())
            data = data.reset_index(drop=True)
            return data.join(result_df)

def clean_standardize_address_columns(data, address_col_list):
    """
    For the moment this is designed for just addr1 and addr2 columns. You pass in one or both and it will clean it up
    """
    data = data.copy()
    road_names_dict = create_road_replacement_dict()
    def standardize_stuff(addr, road_names_dict=road_names_dict):
        temp = str(addr)
        temp = clean_spaces(temp)
        temp = clean_name(temp)
        temp = replace_names(temp, road_names_dict)
        return temp
    for col in address_col_list:
        data[col] = [standardize_stuff(addr) for addr in data[col].to_list()]
    return data