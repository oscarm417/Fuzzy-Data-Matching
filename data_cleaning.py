import numpy as np 
import pandas as pd

def split_long_names(text:str,threshold= 60)->list[str]:
    """Splits text into a list, where each index contains a threshold amount of characters. The default threshold is 60 characters

    Args:
        text (_type_): _description_
        threshold (int, optional): _description_. Defaults to 60.

    Returns:
        _type_: _description_
    """
    text = text.split(" ")
    col_storage = []
    temp_storage = []
    for i in text:
        if len(" ".join(temp_storage+[i])) >= threshold:
            col_storage.append(temp_storage)
            temp_storage = [i]
        else:
            temp_storage.append(i)

    col_storage.append(temp_storage)
    col_storage = [" ".join(i) for i in col_storage]
    return col_storage


target_columns = [
    'REG_ADDRESS_LINE_1',
    'REG_ADDRESS_LINE_2',
    'REG_ADDRESS_LINE_3',
    'REG_ADDRESS_LINE_4',
    'REG_ADDRESS_LINE_5',
    'REG_ADDRESS_LINE_6',
    'REG_ADDRESS_LINE_7',
    'REG_ADDRESS_LINE_8'
]
labels = {
    'Recipient': 'Name2',
   'AddressNumber': 'Address1',
   'AddressNumberPrefix': 'Address1',
   'AddressNumberSuffix': 'Address1',
   'StreetName': 'Address1',
   'NotAddress': 'Address1',
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
   'SecondStreetName': 'Address2',
   'SecondStreetNamePostType': 'Address2',
   'SecondStreetNamePostDirectional': 'Address2',
   'PlaceName': 'City',
   'StateName': 'St',
   'ZipCode': 'PostalCode',
   'CountryName': 'Country'
}

import usaddress
def if_module_breaks(val: list[str,str])->dict:
    """Used to fix usaddresses when the module usaddress fails to properly handle the address.

    Args:
        val (list[str,str]): Pass in the output from the failed parsing

    Returns:
        dict: A dictionary containing the address tag key: address tag infromation
    """
    values = [i[0] for i in val]
    keys = list(set([i[1] for i in val]))
    final = {i:[] for i in keys}
    for v in val:
        val_temp = v[0]
        key_temp = v[1]

        final[key_temp].append(val_temp)
    labels_final = {v:[] for k,v in labels.items()}

    for k in final:
        temp_label = labels[k]
        labels_final[temp_label]+=final[k]


    labels_final = {k:" ".join(v) for k,v in labels_final.items()}

    return labels_final

def split_data(indx,df):
    temp_list = [i for i in df.iloc[indx].tolist()[:6] if str(i) != 'nan']
    temp_list.reverse()
    address_list = []
    name_list = []
    i  = 0 
    name = False
    while name == False:
        if i in range(0,len(temp_list)-1):
            temp_id = dict(usaddress.tag(str(temp_list[i]))[0])
            address_list.append(temp_list[i])

            if ("AddressNumber" in temp_id and "StreetName" in temp_id) or "USPSBoxID" in temp_id:
                name = True 

            i+=1
        else:
            break
    name_list = temp_list[i:]
    name_list.reverse()
    address_list.reverse()
    return [" ".join([str(i) for i in name_list]), " ".join([str(i) for i in address_list])]

# def combined_dictionary
def split_data2(indx,df):
    info = {}
    temp_list = [i for i in df.iloc[indx].tolist() if str(i) != '']
    
    address_list = []
    name_list = []
    i  = len(temp_list)-1
    stateZip = False 
    #adding state and zip 
    while stateZip == False:
        if i in range(0,len(temp_list)+1):
            try: 
                temp_id = dict(usaddress.tag(str(temp_list[i], tag_mapping = labels))[0])
                if "PlaceName" in temp_id and "StateName" in temp_id and "ZipCode" in temp_id:
                    info = dict(list(info.items()) + list(temp_id.items()))
                    stateZip = True 
            except:
                pass 
            i-=1
        else:
            break
    #adding Address 

    addressFound = False
    while addressFound == False:
        print(info)
        if i in range(0,len(temp_list)+1):
            try:
                temp_id = dict(usaddress.tag(str(temp_list[i]),tag_mapping = labels )[0])
                print(temp_id)
                if ("AddressNumber" in temp_id and "StreetName" in temp_id) or "USPSBoxID" in temp_id:
                    addressFound = True 
                    info["Address1"] = temp_list[i]
            except usaddress.RepeatedLabelError as e:
                e = [i[1] for i in e.parsed_string]
                if ("AddressNumber" in e and "StreetName" in e) or "USPSBoxID" in e:
                    info["Address1"] = temp_list[i]

            i-=1
        else:
            break
    name_list = temp_list[:i+1]
    info["Name1"] = " ".join(name_list)
    return info 
    
def split_zip(combined:pd.DataFrame,zip_col_name = "ZipCode")->pd.DataFrame:
    """Splits zip codes into zip and zip4

    Args:
        combined (pd.DataFrame): DataFrame containing a zip code
        zip_col_name (str, optional): Name of Column containing the zip . Defaults to "ZipCode".

    Returns:
        pd.DataFrame: DataFrame containing the split zip 
    """
    zipcodes = []
    max_rows = 0 
    for ind in range(len(combined)):
        tempz = combined.iloc[ind][zip_col_name]

        if "-" in tempz:
            tempz = tempz.split("-")
            zipcodes.append(tempz)
            max_rows = max(max_rows,len(tempz))
        else:
            zipcodes.append([tempz,""])
    print(max_rows)
    combined[['Zip','Zip4']] = np.array(zipcodes,dtype = object)
    return combined
        
def split_data3(indx: int,df: pd.DataFrame)->dict:
    """Structures unstructure addresses.

    Args:
        indx (int): Index of the row you want to parse. If you want to apply it to the whole data frame, then loop through each row
        df (pd.DataFrame): Unparsed data frame you want to structure

    Returns:
        dict: Mapping of the address parts to their corresponding information
    """
    info = {}
    temp_list = [i for i in df.iloc[indx].tolist() if str(i) != '']
    
    address_list = []
    name_list = []
    i  = len(temp_list)-1
    cityFound = False 
    #adding state and zip 
    while cityFound == False:
        if i in range(0,len(temp_list)+1):
            try: 
                temp_id = dict(usaddress.tag(str(temp_list[i]))[0])
                info = dict(list(info.items()) + list(temp_id.items()))
                if "PlaceName" in temp_id:
                    cityFound = True 
            except:
                pass 
            i-=1
        else:
            break
    cityStart = i
    #adding Address 
    addressFound = False
    while addressFound == False:
        if i in range(0,len(temp_list)+1):
            try:
                temp_id = dict(usaddress.tag(str(temp_list[i]) )[0])
                if ("AddressNumber" in temp_id and "StreetName" in temp_id) or "USPSBoxID" in temp_id:
                    addressFound = True 
            except usaddress.RepeatedLabelError as e:
                e = [i[1] for i in e.parsed_string]
                if ("AddressNumber" in e and "StreetName" in e) or "USPSBoxID" in e:
                    addressFound = True
            i-=1
        else:
            break
    address  = temp_list[i+1:cityStart+1]
    name_list = temp_list[:i+1]
    info["Name1"] = " ".join(name_list)
    info["Address1"] = " ".join(address) 
    return info 

def parse_address(df:pd.DataFrame)->pd.DataFrame:
    res = [] 
    for i in range(len(df)):
        res.append(split_data3(i,df))
    result = pd.DataFrame(res)

def order_columns(resultDF:pd.DataFrame, col = [
    'Name1',
    'Address1',
    'PlaceName',
    'StateName',
    'ZipCode',
])-> pd.DataFrame:
    """Reorders the data frames columns

    Args:
        resultDF (pd.DataFrame): Parsed out DataFrame containing the structured addresses
        col (list, optional): _description_. Defaults to [ 'Name1', 'Address1', 'PlaceName', 'StateName', 'ZipCode', ].

    Returns:
        pd.DataFrame: DataFrame with ordered columns
    """
    new_col = col.copy()
    current_columns = resultDF.columns.tolist()

    for i in current_columns:
        if i not in new_col:
            new_col.append(i)

    return resultDF[new_col]

def good_and_bad_df(originalDf:pd.DataFrame, resultDF:pd.DataFrame,goodOutput:str, badOutput:str)->None:
    """Finds Missing Rows due to errors in parsing and puts the well parsed addresses in one excel sheet, and bad addresses on another

    Args:
        originalDf (pd.DataFrame): Originally read in data frame
        resultDF (pd.DataFrame): Dataframe created after parsing the addresses
        goodOutput (str): Well parsed address excel file location
        badOutput (str): Badly parsed address excel file location
    """
    good = resultDF[resultDF['Name1'] != ""]
    empty_col = [col for col in good.columns if good[col].isnull().all()]
    good.drop(empty_col,axis = 1, inplace = True)
    bad = resultDF[resultDF['Name1'] == ""] 
    bad_index = bad.index.tolist() 
    bad = originalDf.iloc[bad_index]
    good = order_columns(good)
    bad.to_excel(badOutput)
    good.to_excel(goodOutput)
    print("Success") 

def split_into_columns(combined: pd.DataFrame ,nameOfcolumn = "Address",threshold = 60)->pd.DataFrame:
    """Splits a column with characters limits into multiple columns where each column conforms to the character limits place on the column

    Args:
        combined (DataFrame): DataFrame containing the column that needs to be split 
        nameOfcolumn (str, optional): The starting name of the Columns we want to split. If the original column name is Address1, you use 'Address' here. Defaults to "Address".
        threshold (int, optional): The character threshold at which you want to split the column. Defaults to 60.

    Returns:
        pd.DataFrame: Returns a Data Frame with the new columns added after the target column was split. 
    """
    split_addresses = [split_long_names(i,threshold) for i in combined[nameOfcolumn+"1"].tolist()]
    max_cols = max([len(i) for i in split_addresses])
    if max_cols >1:
        new_columns = [nameOfcolumn+str(i) for i in range(1,max_cols+1)]
        split_addresses = [i+[""]*(max_cols-len(i)) if len(i)<=max_cols-1 else i for i in split_addresses ]
        combined[new_columns] = split_addresses
    else:
        combined[nameOfcolumn+"1"] = [i[0] for i in split_addresses] 
    combined.fillna("",inplace = True)
    return combined 



def add_leading_zeros(zipCode,Country):
    """Adding Zeros infront of American Zip codes that are less than 5 characters

    Args:
        zipCode (str): Zip Code
        Country (str): Country Name

    Returns:
        str: Formatted zipCode with leading zeros for those with less than 5 characters long, else it returns the original zip code. 
    """
    zipCode =str(zipCode)
    Country = str(Country)
    if len(zipCode) == 5: return zipCode 
    elif len(zipCode)>0 and Country == "" :
        m = "0"*(5-len(zipCode))
        newzip = m+zipCode 
        return newzip 
    else:
        return zipCode
        

def replace_a_with_b(original_dataframe, a, b):
    """Replace Column A with Column B where data is on both rows, else keep Column A
    Args:
        original_dataframe (DataFrame): DataFrame containing the target columns
        a (str): Column Name
        b (str): Column Name

    Returns:
        DataFrame: DataFrame with the data merged/replaced
    """
    new_col = np.where(original_dataframe[b].isnull(),original_dataframe[a], original_dataframe[b])
    original_dataframe[a] = new_col 
    return original_dataframe

def count_cols(df):
    cols = df.columns.tolist()
    counts = dict()
    for i in cols: 
        max_c = max([len(item) for item in df[i].tolist()])
        counts[i] = max_c
    return counts