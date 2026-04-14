import numpy as np
from pandas import DataFrame
from utils.USStates import us_dict 

def state_abbrev(statename) -> str:
    states = us_dict
    for st in sorted(states.keys(), key=len, reverse=True):
        statename = statename.replace(st, states[st])
    return statename

def trim_decimals(in_string: str) -> str:
    if in_string[-2:] == ".0":
        in_string = in_string[:-2]
    return in_string

def get_fill_values(dataFrame: DataFrame) -> dict:
    fill_values = {}
    for column in dataFrame.columns:
        if dataFrame[column].dtype == 'O':
            fill_values[column] = "_missing"
        elif dataFrame[column].dtype in ['int64', 'float64']:
            fill_values[column] = -999
        else:
            fill_values[column] = np.nan
    return fill_values

def account_table_clean_updated(input_df: DataFrame, source: str) -> DataFrame:
    # Handles SF data having addresstype column and condensed address columns
    input_df.name = source
    if source == "sf":
        input_df.fillna("_sf", inplace=True)
        
        if "LOB" not in input_df:
            input_df['LOB'] = "_sf"
        if "DHCID" not in input_df:
            input_df['DHCID'] = "_sf"
        else:
            input_df.fillna("_sf", inplace=True)
            input_df['DHCID'] = input_df['DHCID'].astype(str)
            # print(input_df['DHCID'])
            input_df.DHCID = input_df.DHCID.apply(trim_decimals)
        if "DHCNetworkID" not in input_df:
            input_df['DHCNetworkID'] = "_sf"
        else:
            input_df.fillna("_sf", inplace=True)
            input_df['DHCNetworkID'] = input_df['DHCNetworkID'].astype(str)
            input_df.DHCNetworkID = input_df.DHCNetworkID.apply(trim_decimals)


    elif source == "input":
        fillValues = get_fill_values(input_df)
        input_df.fillna(fillValues, inplace=True)
        input_df['input_key'] = np.arange(0, input_df.shape[0])
        if "LOB" not in input_df:
            input_df['LOB'] = "_in"
        if "DHCID" not in input_df:
            input_df['DHCID'] = "_in"
        else:
            input_df.fillna("_in", inplace=True)
            input_df['DHCID'] = input_df['DHCID'].astype(str)
            input_df.DHCID = input_df.DHCID.apply(trim_decimals)
        if "DHCNetworkID" not in input_df:
            input_df['DHCNetworkID'] = "_in"
        else:
            input_df.fillna("_in", inplace=True)
            input_df['DHCNetworkID'] = input_df['DHCNetworkID'].astype(str)
            input_df.DHCNetworkID = input_df.DHCNetworkID.apply(trim_decimals)

    input_df.Address = input_df.Address.str.upper()
    input_df.City = input_df.City.str.upper()
    input_df.State = input_df.State.apply(state_abbrev).str.upper()

    if "Phone" not in input_df:
        input_df["Phone"] = "_in"
    else:
        input_df.Phone = input_df.Phone.astype(str)
        input_df.Phone = input_df.Phone.apply(trim_decimals)
        input_df.Phone = input_df.Phone.apply(lambda x: 
            x.replace("(","").replace("-","").replace(")","").replace(" ",""))

    
    if "NPI__c" in input_df:
        input_df.NPI__c = input_df.NPI__c.astype(str)
        input_df.NPI__c = input_df.NPI__c.apply(trim_decimals)
    elif "NPI" in input_df:
        input_df.NPI = input_df.NPI.astype(str)
        input_df.NPI = input_df.NPI.apply(trim_decimals)
    elif "NPI__c" not in input_df or "NPI" not in input_df:
        input_df["NPI"] = "_" + input_df.name[:2]
        
    if "Ext" in input_df:
            input_df.Ext = input_df.Ext.astype(str) 
            input_df.Ext = input_df.Ext.apply(trim_decimals)
    input_df.AccountName = input_df.AccountName.astype(str)
    if "TaxID" in input_df:
        input_df.TaxID = input_df.TaxID.astype(str)
        input_df.TaxID = input_df.TaxID.apply(trim_decimals)
    elif "Tax_ID__c" in input_df:
        input_df.Tax_ID__c = input_df.Tax_ID__c.astype(str)
        input_df.Tax_ID__c = input_df.Tax_ID__c.apply(trim_decimals)
        
    if "Taxonomy" in input_df:
        input_df.Taxonomy = input_df.Taxonomy.astype(str)
    elif "Taxonomy_Primary" in input_df:
        input_df.Taxonomy_Primary = input_df.Taxonomy_Primary.astype(str)
    return input_df

def regex_account_name(input_df, column_name) -> DataFrame:
    input_df["NormalizedName"] = input_df[column_name].str.replace("&", " and ")
    input_df["NormalizedName"] = input_df["NormalizedName"].str.replace(r"[^\w\s+]", " "
            , regex=True).str.strip()
    
    collapse_abbreviations = {
    " R D" : " RD",
    " P C" : " PC",
    " M D" : " MD",
    " C N" : " CN",
    " Psy D" : " PsyD",
    " PhD LLC" : "PhDLLC",
    " Ph D" : "PhD",
    " L L C" : " LLC",
    " P L L C" : " PLLC",
    " L P C" : " LPC",
    " L P N" : " LPN",  
    " MD PA " : " MDPA",
    " MD INC" : "MDINC",
    " PA MD " : " PAMD",
    " L C S W" : " LCSW",
    " Co Inc " : " CoInc",
    " PA M D " : " PAMD",
    " M D P A" : " MDPA",
    " P A" : " PA",
    }
    for pattern, collapse in collapse_abbreviations.items():
        input_df["NormalizedName"] = input_df["NormalizedName"].str.replace(
            pattern, collapse)
      
    abbreviations = {        
    " Corporation" : "",
    "PhDLLC" : "",
    " Corp " : "",
    " ASSO " : "",
    "MDINC" : "",
    " INC " : "",
    " Inc " : "",
    " Inc" : "",
    " PLLC " : "",
    " CACIII" : "",
    " LCSW C" : "",
    " LCSW R" : "",
    " LCSW S" : "",
    " DPM " : "",
    " CoInc " : "",
    " LLC " : "",
    " LLP " : "",
    " LTD " : "",
    " LCSWC" : "",
    " LICSW" : "",
    " CO " : "",
    " LC " : "",
    " LP " : "",
    " MD " : "",
    " OD " : "",
    " PA " : "",
    " PC " : "",
    " LCSW" : "",
    " LFMT" : "",
    " LMFT" : "",
    " LMHC" : "",
    " LMSW" : "",
    " MSRD" : "",
    " PLLC" : "",
    " PsyD" : "",
    " DDS" : "",
    " DMD" : "",
    " DPM" : "",
    " DSW" : "",
    " INC" : "",
    " LLC" : "",
    " LLP" : "",
    " LPC" : "",
    " LPN" : "",
    " LTD" : "",
    " MFT" : "",
    " MSW" : "",
    " OSS" : "",
    " PHD" : "",
    " CN" : "",
    " CO" : "",
    " CS" : "",
    " LC" : "",
    " LP" : "",
    " MD" : "",
    " OD" : "",
    " PA" : "",
    " PAU" : "",
    " PC" : "",
    " RD" : "",
    " RN" : "",
    }
    for pattern, replacement in abbreviations.items():
        input_df["NormalizedName"] = input_df["NormalizedName"].str.replace(
            pattern, replacement)
    input_df["NormalizedName"] = input_df.NormalizedName.str.upper()
    input_df.NormalizedName = input_df.NormalizedName.replace(r"[\s]", "", regex=True)
    input_df['NormCo40'] = input_df["NormalizedName"].str.slice(0,40)
    input_df['NormCo30'] = input_df["NormalizedName"].str.slice(0,30)
    input_df['NormCo25'] = input_df["NormalizedName"].str.slice(0,25)
    input_df['NormCo20'] = input_df["NormalizedName"].str.slice(0,20)
    input_df['NormCo15'] = input_df["NormalizedName"].str.slice(0,15)
    input_df['NormCo10'] = input_df["NormalizedName"].str.slice(0,10)
    return input_df

def normalize_account_name(input_df, column_name) -> DataFrame:
    """Function to normalize account names from an input data frame.
       Must specify column name to normalzie.

    Args:
        input_df (input_df): Input input_df
        column_name (string): Name of column to normalize

    Returns:
        input_df: Normalized input_df
    """    
    input_df["NormalizedName"] = input_df[column_name].str.upper()
    input_df['NormalizedName'] = input_df.NormalizedName.apply(
        lambda name: name[:-3] if name[-4:] == ' co.' else name)
    input_df['NormalizedName'] = input_df.NormalizedName.apply(
        lambda name: name[:-4] if name[-5:] == ' corp' else name)
    input_df['NormalizedName'] = input_df.NormalizedName.apply(
        lambda name: name[:-6] if name[-8:] == ' co. inc' else name)
    input_df['NormalizedName'] = input_df.NormalizedName.apply(
        lambda name: name[:-5] if name[-7:] == ' co inc' else name)
    input_df['NormalizedName'] = input_df.NormalizedName.apply(
        lambda name: name[:-4] if name[-5:] == ' pllc' else name)
    input_df['NormalizedName'] = input_df.NormalizedName.apply(
        lambda name: name[:-3] if name[-4:] == ' llc' else name)
    input_df['NormalizedName'] = input_df.NormalizedName.apply(
        lambda name: name[:-3] if name[-4:] == ' llp' else name)
    input_df['NormalizedName'] = input_df.NormalizedName.apply(
        lambda name: name[:-3] if name[-4:] == ' ltd' else name)
    input_df['NormalizedName'] = input_df.NormalizedName.apply(
        lambda name: name[:-4] if name[-5:] == ' inc.' else name)
    input_df['NormalizedName'] = input_df.NormalizedName.apply(
        lambda name: name[:-3] if name[-4:] == ' inc' else name)
    input_df['NormalizedName'] = input_df.NormalizedName.apply(
        lambda name: name[:-5] if name[-6:] == ' corp.' else name)
    input_df['NormalizedName'] = input_df.NormalizedName.apply(
        lambda name: name[:-2] if name[-3:] == ' lp' else name)
    input_df['NormalizedName'] = input_df.NormalizedName.apply(
        lambda name: name[:-2] if name[-3:] == ' lc' else name)
    input_df['NormalizedName'] = input_df.NormalizedName.apply(
        lambda name: name[:-3] if name[-4:] == ' lc.' else name)
    input_df['NormalizedName'] = input_df.NormalizedName.apply(
        lambda name: name[:-2] if name[-3:] == ' pa' else name)
    input_df['NormalizedName'] = input_df.NormalizedName.apply(
        lambda name: name[:-3] if name[-4:] == ' dpm' else name)
    input_df['NormalizedName'] = input_df.NormalizedName.apply(
        lambda name: name[:-4] if name[-5:] == ' m.d.' else name)
    input_df['NormalizedName'] = input_df.NormalizedName.apply(
        lambda name: name[:-2] if name[-3:] == ' md' else name)
    input_df['NormalizedName'] = input_df.NormalizedName.apply(
        lambda name: name[:-2] if name[-3:] == ' pc' else name)
    input_df['NormalizedName'] = input_df.NormalizedName.apply(
        lambda name: name[:-2] if name[-3:] == ' co' else name)
    input_df['NormalizedName'] = input_df.NormalizedName.apply(
        lambda name: name[:-4] if name[-5:] == ' asso' else name)
    input_df['NormalizedName'] = input_df.NormalizedName.apply(
        lambda name: name[:-5] if name[-6:] == ' md pa' else name)
    input_df['NormalizedName'] = input_df.NormalizedName.apply(
        lambda name: name[:-7] if name[-8:] == ' pa m.d.' else name)
    input_df['NormalizedName'] = input_df.NormalizedName.apply(
        lambda name: name[:-5] if name[-6:] == ' pa md' else name)
    input_df['NormalizedName'] = input_df.NormalizedName.apply(
        lambda name: name[:-2] if name[-3:] == ' od' else name)
    input_df['NormalizedName'] = input_df.NormalizedName.str.strip()
    input_df['NormalizedName'] = input_df.NormalizedName.apply(
        lambda name: name[:-3] if name[-4:] == ' co.' else name)
    input_df['NormalizedName'] = input_df.NormalizedName.apply(
        lambda name: name[:-4] if name[-5:] == ' corp' else name)
    input_df['NormalizedName'] = input_df.NormalizedName.apply(
        lambda name: name[:-6] if name[-8:] == ' co. inc' else name)
    input_df['NormalizedName'] = input_df.NormalizedName.apply(
        lambda name: name[:-5] if name[-7:] == ' co inc' else name)
    input_df['NormalizedName'] = input_df.NormalizedName.apply(
        lambda name: name[:-4] if name[-5:] == ' pllc' else name)
    input_df['NormalizedName'] = input_df.NormalizedName.apply(
        lambda name: name[:-3] if name[-4:] == ' llc' else name)
    input_df['NormalizedName'] = input_df.NormalizedName.apply(
        lambda name: name[:-3] if name[-4:] == ' llp' else name)
    input_df['NormalizedName'] = input_df.NormalizedName.apply(
        lambda name: name[:-3] if name[-4:] == ' ltd' else name)
    input_df['NormalizedName'] = input_df.NormalizedName.apply(
        lambda name: name[:-4] if name[-5:] == ' inc.' else name)
    input_df['NormalizedName'] = input_df.NormalizedName.apply(
        lambda name: name[:-3] if name[-4:] == ' inc' else name)
    input_df['NormalizedName'] = input_df.NormalizedName.apply(
        lambda name: name[:-5] if name[-6:] == ' corp.' else name)
    input_df['NormalizedName'] = input_df.NormalizedName.apply(
        lambda name: name[:-2] if name[-3:] == ' lp' else name)
    input_df['NormalizedName'] = input_df.NormalizedName.apply(
        lambda name: name[:-2] if name[-3:] == ' lc' else name)
    input_df['NormalizedName'] = input_df.NormalizedName.apply(
        lambda name: name[:-3] if name[-4:] == ' lc.' else name)
    input_df['NormalizedName'] = input_df.NormalizedName.apply(
        lambda name: name[:-2] if name[-3:] == ' pa' else name)
    input_df['NormalizedName'] = input_df.NormalizedName.apply(
        lambda name: name[:-3] if name[-4:] == ' dpm' else name)
    input_df['NormalizedName'] = input_df.NormalizedName.apply(
        lambda name: name[:-4] if name[-5:] == ' m.d.' else name)
    input_df['NormalizedName'] = input_df.NormalizedName.apply(
        lambda name: name[:-2] if name[-3:] == ' md' else name)
    input_df['NormalizedName'] = input_df.NormalizedName.apply(
        lambda name: name[:-2] if name[-3:] == ' pc' else name)
    input_df['NormalizedName'] = input_df.NormalizedName.apply(
        lambda name: name[:-2] if name[-3:] == ' co' else name)
    input_df['NormalizedName'] = input_df.NormalizedName.apply(
        lambda name: name[:-4] if name[-5:] == ' asso' else name)
    input_df['NormalizedName'] = input_df.NormalizedName.apply(
        lambda name: name[:-5] if name[-6:] == ' md pa' else name)
    input_df['NormalizedName'] = input_df.NormalizedName.apply(
        lambda name: name[:-7] if name[-8:] == ' pa m.d.' else name)
    input_df['NormalizedName'] = input_df.NormalizedName.apply(
        lambda name: name[:-5] if name[-6:] == ' pa md' else name)
    input_df['NormalizedName'] = input_df.NormalizedName.apply(
        lambda name: name[:-2] if name[-3:] == ' od' else name)
    input_df['NormalizedName'] = input_df.NormalizedName.apply(
        lambda name: name.replace(',', '').replace('.', '').
        replace(';', '').replace(':', '').replace("'", '').replace('(', '').
        replace(')', '').replace('[', '').replace(']', '').replace('{', '').
        replace('}', '').replace('|', '').replace('-', '').replace('\\', '').
        replace('/', '').replace('&', 'and').replace(' ', ''))
    
    input_df['NormalizedName'] = input_df.NormalizedName.str.strip()
    input_df['NormCo40'] = input_df.NormalizedName.str[:40]
    input_df['NormCo30'] = input_df.NormalizedName.str[:30]
    input_df['NormCo25'] = input_df.NormalizedName.str[:25]
    input_df['NormCo20'] = input_df.NormalizedName.str[:20]
    input_df['NormCo15'] = input_df.NormalizedName.str[:15]
    input_df['NormCo10'] = input_df.NormalizedName.str[:10]
    input_df['NormCo5'] = input_df.NormalizedName.str[:5]
    
    return input_df
    
def normalize_street(input_df, column_name) -> DataFrame:
    if "AddressType" in input_df.columns:
        input_df['NormAddress'] = input_df[column_name].str.lower()
        input_df["NormAddress"] = input_df.NormAddress.astype(str)
        input_df['NormAddress'] = input_df.NormAddress.apply(
            lambda name: name[: name.find(' ste ') if name.find(' ste ') > 0 
                            else len(name) + 1])
        input_df['NormAddress'] = input_df.NormAddress.apply(
            lambda name: name[: name.find('apt.') if name.find('apt.') > 0 
                            else len(name) + 1])
        input_df['NormAddress'] = input_df.NormAddress.apply(
            lambda name: name[: name.find('ste.') if name.find('ste.') > 0 
                            else len(name) + 1])
        input_df['NormAddress'] = input_df.NormAddress.apply(
            lambda name: name[: name.find('appt') if name.find('appt') > 0 
                            else len(name) + 1])
        input_df['NormAddress'] = input_df.NormAddress.apply(
            lambda name: name[: name.find('no.') if name.find('no.') > 0 
                            else len(name) + 1])
        input_df['NormAddress'] = input_df.NormAddress.apply(
            lambda name: name[: name.find(' unit ') if name.find(' unit ') > 0 
                            else len(name) + 1])
        input_df['NormAddress'] = input_df.NormAddress.apply(
            lambda name: name[: name.find('apartment') if name.find('apartment') > 0 
                            else len(name) + 1])
        input_df['NormAddress'] = input_df.NormAddress.apply(
            lambda name: name[: name.find('apt') if name.find('apt') > 0 
                            else len(name) + 1])
        input_df['NormAddress'] = input_df.NormAddress.apply(
            lambda name: name[: name.find('suite') if name.find('suite') > 0 
                            else len(name) + 1])
        input_df['NormAddress'] = input_df.NormAddress.apply(
            lambda name: name[: name.find('number') if name.find('number') > 0 
                            else len(name) + 1])
        input_df['NormAddress'] = input_df.NormAddress.apply(
            lambda name: name[: name.find('\n') if name.find('\n') > 0 else len(name) + 1])
        input_df['NormAddress'] = input_df.NormAddress.apply(
            lambda name: name.replace(',', '').replace('.', '').replace(';', '').
            replace(':', '').replace('(', '').replace(')', '').replace('[', '').
            replace(']', '').replace('{', '').replace('}', '').replace('|', '').
            replace('-', '').replace('\n', ''))
        input_df['NormAddress'] = input_df['NormAddress'].str.strip()    
    else:
        input_df['NormShipStreet'] = input_df[column_name].str.lower()
        input_df["NormShipStreet"] = input_df.NormShipStreet.astype(str)
        input_df['NormShipStreet'] = input_df.NormShipStreet.apply(
            lambda name: name[: name.find(' ste ') if name.find(' ste ') > 0 
                            else len(name) + 1])
        input_df['NormShipStreet'] = input_df.NormShipStreet.apply(
            lambda name: name[: name.find('apt.') if name.find('apt.') > 0 
                            else len(name) + 1])
        input_df['NormShipStreet'] = input_df.NormShipStreet.apply(
            lambda name: name[: name.find('ste.') if name.find('ste.') > 0 
                            else len(name) + 1])
        input_df['NormShipStreet'] = input_df.NormShipStreet.apply(
            lambda name: name[: name.find('appt') if name.find('appt') > 0 
                            else len(name) + 1])
        input_df['NormShipStreet'] = input_df.NormShipStreet.apply(
            lambda name: name[: name.find('no.') if name.find('no.') > 0 
                            else len(name) + 1])
        input_df['NormShipStreet'] = input_df.NormShipStreet.apply(
            lambda name: name[: name.find(' unit ') if name.find(' unit ') > 0 
                            else len(name) + 1])
        input_df['NormShipStreet'] = input_df.NormShipStreet.apply(
            lambda name: name[: name.find('apartment') if name.find('apartment') > 0 
                            else len(name) + 1])
        input_df['NormShipStreet'] = input_df.NormShipStreet.apply(
            lambda name: name[: name.find('apt') if name.find('apt') > 0 
                            else len(name) + 1])
        input_df['NormShipStreet'] = input_df.NormShipStreet.apply(
            lambda name: name[: name.find('suite') if name.find('suite') > 0 
                            else len(name) + 1])
        input_df['NormShipStreet'] = input_df.NormShipStreet.apply(
            lambda name: name[: name.find('number') if name.find('number') > 0 
                            else len(name) + 1])
        input_df['NormShipStreet'] = input_df.NormShipStreet.apply(
            lambda name: name[: name.find('\n') if name.find('\n') > 0 else len(name) + 1])
        input_df['NormShipStreet'] = input_df.NormShipStreet.apply(
            lambda name: name.replace(',', '').replace('.', '').replace(';', '').
            replace(':', '').replace('(', '').replace(')', '').replace('[', '').
            replace(']', '').replace('{', '').replace('}', '').replace('|', '').
            replace('-', '').replace('\n', ''))
        
        input_df['NormShipStreet'] = input_df['NormShipStreet'].str.strip()

        if 'BillingStreet' in input_df:
            input_df['NormBillStreet'] = input_df.BillingStreet.str.lower()
            input_df["NormBillStreet"] = input_df.NormBillStreet.astype(str)
            input_df['NormBillStreet'] = input_df.NormBillStreet.apply(
                lambda name: name[: name.find(' ste ') if name.find(' ste ') > 0 
                                else len(name) + 1])
            input_df['NormBillStreet'] = input_df.NormBillStreet.apply(
                lambda name: name[: name.find('apt.') if name.find('apt.') > 0 
                                else len(name) + 1])
            input_df['NormBillStreet'] = input_df.NormBillStreet.apply(
                lambda name: name[: name.find('ste.') if name.find('ste.') > 0 
                                else len(name) + 1])
            input_df['NormBillStreet'] = input_df.NormBillStreet.apply(
                lambda name: name[: name.find('appt') if name.find('appt') > 0 
                                else len(name) + 1])
            input_df['NormBillStreet'] = input_df.NormBillStreet.apply(
                lambda name: name[: name.find('no.') if name.find('no.') > 0 
                                else len(name) + 1])
            input_df['NormBillStreet'] = input_df.NormBillStreet.apply(
                lambda name: name[: name.find(' unit ') if name.find(' unit ') > 0 
                                else len(name) + 1])
            input_df['NormBillStreet'] = input_df.NormBillStreet.apply(
                lambda name: name[: name.find('apartment') if name.find('apartment') > 0 
                                else len(name) + 1])
            input_df['NormBillStreet'] = input_df.NormBillStreet.apply(
                lambda name: name[: name.find('apt') if name.find('apt') > 0 
                                else len(name) + 1])
            input_df['NormBillStreet'] = input_df.NormBillStreet.apply(
                lambda name: name[: name.find('suite') if name.find('suite') > 0 
                                else len(name) + 1])
            input_df['NormBillStreet'] = input_df.NormBillStreet.apply(
                lambda name: name[: name.find('number') if name.find('number') > 0 
                                else len(name) + 1])
            input_df['NormBillStreet'] = input_df.NormBillStreet.apply(
                lambda name: name[: name.find('\n') if name.find('\n') > 0 
                                else len(name) + 1])
            input_df['NormBillStreet'] = input_df.NormBillStreet.apply(
                lambda name: name.replace(',', '').replace('.', '').replace(';', '')
                .replace(':', '').replace('(', '').replace(')', '').replace('[', '')
                .replace(']', '').replace('{', '').replace('}', '').replace('|', '')
                .replace('-', '').replace('\n', ''))
            
            input_df['NormBillStreet'] = input_df['NormBillStreet'].str.strip()
        else:
            input_df['NormBillStreet'] = input_df.NormShipStreet

    return input_df

def normalize_suite(input_df, column_name) -> DataFrame:
    input_df['NormAddressSuite'] = input_df[column_name].str.lower()
    input_df["NormAddressSuite"] = input_df.NormAddressSuite.astype(str)
    input_df['NormAddressSuite'] = input_df.NormAddressSuite.apply(
        lambda name: name[name.find(' ste ') if name.find(' ste ') > 0 
                          else 0:len(name) + 1])
    input_df['NormAddressSuite'] = input_df.NormAddressSuite.apply(
        lambda name: name[name.find('apt.') if name.find('apt.') > 0 
                          else 0:len(name) + 1])
    input_df['NormAddressSuite'] = input_df.NormAddressSuite.apply(
        lambda name: name[name.find('ste.') if name.find('ste.') > 0 
                          else 0:len(name) + 1])
    input_df['NormAddressSuite'] = input_df.NormAddressSuite.apply(
        lambda name: name[name.find('appt') if name.find('appt') > 0 
                          else 0:len(name) + 1])
    input_df['NormAddressSuite'] = input_df.NormAddressSuite.apply(
        lambda name: name[name.find('no.') if name.find('no.') > 0 
                          else 0:len(name) + 1])
    input_df['NormAddressSuite'] = input_df.NormAddressSuite.apply(
        lambda name: name[name.find(' unit ') if name.find(' unit ') > 0 
                          else 0:len(name) + 1])
    input_df['NormAddressSuite'] = input_df.NormAddressSuite.apply(
        lambda name: name[name.find('appartment') if name.find('appartment') > 0 
                          else 0:len(name) + 1])
    input_df['NormAddressSuite'] = input_df.NormAddressSuite.apply(
        lambda name: name[name.find('apt') if name.find('apt') > 0 
                          else 0:len(name) + 1])
    input_df['NormAddressSuite'] = input_df.NormAddressSuite.apply(
        lambda name: name[name.find('suite') if name.find('suite') > 0 
                          else 0:len(name) + 1])
    input_df['NormAddressSuite'] = input_df.NormAddressSuite.apply(
        lambda name: name[name.find('number') if name.find('number') > 0 
                          else 0:len(name) + 1])
    input_df['NormAddressSuite'] = input_df.NormAddressSuite.apply(
        lambda name: name[name.find('\n') if name.find('\n') > 0 
                          else 0:len(name) + 1])
    input_df['NormAddressSuite'] = input_df.NormAddressSuite.apply(
        lambda name: name.replace(',', '').replace('.', '').replace(';', '').
        replace(':', '').replace('\n', '').replace('(', '').replace(')', '').
        replace('[', '').replace(']', '').replace('{', '').replace('}', '').
        replace('|', '').replace('-', ''))
    
    ship_mask = input_df["NormAddressSuite"] == input_df["NormAddress"]
    input_df.loc[ship_mask,"NormAddressSuite"] = ""
            
    input_df['NormAddressSuite'] = input_df.NormAddressSuite.apply(
        lambda name: name.replace('ste', '').replace('appt', '').replace('apt', '').
        replace('suite', '').replace('unit', '').replace('apartment', '').
        replace('number', '').replace('no', '').replace('#', ''))
    
    input_df['NormAddressSuite'] = input_df['NormAddressSuite'].str.strip()
    input_df['NormAddressSuite'] = input_df['NormAddressSuite'].replace('',np.nan)

    return input_df
   
def normalize_zipcode(input_df, column_name) -> DataFrame:
    if column_name not in input_df:
        print("{} not in input_df. Creating dummy column".format(column_name))
        input_df[column_name] = "_" + input_df.name[:2]
    input_df['NormZip'] = input_df[column_name].astype('unicode')
    input_df['NormZip'] = input_df.NormZip.apply(
        lambda zipc: zipc[:zipc.find('-')] if zipc.find('-') > 0 else zipc[:5])
    input_df['NormZip'] = input_df.NormZip.apply(
        lambda zipc: zipc.zfill(5) if zipc.isnumeric() else np.nan)
    input_df['NormZip'] = input_df.NormZip.apply(str)
    return input_df
