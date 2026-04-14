# Import Libraries
import os as os
import pathlib as pl
import timeit
from datetime import date as date

import numpy as np
import pandas as pd
import rapidfuzz
import utils.INO_DataFrame_Normalization as nrm
import utils.INO_Paths as iop
import utils.INO_SQLEngine as sqe
import utils.SF_Query as sfq

# TODO: Clean up comments for terminal
# TODO: Implement a logger
# TODO: Delete depreciated code

# Functions
def path_manage(path, filename, output=False) -> pl.Path:
    managed_path = ""
    if output is True:
        managed_path = path + "\\" + filename + "_OUTPUT.csv"
    else:
        managed_path = path + "\\" + filename +  ".xlsx"
    managed_path = pl.Path(managed_path)
    return managed_path

def reshape_addresses(input_df) -> pd.DataFrame:
    fill_values = {"AccountName": "_in", "Phone": "_in", "BillingStreet": "_in", "BillingCity": "_in", "BillingState": "_in", "BillingPostalCode": -999,
               "ShippingStreet": "_in", "ShippingCity": "_in", "ShippingState": "_in", "ShippingPostalCode": -999, "NPI": -999, "Taxonomy": -999, "LOB": -999}
    
    input_df = input_df.fillna(fill_values, inplace = True)
    if 'ShippingStreet' in input_df.columns and 'BillingStreet' in input_df.columns:
        print("Shipping and Billing Addresses Exist.\nChecking if they're the same address.")  # noqa: E501
        shiptup = ("ShippingStreet","ShippingCity","ShippingState","ShippingPostalCode")
        billtup = ("BillingStreet","BillingCity","BillingState","BillingPostalCode")
        address_match = (input_df[shiptup[0]] == input_df[billtup[0]]).all() and (
            input_df[shiptup[1]] == input_df[billtup[1]]).all() and (
                input_df[shiptup[2]] == input_df[billtup[2]]).all() and (
                    input_df[shiptup[3]] == input_df[billtup[3]]).all()
        if address_match:
            print("All Address Data is the same.\n Reshaping....\n")
            reshaped_df = input_df.copy()
            # Condense Shipping and Billing into one column. Rename to match SF naming
            reshaped_df = reshaped_df.drop(columns = list(shiptup))
            reshaped_df.rename(columns = {billtup[0]:"Address", billtup[1]:"City"
                                        , billtup[2]:"State",billtup[3]:"Zip"}
                                        , inplace = True)
            address_pos = reshaped_df.columns.get_loc("Address")
            reshaped_df.insert(address_pos,"AddressType", "Both")
            print("Addresses Reshaped.")
            return reshaped_df
        else:
            print("Different Addresses.\nReshaping into smaller address columns differentiated by type...\n")  # noqa: E501
            #break out into Ship Df
            ship_df = input_df.copy()
            #Drop billing columns
            ship_df = ship_df.drop(list(billtup), axis=1)
                #Rename columns
            ship_df.rename(columns = {shiptup[0]:"Address", shiptup[1]:"City"
                                    , shiptup[2]:"State",shiptup[3]:"Zip"}
                                    , inplace= True)
                #Add AddressType column
            address_pos = ship_df.columns.get_loc("Address")
            ship_df.insert(address_pos,"AddressType", "Shipping")
            #Break out into Bill Df
            bill_df = input_df.copy()
            #Drop billing columns
            bill_df = bill_df.drop(list(shiptup), axis=1)
                #Rename columns
            bill_df.rename(columns = {billtup[0]:"Address", billtup[1]:"City"
                                    , billtup[2]:"State",billtup[3]:"Zip"}
                                    , inplace= True)
                #Add AddressType column
            address_pos = bill_df.columns.get_loc("Address")
            bill_df.insert(address_pos,"AddressType", "Billing")
            #Merge separate dfs together
            merged_df = pd.concat([ship_df, bill_df], axis=0)
            merged_df = merged_df.sort_values(["AccountName", "AddressType"]
                                              ,ascending=[True, True])
            del ship_df, bill_df
            #return merged df
            return merged_df     
    else:
        print("DataFrame does not have multiple addresses to comapre\n")
        return input_df

def match_columns(input_df) -> pd.DataFrame:
    input_df["Match0"] = input_df.NormalizedName.str[:10]
    if "NPI__c" in input_df:
        input_df["Match1"] = input_df.NormalizedName.str[:10] + "|" + input_df.NPI__c.astype(str)
    elif "NPI" in input_df:
        input_df["Match1"] = input_df.NormalizedName.str[:10] + "|" + input_df.NPI.astype(str)
        
    input_df["Match2"] = (input_df.Address)
    try:
        input_df["Match3"] = input_df.Address + "|" + input_df.State
    except TypeError:
        print(TypeError)
              
    input_df["Match4"] = (
        input_df.Address
        + "|"
        + input_df.City
        + "|"
        + input_df.State
    )
    return input_df

def match_criteria_scorer(input_df) -> None:
    new_name_matching(input_df)
    address_scorer(input_df)
    account_extras_scorer(input_df)
    # match_list_maker(input_df)
    final_score_eval(input_df)

def new_name_matching(input_df) -> None:
    name_matches = (input_df.NormalizedName_sf == input_df.NormalizedName_in)
    input_df["MatchList"] = ""
    if "MatchScore" not in input_df:
        input_df["MatchScore"] = 0
        
    input_df.loc[name_matches,"MatchScore"] += 600
    input_df.loc[name_matches,"MatchList"] += "NormalizedName, "

    # Only perform coefficient comparisons for non-name matches
    non_name_matches = ~name_matches
    coefficient_columns = ["NormCo40", "NormCo30", "NormCo25", "NormCo20",
                           "NormCo15", "NormCo10"]
    for col in coefficient_columns:
        match_col = f"{col}_sf"
        input_df.loc[non_name_matches & (input_df[match_col] == input_df[f"{col}_in"])
                     , "MatchScore"] += 10 
        input_df.loc[non_name_matches & (input_df[match_col] == input_df[f"{col}_in"])
                     , "MatchList"] += f"{col}, "

    # return input_df

def address_scorer(input_df) -> None: 
# Add a MatchScore and MatchList column if they don't exist
    if "MatchScore" not in input_df:
        input_df["MatchScore"] = 0
    if "MatchList" not in input_df:
        input_df["MatchList"] = ""
    
    exact_address = (
        (input_df.Address_sf == input_df.Address_in) &
        (input_df.City_sf == input_df.City_in) &
        (input_df.State_sf == input_df.State_in) &
        (input_df.Zip_sf == input_df.Zip_in)
    )
    input_df.loc[exact_address, "MatchScore"] += 1000
    input_df.loc[exact_address, "MatchList"] += "ExactShipAdd, "
    
    # Only perform coefficient comparisons for not exact address matches matches
    non_exact_address = ~exact_address
    addTypes = ((input_df["AddressType_sf"] == "Both")
                , (input_df["AddressType_sf"] == "Billing")
                , (input_df["AddressType_sf"] == "Shipping"))
    co_col = [ "City", "State", "Zip", "Address"]
    CSZ_match = (
        (input_df[f"{co_col[0]}_sf"] == input_df[f"{co_col[0]}_in"]) &
        (input_df[f"{co_col[1]}_sf"] == input_df[f"{co_col[1]}_in"]) &
        (input_df[f"{co_col[2]}_sf"] == input_df[f"{co_col[2]}_in"])
                )
    CS_match = (
        (input_df[f"{co_col[0]}_sf"] == input_df[f"{co_col[0]}_in"]) &
        (input_df[f"{co_col[1]}_sf"] == input_df[f"{co_col[1]}_in"])
                )
    ACS_match = (
        (input_df[f"{co_col[3]}_sf"] == input_df[f"{co_col[3]}_in"]) &
        (input_df[f"{co_col[0]}_sf"] == input_df[f"{co_col[0]}_in"]) &
        (input_df[f"{co_col[1]}_sf"] == input_df[f"{co_col[1]}_in"])
                )
    A_match = (
        (input_df[f"{co_col[3]}_sf"] == input_df[f"{co_col[3]}_in"]) 
                )

    #City State Zip Match
    input_df.loc[non_exact_address & CSZ_match & addTypes[0], "MatchScore"] += 10
    input_df.loc[non_exact_address & CSZ_match & addTypes[1], "MatchScore"] += 5
    input_df.loc[non_exact_address & CSZ_match & addTypes[2], "MatchScore"] += 5
    input_df.loc[non_exact_address & CSZ_match, "MatchList"] += "CityStateZip, "
    
    # City State Match
    input_df.loc[non_exact_address & CS_match & addTypes[0], "MatchScore"] += 8
    input_df.loc[non_exact_address & CS_match & addTypes[1], "MatchScore"] += 4
    input_df.loc[non_exact_address & CS_match & addTypes[2], "MatchScore"] += 4
    input_df.loc[non_exact_address & CS_match, "MatchList"] += "CityState, "

    # Address City State Match
    input_df.loc[non_exact_address & ACS_match & addTypes[0], "MatchScore"] += 6
    input_df.loc[non_exact_address & ACS_match & addTypes[1], "MatchScore"] += 4
    input_df.loc[non_exact_address & ACS_match & addTypes[2], "MatchScore"] += 4
    input_df.loc[non_exact_address & ACS_match, "MatchList"] += "AddressCityState, "

    
    # Address Only Match
    input_df.loc[non_exact_address & A_match & addTypes[0], "MatchScore"] += 3
    input_df.loc[non_exact_address & A_match & addTypes[1], "MatchScore"] += 1
    input_df.loc[non_exact_address & A_match & addTypes[2], "MatchScore"] += 1
    input_df.loc[non_exact_address & A_match, "MatchList"] += "Address, "


    # coeff_columns = [["ShippingCity", "ShippingState", "ShippingPostalCode"],
    #                  ["BillingCity", "BillingState", "BillingPostalCode"]]
    # for col in coeff_columns:
    #     CSZ_match = (
    #         (input_df[f"{col[0]}_sf"] == input_df[f"{col[0]}_in"]) &
    #         (input_df[f"{col[1]}_sf"] == input_df[f"{col[1]}_in"]) &
    #         (input_df[f"{col[2]}_sf"] == input_df[f"{col[2]}_in"])
    #     )
    #     CS_match = (
    #         (input_df[f"{col[0]}_sf"] == input_df[f"{col[0]}_in"]) &
    #         (input_df[f"{col[1]}_sf"] == input_df[f"{col[1]}_in"])
    #     )
    #     input_df.loc[non_exact_address & CSZ_match, "MatchScore"] += 10
    #     input_df.loc[non_exact_address & CSZ_match, "MatchList"] += "CityStateZip, "
        
    #             # City State Match
    #     input_df.loc[non_exact_address & CS_match, "MatchScore"] += 10
    #     input_df.loc[non_exact_address & CS_match, "MatchList"] += "CityState, "
    # Remove trailing comma and space from MatchList
    input_df["MatchList"] = input_df["MatchList"].str.rstrip(", ")

def account_extras_scorer(input_df) -> None:
    if "MatchScore" not in input_df:
        input_df["MatchScore"] = 0
    # if "MatchScore" in input_df: 
    #     input_df["MatchScore"] += (input_df.Phone_sf == input_df.Phone_in).astype(
    #         int) * 10
    # else:
    #     input_df["MatchScore"] = (input_df.Phone_sf == input_df.Phone_in).astype(
    #         int) * 10   
    if "DHCID_in" in input_df:
        match = input_df.DHCID_in == input_df.DHCID_sf
        input_df.loc[match,"MatchScore"] += 10
        input_df.loc[match, "MatchList"] += "DHCID, "
    if "DHCNetworkID_in" in input_df:
        match = input_df.DHCNetworkID_in == input_df.DHCNetworkID_sf
        input_df.loc[match,"MatchScore"] += 10
        input_df.loc[match, "MatchList"] += "DHCNetworkID, "
    if "Ext_in" in input_df:
        match = input_df.Ext == input_df.Ext__c
        input_df.loc[match,"MatchScore"] += 10
        input_df.loc[match,"MatchList"] += "Ext, "
        # input_df["MatchScore"] += (input_df.Ext == input_df.Ext__c).astype(int) * 10
    if "NPI_sf" in input_df:
        match = input_df.NPI_sf == input_df.NPI_in
        input_df.loc[match,"MatchScore"] += 10
        input_df.loc[match,"MatchList"] += "NPI, "
        # input_df["MatchScore"] += (input_df.NPI_sf == input_df.NPI_in).astype(int) * 10  # noqa: E501
    if "FuzzyNameMatch" in input_df:
        match = input_df.FuzzyNameMatch >= 80
        input_df.loc[match,"MatchScore"] += 8
        input_df.loc[match,"MatchList"] += "FuzzyName, "
        # input_df["MatchScore"] += (input_df.FuzzyNameMatch >= 80).astype(int) * 8
    if "FuzzyAddressMatch" in input_df:
        match = input_df.FuzzyAddressMatch >= 80
        input_df.loc[match,"MatchScore"] += 8
        input_df.loc[match,"MatchList"] += "FuzzyAddress, "
        # input_df["MatchScore"] += (input_df.FuzzyAddressMatch >= 80).astype(int) * 8
    if "Taxonomy" in input_df:
        match = input_df.Taxonomy == input_df.Taxonomy_Primary
        input_df.loc[match,"MatchScore"] += 5
        input_df.loc[match,"MatchList"] += "Taxonomy, "
        # input_df["MatchScore"] += (input_df.Taxonomy == 
        #     input_df.Taxonomy_Primary).astype(int) * 5
    if "TaxID_in" in input_df:
        match = input_df.TaxID_in == input_df.TaxID_sf
        input_df.loc[match,"MatchScore"] += 5
        input_df.loc[match,"MatchList"] += "TaxID, "
        # input_df["MatchScore"] += (input_df.TaxID_in ==
        #     input_df.TaxID_sf).astype(int) * 5
        
def final_score_eval(input_df) -> None:
    if "LOB_sf" in input_df:
        input_df["LOBMatch"] = (input_df.LOB_sf == input_df.LOB_in).astype(int) + 1
    else:
        input_df["LOBMatch"] = 1
    input_df["FullNameMatch"] = (input_df.NormalizedName_sf == 
                                 input_df.NormalizedName_in).astype(int) + 1
    input_df["MatchScore"] = (input_df.MatchScore * input_df.LOBMatch).astype(int)
    input_df["MatchScore"] = (input_df.MatchScore * input_df.FullNameMatch).astype(int)
    # removes comma at the beginning of Match List
    input_df["NameLevelMatch"] = input_df.MatchList.apply(
        lambda name: name[:7].replace(",",""))

def match_list_maker(input_df) -> None:
    if "FuzzyNameMatch" in input_df:
        input_df["FuzzyName"] = (input_df.FuzzyNameMatch >= 80).astype(
        str).replace("True",",fuzzyname").replace("False","")
    if "FuzzyAddressMatch" in input_df:
        input_df["FuzzyAddress"] = (input_df.FuzzyAddressMatch >= 80).astype(
    str).replace("True",",fuzzyaddress").replace("False","") 
    input_df["Phone"] = (input_df.Phone_sf == input_df.Phone_in).astype(str).replace(
                                        "True",",Phone").replace("False","")
    if "Ext" in input_df:
        input_df["Ext"] = (input_df.Ext__c == input_df.Ext).astype(str).replace(
                                            "True",",Ext").replace("False","")
    if "NPI_in" in input_df:
        input_df["NPI"] = (input_df.NPI_in == input_df.NPI_sf).astype(str).replace(
                                            "True",",NPI").replace("False","")
    if "LOB_in" in input_df:
        input_df["PLOB"] = (input_df.LOB_sf == input_df.LOB_in).astype(str).replace(
                                               "True",",PLOB").replace("False","")
    if "Taxonomy" in input_df:
        input_df["Taxonomy"] = (input_df.Taxonomy == input_df.Taxonomy_Primary
                                ).astype(str).replace("True",",Taxonomy").replace("False","")
    if "TaxID_in" in input_df:
        input_df["TaxID"] = (input_df.TaxID_in == input_df.TaxID_sf).astype(
            str).replace("True",",TaxID").replace("False","")
    if "FuzzyNameMatch" in input_df:
        input_df["MatchList"] += input_df["FuzzyName"]
    if "FuzzyAddressMatch" in input_df:
        input_df["MatchList"] += input_df["FuzzyAddress"]
    if "Ext" in input_df:
        input_df["MatchList"] += input_df["Ext"]
    if "NPI" in input_df:
        input_df["MatchList"] += input_df["NPI"]
    if "PLOB" in input_df:
        input_df["MatchList"] += input_df["PLOB"]
    if "Taxonomy" in input_df:
        input_df["MatchList"] += input_df["Taxonomy"]
    if "TaxID" in input_df:
        input_df["MatchList"] += input_df["TaxID"]
    input_df.MatchList.apply(lambda matchtype:matchtype[1:] if len(matchtype) > 0
                              else matchtype)

def UP_Match(input_df) -> pd.DataFrame:

    ultimate_parent_candidates = input_df['UltimateParentAccountNumber'].unique()
    input_df['IsUltimateParentMatch'] = input_df['AccountNumber'].isin(
        ultimate_parent_candidates)
    return input_df

def tie_breaker(input_df) -> pd.DataFrame:
    timeit.default_timer()
    start = timeit.default_timer()
    # Group by Input Account Name
    grouped = input_df.groupby('input_key')
    # UP Matcher
    results = grouped.apply(UP_Match)
    stop = timeit.default_timer()
    print("Ultimate Parent Matches made: {:.2f} seconds".format(stop - start))

    ungrouped = results.droplevel(0, axis= 0)
    date_prep = pd.merge(input_df,ungrouped["IsUltimateParentMatch"], left_index=True
                         , right_index=True)
    # Max Date by Input Account Name Group Match
    date_prep["MaxDate"] = date_prep.groupby("input_key"
                                             ).LastModifiedDate.transform('max')
    date_prep["DateCheck"] = date_prep["LastModifiedDate"] == date_prep["MaxDate"]
    
    status_values = [date_prep["AccountStatusScore"].eq(1), date_prep[
                        "AccountStatusScore"].eq(2)
                     ,date_prep["AccountStatusScore"].eq(3), date_prep[
                        "AccountStatusScore"].eq(4)
                     ,date_prep["AccountStatusScore"].eq(5)]
    status_scores = [45, 5, 3, 0, 0]
    
    date_prep["StatusScoring"] = np.select(status_values, status_scores, default = 0)
   
    date_prep.IsUltimateParentMatch = date_prep.IsUltimateParentMatch.replace(True,"25"
                                                                             ).replace(False,"0")
    date_prep.DateCheck = date_prep.DateCheck.replace(True,"25").replace(False,"0")
    
    date_prep["MatchScore"] += date_prep["StatusScoring"]
    date_prep["MatchScore"] += date_prep["IsUltimateParentMatch"].astype(int)
    date_prep["MatchScore"] += date_prep["DateCheck"].astype(int)
    
    
    # Dropping the account_match_group_evaluation columns allows me to re-run data 
    # through that process without hitting errors for existing columns
    output = date_prep.drop(columns=["MaxDate","Count","Rank","eval_result"
                                     ,"Review_Groups","Merge_Groups","BestMatch"])
    del grouped, results, ungrouped, date_prep
    stop = timeit.default_timer()
    print("Large Count Ties Broken: {:.2f} seconds".format(stop - start))
    return output

def account_match_group_evaluation(input_df) -> pd.DataFrame:
    timeit.default_timer()
    start = timeit.default_timer()
    # Creates column of count by Account Name to show # of matches
    series_MatchCount = input_df.groupby("input_key")["input_key"].count()
    df_matchCount = pd.DataFrame(data= series_MatchCount, columns=["Count",
                                                                   "input_key"])
    df_matchCount["Count"] = df_matchCount["input_key"]
    match_ready_df = pd.merge(input_df, df_matchCount["Count"],
                        how="right", left_on="input_key",
                        right_on= df_matchCount.index)

    # Rank the matches score against the other scores in AccountName group.
    # Average prevents matches from having multiple 1st picks
    match_ready_df["Rank"] = match_ready_df.groupby("input_key")[
        "MatchScore"].rank(ascending=False,method="average").to_frame() # type: ignore
    match_ready_df["eval_result"] = match_ready_df.groupby(
        "input_key")["MatchScore"].transform(lambda x: x.sum()
                                                  / match_ready_df["Count"])
    # Used to determine if there is no best match in an account name group.
    # Must be reviewed manually.
    match_ready_df["Review_Groups"] = match_ready_df["input_key"].map(
        match_ready_df.groupby("input_key").apply(lambda x: x["Rank"].ne(1).all(  
        ))) 
        # Returns True if all rows in a group that does not have a Rank of 1
    # Used to determine if there is a best match,
    # then all the other matches in an account name group will be set TRUE.
    match_ready_df["Merge_Groups"] = match_ready_df["input_key"].map(
        match_ready_df.groupby("input_key").apply(lambda x: x["Rank"].eq(1).any(
        ))) 
        # Returns True if any row in a group has a Rank of 1

    master_conditions = [
        np.isnan(match_ready_df["MatchScore"]),
        (match_ready_df["Rank"].eq(1) & match_ready_df["eval_result"].ne(match_ready_df[
                "MatchScore"])) | (match_ready_df["Rank"].eq(1) 
                                & match_ready_df["Count"].eq(1)),
        match_ready_df["Rank"].ne(1) & match_ready_df["Merge_Groups"].eq(True),
        match_ready_df["eval_result"].eq(match_ready_df["MatchScore"]) 
                | (match_ready_df["Rank"] % 1).ne(0) 
                | match_ready_df["Review_Groups"].eq(True)
        ]

    master_choices = ["No Match Found","Best Account Record","Not Best Account Match",
                      "Review Account Group"]
    
    # Uses groupby columns to identify best match for each account
    # All non-best matches will be marked accordingly,
    # so there is one best match per input record
    # If Python determines there is no best match, they will be marked as review.
    match_ready_df["BestMatch"] = np.select(master_conditions,master_choices,
                                      default="Merge")
    del series_MatchCount, df_matchCount
    stop = timeit.default_timer()
    print("Match Choices Processed: {:.2f} seconds".format(stop - start))

    return match_ready_df
    
def accountmatcher(name, fuzzy = False, all_matches = False) -> None:
    filename = str(name)  
    input_path = iop.input_path
    output_path = iop.output_path
    # Grab account data from Salesforce
    time1_start = timeit.default_timer()
    start = time1_start
    sf_account_df = sqe.sql_extract(query = sfq.view_query, server = "SERVERNAME",
                                db = "Analytics", conn_type = "")
    stop = timeit.default_timer()
    print("Query success. Time: {:.2f} seconds".format(stop - start))

    # Start cleaning data in table
    start = timeit.default_timer()
    sf_account_df = reshape_addresses(sf_account_df)
    sf_account_df = nrm.account_table_clean_updated(sf_account_df, source="sf")
    stop = timeit.default_timer()
    print("Table Cleaned. Time: {:.2f} seconds".format(stop - start))

    # Normalize Column Data
    start = timeit.default_timer()
    nrm.regex_account_name(sf_account_df, "AccountName")
    nrm.normalize_street(sf_account_df, "Address")
    nrm.normalize_suite(sf_account_df, "Address")
    nrm.normalize_zipcode(sf_account_df, "Zip")

    stop = timeit.default_timer()
    print("Normalization success. Time: {:.2f} seconds".format(stop - start))    

    # Create Base Match columns
    start = timeit.default_timer()
    match_columns(sf_account_df)
    stop = timeit.default_timer()
    print("Base Match Columns created. Time: {:.2f} seconds".format(stop - start))
    time1_stop = timeit.default_timer()
    print("SF Data Processing time: {:.2f} seconds".format(time1_stop - time1_start))

    # Grab and clean input file data
    input_start = timeit.default_timer()
    input_path = path_manage(input_path, filename, output=False) #type: ignore
    input_account_df = pd.read_excel(input_path, engine="openpyxl")
    start = timeit.default_timer()
    
    # Start cleaning data in table
    input_account_df = reshape_addresses(input_account_df)
    input_account_df = nrm.account_table_clean_updated(input_account_df, source="input")
    stop = timeit.default_timer()
    print("Input Table Cleaned. Time: {:.2f} seconds".format(stop - start))

    # Normalize Column Data
    start = timeit.default_timer()
    nrm.regex_account_name(input_account_df, "AccountName")
    try:
        nrm.normalize_street(input_account_df, "Address")
        nrm.normalize_suite(input_account_df, "Address")
        nrm.normalize_zipcode(input_account_df, "Zip")

        # nrm.normalize_street(input_account_df, "ShippingStreet")
        # nrm.normalize_suite(input_account_df, "ShippingStreet")
        # nrm.normalize_zipcode(input_account_df, "ShippingPostalCode")
        # nrm.normalize_street(input_account_df, "BillingStreet")
        # nrm.normalize_suite(input_account_df, "BillingStreet")
        # nrm.normalize_zipcode(input_account_df, "BillingPostalCode")
    except AttributeError:
        AttributeError("ERROR")
        
    stop = timeit.default_timer()
    print("Input Normalization success. Time: {:.2f} seconds".format(stop - start))    

    # Create Base Match columns
    start = timeit.default_timer()
    match_columns(input_account_df)
    stop = timeit.default_timer()
    print("Base Match Columns created. Time: {:.2f} seconds".format(stop - start))
    input_stop = timeit.default_timer()
    print("Input Processing time: {:.2f} seconds".format(input_stop - input_start))
    # Creates new dataframes from match columns
    match_score_start = timeit.default_timer()
    start = timeit.default_timer()
    # These are 'base matches' that we can use to create a table
    # of potential matches to our input file
    M0 = pd.merge(input_account_df, sf_account_df, suffixes=("_in","_sf"),
                  left_on="Match0", right_on="Match0")
    M1 = pd.merge(input_account_df, sf_account_df, suffixes=("_in","_sf"),
                  left_on="Match1", right_on="Match1")
    M2 = pd.merge(input_account_df, sf_account_df, suffixes=("_in","_sf"),
                  left_on="Match2", right_on="Match2")
    M3 = pd.merge(input_account_df, sf_account_df, suffixes=("_in","_sf"),
                  left_on="Match3", right_on="Match3")
    M4 = pd.merge(input_account_df, sf_account_df, suffixes=("_in","_sf"),
                  left_on="Match4", right_on="Match4")

    # Delete sf_accounts_df. All the data we need from it was pulled into dfs M0 - M7
    del sf_account_df
    basematches_df = pd.concat([M0,M1,M2,M3,M4])

    # Delete merged dfs since they won't be used again
    del M0, M1, M2, M3, M4
    
    # Drop any duplicates from base match merges
    basematches_df = basematches_df.drop_duplicates(subset=["input_key","AccountID"])
    stop = timeit.default_timer()
    print("Base matches and table created. Time: {:.2f} seconds".format(stop - start))

    if fuzzy is True:
        # Start Fuzzy Name Matching
        start = timeit.default_timer()
        basematches_df["FuzzyNameMatch"] = basematches_df.apply(
            lambda x:rapidfuzz.fuzz.token_sort_ratio(
            x.AccountName_in, x.AccountName_sf), axis=1).to_list()
        stop = timeit.default_timer()
        print("Fuzzy Name Matching Completed. Time: {:.2f} seconds".format(
            stop - start))
        
        # Start Fuzzy Address Matching
        start = timeit.default_timer()
        basematches_df["FuzzyAddressMatch"] = basematches_df.apply(
            lambda x:rapidfuzz.fuzz.token_sort_ratio(
            x.Address_in, x.Address_sf), axis=1).to_list()
        stop = timeit.default_timer()
        print("Fuzzy Address Matching Completed. Time: {:.2f} seconds".format(
            stop - start))
    # print("\n\n***Review Table Columns")     
    # print(basematches_df.columns,"\n***\n")
    # creating a df copy and then applying scoring reduces performance warnings
    start = timeit.default_timer()
    scored_df = basematches_df.copy()
    # Score all the account matches
    match_criteria_scorer(scored_df)
    # delete df not being used anymore
    del basematches_df
    stop = timeit.default_timer()
    print("Match Scoring Completed. Time: {:.2f} seconds".format(stop - start))
    match_score_stop = timeit.default_timer()
    print("Match and Scoring Processing Time: {:.2f} seconds".format(
        match_score_stop - match_score_start))

    # Evaluate groups
    start = timeit.default_timer()
    
    matched_df = account_match_group_evaluation(scored_df)    
        
    # Handle tie-breaking
    review_group = matched_df[matched_df["BestMatch"] == "Review Account Group"]

    review_group.groupby("input_key")["AccountName_in"].count()
    matches_for_tieBreaking = review_group[review_group.groupby('input_key')[
                                        'AccountName_in'].transform('count') > 5]

    if matches_for_tieBreaking.empty:
        print("No data")
        df_matched = matched_df.copy()
        del matches_for_tieBreaking, review_group, scored_df
    else:
        # print("Data")
        # Create auto-picked group table
        finalized_group = matched_df[matched_df["BestMatch"] != "Review Account Group"]
        # Grab need reviews with 5 or less rows in group
        passable_reviews = review_group[review_group.groupby('input_key')[
            'AccountName_in'].transform('count') <= 5]
        # Merge passables together that don't need tie breaker
        end_group = pd.concat([finalized_group, passable_reviews], axis = 0)
        del matched_df
        # Re-evaluate large groups of Review Needed
        ties_broken = tie_breaker(matches_for_tieBreaking)
        re_evaluated = account_match_group_evaluation(ties_broken)
        print("Trimming tie broken records to top 3 per group")
        re_evaluated.sort_values(["input_key","Rank"], ascending = [True, True]
                                 , inplace = True)
        tiebroken_sorted = re_evaluated.groupby("input_key").head(3)
        # tiebroken_sorted = tiebroken_sorted.droplevel(0, axis= 0)
        # Merge tie-broken data with clean passed data
        df_matched = pd.concat([tiebroken_sorted, end_group], axis= 0)
        del finalized_group, passable_reviews, matches_for_tieBreaking, ties_broken, re_evaluated  # noqa: E501
        
    stop = timeit.default_timer()
    print("Best Match picked. Time: {:.2f} seconds".format(stop - start))
    
    # Cleanup and Output
    timeit.default_timer()
    start = timeit.default_timer()
    df_clean = df_matched.copy()

    # Sort data, drop any duplicates, and drop match columns not needed in output file
    df_clean.sort_values(["MatchScore", "AccountStatusScore", "ParentScore"],
                         ascending=[False, True, True])
    df_clean.drop_duplicates(subset=["input_key"])
    # if "NPI__c" in df_clean:
    #     df_final = df_clean[["input_key","AccountID","AccountName_sf","AccountNumber",
    #                         "AccountStatus","Phone_sf","NPI__c",
    #                         "Taxonomy_Primary__c","Tax_ID__c","AddressType_sf","Address_sf","City_sf",
    #                         "State_sf","Zip_sf","LOB_sf","UltimateParentAccountNumber",
    #                         "AccountStatusScore","ParentScore","MatchScore","MatchList","Count","Rank","Review_Groups","Merge_Groups","BestMatch"]]
    # else:
    #     if "TaxID_sf" not in df_clean:
    #         df_final = df_clean[["input_key","AccountID","AccountName_sf",
    #                     "AccountNumber","AccountStatus","Phone_sf","NPI_sf",
    #                     "Taxonomy_Primary_sf","AddressType_sf","Address_sf","City_sf",
    #                     "State_sf","Zip_sf","LOB_sf","UltimateParentAccountNumber",
    #                     "AccountStatusScore","ParentScore","MatchScore","MatchList","Count","Rank","Review_Groups","Merge_Groups","BestMatch"]]
    #     elif "Taxonomy_Primary_sf" not in df_clean:
    #         df_final = df_clean[["input_key","AccountID","AccountName_sf",
    #         "AccountNumber","AccountStatus","Phone_sf","NPI_sf",
    #         "Taxonomy","TaxID_sf","AddressType_sf","Address_sf","City_sf",
    #         "State_sf","Zip_sf","LOB_sf","UltimateParentAccountNumber",
    #         "AccountStatusScore","ParentScore","MatchScore","MatchList","Count","Rank","Review_Groups","Merge_Groups","BestMatch"]]

       
    #     else:
    #         df_final = df_clean[["input_key","AccountID","AccountName_sf",
    #         "AccountNumber","AccountStatus","Phone_sf","NPI_sf",
    #         "Taxonomy_Primary_sf","TaxID_sf","AddressType_sf","Address_sf","City_sf",
    #         "State_sf","Zip_sf","LOB_sf","UltimateParentAccountNumber",
    #         "AccountStatusScore","ParentScore","MatchScore","MatchList","Count","Rank","Review_Groups","Merge_Groups","BestMatch"]]

    df_final = df_clean[["input_key","AccountID","AccountName_sf",
            "AccountNumber","AccountStatus","Phone_sf","NPI_sf",
            "Taxonomy_Primary","TaxID_sf","AddressType_sf","Address_sf","City_sf",
            "State_sf","Zip_sf","LOB_sf","UltimateParentAccountNumber",
            "AccountStatusScore","ParentScore","MatchScore","MatchList","Count","Rank","Review_Groups","Merge_Groups","BestMatch"]]


        
    df_final = pd.merge(input_account_df, df_final, suffixes=("_in","_sf"), how="left",
                        left_on="input_key", right_on="input_key")
    df_final.sort_values(["MatchScore","AccountName","AccountStatusScore","ParentScore"],
                         ascending=[False, False, True, True], inplace=True)
    df_final["BestMatch"] = df_final["BestMatch"].fillna("No Match Found")
    stop = timeit.default_timer()
    print("Output Table Created and Cleaned. Time: {:.2f} seconds".format(stop - start))

    # Drops match columns from input dataframe. Not needed in output.
    df_final.drop(columns=['NormalizedName', 'NormCo40',
        'NormCo30', 'NormCo25', 'NormCo20', 'NormCo15', 'NormCo10',
        'NormAddress','NormZip',
        'Match0', 'Match1', 'Match2', 'Match3', 'Match4'], inplace= True)
    df_final.drop_duplicates(keep="first")
    if all_matches is True:
        df_export = df_final.copy()
    else:
        fil_out = ((df_final["MatchScore"] >= 50) & (df_final["BestMatch"] 
                                                     == "Review Account Group"))
        others = df_final["BestMatch"] != "Not Best Account Match"
        df_export = pd.concat([df_final[fil_out],df_final[others]])
        df_export = df_export.reset_index().drop_duplicates(subset='index'
                                                            ,keep='first').set_index('index')
  
    del df_final
    
    output_path = path_manage(output_path, filename, output=True)
    # Saves the file.
    df_export.to_csv(output_path, index=False)
    #end_group.to_csv(r"C:\SVNNew\Analytics-SVN\Python\AccountAudit\Output\endgroup.csv"
    # , index = False)
    stop = timeit.default_timer()
    print("Processed File Exported. Time: {:.2f} seconds".format(stop - start))
    print("Account Matching Completed. Total Time: {:.2f} seconds".format(
        stop - time1_start))