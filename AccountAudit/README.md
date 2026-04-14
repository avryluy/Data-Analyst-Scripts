# Inovalon Account Matching Tool README

This tool was written to assist the identification of existing customer within our Salesforce instance. Data Governance asssists other departments in automating data to expedite large customer onboarding efforts. The purpose of this tool is to take an input spreadsheet of customer to find. Then the tool digests the table, standardizes it, and identifies the best possible matches on account existing in our Salesforce instance. The tool will return a CSV of best possible matches, as well as input accounts that do not seem to exist as Salesforce records.

## Required Libraries
- Pandas <= 2.0.2
- numpy <= 1.24.3
- SQL Alchemy <= 2.0.15
- rapidfuzz <= 3.1.1

## Instructions
1. Make sure the package dependencies are installed and accessible via environment variables.
2. Create an INPUT and OUTPUT folder in the tool's head directory.
3. Review SF_Query.py with the query you need to query your database.
4. Place an Excel spreadsheet you want to identify account matches for into the INPUT folder.
5. Open up Powershell in the head directory. Execute "python main.py -n [FILENAME]"
    1. Add the -f flag if you'd like to process Account Name and Address fuzzy matches.
    2. Add the -a flag if you'd like to receive all matches, instead of the best 3 matches.
6. Wait until the tool finishes running, then open up the file created in the OUTPUT folder to view the results.

## Input Table Template
|AccountName|BillingStreet|BillingCity|BillingState|BillingPostalCode|ShippingStreet|ShippingCity|ShippingState|ShippingPostalCode|Phone|Taxonomy_Primary|  LOB   | NPI |
|-----------|-------------|-----------|------------|-----------------|--------------|------------|-------------|------------------|-----|----------------|--------| --- |
|  *string* |   *string*  | *string*  |  *string*  |      *int*      |   *string*   |  *string*  |   *string*  |      *int*       |*int*|    *string*    |*string*|*int*|
