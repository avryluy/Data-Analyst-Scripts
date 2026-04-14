# import sys
import argparse
import AccountMatcher as am

arg_description = """
Inovalon Account Matcher

Finds best matches for accounts existing in Provider Salesforce base on input account data 
"""  # noqa: E501


parser = argparse.ArgumentParser(prog="Account Matcher",
                                 formatter_class= argparse.RawTextHelpFormatter,
                                 description = arg_description)

parser.add_argument("-n", "--filename",
                    help = """Name of input file. Does NOT require file extension""")
parser.add_argument('-f','--fuzzy', action= "store_true", 
                    help="""True/False for running the fuzzy match processors""")
parser.add_argument("-a", "--all_matches", action= "store_true",
                    help= """If TRUE, results file will include every possible match reviewed by Python""")  # noqa: E501
args = parser.parse_args()

filename = args.filename
fuzzy = bool(args.fuzzy)
all_matches = bool(args.all_matches)

if args.fuzzy:

    print("\nFuzzy Matching Enabled")
else:

    print("\nFuzzy Matching Disabled")

if args.all_matches:
    print("\nExtensive results file requested")
else:
    print("\nConcise results file requested")
    
if __name__ == "__main__":
    print("\n")
    print("Running Account Matcher on: {}".format(str(filename)))
    am.accountmatcher(filename, fuzzy, all_matches)


