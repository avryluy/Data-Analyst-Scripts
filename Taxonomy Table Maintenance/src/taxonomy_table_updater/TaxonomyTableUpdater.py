from datetime import date
from typing import Tuple
import os
import re
# from pathlib import Path
import pandas as pd
from urllib import request
from urllib.error import URLError, HTTPError
from email.utils import parsedate_to_datetime
import glob as glob
# import sys
# sys.path.append("./utils")
import utils.INO_SQLEngine as sq
import utils.INO_SQL_Creds as ss

# Define Variables
nucc = "https://nucc.org/images/stories/CSV/nucc_taxonomy_"
nucc_index = "https://nucc.org/index.php/code-sets-mainmenu-41/provider-taxonomy-mainmenu-40/csv-mainmenu-57"
log_path = os.path.join(os.path.join(os.environ['USERPROFILE']), 'Desktop') 
sql_last_ver = 0.0
sql_last_update = date(1900, 1, 1)
file_name = "taxonomy_codes_v"
file_ext = ".csv"
query = ss.query_1 + f'{ss.db_pm}.dbo.{ss.tb_tax}'
sql_dtypes = {"Code": sq.CHAR(10), "Grouping": sq.VARCHAR(100), "Classification": sq.VARCHAR(150),
              "Specialization": sq.VARCHAR(100), "DisplayName": sq.VARCHAR(150),
              "NUCCVersionLastUpdated": sq.DECIMAL(3,1), "NUCCVersionDateLastUpdated": sq.DATE}

today = date.today()
cur_q1 = date(today.year, 1, 1)
cur_q3 = date(today.year,7, 1)

engine_an = sq.Engine
engine_pm = sq.Engine

# Define Functions
def init_sql_engine(mServer: str, mDatabase: str, mConType: str = ""):
    connection = sq.active_credential_sign_in(mServer, mDatabase, mConType)
    if connection is None:
        print("Error initializing SQL Engine\n")
        return None
    print("Connection to {} - {} completed!\n".format(mServer, mDatabase))
    return connection
   
def base_version(mDate: date, current_sql_ver: float | None = None) -> float:
    """Return the target NUCC version to check.

    If `current_sql_ver` is provided, compute the next incremental version:
      - if current is X.0 -> return X.1
      - if current is X.1 -> return (X+1).0
    If `current_sql_ver` is None, fall back to date-based logic (existing behavior).
    """
    if current_sql_ver is not None:
        try:
            major = int(current_sql_ver)
            minor = int(round((current_sql_ver - major) * 10))
        except Exception:
            # Fallback to date-based if parsing fails
            current_sql_ver = None

    if current_sql_ver is not None:
        if minor == 0:
            return float(f"{major}.{1}")
        else:
            return float(f"{major+1}.{0}")

    # Fallback: original behavior based on date
    base = str(mDate.year)
    base = float(base[-2:])

    dec = 0.0
    if mDate.month in (1, 2, 3, 4, 5, 6):
        dec = dec
    else:
        dec = 0.1

    ver = base + dec
    return ver

def date_quarter(mDate: date) -> int:
    if mDate.month in (1, 2, 3):
        qtr = 1
    elif mDate.month in (4, 5, 6):
        qtr = 2
    elif mDate.month in (7, 8, 9):
        qtr = 3
    elif mDate.month in (10, 11, 12):
        qtr = 4
    else:
        qtr= -1
    return qtr

def download_update(mVer: float, mfileName: str, mfileExt: str, mQtr: date) -> pd.DataFrame:
    """Download the CSV for the given version or from a specified URL.

    If `mVer` is a float it will be used to construct the filename as before.
    If `mVer` is None and `file_url` is provided (passed via kwargs), the function will use that URL.
    """
    # allow caller to pass explicit file_url via kwargs
    file_url = None
    if isinstance(mVer, dict):
        # legacy: caller passed a dict as first arg accidentally — disallow
        raise ValueError("mVer must be a float or None; do not pass dict")
    # build path and url
    strVer =  str(mVer)
    strVer = strVer.replace(".","")
    mfilePath = r"\SVNNew\Analytics-SVN\Python\Taxonomy Table Maintenance\cache" + "\\" + mfileName + strVer + "_" + str(mQtr).replace("-","") + mfileExt
    print("mFilePath: " + mfilePath)
    
    if len(glob.glob(mfilePath)) == 0:
        # download file if not in cache
        print("Downloading file from NUCCC...")
        url = nucc + strVer + ".csv"
        try:
            fetchCodes = request.urlretrieve(url = url)
        except (HTTPError, URLError) as e:
            print("Failed to retrieve CSV from %s", url)
            raise
        # df = pd.DataFrame(fetchCodes)
        # df.to_csv(mfilePath)
        print("File Downloaded: {}".format(fetchCodes[0]))
        newData = pd.read_csv(fetchCodes[0])        
    else:
        # read cached file
        print("File already downloaded. Reading cached file...")
        # print(url)
        newData = pd.read_csv(mfilePath)   
        print("Dataframe created.")
    
    return newData


def get_latest_version_from_site() -> tuple[float, str]:
    """Scrape the NUCC CSV index page and return (version_float, file_url).

    The page lists CSVs; we search for the first occurrence of a link to a
    file named like 'nucc_taxonomy_240.csv'. Returns (version, absolute_url).
    """
    try:
        resp = request.urlopen(nucc_index)
        html = resp.read().decode('utf-8', errors='ignore')
    except Exception as e:
        print("Unable to fetch NUCC index page: %s", nucc_index)
        raise

    # find first occurrence of nucc_taxonomy_XXX.csv
    m = re.search(r'href\s*=\s*"([^"]*nucc_taxonomy_(\d+)\.csv)"', html, re.IGNORECASE)
    if not m:
        # fallback: search for any nucc_taxonomy_\d+\.csv without href quotes
        m2 = re.search(r'(nucc_taxonomy_(\d+)\.csv)', html, re.IGNORECASE)
        if not m2:
            print("Could not find a nucc_taxonomy csv link on the index page")
            raise RuntimeError("CSV link not found on NUCC index page")
        filename = m2.group(1)
        digits = m2.group(2)
        file_url = 'https://nucc.org/images/stories/CSV/' + filename
    else:
        file_url = m.group(1)
        digits = m.group(2)
        if not file_url.lower().startswith('http'):
            # make absolute
            file_url = 'https://nucc.org' + file_url

    # convert digits like 240 -> 24.0, 241 -> 24.1
    try:
        if len(digits) >= 2:
            major = int(digits[:-1])
            minor = int(digits[-1])
            version = float(f"{major}.{minor}")
        else:
            version = float(digits)
    except Exception:
        print("Failed to parse version digits from %s", digits)
        raise

    print(f"Latest site version: {version} at {file_url}")
    return version, file_url


def get_remote_last_modified(file_url: str) -> date | None:
    """Return the Last-Modified date for a remote file URL, or None if unavailable."""
    try:
        req = request.Request(file_url, method='HEAD')
        with request.urlopen(req) as r:
            lm = r.headers.get('Last-Modified')
            if lm:
                dt = parsedate_to_datetime(lm)
                return dt.date()
    except Exception:
        print("Could not obtain Last-Modified for %s", file_url)
    return None

def prep_dataFrame(mFrame: pd.DataFrame, mVer: float, mDate: date) -> pd.DataFrame:
    idx = mFrame.columns[[4,5,7]]
    outFrame = mFrame.drop(idx, axis=1)
    outFrame["NUCCVersion"] = mVer
    outFrame["NUCCVersionDate"] = date(mDate.year, mDate.month, mDate.day)
    print("DataFrame ready for SQL loading.")
    return outFrame

def get_rowcount(engine, table_name) -> int:
    query = sq.text(ss.query_2 + f"{table_name}")
    result = engine.execute(query)
    count = result.scalar()
    # engine.close()
    return count

def truncateSQL(mEngine) -> int:
    conn = mEngine
    
    try:
        truncate = sq.text(ss.query_3 + sq.creds.db_analytics + ".dbo." + sq.creds.tb_taxs)
        conn.execute(truncate)
    except Exception as e:
        print(e)
        conn.rollback()
        return -1
    conn.commit()
    print("Staging trable truncated.")
    # conn.close()
    return 0

def loadSQL(mEngine, mFrame: pd.DataFrame, mTypes: dict) -> int:
    # MAKE SURE NUCC VERSION IS A FLOAT
    conn = mEngine
    
    try:
        mFrame.to_sql(name = sq.creds.tb_taxs, con = conn, schema="dbo",index=False, if_exists="replace", dtype= mTypes)
        result = get_rowcount(conn, sq.creds.tb_taxs)
    except Exception as e:
        print(e)
        conn.rollback()
        return -1
    else:
        if result == len(mFrame):
            conn.commit()
            print("SQL table load completed successfully")
            print(f"Rows to load: {len(mFrame)} | Rows Loaded: {result}")
        else:
            print("SQL load did not match the length of the table you attempted to load. Check your table and try again")
            conn.rollback()
            return -1
        # conn.close()
        return 0

def exec_sp(mEngine, proc: str) -> None:
    conn = mEngine
    print("Proc: {} | Type: {}".format(proc, type(proc)))
    
    crsr = conn.cursor()
    try:
        crsr.execute(proc)
        for msg in crsr.messages:
            m = re.match(r"\[Microsoft].*\[SQL Server](.*)", msg[1])
            
    except Exception as e:
        print("Exception raised when executing stored procedure: \n", e)
        crsr.rollback()
        conn.close()
           
    if "rows affected in Taxonomy table" in str(m):
        crsr.commit()
        print("PM Taxonomy Table loaded correctly.")
    elif "rows affected in Taxonomy table" in str(msg):
        crsr.commit()
        print("PM Taxonomy Table loaded correctly.")
    elif "rows with NULLS; they cannot be loaded into final table" in str(m):
        print("Taxonomy Staging has NULL values. Please review and correct the data.")
        crsr.rollback()
    else:
        print("Something else went wrong: \n")
        print(m)
        print(f"Full msg: {msg}")
        crsr.rollback()
    print("Stored Procedure Executed.")
    conn.close()
    return

def runUpdate(mEngine, mVer: float, mDate: date, mProc: str) -> None:
    mFrame = download_update(mVer, file_name, file_ext, mDate)
    finFrame = prep_dataFrame(mFrame, mVer, mDate)
    truncateSQL(mEngine)
    err = loadSQL(mEngine, finFrame, sql_dtypes)
    mEngine.close()
    if err != 0:
      print("Error with loading SQL staging table.\n")  
      return
    raw_engine = init_sql_engine(ss.server, ss.db_analytics, "raw")
    exec_sp(raw_engine, mProc)   
    return

def next_version_update(mToday: date, mBase: float, sqlVer: float, mEngine) -> int:
    baseVer = mBase
    qtr = date_quarter(mToday)
    
    print("Today: {}\nCurrent Quarter: {} \n".format(today, qtr))
    print("Base Version: {}\nSQL Last Updated Version: {}".format(baseVer, sqlVer))
    
    try:
        if sqlVer < baseVer:
            print("New version needed. Updating to from ver {} to ver {}...".format(sqlVer, (baseVer)))
            runUpdate(mEngine= mEngine, mVer=baseVer, mDate = cur_q3, mProc= ss.db_analytics + ".dbo." 
                      + "uspTaxonomy_Python_Load")
            return 0
        # if qtr == 1 and sqlVer == baseVer:
        #     print("You already have the latest .0 verion. \n This version Gets released in Jan of Q1.\n Your version: {}"
        #           .format(sqlVer))
        #     return 1
        # elif qtr == 1 and sqlVer != baseVer:
        #     print("New version needed. Updating to from ver {} to ver {}...".format(sqlVer, (sqlVer + 0.9)))
        #     runUpdate(mEngine= mEngine, mVer=baseVer, mDate = cur_q1, mProc= ss.db_analytics + ".dbo."
        #               + "uspTaxonomy_Python_Load")
        #     return 0
        # elif qtr == 3 and sqlVer == baseVer:
        #     print("You already have the latest .1 verion. \n This version Gets released in July of Q3.\n Your version: {}"
        #           .format(sqlVer))
        #     return 1
        # elif qtr == 3 and  sqlVer != baseVer:
        #     print("New version needed. Updating to from ver {} to ver {}...".format(sqlVer, (sqlVer + 0.1)))
        #     runUpdate(mEngine= mEngine, mVer=baseVer, mDate = cur_q3, mProc= ss.db_analytics + ".dbo."
        #               + "uspTaxonomy_Python_Load")
        #     return 0
        # elif sqlVer < baseVer:
        #     print("New version needed. Updating to from ver {} to ver {}...".format(sqlVer, (baseVer)))
        #     runUpdate(mEngine= mEngine, mVer=baseVer, mDate = cur_q3, mProc= ss.db_analytics + ".dbo." 
        #               + "uspTaxonomy_Python_Load")
        #     return 0
        else:
            print("No action needed")    
            return 1
    except (ValueError, TypeError) as e :
        # print("Input value is of type: int | SQL Version variable of type: {}".format(type(sqlVer)))
        
        print("Error raised when attempting to run update\n", e)
        # print("Exception logged in this location: {}".format(log_path))
        return -1    
# Script

def pullTaxonomyUpdate(query, server, database) -> Tuple:
    try:
        data = sq.sql_extract(query, server, database, conn_type = "")
        if len(data) != 0:
            mVer = data.iloc[0, 0]
            mDate = data.iloc[0, 1]
            return mVer, mDate
        else:
            return 0, 0
    except Exception as e:
        print("pullTaxonomyUpdate() error: {}\n Data variable: {}".format(e, type(e))) 
        return  -1, None

def main():
    engine_an = init_sql_engine(ss.server, ss.db_analytics, "")
    sql_last_ver, sql_last_update = pullTaxonomyUpdate(query, ss.server, ss.db_pm)

    # Pull latest info from NUCC site and decide if download is needed
    try:
        site_ver, site_url = get_latest_version_from_site()
    except Exception as e:
        print("Unable to determine latest site version; falling back to date-based behavior")
        baseVer = base_version(today, sql_last_ver if isinstance(sql_last_ver, (int, float)) else None)
        site_ver = baseVer
        site_url = None

    print(f"NUCC site version: {site_ver} | INO PM Taxonomy Version: {sql_last_ver}")

    # If DB has same or newer date/version, skip download
    remote_date = None
    if site_url:
        remote_date = get_remote_last_modified(site_url)
        if sql_last_update and remote_date and sql_last_update >= remote_date:
            print(f"Database already up-to-date (DB date {sql_last_update} >= remote date {remote_date}). Exiting.")
            return

    # if site version isn't newer than sql version, no action
    if isinstance(sql_last_ver, (int, float)) and site_ver <= sql_last_ver:
        print(f"Site version ({site_ver}) is not newer than DB version ({sql_last_ver}). No update needed.")
        return

    # Proceed to update using the site version and URL
    target_ver = site_ver
    target_date = remote_date or today
    print(f"Updating to site version {target_ver} (date {target_date})")
    if sql_last_ver < 0:
        print("Something went wrong. Exiting\n")
        return
    
    # Use existing update functions to download and load the site CSV
    try:
        runUpdate(mEngine=engine_an, mVer=target_ver, mDate=target_date, mProc= ss.db_analytics + ".dbo." + "uspTaxonomy_Python_Load")
        print("Table updated successfully!\nExiting...")
    except Exception as e:
        print("Update failed")
        print("Something went wrong.\nExiting...")
    
          

if __name__ == "__main__":
    main()
    x = input("Press any key to exit")