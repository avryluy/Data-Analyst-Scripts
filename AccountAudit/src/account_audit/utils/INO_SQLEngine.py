import pandas as pd
from . import INO_SQL_Creds as creds
from sqlalchemy import URL, create_engine, text, CHAR, VARCHAR, DECIMAL, DATE, FLOAT, Engine  # noqa: F401

def active_credential_sign_in(server_name, database_name, connection_type=""):
    connection_url = URL.create(
    "mssql+pyodbc",
    username="",
    password="",
    host=server_name,
    port=1433,
    database=database_name,
    query={
        "driver": "ODBC Driver 17 for SQL Server",
        "TrustServerCertificate": "yes",
        "authentication": "ActiveDirectoryIntegrated",
    },
    )    
    engine = create_engine(connection_url,
    use_insertmanyvalues=True,
    fast_executemany=True)
    if connection_type == "raw":
        connection = engine.raw_connection()
        if connection.is_valid is True:
            print("Connection Successful.")
        else:
            print("Connection Unsuccessful. Check Your VPN connection and try again.")
            return
    
        return connection
    else:
        connection = engine.connect()
        if connection.connection.is_valid is True:
            print("Connection Successful.")
        else:
            print("Connection Unsuccessful. Check Your VPN connection and try again.")
            return
        return connection
    
def user_credential_sign_in(server_name,
                            database_name,
                            port = 1433,
                            connection_type=""):
    connection_url = URL.create(
        "mssql+pyodbc",
        username = creds.user_name,
        password = creds.user_pw,
        host = server_name,
        port = port,
        database = database_name,
        query= {"driver": "ODBC Driver 17 for SQL Server"}
    )
    
    engine = create_engine(connection_url)
    
    if connection_type == "raw":
        connection = engine.raw_connection()
        if connection.is_valid is True:
            print("Connection Successful.")
        else:
            print("""Connection Unsuccessful.\n\"
                Check the INO_SQL_Creds.py file 
                to ensure user credentials are correct.\n\
                Also check your server name, and database name. Then try again.\n""")
            return
        return connection
    else:
        connection = engine.connect()
        if connection.connection.is_valid is True:
            print("Connection Successful.")
            
        else:
            print("""Connection Unsuccessful.\n\
                Check the INO_SQL_Creds.py file 
                to ensure user credentials are correct.\n\
                Also check your server name, and database name. Then try again.\n""")
            return
        return connection
    
    
def sql_extract(query, server, db, conn_type):
    conn = active_credential_sign_in(server_name = server, database_name = db,
                                     connection_type = conn_type)
    df = pd.read_sql_query(query, con=conn) # type: ignore
    return df
