import pandas as pd
from common_utilities.db_connection import *

def read_source_file(file_path,file_type):
    if file_type=='csv':
        df = pd.read_csv(file_path)
    elif file_type=='excel':
        df = pd.read_excel(file_path)
    elif file_type=='json':
        df = pd.read_json(file_path)
    elif file_type== 'xml':
        df = pd.read_xml(file_path,xpath=".//item",parser='etree')
    else:
        raise ValueError(f'Unrecognised file type {file_type}')
    return df

def read_source_database(sql,database_type):
    if database_type=='mysql':
        df=pd.read_sql(sql,mysql_engine)
    elif database_type=='oracle':
        df=pd.read_sql(sql,oracle_engine)
    else:
        raise ValueError(f'Unrecognised database type {database_type}')
    return df


def load_data_to_stage(df,table_name):
    df.to_sql(table_name,mysql_engine,if_exists='replace',index=False)

