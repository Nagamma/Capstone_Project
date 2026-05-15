import pandas as pd
from common_utilities.db_connection import *

def transform_data(self,sql,table_name):
    df = pd.read_sql(sql, mysql_engine)
    df.to_sql(table_name,mysql_engine,index=False)
