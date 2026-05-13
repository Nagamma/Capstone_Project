from sqlalchemy import create_engine
from common_utilities.db_config import *
#database connections
#Target DB connection
mysql_engine = create_engine(f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
print("Password=",DB_PASSWORD)
print(mysql_engine)