from common_utilities.db_connection import mysql_engine

def loading_data_to_target(self,query,logger):
    with mysql_engine.connect() as conn:
        logger.info(query)
        conn.execute(query)
        conn.commit()