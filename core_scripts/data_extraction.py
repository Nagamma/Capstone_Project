import pandas as pd
from sqlalchemy import create_engine

import logging

#Logging to both console and file
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.FileHandler('logs/etljob.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

#database conncetions
#Target DB connection
mysql_engine = create_engine('mysql+pymysql://root:admin%401990@localhost:3306/Feb2026Retaildwh')

class DataExtraction:
    def extract_product_data_load_stag(self):
        logger.info("Product data extraction started.....")
        df = pd.read_csv("source_systems/product_data.csv")
        df.to_sql("stag_product",mysql_engine, index=False)
        logger.info("Product data extraction finished and loaded into staging area....")

    def extract_sales_data_load_stag(self):
        logger.info("Sales data extraction started.....")
        df = pd.read_csv("source_systems/sales_data_linux.csv")
        df.to_sql("stag_sales",mysql_engine, index=False)
        logger.info("Sales data extraction finished and loaded into staging area....")

    def extract_inventory_data_load_stag(self):
        logger.info("Inventory data extraction started.....")
        df=pd.read_xml("source_systems/inventory_data.xml",xpath=".//item",parser="etree")
        df.to_sql("stag_inventory",mysql_engine, index=False)
        logger.info("Inventory data extraction finished and loaded into staging area....")

    def extract_suplier_data_load_stag(self):
        logger.info("Supplier data extraction started.....")
        df = pd.read_json("source_systems/supplier_data.json")
        df.to_sql("stag_suplier",mysql_engine, index=False)
        logger.info("Supplier data extraction finished and loaded into staging area....")


if __name__ == "__main__":
    de = DataExtraction()
    de.extract_product_data_load_stag()
    de.extract_sales_data_load_stag()
    de.extract_inventory_data_load_stag()
    de.extract_suplier_data_load_stag()
