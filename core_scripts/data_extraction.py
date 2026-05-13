import logging

from common_utilities.source_to_stage import *
#Logging to both console and file
logging.basicConfig(

    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.FileHandler('logs/etljob.log', mode='w'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


class DataExtraction:
    def extract_product_data_load_stag(self,file_path,file_type):
        logger.info("Product data extraction started.....")
        df = read_source_file(file_path,file_type)
        load_data_to_stage(df,"stag_product")
        logger.info("Product data extraction finished and loaded into staging area....")

    def extract_sales_data_load_stag(self,file_path,file_type):
        logger.info("Sales data extraction started.....")
        df = read_source_file(file_path,file_type)
        load_data_to_stage(df, "stag_sales")
        logger.info("Sales data extraction finished and loaded into staging area....")

    def extract_inventory_data_load_stag(self,file_path,file_type):
        logger.info("Inventory data extraction started.....")
        df = read_source_file(file_path,file_type)
        load_data_to_stage(df, "stag_inventory")
        logger.info("Inventory data extraction finished and loaded into staging area....")

    def extract_suplier_data_load_stag(self,file_path,file_type):
        logger.info("Supplier data extraction started.....")
        df = read_source_file(file_path,file_type)
        load_data_to_stage(df, "stag_suplier")
        logger.info("Supplier data extraction finished and loaded into staging area....")

    def extract_stores_data_load_stage(self,database_type):
        logger.info("Store data extraction started.....")
        sql = 'select * from stores'
        df = read_source_database(sql,database_type)
        #print(df.head())
        load_data_to_stage(df, "stag_stores")
        logger.info("Store data extraction finished and loaded into staging area....")



if __name__ == "__main__":
    de = DataExtraction()
    de.extract_product_data_load_stag("source_systems/product_data.csv","csv")
    de.extract_sales_data_load_stag("source_systems/sales_data_linux.csv", "csv")
    de.extract_inventory_data_load_stag("source_systems/inventory_data.xml", "xml")
    de.extract_suplier_data_load_stag("source_systems/supplier_data.json", "json")
    de.extract_stores_data_load_stage("oracle")

