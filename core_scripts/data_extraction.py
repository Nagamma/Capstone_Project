

from common_utilities.source_to_stage import *
from project_config.logger_config import logger


class DataExtraction:
    def extract_product_data_load_stag(self,file_path,file_type):
        logger.info("Product data extraction started.....")
        try:
            df = read_source_file(file_path,file_type)
            load_data_to_stage(df,"stag_product")
        except Exception as e:
            logger.error(f"Error encountered while extracting product data:{e}",exc_info=True)
        logger.info("Product data extraction finished and loaded into staging area....")

    def extract_sales_data_load_stag(self,file_path,file_type):
        logger.info("Sales data extraction started.....")
        try:
            df = read_source_file(file_path,file_type)
            load_data_to_stage(df, "stag_sales")
        except Exception as e:
            logger.error(f"Error encountered while extracting sales data:{e}",exc_info=True)
        logger.info("Sales data extraction finished and loaded into staging area....")

    def extract_inventory_data_load_stag(self,file_path,file_type):
        logger.info("Inventory data extraction started.....")
        try:
            df = read_source_file(file_path,file_type)
            load_data_to_stage(df, "stag_inventory")
        except Exception as e:
            logger.error(f"Error encountered while extracting inventory data:{e}",exc_info=True)
        logger.info("Inventory data extraction finished and loaded into staging area....")

    def extract_suplier_data_load_stag(self,file_path,file_type):
        logger.info("Supplier data extraction started.....")
        try:
            df = read_source_file(file_path,file_type)
            load_data_to_stage(df, "stag_suplier")
        except Exception as e:
            logger.error(f"Error encountered while extracting suplier data:{e}",exc_info=True)
        logger.info("Supplier data extraction finished and loaded into staging area....")

    def extract_stores_data_load_stage(self,database_type):
        logger.info("Store data extraction started.....")
        try:
            sql = 'select * from stores'
            df = read_source_database(sql,database_type)
        except Exception as e:
            logger.error(f"Error encountered while extracting stores data:{e}",exc_info=True)
        #print(df.head())
        load_data_to_stage(df, "stag_stores")
        logger.info("Store data extraction finished and loaded into staging area....")



if __name__ == "__main__":
    de = DataExtraction()
    de.extract_product_data_load_stag("source_systems/product_data.csv","csv")
    de.extract_sales_data_load_stag("source_systems/sales_data.csv", "csv")
    de.extract_inventory_data_load_stag("source_systems/inventory_data.xml", "xml")
    de.extract_suplier_data_load_stag("source_systems/supplier_data.json", "json")
    de.extract_stores_data_load_stage("oracle")

