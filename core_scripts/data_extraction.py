import pandas as pd
from sqlalchemy import create_engine

#database conncetions

#Target DB connection
mysql_engine = create_engine('mysql+pymysql://root:admin%401990@localhost:3306/Feb2026Retaildwh')

class DataExtraction:
    def extract_product_data_load_stag(self):
        print("Product data extraction started.....")
        df = pd.read_csv("source_systems/product_data.csv")
        df.to_sql("stag_product",mysql_engine, index=False)
        print("Product data extraction finished and loaded into staging area....")

    def extract_sales_data_load_stag(self):
        print("Sales data extraction started.....")
        df = pd.read_csv("source_systems/sales_data_linux.csv")
        df.to_sql("stag_sales",mysql_engine, index=False)
        print("Sales data extraction finished and loaded into staging area....")

    def extract_inventory_data_load_stag(self):
        print("Inventory data extraction started.....")
        df=pd.read_xml("source_systems/inventory_data.xml",xpath=".//item",parser="etree")
        df.to_sql("stag_inventory",mysql_engine, index=False)
        print("Inventory data extraction finished and loaded into staging area....")

    def extract_suplier_data_load_stag(self):
        print("Supplier data extraction started.....")
        df = pd.read_json("source_systems/supplier_data.json")
        df.to_sql("stag_suplier",mysql_engine, index=False)
        print("Supplier data extraction finished and loaded into staging area....")


if __name__ == "__main__":
    de = DataExtraction()
    de.extract_product_data_load_stag()
    de.extract_sales_data_load_stag()
    de.extract_inventory_data_load_stag()
    de.extract_suplier_data_load_stag()
