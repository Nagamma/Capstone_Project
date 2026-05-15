from project_config.logger_config import logger
from common_utilities.stage_to_transformation import transform_data

class DataTransformation():
    def transform_filter_sales_data(self):
        logger.info("filter Sales Data on particular date has started...")
        try:
            sql = """SELECT * FROM stag_sales where sale_date > '2025-09-10'"""
            table_name = "filtered_sales_data"
            transform_data(self,sql,table_name)
        except Exception as e:
            logger.error(f"Error occured while trasforming sales data:{e}",exc_info=True)
        logger.info("Sales data transformed successfully")

    def transform_router_high_rigion_sales(self):
        logger.info("Router Transformation of High Region sales data has started...")
        try:
            sql = """select * from filtered_sales_data where region = 'High'"""
            table_name = "high_sales_data"
            transform_data(self, sql, table_name)
        except Exception as e:
            logger.error(f"Error occured while trasforming high region sales data:{e}", exc_info=True)
        logger.info("Router Transformation of High Region sales data has completed")

    def transform_router_low_rigion_sales(self):
        logger.info("Router Transformation of Low Region sales data has started...")
        try:
            sql = """select * from filtered_sales_data where region = 'Low'"""
            table_name = "low_sales_data"
            transform_data(self, sql, table_name)
        except Exception as e:
            logger.error(f"Error occured while trasforming low region sales data:{e}", exc_info=True)
        logger.info("Router Transformation of Low Region sales data has completed")

    def transform_aggregator_sales_data(self):
        logger.info("Aggregator Transformation on sales data has started...")
        try:
             sql="""select product_id, year(sale_date) as year, month(sale_date) as month, sum(price*quantity) as total_sales from filtered_sales_data group by product_id,year(sale_date),month(sale_date)"""
             table_name = "monthly_total_sales_amt_data"
             transform_data(self, sql, table_name)
        except Exception as e:
            logger.error(f"Error occured while trasforming aggregator sales data:{e}", exc_info=True)
        logger.info("Aggregator Transformation on sales data has has completed")

    def transform_joiner_sales_product_stores(self):
        logger.info("Joiner Transformation for sales data has started...")
        try:
            sql = """select fs.sales_id,fs.quantity,fs.price,fs.quantity*fs.price as sales_amt,fs.sale_date,p.product_id,p.product_name,s.store_id,s.store_name 
                    from filtered_sales_data as fs inner join stag_product as p on fs.product_id = p.product_id 
                    inner join stag_stores as s on fs.store_id = s.store_id"""
            table_name = "sales_with_details"
            transform_data(self, sql, table_name)
        except Exception as e:
            logger.error(f"Error occured while trasforming joiner sales data:{e}", exc_info=True)
        logger.info("Joiner Transformation for sales data has completed")

    def transform_aggregator_inventory_level(self):
        logger.info("Aggregator Transformation for inventory level data has started...")
        try:
            sql="""select store_id, sum(Quantity_on_hand) as total_inventory from stag_inventory group by store_id"""
            table_name = "aggregated_inventory_level"
            transform_data(self, sql, table_name)
        except Exception as e:
            logger.error(f"Error occured while trasforming aggregator inventory level:{e}", exc_info=True)
        logger.info("Aggregator Transformation for inventory level has completed")

if __name__ == "__main__":
    dt = DataTransformation()
    dt.transform_filter_sales_data()
    dt.transform_router_high_rigion_sales()
    dt.transform_router_low_rigion_sales()
    dt.transform_aggregator_sales_data()
    dt.transform_joiner_sales_product_stores()
    dt.transform_aggregator_inventory_level()
