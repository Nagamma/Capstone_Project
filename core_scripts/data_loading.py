from project_config.logger_config import logger
from common_utilities.load_transform_to_dwh import loading_data_to_target
from sqlalchemy import text
class DataLoading:
    def load_fact_sales_table(self):
        logger.info('Loading fact_sales_table....')
        try:
            query = text("""insert into fact_sales(sales_id,product_id,store_id,quantity,total_sales,sale_date) 
                    select sales_id,product_id,store_id,quantity,sales_amt,sale_date 
                    from sales_with_details""")
            loading_data_to_target(self,query,logger)
        except Exception as e:
            logger.error(f'Error while loading fact_sales_table: {e}',exc_info=True)
        logger.info('fact_sales_table loading is completed....')

    def load_inventory_table(self):
        logger.info('Loading inventory_table....')
        try:
            query= text("""insert into fact_inventory(product_id,store_id,quantity_on_hand,last_updated) 
                select product_id,store_id,quantity_on_hand,last_updated from stag_inventory""")
            loading_data_to_target(self,query,logger)
        except Exception as e:
            logger.error(f'Error while loading inventory_table: {e}',exc_info=True)
        logger.info('inventory_table loading is completed....')

    def load_monthly_sales_summary_table(self):
      logger.info('Loading monthly_sales_summary_table....')
      try:
          query = text("""insert into monthly_sales_summary(product_id,month,year,total_sales)
                       select product_id,month,year,total_sales from monthly_total_sales_amt_data""")
          loading_data_to_target(self,query,logger)
      except Exception as e:
           logger.error(f'Error while loading monthly_sales_summary_table: {e}',exc_info=True)
      logger.info('monthly_sales_summary_table loading is completed....')

    def load_inventory_level_by_stores_table(self):
      logger.info('Loading inventory_levels_by_store_table....')
      try:
          query = text("""insert into inventory_levels_by_store(store_id,total_inventory)
                          select store_id,total_inventory from aggregated_inventory_level""")
          loading_data_to_target(self,query,logger)
      except Exception as e:
          logger.error(f'Error while loading inventory_levels_by_store_table: {e}',exc_info=True)
      logger.info('inventory_levels_by_store_table loading is completed....')

if __name__ == '__main__':
    dl = DataLoading()
    dl.load_fact_sales_table()
    dl.load_inventory_table()
    dl.load_monthly_sales_summary_table()
    dl.load_inventory_level_by_stores_table()