import pandas as pd

# SQL Query - getting raw data:
get_raw_data = """
    select  
        user_id,loan_id,disbursed_date, ever11DPD_in_90days,
        ever11DPD_in_60days,
        ever15DPD_in_90days as BAD_FLAG,
        ever15DPD_in_60days
        ,LAST_1_MONTH_CUSTOMERS
        ,LAST_2_MONTH_CUSTOMERS
        ,LAST_3_MONTH_CUSTOMERS
        ,RATIO_OF_CUSTOMERS_ADDED_L1_L3_MNTH
        ,DELETED_CUSTOMERS
        ,LAST_1_MONTH_BOOKS
        ,LAST_2_MONTH_BOOKS
        ,LAST_3_MONTH_BOOKS
        ,RATIO_OF_BOOKS_ADDED_L1_L3_MNTH
        ,DELETED_BOOKS
        ,TOTAL_APPS
        ,BUSINESS_APPS
        ,FINANCE_APPS
        ,BUSINESS_TOTALAPPS_PROPORTION
        ,FINANCE_TOTALAPPS_PROPORTION
        ,DEVICE_BRAND_GD
        ,DEVICE_CARRIER_GROUP
        ,PRICE
        ,Business_Card_Flag
    from ANALYTICS.KB_ANALYTICS.A4_UW_KB_Activity_data_v2
    where disbursed_date>=date('{sd}')
    and date(disbursed_date)<=date('{ed}')
    """
# SQL Query - getting transformed data:
get_transformed_data = """
        SELECT * from ANALYTICS.KB_ANALYTICS.airflow_demo_write_transformed_kb_activity_module 
        """

get_transformed_woe_data = """
        SELECT * from ANALYTICS.KB_ANALYTICS.airflow_demo_write_transformed_woe_kb_activity_module 
        """
