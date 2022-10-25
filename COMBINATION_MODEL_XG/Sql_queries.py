import pandas as pd

# SQL Query - getting txn module data:
get_txn_data = """
    select * from ANALYTICS.KB_ANALYTICS.airflow_demo_write_result_xgb_kb_txn_module ; 
    """
# SQL Query - getting activity module data:
get_activity_data = """
    select * from ANALYTICS.KB_ANALYTICS.airflow_demo_write_result_xgb_kb_activity_module ; 
    """
# SQL Query - getting bureau module data
get_bureau_data = """
    select * from ANALYTICS.KB_ANALYTICS.airflow_demo_write_result_xgb_kb_bureau_module ; 
    """
