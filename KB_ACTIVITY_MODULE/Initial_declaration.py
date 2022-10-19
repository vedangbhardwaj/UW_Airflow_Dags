#### initial declarations
import pandas as pd

missing_value_num = -99999  ### missing value assignment
missing_value_cat = "missing"
start_date = "2021-08-01"  ### Start date of modelling sample data
end_date = "2022-07-31"  ### End date of modelling sample data
partition_date = "2022-06-30"  ## Train and OOT partition date
IV_threshold = 0.0149  ### threshold for IV (IV should be accepted
var_threshold = (
    0.75  ### 75% of variantion in the features gets captured with PCA components
)

colsList = [
    "LAST_1_MONTH_CUSTOMERS",
    "LAST_2_MONTH_CUSTOMERS",
    "LAST_3_MONTH_CUSTOMERS",
    "RATIO_OF_CUSTOMERS_ADDED_L1_L3_MNTH",
    "DELETED_CUSTOMERS",
    "DELETED_BOOKS",
    "TOTAL_APPS",
    "BUSINESS_APPS",
    "FINANCE_APPS",
    "BUSINESS_TOTALAPPS_PROPORTION",
    "FINANCE_TOTALAPPS_PROPORTION",
    "PRICE",
]

ID_cols = ["USER_ID", "LOAN_ID", "DISBURSED_DATE", "BAD_FLAG"]
input_path = "/Users/vedang.bhardwaj/Desktop/work_mode/airflow_learn/UW_Airflow_Dags/KB_ACTIVITY_MODULE/data/raw/"
interim_path = "/Users/vedang.bhardwaj/Desktop/work_mode/airflow_learn/UW_Airflow_Dags/KB_ACTIVITY_MODULE/data/interim/"
output_path = "/Users/vedang.bhardwaj/Desktop/work_mode/airflow_learn/UW_Airflow_Dags/KB_ACTIVITY_MODULE/reports/output/"
plot_path = "/Users/vedang.bhardwaj/Desktop/work_mode/airflow_learn/UW_Airflow_Dags/KB_ACTIVITY_MODULE/reports/figures/Trend_plots/"
model_path = "/Users/vedang.bhardwaj/Desktop/work_mode/airflow_learn/KB_ACTIVITY_MODULE/UW_Airflow_Dags/models/"
feature_list = pd.read_csv(input_path + "KB_activity_module_variables.csv")
