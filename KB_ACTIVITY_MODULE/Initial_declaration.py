#### initial declarations
from xml.sax.handler import feature_external_ges
import pandas as pd

missing_value_num = -99999  ### missing value assignment
missing_value_cat = "missing"
start_date = "2021-08-01"  ### Start date of modelling sample data
end_date = "2022-07-31"  ### End date of modelling sample data
partition_date = "2022-06-30"  ## Train and OOT partition date
IV_threshold = 0.02  ### threshold for IV (IV should be accepted
var_threshold = (
    0.75  ### 75% of variantion in the features gets captured with PCA components
)

colsList = [
    "TOTAL_KHATAS",
    "TOTAL_CUSTOMERS",
    "NO_KB_CUSTOMERS",
    "DAYS_SINCE_LAST_CUST_ADDED",
    "TOTAL_TRXNS",
    "TOTAL_TXN_MONTHS",
    "ACTIVE_KHATAS",
    "ACTIVE_DAYS",
    "DEBIT_TRXNS",
    "CREDIT_TRXNS",
    "DEBIT_AMNT",
    "CREDIT_AMNT",
    "TOTAL_TXN_AMNT",
    "MONTHS_SINCE_LAST_TXN",
    "D_1_TXNS",
    "D_30_TXNS",
    "AVG_TRXNS_CNT_L1MONTH_DAILY",
    "AVG_TRXNS_CNT_L2MONTH_DAILY",
    "AVG_TRXNS_CNT_L3MONTH_DAILY",
    "AVG_TRXNS_CNT_L6MONTH_DAILY",
    "AVG_TRXNS_CNT_L12MONTH_DAILY",
    "AVG_TRXNS_AMOUNT_L1MONTH_DAILY",
    "AVG_TRXNS_AMT_L2MONTH_DAILY",
    "AVG_TRXNS_AMT_L3MONTH_DAILY",
    "AVG_CREDIT_TRXNS_CNT_L1MONTH_DAILY",
    "AVG_CREDIT_TRXNS_CNT_L2MONTH_DAILY",
    "AVG_CREDIT_TRXNS_CNT_L6MONTH_DAILY",
    "AVG_CREDIT_TRXNS_CNT_L12MONTH_DAILY",
    "AVG_DEBIT_TRXNS_CNT_L1MONTH_DAILY",
    "AVG_DEBIT_TRXNS_CNT_L2MONTH_DAILY",
    "AVG_DEBIT_TRXNS_CNT_L3MONTH_DAILY",
    "AVG_DEBIT_TRXNS_CNT_L6MONTH_DAILY",
    "AVG_DEBIT_TRXNS_CNT_L12MONTH_DAILY",
    "AVG_TRXNS_CNT_L1MONTH_MONTHLY",
    "AVG_TRXNS_CNT_L2MONTH_MONTHLY",
    "AVG_TRXNS_CNT_L3MONTH_MONTHLY",
    "AVG_TRXNS_CNT_L6MONTH_MONTHLY",
    "AVG_TRXNS_CNT_L12MONTH_MONTHLY",
    "AVG_TRXNS_AMOUNT_L1MONTH_MONTHLY",
    "AVG_TRXNS_AMT_L2MONTH_MONTHLY",
    "AVG_TRXNS_AMT_L3MONTH_MONTHLY",
    "AVG_CREDIT_TRXNS_CNT_L1MONTH_MONTHLY",
    "AVG_CREDIT_TRXNS_CNT_L2MONTH_MONTHLY",
    "AVG_CREDIT_TRXNS_CNT_L6MONTH_MONTHLY",
    "AVG_CREDIT_TRXNS_CNT_L12MONTH_MONTHLY",
    "AVG_DEBIT_TRXNS_CNT_L1MONTH_MONTHLY",
    "AVG_DEBIT_TRXNS_CNT_L2MONTH_MONTHLY",
    "AVG_DEBIT_TRXNS_CNT_L3MONTH_MONTHLY",
    "AVG_DEBIT_TRXNS_CNT_L6MONTH_MONTHLY",
    "AVG_DEBIT_TRXNS_CNT_L12MONTH_MONTHLY",
    "TOTAL_TRXNS_CNT_3_BY_6",
    "TOTAL_TRXNS_CNT_3_BY_12",
    "RATIO_CNT_L1_L3_MNTH_DAILY",
    "RATIO_AMNT_L1_L3_MNTH_DAILY",
    "RATIO_CNT_L1_L3_MNTH_MONTHLY",
    "RATIO_AMNT_L1_L3_MNTH_MONTHLY",
    "AVG_TRAN_SIZE_1_MNTH_DAILY",
    "AVG_TRAN_SIZE_2_MNTH_DAILY",
    "AVG_TRAN_SIZE_3_MNTH_DAILY",
    "AVG_TRAN_SIZE_1_MNTH_MONTHLY",
    "AVG_TRAN_SIZE_2_MNTH_MONTHLY",
    "AVG_TRAN_SIZE_3_MNTH_MONTHLY",
    "TOTAL_COUNTERPARTY_TRXNS",
    "TOTAL_COUNTERPARTY_TXN_AMT",
    "AVG_TXN_CNT_WITH_KB_COUNTERPARTY_USERS_1MNTH_DAILY",
    "AVG_TXN_CNT_WITH_KB_COUNTERPARTY_USERS_2MNTH_DAILY",
    "AVG_TXN_CNT_WITH_KB_COUNTERPARTY_USERS_3MNTH_DAILY",
    "AVG_TXN_AMT_WITH_KB_COUNTERPARTY_USERS_1MNTH_DAILY",
    "AVG_TXN_AMT_WITH_KB_COUNTERPARTY_USERS_2MNTH_DAILY",
    "AVG_TXN_AMT_WITH_KB_COUNTERPARTY_USERS_3MNTH_DAILY",
    "AVG_TXN_CNT_WITH_KB_COUNTERPARTY_USERS_1MNTH_MONTHLY",
    "AVG_TXN_CNT_WITH_KB_COUNTERPARTY_USERS_2MNTH_MONTHLY",
    "AVG_TXN_CNT_WITH_KB_COUNTERPARTY_USERS_3MNTH_MONTHLY",
    "AVG_TXN_AMT_WITH_KB_COUNTERPARTY_USERS_1MNTH_MONTHLY",
    "AVG_TXN_AMT_WITH_KB_COUNTERPARTY_USERS_2MNTH_MONTHLY",
    "AVG_TXN_AMT_WITH_KB_COUNTERPARTY_USERS_3MNTH_MONTHLY",
    "ACTIVE_KB_COUNTERPARTY_USERS",
    "CREDIT_EXTENDED_TOTAL",
    "AMT_RECOVERED_31TO60",
    "AMT_RECOVERED_61TO90",
    "PARTIAL_SETTLED_TRXNS",
    "FULL_SETTLED_TRXNS",
    "UNSETTLED_TRXNS",
    "CREDIT_EXTENDED_3MNTH_AVG_DAILY",
    "CREDIT_EXTENDED_6MNTH_AVG_DAILY",
    "CREDIT_EXTENDED_12MNTH_AVG_DAILY",
    "CREDIT_EXTENDED_OVERALL_AVG_DAILY",
    "CREDIT_RECOVERED_OVERALL_AVG_DAILY",
    "AMT_RECOVERED_0TO30_OVERALL_AVG_DAILY",
    "AMT_RECOVERED_31TO60_OVERALL_AVG_DAILY",
    "AMT_RECOVERED_61TO90_OVERALL_AVG_DAILY",
    "PARTIAL_SETTLED_TRXNS_3MNTH_AVG_DAILY",
    "PARTIAL_SETTLED_TRXNS_6MNTH_AVG_DAILY",
    "PARTIAL_SETTLED_TRXNS_12MNTH_AVG_DAILY",
    "PARTIAL_SETTLED_TRXNS_OVERALL_AVG_DAILY",
    "FULL_SETTLED_TRXNS_3MNTH_AVG_DAILY",
    "FULL_SETTLED_TRXNS_6MNTH_AVG_DAILY",
    "FULL_SETTLED_TRXNS_12MNTH_AVG_DAILY",
    "FULL_SETTLED_TRXNS_OVERALL_AVG_DAILY",
    "UNSETTLED_TRXNS_3MNTH_AVG_DAILY",
    "UNSETTLED_TRXNS_6MNTH_AVG_DAILY",
    "UNSETTLED_TRXNS_12MNTH_AVG_DAILY",
    "UNSETTLED_TRXNS_OVERALL_AVG_DAILY",
    "CREDIT_EXTENDED_3MNTH_AVG_MONTHLY",
    "CREDIT_EXTENDED_6MNTH_AVG_MONTHLY",
    "CREDIT_EXTENDED_12MNTH_AVG_MONTHLY",
    "CREDIT_EXTENDED_OVERALL_AVG_MONTHLY",
    "AMT_RECOVERED_31TO60_OVERALL_AVG_MONTHLY",
    "AMT_RECOVERED_61TO90_OVERALL_AVG_MONTHLY",
    "PARTIAL_SETTLED_TRXNS_3MNTH_AVG_MONTHLY",
    "PARTIAL_SETTLED_TRXNS_6MNTH_AVG_MONTHLY",
    "PARTIAL_SETTLED_TRXNS_12MNTH_AVG_MONTHLY",
    "PARTIAL_SETTLED_TRXNS_OVERALL_AVG_MONTHLY",
    "FULL_SETTLED_TRXNS_3MNTH_AVG_MONTHLY",
    "FULL_SETTLED_TRXNS_6MNTH_AVG_MONTHLY",
    "FULL_SETTLED_TRXNS_12MNTH_AVG_MONTHLY",
    "FULL_SETTLED_TRXNS_OVERALL_AVG_MONTHLY",
    "UNSETTLED_TRXNS_3MNTH_AVG_MONTHLY",
    "UNSETTLED_TRXNS_6MNTH_AVG_MONTHLY",
    "UNSETTLED_TRXNS_12MNTH_AVG_MONTHLY",
    "UNSETTLED_TRXNS_OVERALL_AVG_MONTHLY",
    "CREDIT_EXTENDED_LAST_MONTH_VALUE",
    "CREDIT_EXTENDED_3_BY_6",
    "CREDIT_RECOVERED_3_BY_6",
    "AMT_RECOVERED_0TO30_3_BY_6",
    "AMT_RECOVERED_31TO60_3_BY_6",
    "AMT_RECOVERED_61TO90_3_BY_6",
    "PARTIAL_SETTLED_TRXNS_3_BY_6",
    "FULL_SETTLED_TRXNS_3_BY_6",
    "UNSETTLED_TRXNS_3_BY_6",
    "CREDIT_EXTENDED_3_BY_12",
    "CREDIT_RECOVERED_3_BY_12",
    "AMT_RECOVERED_0TO30_3_BY_12",
    "AMT_RECOVERED_31TO60_3_BY_12",
    "AMT_RECOVERED_61TO90_3_BY_12",
    "PARTIAL_SETTLED_TRXNS_3_BY_12",
    "FULL_SETTLED_TRXNS_3_BY_12",
    "UNSETTLED_TRXNS_3_BY_12",
    "CREDIT_RECOVERY_RATIO_3_MNTH_AVG_DAILY",
    "CREDIT_RECOVERY_RATIO_6_MNTH_AVG_DAILY",
    "CREDIT_RECOVERY_RATIO_12_MNTH_AVG_DAILY",
    "CREDIT_RECOVERY_RATIO_OVERALL_MNTH_AVG_DAILY",
    "CREDIT_RECOVERY_RATIO_3_MNTH_AVG_MONTHLY",
    "CREDIT_RECOVERY_RATIO_6_MNTH_AVG_MONTHLY",
    "CREDIT_RECOVERY_RATIO_12_MNTH_AVG_MONTHLY",
    "CREDIT_RECOVERY_RATIO_OVERALL_MNTH_AVG_MONTHLY",
    "CREDIT_EXTENDED_3MNTH_SUM",
    "CREDIT_RECOVERY_RATIO_3_MNTH",
    "CREDIT_EXTENDED_6MNTH_SUM",
    "CREDIT_RECOVERY_RATIO_6_MNTH",
    "CREDIT_EXTENDED_12MNTH_SUM",
    "CREDIT_RECOVERY_RATIO_12_MNTH",
    "CREDIT_RECOVERY_RATIO_3_BY_6",
    "CREDIT_RECOVERY_RATIO_3_BY_12",
]

ID_cols = ["USER_ID", "LOAN_ID", "DISBURSED_DATE", "BAD_FLAG"]
input_path = (
    "/Users/vedang.bhardwaj/Desktop/work_mode/airflow_learn/UW_Airflow_Dags/KB_TXN_MODULE/data/raw/"
)
interim_path = (
    "/Users/vedang.bhardwaj/Desktop/work_mode/airflow_learn/UW_Airflow_Dags/KB_TXN_MODULE/data/interim/"
)
output_path = "/Users/vedang.bhardwaj/Desktop/work_mode/airflow_learn/UW_Airflow_Dags/KB_TXN_MODULE/reports/output/"
plot_path = "/Users/vedang.bhardwaj/Desktop/work_mode/airflow_learn/UW_Airflow_Dags/KB_TXN_MODULE/reports/figures/Trend_plots/"
model_path = (
    "/Users/vedang.bhardwaj/Desktop/work_mode/airflow_learn/KB_TXN_MODULE/UW_Airflow_Dags/models/"
)
feature_list = pd.read_csv(input_path + "KB_transaction_module_variables.csv")