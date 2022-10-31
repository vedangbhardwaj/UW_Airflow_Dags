import pandas as pd
import numpy as np
import math
import statsmodels.api as sm
import snowflake.connector
from snowflake.connector.pandas_tools import pd_writer
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
import yaml
import pickle

### importing sql queries declaration ##
import sql_queries as Get_query


missing_value_num = -99999  ### missing value assignment
missing_value_cat = "missing"
start_date = "2021-08-01"  ### Start date of modelling sample data
end_date = "2022-07-31"  ### End date of modelling sample data
partition_date = "2022-06-30"  ## Train and OOT partition date
IV_threshold = 0.0149  ### threshold for IV (IV should be accepted
var_threshold = (
    0.75  ### 75% of variantion in the features gets captured with PCA components
)

model_path = "/Users/vedang.bhardwaj/Desktop/work_mode/airflow_learn/UW_Airflow_Dags/COMBINATION_MODEL_XG/models/"

with open("airflow_config.yml") as config_file:
    config = yaml.full_load(config_file)

conn = snowflake.connector.connect(
    user=config["user"],
    password=config["password"],
    account=config["account"],
    role=config["role"],
    warehouse=config["warehouse"],
    database=config["database"],
)
cur = conn.cursor()


def predict(dataset_name, **context):
    def get_data(module_name):
        sql_cmd = None
        if module_name == "KB_TXN_MODULE":
            sql_cmd = get_data(Get_query(dataset_name).get_txn_data)
        if module_name == "KB_ACTIVITY_MODULE":
            sql_cmd = get_data(Get_query(dataset_name).get_activity_data)
        if module_name == "KB_BUREAU_MODULE":
            sql_cmd = get_data(Get_query(dataset_name).get_bureau_data)
        cur.execute(sql_cmd)
        df = pd.DataFrame(cur.fetchall())
        colnames = [desc[0] for desc in cur.description]
        df.columns = [i for i in colnames]
        return df

    Transaction_module_data = get_data("KB_TXN_MODULE")
    Activity_module_data = get_data("KB_ACTIVITY_MODULE")
    Bureau_module_data = get_data("KB_BUREAU_MODULE")

    ### converting pd score to log odds
    Transaction_module_data["trx_logodds"] = np.log(
        Transaction_module_data["PRED_TRAIN_XGB"]
        / (1 - Transaction_module_data["PRED_TRAIN_XGB"])
    )

    Activity_module_data["act_logodds"] = np.log(
        Activity_module_data["PRED_TRAIN_XGB"]
        / (1 - Activity_module_data["PRED_TRAIN_XGB"])
    )

    Bureau_module_data["br_logodds"] = np.log(
        Bureau_module_data["PRED_TRAIN_XGB"]
        / (1 - Bureau_module_data["PRED_TRAIN_XGB"])
    )

    ### combining the training data
    data_merge = Activity_module_data[
        ["USER_ID", "LOAN_ID", "DISBURSED_DATE", "BAD_FLAG", "act_logodds"]
    ].merge(
        Transaction_module_data[["LOAN_ID", "trx_logodds"]], on="LOAN_ID", how="left"
    )

    data_merge = data_merge.merge(
        Bureau_module_data[["LOAN_ID", "br_logodds"]], on="LOAN_ID", how="left"
    )

    combination_train = data_merge.dropna()
    combination_train.shape

    combination_train["comb_score"] = (
        (46 / 100) * combination_train["trx_logodds"]
        + (31 / 100) * combination_train["br_logodds"]
        + (23 / 100) * combination_train["act_logodds"]
    )

    combination_train["PD_score"] = 1 / (1 + np.exp(-combination_train["comb_score"]))

    model_calib = pickle.load(
        open(f"{id.model_path}Model_LR_calibration_xgb.pkl", "rb")
    )

    combination_train["Calib_PD"] = model_calib.predict(
        sm.add_constant(combination_train["comb_score"])
    )

    # combination_train.to_csv("XG_combined_op.csv", index=False)
