# uncomment os env variables
import logging
import boto3
import pandas as pd
import snowflake.connector
import logging
from datetime import datetime, timedelta
from airflow.models import Variable
from snowflake.connector.pandas_tools import pd_writer
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
import sys
from uw_utility_functions import write_to_snowflake
import os
from uw_sql_queries import Get_query
import copy
from sklearn.metrics import roc_curve, auc
from itertools import product
from uw_utility_functions import (
    write_to_snowflake,
    ks,
    calculate_psi_cat,
    calculate_psi_num,
)
import numpy as np

config = Variable.get("underwriting_dags", deserialize_json=True)
s3 = boto3.resource("s3")
s3_bucket = config["s3_bucket"]
# sys.path.append(
#     "/Users/vedang.bhardwaj/Desktop/work_mode/airflow_learn/UW_Airflow_Dags"
# )

conn = snowflake.connector.connect(
    user=config["user"],
    password=config["password"],
    account=config["account"],
    # user=os.environ.get("SNOWFLAKE_UNAME"),
    # password=os.environ.get("SNOWFLAKE_PASS"),
    # account=os.environ.get("SNOWFLAKE_ACCOUNT"),
    role=config["role"],
    warehouse=config["warehouse"],
    database=config["database"],
)
cur = conn.cursor()

## PSI calculation


def truncate_table(identifier, dataset_name):
    sql_cmd = f"TRUNCATE TABLE IF EXISTS analytics.kb_analytics.airflow_demo_write_{identifier}_{dataset_name}"
    try:
        cur.execute(sql_cmd)
    except Exception as error:
        logging.info(f"Error on truncate_table:{error}")
    return


def calculate_PSI(dataset_name):
    def var_type(var1):
        if var1 in cat_col:
            return "Categorical"
        elif var1 in num_col:
            return "Numerical"
        else:
            return "Others"

    def missing_ind_convert_num(df):
        for var in df.columns:
            if var_type(var) == "Numerical":
                df[var] = pd.to_numeric(df[var])
                df[var] = df[var].fillna(missing_value_num)
        for var in df.columns:
            if var_type(var) == "Categorical":
                df[var] = df[var].fillna(missing_value_cat)
                df[var] = df[var].replace("--", missing_value_cat)
                df[var] = pd.Categorical(df[var])
        return df

    def PSI_calculation(Train, OOT, module, month):
        # psi def- Train, OOT, missing_value_num, cat_col, num_col, module, month
        # missing_ind def - Train, missing_value_num, missing_value_cat, cat_col, num_col
        psi_list = []
        feature_list = []
        list_psi = {}
        Train = missing_ind_convert_num(Train)
        OOT = missing_ind_convert_num(OOT)
        for var in Train.columns:
            if var_type(var) == "Numerical":
                psi_t = calculate_psi_num(Train[var], OOT[var])
                psi_list.append(psi_t)
                feature_list.append(var)
            elif var_type(var) == "Categorical":
                psi_t = calculate_psi_cat(Train[var], OOT[var])
                psi_list.append(psi_t)
                feature_list.append(var)

        list_psi["COL_MODULE"] = module.upper()
        list_psi["PSI_MONTH"] = month.upper()
        list_psi["PSI_VAR"] = feature_list
        list_psi["PSI"] = psi_list
        psi_df = pd.DataFrame(list_psi)
        return psi_df

    def get_data():
        sql_query = Get_query(dataset_name).get_gini_table
        data = pd.read_sql(sql_query, con=conn)
        if len(data) == 0:
            raise ValueError("Data shape not correct")
        return data

    def read_file(bucket_name, file_name):
        obj = s3.meta.client.get_object(Bucket=bucket_name, Key=file_name)
        return obj["Body"]

    modules = [
        # ["CALIB_PD", True],
        ["activity_module", "ACT_PRED", "IS_ACTIVITY_FEATURES_AVAILABLE"],
        ["transaction_module", "TXN_PRED", "IS_LEDGER_FEATURES_AVAILABLE"],
        ["bureau_module", "BR_PRED", "IS_BUREAU_FEATURES_AVAILBALE"],
        # ["SCORE_CD_V2_EMSEMBLE_PROBABILITY", True],
    ]

    oot_data = get_data()
    oot_data = oot_data.loc[oot_data["MODEL_TYPE"] == "XGBOOST", :]
    months = list(oot_data["MONTH_YEAR"].value_counts().keys().values)
    frames = pd.DataFrame()
    for value in modules:
        module = value[0]
        col = value[1]
        col_check = value[2]
        module_data = copy.deepcopy(
            oot_data[
                (oot_data[f"{col}"].notnull()) & (oot_data[f"{col_check}"] == True)
            ]
        )
        config_var = Variable.get("underwriting_dags", deserialize_json=True)[
            f"{module}"
        ]
        train_data = pd.read_csv(f"debug/{module}_train_data_with_Scores.csv")
        missing_value_num = config_var["missing_value_num"]
        missing_value_cat = config_var["missing_value_cat"]
        input_path = config_var["input_path"]
        feature_list = pd.read_csv(
            read_file(s3_bucket, input_path + f"KB_{module}_variables.csv")
        )
        cat_col = list(feature_list["variables"][feature_list["Type"] == "Categorical"])
        num_col = list(feature_list["variables"][feature_list["Type"] == "Numerical"])
        module_psi = PSI_calculation(
            train_data,
            module_data,
            # missing_value_num,
            # cat_col,
            # num_col,
            module,
            month="OVERALL",
        )
        frames = frames.append(module_psi)
        for month in months:
            module_data_month = module_data.groupby(["MONTH_YEAR"])
            module_data_month = module_data_month.get_group(f"{month}")
            
            print(f"***************{module}***************")
            print(f"***************{module_data.shape}***************")
            print(f"***************{month}***************")
            print(f"***************{module_data_month.shape}***************")
            
            module_psi = PSI_calculation(
                train_data,
                module_data_month,
                # missing_value_num,
                # cat_col,
                # num_col,
                module,
                month=month,
            )
            frames = frames.append(module_psi)

    frames.reset_index(inplace=True, drop=True)
    truncate_table("psi_results", dataset_name.lower())
    write_to_snowflake(frames, "psi_results", dataset_name.lower())


calculate_PSI("MASTER_TABLE_GENERATION")

# frames.head(200).iloc[:,-3:]
# len(frames.PSI_VAR.unique())
# frames.shape


module_data = copy.deepcopy(
    oot_data[
    (oot_data[f"TXN_PRED"].notnull()) & (oot_data[f"IS_LEDGER_FEATURES_AVAILABLE"] == True)
    ]
)
module_data[module_data["MONTH_YEAR"] == "Jul-2022"]
module_data.shape

module_data[module_data["CUSTOMER_ID"]=="074c0d80-fc4d-4e54-8fd9-a8c09e86d0ac"]["TXN_PRED"]


oot_data.MONTH_YEAR.value_counts()

t2 = module_data[module_data["MONTH_YEAR"] == "Jul-2022"]
t2.shape
len(oot_data.loc[oot_data["MONTH_YEAR"] == "SEP-2022", :])


jul_month = txn_df.loc[(txn_df["PSI_MONTH"] == "JUL-2022")]
jul_month.shape

t1 = frames.groupby(["COL_MODULE"])
txn_df = t1.get_group("BUREAU_MODULE")
txn_df.PSI_MONTH.value_counts()

qc_check = txn_df.loc[
    (txn_df["PSI_MONTH"] == "JUL-2022"),:
]

q1 = qc_check.loc[qc_check["PSI"]<0.1,:]
q1.shape

q2 = qc_check.loc[(qc_check["PSI"]>=0.1)&(qc_check["PSI"]<=0.25),:]
q2

q3 = qc_check.loc[qc_check["PSI"]>0.25,:]
q3.shape

qc_check.to_csv('debug/psi_qc_br_jul.csv',index=False)


# oot_data.shape
# train_data = pd.read_csv('debug/Transaction_Module_train_data_with_Scores.csv')
# temp.shape
# temp[195:]
# frames = []
# frames.append(oot_data)
# frames.append(temp)
# temp1 = pd.concat(frames,axis=1)
