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
from uw_utility_functions import write_to_snowflake,ks, calculate_psi_cat, calculate_psi_num
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

def truncate_table(identifier, dataset_name):
    sql_cmd = f"TRUNCATE TABLE IF EXISTS analytics.kb_analytics.airflow_demo_write_{identifier}_{dataset_name}"
    try:
        cur.execute(sql_cmd)
    except Exception as error:
        logging.info(f"Error on truncate_table:{error}")
    return


def merge_repayments_data(dataset_name):
    from uw_sql_queries import Get_query

    def merge_data():
        sql_query = Get_query(dataset_name).merge_master_table
        data = pd.read_sql(sql_query, con=conn)
        if len(data) == 0:
            raise ValueError("Data shape not correct")

    merge_data()
    return


def calculate_gini_score(dataset_name):
    def get_data(dataset_name):
        sql_query = Get_query(dataset_name).get_master_table
        data = pd.read_sql(sql_query, con=conn)
        if len(data) == 0:
            raise ValueError("Data shape not correct")
        return data

    data = get_data(dataset_name)
    data.shape
    data["MONTH_YEAR"] = pd.to_datetime(data["DISBURSED_DATE"]).dt.strftime("%b-%Y")
    model_types = list(data["MODEL_TYPE"].value_counts().keys().values)
    months = list(data["MONTH_YEAR"].value_counts().keys().values)
    comb_model_month = list(product(months, model_types))
    gini_cols = [
        ["CALIB_PD", True],
        ["ACT_PRED", "IS_ACTIVITY_FEATURES_AVAILABLE"],
        ["TXN_PRED", "IS_LEDGER_FEATURES_AVAILABLE"],
        ["BR_PRED", "IS_BUREAU_FEATURES_AVAILBALE"],
        ["SCORE_CD_V2_EMSEMBLE_PROBABILITY", True],
    ]

    for columns in gini_cols:
        col = columns[0]
        col_check = columns[1]
        for model in model_types:
            data1 = copy.deepcopy(data)
            data1 = data1.loc[
                pd.to_datetime(data1[f"DISBURSED_DATE"]).dt.strftime("%Y-%m-%d")
                >= "2022-08-01",
                :,
            ]
            data1 = data1.groupby(["MODEL_TYPE"])
            data1 = data1.get_group((f"{model}"))
            if col == "CALIB_PD" or col == "SCORE_CD_V2_EMSEMBLE_PROBABILITY":
                data1 = data1[(data1[f"{col}"].notnull())]
            else:
                data1 = data1[
                    (data1[f"{col}"].notnull()) & (data1[f"{col_check}"] == True)
                ]

            fpr, tpr, thresholds = roc_curve(
                data1["EVER_15DPD_IN_90DAYS"], data1[f"{col}"]
            )
            roc_auc = auc(fpr, tpr)
            GINI_score = (2 * roc_auc) - 1
            data1[f"{col}_GINI_OVERALL"] = GINI_score
            data.loc[data.index.isin(data1.index), [f"{col}_GINI_OVERALL"]] = data1[
                [f"{col}_GINI_OVERALL"]
            ].values
        for combinations in comb_model_month:
            month = combinations[0]
            model = combinations[1]
            df = data.groupby(["MONTH_YEAR", "MODEL_TYPE"])
            df = df.get_group((f"{month}", f"{model}"))
            if col == "CALIB_PD" or col == "SCORE_CD_V2_EMSEMBLE_PROBABILITY":
                df1 = df[df[f"{col}"].notnull()]
            else:
                df1 = df[df[f"{col}"].notnull() & (df[f"{col_check}"] == True)]
            # print(f"Subset dataframe shape **********************{df.shape}**********************")
            fpr, tpr, thresholds = roc_curve(df1["EVER_15DPD_IN_90DAYS"], df1[f"{col}"])
            roc_auc = auc(fpr, tpr)
            GINI_score = (2 * roc_auc) - 1
            df1[f"{col}_GINI"] = GINI_score
            # print(f"GINI score for {model} and {month} and {col} dataframe shape **********************{GINI_score}**********************")
            data.loc[data.index.isin(df1.index), [f"{col}_GINI"]] = df1[
                [f"{col}_GINI"]
            ].values
            if col != "SCORE_CD_V2_EMSEMBLE_PROBABILITY":
                if col == "BR_PRED":
                    df2 = df[
                        (df[f"{col}"].notnull())
                        & (df["SCORE_CD_V2_EMSEMBLE_PROBABILITY"].notnull())
                        & (df[f"{col_check}"] == True)
                    ]
                else:
                    df2 = df[
                        (df[f"{col}"].notnull())
                        & (df["SCORE_CD_V2_EMSEMBLE_PROBABILITY"].notnull())
                        # & (df[f"{col_check}"]==True)
                    ]
                fpr, tpr, thresholds = roc_curve(
                    df2["EVER_15DPD_IN_90DAYS"],
                    df2[f"SCORE_CD_V2_EMSEMBLE_PROBABILITY"],
                )
                roc_auc = auc(fpr, tpr)
                GINI_score = (2 * roc_auc) - 1
                df2[f"{col}_AND_FB_SCORE_GINI"] = GINI_score
                # print(f"GINI score for {model} and {month} and {col} dataframe shape **********************{GINI_score}**********************")
                data.loc[
                    data.index.isin(df2.index), [f"{col}_AND_FB_SCORE_GINI"]
                ] = df2[[f"{col}_AND_FB_SCORE_GINI"]].values

    truncate_table("verdict_with_gini_psi", dataset_name.lower())
    write_to_snowflake(data, "verdict_with_gini_psi", dataset_name.lower())


def calculate_ks_score(dataset_name):
    def get_data(dataset_name):
        sql_query = Get_query(dataset_name).get_master_table
        data = pd.read_sql(sql_query, con=conn)
        if len(data) == 0:
            raise ValueError("Data shape not correct")
        return data

    data = get_data(dataset_name)
    ks_score_cols = [
        "CALIB_PD",
        "ACT_PRED",
        "TXN_PRED",
        "BR_PRED",
    ]
    model_types = list(data["MODEL_TYPE"].value_counts().keys().values)
    df_result = pd.DataFrame(
        columns=[
            "PRED_COL",
            "Min_prob",
            "Max_prob",
            "Bads",
            "Goods",
            "Distribution Goods",
            "Distribution Bads",
            "%Cum_bads",
            "%Cum_goods",
            "%Cum_difference",
        ]
    )

    for pred_col in ks_score_cols:
        for model in model_types:
            data_subset = ks(
                data=data, target="EVER_15DPD_IN_90DAYS", prob=pred_col, model=model
            )
            prediction_col_name = pred_col + "_" + model
            data_subset["PRED_COL"] = prediction_col_name
            df_result = df_result.append(data_subset)

    truncate_table(df_result, "modules_ks_scores")
    write_to_snowflake(df_result, "modules_ks_scores", dataset_name.lower())

# data2 = calculate_gini_score("MASTER_TABLE_GENERATION")
# data2["DISBURSED_DATE"].max()

# data2 = data2.loc[
#     (pd.to_datetime(data2[f"DISBURSED_DATE"]).dt.strftime("%Y-%m-%d") >= "2022-08-01")
#     & (
#         pd.to_datetime(data2[f"DISBURSED_DATE"]).dt.strftime("%Y-%m-%d") <= "2022-08-30"
#     ),
#     :,
# ]
# data2.shape
# data3 = data2.loc[
#     (data2[f"BR_PRED"].notnull())
#     & (data2.IS_BUREAU_FEATURES_AVAILBALE == True)
#     & (data2["MODEL_TYPE"] == "LOGISTIC_REGRESSION"),
#     :,
# ]
# data3["EVER_15DPD_IN_90DAYS"].value_counts()

# data2[["CALIB_PD_GINI", "MODEL_TYPE"]].value_counts()

# ## testing
# # data.sample(50)
# # data.shape
# # data.CALIB_PD_GINI_OVERALL.value_counts()
# # data.SCORE_CD_V2_EMSEMBLE_PROBABILITY_GINI.value_counts()
# # data = data.merge(df[["CALIB_PD_GINI","CUSTOMER_ID","BRE_RUN_ID","MODEL_TYPE"]], on =["CUSTOMER_ID","BRE_RUN_ID","MODEL_TYPE"], how='left')

# # validate = data.loc[(data.MODEL_TYPE=="XGBOOST")&(data.MONTH_YEAR=="Jul-2022")&(data["CALIB_PD_GINI"].notnull()),:]
# # validate.shape


# def get_data(dataset_name):
#     sql_query = Get_query(dataset_name).get_master_table
#     data = pd.read_sql(sql_query, con=conn)
#     if len(data) == 0:
#         raise ValueError("Data shape not correct")
#     return data


# data = get_data("MASTER_TABLE_GENERATION")

# data["DISBURSED_DATE"].max()

# data["MONTH_YEAR"] = pd.to_datetime(data["DISBURSED_DATE"]).dt.strftime("%b-%Y")
# model_types = list(data["MODEL_TYPE"].value_counts().keys().values)
# months = list(data["MONTH_YEAR"].value_counts().keys().values)
# comb_model_month = list(product(months, model_types))
# gini_cols = [
#     ["CALIB_PD", True],
#     ["ACT_PRED", "IS_ACTIVITY_FEATURES_AVAILABLE"],
#     ["TXN_PRED", "IS_LEDGER_FEATURES_AVAILABLE"],
#     ["BR_PRED", "IS_BUREAU_FEATURES_AVAILBALE"],
#     ["SCORE_CD_V2_EMSEMBLE_PROBABILITY", True],
# ]

# for columns in gini_cols:
#     col = columns[0]
#     col_check = columns[1]
#     for model in model_types:
#         data1 = copy.deepcopy(data)
#         data1 = data1.loc[
#             pd.to_datetime(data1[f"DISBURSED_DATE"]).dt.strftime("%Y-%m-%d")
#             >= "2022-08-01",
#             :,
#         ]
#         data1 = data1.groupby(["MODEL_TYPE"])
#         data1 = data1.get_group((f"{model}"))
#         if col == "CALIB_PD" or col == "SCORE_CD_V2_EMSEMBLE_PROBABILITY":
#             data1 = data1[(data1[f"{col}"].notnull())]
#         else:
#             data1 = data1[(data1[f"{col}"].notnull()) & (data1[f"{col_check}"] == True)]
#         fpr, tpr, thresholds = roc_curve(data1["EVER_15DPD_IN_90DAYS"], data1[f"{col}"])
#         roc_auc = auc(fpr, tpr)
#         GINI_score = (2 * roc_auc) - 1
#         data1[f"{col}_GINI_OVERALL"] = GINI_score
#         data.loc[data.index.isin(data1.index), [f"{col}_GINI_OVERALL"]] = data1[
#             [f"{col}_GINI_OVERALL"]
#         ].values
#     for combinations in comb_model_month:
#         month = combinations[0]
#         model = combinations[1]
#         df = data.groupby(["MONTH_YEAR", "MODEL_TYPE"])
#         df = df.get_group((f"{month}", f"{model}"))
#         df1 = df[df[f"{col}"].notnull()]
#         # print(f"Subset dataframe shape **********************{df.shape}**********************")
#         fpr, tpr, thresholds = roc_curve(df1["EVER_15DPD_IN_90DAYS"], df1[f"{col}"])
#         roc_auc = auc(fpr, tpr)
#         GINI_score = (2 * roc_auc) - 1
#         df1[f"{col}_GINI"] = GINI_score
#         # print(f"GINI score for {model} and {month} and {col} dataframe shape **********************{GINI_score}**********************")
#         data.loc[data.index.isin(df1.index), [f"{col}_GINI"]] = df1[
#             [f"{col}_GINI"]
#         ].values
#         if col != "SCORE_CD_V2_EMSEMBLE_PROBABILITY":
#             df2 = df[
#                 (df[f"{col}"].notnull())
#                 & (df["SCORE_CD_V2_EMSEMBLE_PROBABILITY"].notnull())
#             ]
#             fpr, tpr, thresholds = roc_curve(
#                 df2["EVER_15DPD_IN_90DAYS"], df2[f"SCORE_CD_V2_EMSEMBLE_PROBABILITY"]
#             )
#             roc_auc = auc(fpr, tpr)
#             GINI_score = (2 * roc_auc) - 1
#             df2[f"{col}_AND_FB_SCORE_GINI"] = GINI_score
#             # print(f"GINI score for {model} and {month} and {col} dataframe shape **********************{GINI_score}**********************")
#             data.loc[data.index.isin(df2.index), [f"{col}_AND_FB_SCORE_GINI"]] = df2[
#                 [f"{col}_AND_FB_SCORE_GINI"]
#             ].values

# ## individual trials
# data = get_data("MASTER_TABLE_GENERATION")
# data.shape
# data["DISBURSED_DATE"].max()

# data1 = copy.deepcopy(data)
# data1.shape
# data1.columns.to_list()

# data1 = data1.loc[
#     (pd.to_datetime(data1[f"DISBURSED_DATE"]).dt.strftime("%Y-%m-%d") >= "2022-07-01")
#     & (
#         pd.to_datetime(data1[f"DISBURSED_DATE"]).dt.strftime("%Y-%m-%d") <= "2022-07-31"
#     ),
#     :,
# ]

# data1.shape

# data1 = data1.groupby(["MODEL_TYPE"])
# data1 = data1.get_group((f"XGBOOST"))
# data1.shape
# data1 = data1.loc[
#     (data1[f"BR_PRED"].notnull()) & (data.IS_BUREAU_FEATURES_AVAILBALE == True), :
# ]
# data1.shape

# fpr, tpr, thresholds = roc_curve(data1["EVER_15DPD_IN_90DAYS"], data1["BR_PRED"])
# roc_auc = auc(fpr, tpr)
# GINI_score = (2 * roc_auc) - 1
# data1[f"BR_PRED_GINI"] = GINI_score
# data.loc[data.index.isin(data1.index), [f"BR_PRED_GINI"]] = data1[
#     [f"BR_PRED_GINI"]
# ].values

# data["BR_PRED_GINI"].value_counts()

# data1.drop(["ACT_PRED_GINI"], axis=1, inplace=True)
# data1.columns.to_list()[-1]
# data1["EVER_15DPD_IN_90DAYS"].value_counts()

# data1[["LOAN_ID", "BR_PRED", "EVER_15DPD_IN_90DAYS"]].to_csv(
#     "debug/br_pred_qc_jul.csv", index=False
# )


# data1.loc[data1["LOAN_ID"] == 25987, "FULL_SETTLED_TRXNS_3_BY_6"]


# for combinations in comb_model_month:
#     month = combinations[0]
#     model = combinations[1]
#     df = data.groupby(["MONTH_YEAR", "MODEL_TYPE"])
#     df = df.get_group((f"{month}", f"{model}"))
#     df1 = df[df[f"{col}"].notnull()]
#     # print(f"Subset dataframe shape **********************{df.shape}**********************")
#     fpr, tpr, thresholds = roc_curve(df1["EVER_15DPD_IN_90DAYS"], df1[f"{col}"])
#     roc_auc = auc(fpr, tpr)
#     GINI_score = (2 * roc_auc) - 1
#     df1[f"{col}_GINI"] = GINI_score
#     # print(f"GINI score for {model} and {month} and {col} dataframe shape **********************{GINI_score}**********************")
#     data.loc[data.index.isin(df1.index), [f"{col}_GINI"]] = df1[[f"{col}_GINI"]].values
#     if col != "SCORE_CD_V2_EMSEMBLE_PROBABILITY":
#         df2 = df[
#             (df[f"{col}"].notnull())
#             & (df["SCORE_CD_V2_EMSEMBLE_PROBABILITY"].notnull())
#         ]
#         fpr, tpr, thresholds = roc_curve(
#             df2["EVER_15DPD_IN_90DAYS"], df2[f"SCORE_CD_V2_EMSEMBLE_PROBABILITY"]
#         )
#         roc_auc = auc(fpr, tpr)
#         GINI_score = (2 * roc_auc) - 1
#         df2[f"{col}_AND_FB_SCORE_GINI"] = GINI_score
#         # print(f"GINI score for {model} and {month} and {col} dataframe shape **********************{GINI_score}**********************")
#         data.loc[data.index.isin(df2.index), [f"{col}_AND_FB_SCORE_GINI"]] = df2[
#             [f"{col}_AND_FB_SCORE_GINI"]
#         ].values

# data.head(20)
# data.TXN_PRED_AND_FB_SCORE_GINI.value_counts()
# data.BR_PRED_GINI.value_counts()
# data.shape
# # truncate_table("verdict_with_gini_psi", dataset_name.lower())
# # write_to_snowflake(data, "verdict_with_gini_psi", "MASTER_TABLE_GENERATION".lower())

# # len = data.loc[(pd.to_datetime(data[f"DISBURSED_DATE"]).dt.strftime("%Y-%m-%d") >="2022-06-01")
# #                 &(pd.to_datetime(data[f"DISBURSED_DATE"]).dt.strftime("%Y-%m-%d") <="2022-09-06")
# #                 ,:]
# # len.shape
# # validate = data.loc[(data.MODEL_TYPE=="XGBOOST")&(data.MONTH_YEAR=="Aug-2022")
# #                     &(data["ACT_PRED_GINI_OVERALL"].notnull())
# #                     # &(data["TXN_PRED"].notnull())
# #                     ,:]
# # validate.shape
# # data.SCORE_CD_V2_EMSEMBLE_PROBABILITY_GINI_OVERALL.value_counts()

# df = pd.read_csv(
#     "/Users/vedang.bhardwaj/Desktop/work_mode/Underwriting_Model/15NOV_S3_COPY/txn_module/data/Model_selected.csv",
#     warn_bad_lines=False,
# )
# df1 = df[df["Model_no"] == 19057]
# list(df["variable"].unique())


    
