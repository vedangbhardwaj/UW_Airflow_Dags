import pickle
import boto3
import numpy as np
import pandas as pd
import snowflake.connector
import statsmodels.api as sm
import logging
from airflow.models import Variable
from snowflake.connector.pandas_tools import pd_writer
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from uw_sql_queries import Get_query
from uw_policy_rules_dag_v1 import apply_rules, policy_with_fb
from uw_utility_functions import write_to_snowflake
import os
import datetime

st = datetime.datetime.now()
def run():
    import time
    model_path = "underwriting_assets/combination_model_lr/models/"

    config = Variable.get("underwriting_dags", deserialize_json=True)
    s3 = boto3.resource("s3")
    s3_bucket = config["s3_bucket"]

    config_var = Variable.get("underwriting_dags", deserialize_json=True)[
        "combination_lr_weights"
    ]
    config_thresh = Variable.get("underwriting_dags", deserialize_json=True)[
        "logistic_reg_thresholds"
    ]

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


    def predict(dataset_name, **context):
        def get_data(module_name):
            sql_cmd = None
            if module_name == "KB_TXN_MODULE":
                sql_cmd = Get_query(dataset_name).get_txn_data
            if module_name == "KB_ACTIVITY_MODULE":
                sql_cmd = Get_query(dataset_name).get_activity_data
            if module_name == "KB_BUREAU_MODULE":
                sql_cmd = Get_query(dataset_name).get_bureau_data
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
            Transaction_module_data["PRED_TRAIN"]
            / (1 - Transaction_module_data["PRED_TRAIN"])
        )

        Activity_module_data["act_logodds"] = np.log(
            Activity_module_data["PRED_TRAIN"] / (1 - Activity_module_data["PRED_TRAIN"])
        )

        Bureau_module_data["br_logodds"] = np.log(
            Bureau_module_data["PRED_TRAIN"] / (1 - Bureau_module_data["PRED_TRAIN"])
        )

        # checking if all cols are available
        # def mask_with_values(df, col):
        #     mask = df[f"{col}"].values == True
        #     # print(f"shape before **************{df.shape} **************")
        #     # print(f"**************{df[mask].columns.tolist()}**************")
        #     # print(f"shape after **************{df[mask].shape} **************")
        #     return df[mask]

        # Transaction_module_data = mask_with_values(
        #     Transaction_module_data, "IS_LEDGER_FEATURES_AVAILABLE"
        # )
        # Activity_module_data = mask_with_values(
        #     Activity_module_data, "IS_ACTIVITY_FEATURES_AVAILABLE"
        # )
        # Bureau_module_data = mask_with_values(
        #     Bureau_module_data, "IS_BUREAU_FEATURES_AVAILBALE"
        # )

        Transaction_module_data.rename(columns={"PRED_TRAIN": "TXN_PRED"}, inplace=True)
        Activity_module_data.rename(columns={"PRED_TRAIN": "ACT_PRED"}, inplace=True)
        Bureau_module_data.rename(columns={"PRED_TRAIN": "BR_PRED"}, inplace=True)

        print(f"before merging AND condition ***********{Transaction_module_data.shape}***********")
        print(f"before merging AND condition ***********{Activity_module_data.shape}***********")
        print(f"before merging AND condition ***********{Bureau_module_data.shape}***********")

        data_merge = Activity_module_data[
            # ["USER_ID", "LOAN_ID", "DISBURSED_DATE", "BAD_FLAG", "act_logodds"]
            [
                "USER_ID",
                "BRE_RUN_ID",
                "DISBURSED_DATE",
                "ACT_PRED",
                "act_logodds",
                "IS_ACTIVITY_FEATURES_AVAILABLE",
            ]
        ].merge(
            Transaction_module_data[
                ["BRE_RUN_ID", "TXN_PRED", "trx_logodds", "IS_LEDGER_FEATURES_AVAILABLE"]
            ],
            on="BRE_RUN_ID",
            how="left",
        )

        data_merge = data_merge.merge(
            Bureau_module_data[
                ["BRE_RUN_ID", "BR_PRED", "br_logodds", "IS_BUREAU_FEATURES_AVAILBALE"]
            ],
            on="BRE_RUN_ID",
            how="left",
        )

        combination_train = data_merge.loc[
            (data_merge["IS_ACTIVITY_FEATURES_AVAILABLE"] == True)
            & (data_merge["IS_LEDGER_FEATURES_AVAILABLE"] == True)
            & (data_merge["IS_BUREAU_FEATURES_AVAILBALE"] == True),
            :,
        ]
        print(f" before merging AND condition ***********{data_merge.shape}***********")
        print(
            f" after merging AND condition ***********{combination_train.shape}***********"
        )
        combination_train = combination_train.dropna()
        print(
            f" combination train post dropping na ***********{combination_train.shape}***********"
        )

        txn_weight = config_var["txn_model_weight"] / 100
        bureau_weight = (config_var["bureau_model_weight"] / 100,)
        activity_weight = config_var["activity_model_weight"] / 100

        combination_train["comb_score"] = (
            (txn_weight * combination_train["trx_logodds"])
            + (bureau_weight * combination_train["br_logodds"])
            + (activity_weight * combination_train["act_logodds"])
        )

        combination_train["PD_score"] = 1 / (1 + np.exp(-combination_train["comb_score"]))

        model_calib = pickle.loads(
            s3.Bucket(s3_bucket)
            .Object(f"{model_path}Model_LR_calibration_LR.pkl")
            .get()["Body"]
            .read()
        )
        combination_train["CALIB_PD"] = model_calib.predict(
            sm.add_constant(combination_train["comb_score"])
        )
        # print(f" combination_train ***********{combination_train.shape}***********")
        # print(combination_train.columns.tolist())
        combination_train_final = data_merge.merge(
            combination_train[["comb_score", "PD_score", "CALIB_PD", "BRE_RUN_ID"]],
            on="BRE_RUN_ID",
            how="left",
        )
        print(
            f" combination_train final ***********{combination_train_final.shape}***********"
        )
        # print(combination_train_final.columns.tolist())
        truncate_table("final_result", dataset_name.lower())
        write_to_snowflake(combination_train_final, "final_result", dataset_name.lower())
        return


    def result_generation(dataset_name):
        from uw_sql_queries import Get_query

        def get_data():
            sql_query = Get_query(dataset_name).get_policy_run_gen_table
            data = pd.read_sql(sql_query, con=conn)
            if len(data) == 0:
                raise ValueError("Data shape not correct")
            return data

        def rule_engine_verdict(data):
            data["MODEL_TYPE"] = "LOGISTIC_REGRESSION"
            score_columns = [
                ("ACT_PRED", config_thresh["ACT_PRED"]),
                ("TXN_PRED", config_thresh["TXN_PRED"]),
                ("BR_PRED", config_thresh["BR_PRED"]),
                ("CALIB_PD", config_thresh["CALIB_PD"]),
            ]

            data["BUREAUSCORE"] = pd.to_numeric(data["BUREAUSCORE"])
            data["NTC_flag"] = [
                1 if (x < 300 or pd.isna(x)) else 0 for x in data["BUREAUSCORE"]
            ]
            # data['Rule1:MCA']=['PASS' if x>=5000 else 'FAIL' if x<5000 else 'NOT_DECIDED' for x in data['TOTAL_CREDIT_FOR_RULE']]
            data["Rule2_OVERALL_BOUNCE_M0123_rule_decision"] = [
                "PASS" if (x < 5) else "FAIL" if (x >= 5) else "NOT_DECIDED"
                for x in data["OVERALL_BOUNCE_M0123"]
            ]
            # data['Rule3:device_connect_rule']=['PASS' if (x<5) else 'FAIL' if (x>=5) else 'NOT_DECIDED'  for x in data['OVERALL_BOUNCE_M0123']]
            data["Rule4_AUTO_DEBIT_BOUNCE_rule_decision"] = [
                "PASS"
                if (x == "false" or y == "false")
                else "FAIL"
                if (x == "true" and y == "true")
                else "NOT_DECIDED"
                for (x, y) in zip(
                    data["AUTO_DEBIT_BOUNCE_M0"], data["AUTO_DEBIT_BOUNCE_M1"]
                )
            ]
            data["Rule5_WRITTEN_OFF_SETTLED_rule_decision"] = [
                "PASS" if x == 0 else "FAIL" if x > 0 else "NOT_DECIDED"
                for x in data["WRITTEN_OFF_SETTLED_24MONTHS_CNT_FOR_RULE"]
            ]
            data["Rule6_WRITTEN_OFF_AMT_TOTAL_FOR_RULE_decision"] = [
                "PASS" if x <= 500 else "FAIL" if x > 500 else "NOT_DECIDED"
                for x in data["WRITTEN_OFF_AMT_TOTAL_FOR_RULE"]
            ]
            data["Rule7_No_foll_suit_filed_default_RULE_decision"] = [
                "PASS" if x == 0 else "FAIL" if x > 0 else "NOT_DECIDED"
                for x in data["SUITFILED_WILFULDEFAULT_24MONTHS_FOR_RULE"]
            ]
            data["Rule8_No_foll_suit_filed_Written_OFF_RULE_decision"] = [
                "PASS" if x == 0 else "FAIL" if x > 0 else "NOT_DECIDED"
                for x in data["SUITFILEDWILLFULDEFAULTWRITTENOFFSTATUS_24MONTHS_FOR_RULE"]
            ]
            data["Rule9_MAX_DPD_2_YEARS_FOR_RULE"] = [
                "PASS" if x <= 30 else "FAIL" if x > 30 else "NOT_DECIDED"
                for x in data["MAX_DPD_2_YEARS_FOR_RULE"]
            ]
            data["Rule10_No_foll_PAYMENTHISTORYPROFILE_12MONTHS_FOR_RULE"] = [
                "PASS" if x == 0 else "FAIL" if x > 0 else "NOT_DECIDED"
                for x in data["PAYMENTHISTORYPROFILE_12MONTHS_FOR_RULE"]
            ]
            data["Rule11_No_foll_status_in_24M_rule"] = [
                "PASS" if (x == 0) else "FAIL" if x > 0 else "NOT_DECIDED"
                for x in data["NEGATIVEACCOUNSTATUS_24MONTHS_FOR_RULE"]
            ]
            data["Rule12_Experian_score"] = [
                "PASS"
                if x >= 700
                else "FAIL"
                if (x < 700 and x >= 300)
                else "NOT_DECIDED"
                if (x < 300 or pd.isna(x))
                else "NOT_DECIDED"
                for x in data["BUREAUSCORE"]
            ]
            data["Rule13_NTC_FIS_PD_score_rule_decision"] = [
                "PASS"
                if (x == 1 and y <= 0.0284)
                else "FAIL"
                if (x == 1 and y > 0.0284)
                else "NOT_DECIDED"
                for (x, y) in zip(
                    data["NTC_flag"], data["SCORE_CD_V2_EMSEMBLE_PROBABILITY"]
                )
            ]
            data["Rule14_FIS_PD_score_rule_decision"] = [
                "PASS" if (x <= 0.0354) else "FAIL" if (x > 0.0354) else "NOT_DECIDED"
                for x in data["SCORE_CD_V2_EMSEMBLE_PROBABILITY"]
            ]

            data[f"verdict_policy_alone"] = data.apply(apply_rules, axis=1)
            data[f"verdict_policy_with_fb"] = [
                "FAIL" if (x == "FAIL" or y == "FAIL" or z == "FAIL") else "PASS"
                for (x, y, z) in zip(
                    data["verdict_policy_alone"],
                    data["Rule14_FIS_PD_score_rule_decision"],
                    data["Rule13_NTC_FIS_PD_score_rule_decision"],
                )
            ]
            for val in score_columns:
                col = val[0]
                thresholds = val[1]
                count = 1
                for threshold in thresholds:
                    data[f"{col}_verdict_model_thresh_{count}"] = data[f"{col}"].apply(
                        lambda x: "FAIL"
                        if ~pd.isna(x) and x > threshold
                        else "NOT_DECIDED"
                        if pd.isna(x)
                        else "PASS"
                    )
                    data[f"{col}_verdict_policy_and_model_thresh_{count}"] = data.apply(
                        # lambda x: apply_rules(x, x[f"{col}"], threshold, 'model_and_policy'), axis=1
                        lambda x: "FAIL"
                        if x["verdict_policy_alone"] == "FAIL"
                        or x[f"{col}_verdict_model_thresh_{count}"] == "FAIL"
                        else "NOT_DECIDED"
                        if x[f"{col}_verdict_model_thresh_{count}"] == "NOT_DECIDED"
                        else "PASS",
                        axis=1,
                    )
                    count += 1
            return data

        data = get_data()

        verdict_data = rule_engine_verdict(data)
        truncate_table("verdict_with_thresh", dataset_name.lower())
        write_to_snowflake(verdict_data, "verdict_with_thresh", dataset_name.lower())
        return
    
    predict_st_time_wall = time.time()
    predict_st_cpu = time.process_time()
    predict("COMBINATION_MODEL_LR")
    predict_end_cpu = time.process_time()
    predict_end_time_wall = time.time()

    result_st_time_wall = time.time()
    result_st_cpu = time.process_time()
    result_generation("COMBINATION_MODEL_LR")
    result_end_time_cpu = time.process_time()
    result_end_time_wall = time.time()

    print(f"Getting data function execution - cpu time : {predict_end_cpu-predict_st_cpu} seconds ")
    print(f"Getting data function execution - cpu time : {result_end_time_cpu-result_st_cpu} seconds ")

    print(f"Getting data function execution - cpu time : {predict_end_time_wall-predict_st_time_wall} seconds ")
    print(f"Getting data function execution - cpu time : {result_end_time_wall-result_st_time_wall} seconds ")

et = datetime.datetime.now()
run()
elapsed_time = et - st
print('Combined module XGB script execution time:', elapsed_time, 'seconds') 

# def merge_master_table(dataset_name):
#     from uw_sql_queries import Get_query

#     def merge_data():
#         sql_query = Get_query(dataset_name).merge_master_table
#         data = pd.read_sql(sql_query, con=conn)
#         if len(data) == 0:
#             raise ValueError("Data shape not correct")

#     merge_data()
#     return