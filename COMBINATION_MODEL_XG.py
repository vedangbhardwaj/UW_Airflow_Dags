import pickle
import numpy as np
import pandas as pd
import snowflake.connector
import statsmodels.api as sm
from airflow.models import Variable
from snowflake.connector.pandas_tools import pd_writer
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine

### importing sql queries declaration ##
from sql_queries import Get_query
import boto3

missing_value_num = -99999  ### missing value assignment
missing_value_cat = "missing"
start_date = "2021-08-01"  ### Start date of modelling sample data
end_date = "2022-07-31"  ### End date of modelling sample data
partition_date = "2022-06-30"  ## Train and OOT partition date
IV_threshold = 0.0149  ### threshold for IV (IV should be accepted
var_threshold = (
    0.75  ### 75% of variantion in the features gets captured with PCA components
)

model_path = "underwriting_assets/combination_model_xg/models/"

config = Variable.get("underwriting_dags", deserialize_json=True)
s3 = boto3.resource("s3")
s3_bucket = config["s3_bucket"]

conn = snowflake.connector.connect(
    user=config["user"],
    password=config["password"],
    account=config["account"],
    role=config["role"],
    warehouse=config["warehouse"],
    database=config["database"],
)
cur = conn.cursor()


def truncate_table(identifier, dataset_name):
    sql_cmd = f"TRUNCATE TABLE IF EXISTS analytics.kb_analytics.airflow_demo_write_{identifier}_{dataset_name}"
    cur.execute(sql_cmd)
    return


def write_to_snowflake(data, identifier, dataset_name):
    data1 = data.copy()
    from sqlalchemy.types import (
        Boolean,
        Date,
        DateTime,
        Float,
        Integer,
        Interval,
        Text,
        Time,
    )

    dtype_dict = data1.dtypes.apply(lambda x: x.name).to_dict()
    for i in dtype_dict:
        if dtype_dict[i] == "datetime64[ns]":
            dtype_dict[i] = DateTime
        if dtype_dict[i] == "object":
            dtype_dict[i] = Text
        if dtype_dict[i] == "category":
            dtype_dict[i] = Text
        if dtype_dict[i] == "float64":
            dtype_dict[i] = Float
        if dtype_dict[i] == "float32":
            dtype_dict[i] = Float
        if dtype_dict[i] == "int64":
            dtype_dict[i] = Integer
    dtype_dict
    engine = create_engine(
        URL(
            account=config["account"],
            user=config["user"],
            password=config["password"],
            database=config["database"],
            schema=config["schema"],
            warehouse=config["warehouse"],
            role=config["role"],
        )
    )

    # con = engine.raw_connection()
    data1.columns = map(lambda x: str(x).upper(), data1.columns)
    name = f"airflow_demo_write_{identifier}_{dataset_name.lower()}"
    data1.to_sql(
        name=name,
        con=engine,
        if_exists="replace",
        index=False,
        index_label=None,
        dtype=dtype_dict,
        method=pd_writer,
    )
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
        (34 / 100) * combination_train["trx_logodds"]
        + (35 / 100) * combination_train["br_logodds"]
        + (31 / 100) * combination_train["act_logodds"]
    )

    combination_train["PD_score"] = 1 / (1 + np.exp(-combination_train["comb_score"]))

    # model_calib = pickle.load(
    #     open(f"{id.model_path}Model_LR_calibration_xgb.pkl", "rb")
    # )
    model_calib = pickle.loads(
        s3.Bucket(s3_bucket)
        .Object(f"{model_path}Model_LR_calibration_xgb.pkl")
        .get()["Body"]
        .read()
    )

    combination_train["Calib_PD"] = model_calib.predict(
        sm.add_constant(combination_train["comb_score"])
    )

    # combination_train.to_csv("XG_combined_op.csv", index=False)
    # cur.close()
    # conn.close()
    truncate_table("final_result", dataset_name.lower())
    write_to_snowflake(combination_train, "final_result", dataset_name.lower())
    return
