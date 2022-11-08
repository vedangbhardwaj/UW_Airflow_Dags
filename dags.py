import logging
import boto3
import pandas as pd
import snowflake.connector
from airflow.models import Variable
from snowflake.connector.pandas_tools import pd_writer
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine

logger = logging.getLogger("airflow.task")
logging.info("Program started ....")

missing_value_num = -99999  ### missing value assignment
missing_value_cat = "missing"
start_date = "2021-08-01"  ### Start date of modelling sample data
end_date = "2022-07-31"  ### End date of modelling sample data
partition_date = "2022-06-30"  ## Train and OOT partition date
IV_threshold = 0.015  ### threshold for IV (IV should be accepted
var_threshold = (
    0.90  ### 75% of variantion in the features gets captured with PCA components
)

ID_cols = ["USER_ID", "LOAN_ID", "DISBURSED_DATE", "BAD_FLAG"]
input_path = "underwriting_assets/bureau_module/data/raw/"
data_path = "underwriting_assets/bureau_module/data/"
model_path = "underwriting_assets/bureau_module/models/"

config = Variable.get("underwriting_dags", deserialize_json=True)
s3 = boto3.resource("s3")
s3_bucket = config["s3_bucket"]


def read_file(bucket_name, file_name):
    obj = s3.meta.client.get_object(Bucket=bucket_name, Key=file_name)
    return obj["Body"]

def truncate_table(identifier, dataset_name):
    sql_cmd = f"TRUNCATE TABLE IF EXISTS ANALYTICS.KB_ANALYTICS.airflow_demo_write_{identifier}_{dataset_name}"
    cur.execute(sql_cmd)
    return

def write_to_snowflake(data,identifier,dataset_name):
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
    name = f'airflow_demo_write_{identifier}_{dataset_name.lower()}'
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

feature_list = pd.read_csv(
    read_file(s3_bucket, input_path + "KB_bureau_module_variables.csv")
)

config = Variable.get("underwriting_dags", deserialize_json=True)

conn = snowflake.connector.connect(
    user=config["user"],
    password=config["password"],
    account=config["account"],
    role=config["role"],
    warehouse=config["warehouse"],
    database=config["database"],
    insecure_mode=True,
)
cur = conn.cursor()


def getting_data(dataset_name, **context):
    from sql_queries import Get_query

    def get_data(start_date, end_date):
        sql_cmd = Get_query(dataset_name).get_raw_data.format(
            sd=start_date, ed=end_date
        )
        cur.execute(sql_cmd)
        df = pd.DataFrame(cur.fetchall())
        colnames = [desc[0] for desc in cur.description]
        df.columns = [i for i in colnames]
        return df

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

    cat_col = list(feature_list["variables"][feature_list["Type"] == "Categorical"])
    num_col = list(feature_list["variables"][feature_list["Type"] == "Numerical"])
    data = get_data(start_date, end_date)
    data = missing_ind_convert_num(data)
    truncate_table("transformed", dataset_name.lower())
    write_to_snowflake(data,"transformed", dataset_name.lower())
    # cur.close() 
    # conn.close() 


def woe_calculation(dataset_name):
    import scorecardpy as sc
    from sql_queries import Get_query

    def get_data():
        sql_query = Get_query(dataset_name).get_transformed_data
        data = pd.read_sql(sql_query, con=conn)
        return data

    def woe_Apply(data, final_bin1):
        new_bin = final_bin1[final_bin1.columns[0:13]]
        data_w = sc.woebin_ply(data, new_bin)
        data_w_features = data_w.filter(regex="_woe$", axis=1)
        # data_w_bad = data_w["BAD_FLAG"]
        # data_woe = pd.concat([data_w_bad, data_w_features], axis=1)
        data_woe=data_w_features
        return data_woe

    data = get_data()
    Final_bin_gini = pd.read_csv(
        # "/Users/vedang.bhardwaj/Desktop/work_mode/airflow_learn/UW_Airflow_Dags/KB_BUREAU_MODULE/data/Final_bin_gini_performance.csv"
        read_file(s3_bucket, data_path + "Final_bin_gini_performance.csv")
    )
    data_woe = woe_Apply(data, Final_bin_gini)
    truncate_table("transformed_woe", dataset_name.lower())
    write_to_snowflake(data_woe,"transformed_woe", dataset_name.lower())
    # cur.close() 
    # conn.close() 


def model_prediction(dataset_name):
    import pickle
    import statsmodels.api as sm
    from sql_queries import Get_query

    def get_data():
        sql_query = Get_query(dataset_name).get_raw_data.format(
            sd=start_date, ed=end_date
        )
        data = pd.read_sql(sql_query, con=conn)
        return data

    def get_data_woe():
        sql_query = Get_query(dataset_name).get_transformed_woe_data
        data = pd.read_sql(sql_query, con=conn)
        return data

    data = get_data()
    data_woe = get_data_woe()

    model_perf2 = pd.read_csv(
        # f"{data_path}Model_selected.csv"
        read_file(s3_bucket, data_path + "Model_selected.csv")
    )
    Top_models = [25535]
    j = 25535
    # Keep_cols=['CUSTOMER_ID','DECISION_DATE','IS_FAIL_FLAG','ACCEPT_REJECT','LOAN_ID','DISBURSED_DATE','BAD_FLAG','DAYS_SINCE_LAST_TXN']

    # for j in Top_models:
    Final_model_vars = list(model_perf2["variable"][model_perf2["Model_no"] == j])
    Final_model_vars = Final_model_vars[1 : len(Final_model_vars)]
    Final_model_vars = [str(x).upper() for x in Final_model_vars]

    Final_scoring_data = pd.concat([data[ID_cols], data_woe[Final_model_vars]], axis=1)
    pred_data = Final_scoring_data[Final_model_vars]
    pred_data = sm.add_constant(pred_data)

    # pickled_model = pickle.load(
    #     open(
    #         f"{model_path}Model_{j}.pkl",
    #         "rb",
    #     )
    # )
    pickled_model = pickle.loads(
        s3.Bucket(s3_bucket).Object(f"{model_path}Model_{j}.pkl").get()["Body"].read()
    )

    # Final model prediction
    Final_scoring_data["pred_train"] = pickled_model.predict(pred_data)
    truncate_table("result", dataset_name.lower())
    write_to_snowflake(Final_scoring_data,"result", dataset_name.lower())


def xgboost_model_prediction(dataset_name):
    import pickle
    import numpy as np
    import statsmodels.api as sm
    from sql_queries import Get_query

    def get_data(start_date, end_date):
        sql_cmd = Get_query(dataset_name).get_raw_data.format(
            sd=start_date, ed=end_date
        )
        cur.execute(sql_cmd)
        df = pd.DataFrame(cur.fetchall())
        colnames = [desc[0] for desc in cur.description]
        df.columns = [i for i in colnames]
        return df

    cat_col = list(feature_list["variables"][feature_list["Type"] == "Categorical"])
    num_col = list(feature_list["variables"][feature_list["Type"] == "Numerical"])

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
        for var in df.columns:
            if var_type(var) == "Categorical":
                # df[var] = df[var].replace("--", missing_value_cat)
                df[var] = pd.Categorical(df[var])
        return df

    data = get_data(start_date, end_date)
    data = missing_ind_convert_num(data)

    XGB_keep_var_list = pd.read_csv(
        # f"{model_path}XGBoost_feature_list.csv"
        read_file(s3_bucket, model_path + "XGBoost_feature_list.csv")
    )
    keep_var_list = list(XGB_keep_var_list["variables"])
    data1 = data[keep_var_list]

    pickled_model = pickle.loads(
        s3.Bucket(s3_bucket).Object(f"{model_path}Model_xgb.pkl").get()["Body"].read()
    )

    data["pred_train"] = pickled_model.predict_proba(data1)[:, 1]
    data["logodds_score"] = np.log(data["pred_train"] / (1 - data["pred_train"]))

    model_xgb_calib = pickle.loads(
        s3.Bucket(s3_bucket)
        .Object(f"{model_path}Model_LR_calibration_xgb.pkl")
        .get()["Body"]
        .read()
    )
    data["pred_train_xgb"] = model_xgb_calib.predict(
        sm.add_constant(data["logodds_score"])
    )
    truncate_table("result_xgb", dataset_name.lower())
    write_to_snowflake(data,"result_xgb", dataset_name.lower())
    logging.info("Finished Model prediction 2")
    # cur.close() 
    # conn.close() 
