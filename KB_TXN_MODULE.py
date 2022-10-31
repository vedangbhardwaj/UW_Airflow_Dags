#### initial declarations
from xml.sax.handler import feature_external_ges
import pandas as pd
from sql_queries import Get_query
import re
import snowflake.connector
from datetime import date, datetime, timedelta
from snowflake.connector.pandas_tools import pd_writer
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
import builtins
from termcolor import colored
from sklearn import metrics
import scorecardpy as sc
import statsmodels.api as sm
import yaml
import pickle
import numpy as np

# Initial declaration
missing_value_num = -99999  ### missing value assignment
missing_value_cat = "missing"
start_date = "2021-08-01"  ### Start date of modelling sample data
end_date = "2022-07-31"  ### End date of modelling sample data
partition_date = "2022-06-30"  ## Train and OOT partition date
IV_threshold = 0.02  ### threshold for IV (IV should be accepted
var_threshold = (
    0.75  ### 75% of variantion in the features gets captured with PCA components
)

## To-Do consume files at run directly from s3.
ID_cols = ["USER_ID", "LOAN_ID", "DISBURSED_DATE", "BAD_FLAG"]
input_path = "/Users/vedang.bhardwaj/Desktop/work_mode/airflow_learn/UW_Airflow_Dags/KB_TXN_MODULE/data/raw/"
data_path = "/Users/vedang.bhardwaj/Desktop/work_mode/airflow_learn/UW_Airflow_Dags/KB_TXN_MODULE/data/"
model_path = "/Users/vedang.bhardwaj/Desktop/work_mode/airflow_learn/UW_Airflow_Dags/KB_TXN_MODULE/models/"
feature_list = pd.read_csv(input_path + "KB_transaction_module_variables.csv")

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


def getting_data(dataset_name, **context):
    def get_data(start_date, end_date):

        sql_cmd = Get_query(dataset_name).get_raw_data.format(
            sd=start_date, ed=end_date
        )
        cur.execute(sql_cmd)
        df = pd.DataFrame(cur.fetchall())
        colnames = [desc[0] for desc in cur.description]
        df.columns = [i for i in colnames]

        return df

    def write_to_snowflake(data, module_name=dataset_name):
        data1 = data.copy()
        from sqlalchemy.types import (
            Boolean,
            Date,
            DateTime,
            Float,
            Integer,
            Text,
            Time,
            Interval,
        )

        dtype_dict = data1.dtypes.apply(lambda x: x.name).to_dict()
        for i in dtype_dict:
            if dtype_dict[i] == "datetime64[ns]":
                dtype_dict[i] = DateTime
            if dtype_dict[i] == "object":
                dtype_dict[i] = Text
            if dtype_dict[i] == "float64":
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
        data1.to_sql(
            f"airflow_demo_write_transformed_{module_name}",
            engine,
            if_exists="replace",
            index=False,
            index_label=None,
            dtype=dtype_dict,
            method=pd_writer,
        )
        return

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
    # print(data.memory_usage(deep=True).sum())
    data = missing_ind_convert_num(data)
    write_to_snowflake(data)


def woe_calculation(dataset_name):
    def get_data():
        sql_query = Get_query(dataset_name).get_transformed_data
        data = pd.read_sql(sql_query, con=conn)
        return data

    def woe_Apply(data, final_bin1):
        new_bin = final_bin1[final_bin1.columns[0:13]]
        data_w = sc.woebin_ply(data, new_bin)
        data_w_features = data_w.filter(regex="_woe$", axis=1)
        data_w_bad = data_w["BAD_FLAG"]

        data_woe = pd.concat([data_w_bad, data_w_features], axis=1)
        return data_woe

    def write_to_snowflake(data, module_name=dataset_name):
        data1 = data.copy()
        from sqlalchemy.types import (
            Boolean,
            Date,
            DateTime,
            Float,
            Integer,
            Text,
            Time,
            Interval,
        )

        dtype_dict = data1.dtypes.apply(lambda x: x.name).to_dict()
        for i in dtype_dict:
            if dtype_dict[i] == "datetime64[ns]":
                dtype_dict[i] = DateTime
            if dtype_dict[i] == "object":
                dtype_dict[i] = Text
            if dtype_dict[i] == "float64":
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

        con = engine.raw_connection()
        data1.columns = map(lambda x: str(x).upper(), data1.columns)
        data1.to_sql(
            f"airflow_demo_write_transformed_woe_{module_name}",
            engine,
            if_exists="replace",
            index=False,
            index_label=None,
            dtype=dtype_dict,
            method=pd_writer,
        )
        return

    data = get_data()
    Final_bin_gini = pd.read_csv(
        "/Users/vedang.bhardwaj/Desktop/work_mode/airflow_learn/UW_Airflow_Dags/KB_TXN_MODULE/data/Final_bin_gini_performance.csv"
    )
    data_woe = woe_Apply(data, Final_bin_gini)
    write_to_snowflake(data_woe)


def model_prediction(dataset_name):
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

    def write_to_snowflake(data, module_name=dataset_name):
        data1 = data.copy()
        from sqlalchemy.types import (
            Boolean,
            Date,
            DateTime,
            Float,
            Integer,
            Text,
            Time,
            Interval,
        )

        dtype_dict = data1.dtypes.apply(lambda x: x.name).to_dict()
        for i in dtype_dict:
            if dtype_dict[i] == "datetime64[ns]":
                dtype_dict[i] = DateTime
            if dtype_dict[i] == "object":
                dtype_dict[i] = Text
            if dtype_dict[i] == "float64":
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
        data1.to_sql(
            f"airflow_demo_write_result_{module_name}",
            engine,
            if_exists="replace",
            index=False,
            index_label=None,
            dtype=dtype_dict,
            method=pd_writer,
        )
        return

    data = get_data()
    data_woe = get_data_woe()

    model_perf2 = pd.read_csv(
        "/Users/vedang.bhardwaj/Desktop/work_mode/airflow_learn/UW_Airflow_Dags/KB_TXN_MODULE/data/Model_selected.csv"
    )
    Top_models = [141553]
    j = 141553

    # for j in Top_models:
    Final_model_vars = list(model_perf2["variable"][model_perf2["Model_no"] == j])
    Final_model_vars = Final_model_vars[1 : len(Final_model_vars)]
    Final_model_vars = [str(x).upper() for x in Final_model_vars]

    Final_scoring_data = pd.concat([data[ID_cols], data_woe[Final_model_vars]], axis=1)
    Final_scoring_data.shape
    pred_data = Final_scoring_data[Final_model_vars]
    # adding has_constant to add_constant col
    pred_data = sm.add_constant(pred_data)
    # pred_data = sm.add_constant(pred_data, has_constant="add")
    pickled_model = pickle.load(
        open(
            f"/Users/vedang.bhardwaj/Desktop/work_mode/airflow_learn/UW_Airflow_Dags/KB_TXN_MODULE/models/Model_{j}.pkl",
            "rb",
        )
    )

    Final_scoring_data["pred_train"] = pickled_model.predict(pred_data)

    write_to_snowflake(Final_scoring_data)


def xgboost_model_prediction(dataset_name):
    def get_data(start_date, end_date):
        sql_cmd = Get_query(dataset_name).get_raw_data.format(
            sd=start_date, ed=end_date
        )
        cur.execute(sql_cmd)
        df = pd.DataFrame(cur.fetchall())
        colnames = [desc[0] for desc in cur.description]
        df.columns = [i for i in colnames]
        return df

    def write_to_snowflake(data, module_name="kb_txn_module"):
        data1 = data.copy()
        from sqlalchemy.types import (
            Boolean,
            Date,
            DateTime,
            Float,
            Integer,
            Text,
            Time,
            Interval,
        )

        dtype_dict = data1.dtypes.apply(lambda x: x.name).to_dict()
        for i in dtype_dict:
            if dtype_dict[i] == "datetime64[ns]":
                dtype_dict[i] = DateTime
            if dtype_dict[i] == "object":
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
        data1.to_sql(
            f"airflow_demo_write_result_xgb_{module_name}",
            engine,
            if_exists="replace",
            index=False,
            index_label=None,
            dtype=dtype_dict,
            method=pd_writer,
        )
        return

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

    XGB_keep_var_list = pd.read_csv(f"{model_path}XGBoost_feature_list.csv")
    keep_var_list = list(XGB_keep_var_list["variables"])

    data1 = data[keep_var_list]

    pickled_model = pickle.load(
        open(
            f"{model_path}Model_xgb.pkl",
            "rb",
        )
    )

    data["pred_train"] = pickled_model.predict_proba(data1)[:, 1]

    data["logodds_score"] = np.log(data["pred_train"] / (1 - data["pred_train"]))

    model_xgb_calib = pickle.load(
        open(
            f"{model_path}Model_LR_calibration_xgb.pkl",
            "rb",
        )
    )
    data["pred_train_xgb"] = model_xgb_calib.predict(
        sm.add_constant(data["logodds_score"])
    )
    write_to_snowflake(data)