if __name__ == "__main__":
    ### initial declaration
    import Initial_declaration as idc

    ### sql queries
    import Sql_queries as sq
    import Getting_data as gd
    import pandas as pd
    import numpy as np
    import statsmodels.api as sm
    import pickle
    from termcolor import colored
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import roc_curve, auc
    import matplotlib.pyplot as plt
    import random
    import yaml

    # import seaborn as sns
    import scorecardpy as sc
    from sqlalchemy import create_engine
    import snowflake.connector
    from snowflake.connector.pandas_tools import pd_writer
    from snowflake.sqlalchemy import URL

    # Model and performance evaluation
    import statsmodels.api as sm
    from sklearn.metrics import roc_curve, auc

    with open("../airflow_config.yml") as config_file:
        config = yaml.full_load(config_file)

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

    def get_data(start_date, end_date):
        sql_cmd = sq.get_raw_data.format(sd=idc.start_date, ed=idc.end_date)
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

    cat_col = list(
        idc.feature_list["variables"][idc.feature_list["Type"] == "Categorical"]
    )
    num_col = list(
        idc.feature_list["variables"][idc.feature_list["Type"] == "Numerical"]
    )

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
                # df[var] = df[var].replace("--", idc.missing_value_cat)
                df[var] = pd.Categorical(df[var])
        return df

    data = get_data(idc.start_date, idc.end_date)
    data = missing_ind_convert_num(data)

    XGB_keep_var_list = pd.read_csv(f"{idc.model_path}XGBoost_feature_list.csv")
    keep_var_list = list(XGB_keep_var_list["variables"])

    data1 = data[keep_var_list]

    pickled_model = pickle.load(
        open(
            f"{idc.model_path}Model_xgb.pkl",
            "rb",
        )
    )

    data["pred_train"] = pickled_model.predict_proba(data1)[:, 1]

    data["logodds_score"] = np.log(data["pred_train"] / (1 - data["pred_train"]))

    model_xgb_calib = pickle.load(
        open(
            f"{idc.model_path}Model_LR_calibration_xgb.pkl",
            "rb",
        )
    )
    data["pred_train_xgb"] = model_xgb_calib.predict(
        sm.add_constant(data["logodds_score"])
    )
    write_to_snowflake(data)
