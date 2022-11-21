if __name__ == "__main__":
    ### initial declaration
    import Initial_declaration as idc

    ### sql queries
    import Sql_queries as sq
    import Getting_data as gd
    import pandas as pd
    import numpy as np
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

    def get_data():
        sql_query = sq.get_transformed_data
        data = pd.read_sql(sql_query, con=conn)
        return data

    def woe_Apply(data, final_bin1):
        new_bin = final_bin1[final_bin1.columns[0:13]]
        data_w = sc.woebin_ply(data, new_bin)
        data_w_features = data_w.filter(regex="_woe$", axis=1)
        data_w_bad = data_w["BAD_FLAG"]

        data_woe = pd.concat([data_w_bad, data_w_features], axis=1)
        return data_woe

    def write_to_snowflake(data, module_name="kb_bureau_module"):
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
            if dtype_dict[i] == "category":
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
        "/Users/vedang.bhardwaj/Desktop/work_mode/airflow_learn/UW_Airflow_Dags/KB_BUREAU_MODULE/data/Final_bin_gini_performance.csv"
    )
    data_woe = woe_Apply(data, Final_bin_gini)
    write_to_snowflake(data_woe)
