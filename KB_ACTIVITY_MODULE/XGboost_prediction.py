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
    from xgboost import XGBClassifier
    import xgboost as xgb
    from sklearn.metrics import precision_recall_fscore_support as score
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
    )
    cur = conn.cursor()

    def get_data():
        sql_query = sq.get_raw_data.format(sd=idc.start_date, ed=idc.end_date)
        data = pd.read_sql(sql_query, con=conn)
        return data

    data = get_data()
    # print(idc.colsList)
    data = data[idc.colsList]
    print(data.columns)
    pickled_model = pickle.load(
        open(
            f"/Users/vedang.bhardwaj/Desktop/work_mode/airflow_learn/UW_Airflow_Dags/KB_ACTIVITY_MODULE/models/Model_xgb.pkl",
            "rb",
        )
    )
    Final_scoring_data = data
    print(Final_scoring_data.columns.dtype)
    Final_scoring_data.PRICE.head()
    Final_scoring_data["PRICE"] = Final_scoring_data["PRICE"].astype("str")
    Final_scoring_data["pred_train"] = pickled_model.predict_proba(data)[:, 1]
    Final_scoring_data["logodds_score"] = np.log(
        Final_scoring_data["pred_train"] / (1 - Final_scoring_data["pred_train"])
    )
    Final_scoring_data["Calib_PD"] = pickled_model.predict(
        Final_scoring_data["logodds_score"]
    )
