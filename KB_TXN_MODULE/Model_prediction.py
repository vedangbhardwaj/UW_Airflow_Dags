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
    import yaml

    # import seaborn as sns
    import scorecardpy as sc
    import snowflake.connector

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
        sql_query = sq.get_raw_data
        data = pd.read_sql(sql_query, con=conn)
        return data

    def get_data_woe():
        sql_query = sq.get_transformed_data
        data = pd.read_sql(sql_query, con=conn)
        return data

    data = get_data()
    data_woe = get_data_woe()

    model_perf2 = pd.read_csv(
        "/Users/vedang.bhardwaj/Desktop/work_mode/airflow_learn/UW_Airflow_Dags/KB_TXN_MODULE/data/Model_selected.csv"
    )
    Top_models = [103138]
    j = 103138
    # Keep_cols=['CUSTOMER_ID','DECISION_DATE','IS_FAIL_FLAG','ACCEPT_REJECT','LOAN_ID','DISBURSED_DATE','BAD_FLAG','DAYS_SINCE_LAST_TXN']

    Val_scoring_data = None

    # for j in Top_models:
    Final_model_vars = list(model_perf2["variable"][model_perf2["Model_no"] == j])
    print(Final_model_vars)

    Final_model_vars = Final_model_vars[1 : len(Final_model_vars)]
    Final_raw_vars = [sub[:-4] for sub in Final_model_vars]
    Final_model_vars = [str(x).upper() for x in Final_model_vars]
    Val_data = pd.concat([data[Final_raw_vars], data_woe[Final_model_vars]], axis=1)
    # Val_scoring_data=pd.concat([Val_scoring_data,Val_data],axis=1)
    # Val_scoring_data = Val_data
    Val_var1 = Val_data[Final_model_vars]
    Val_var1 = sm.add_constant(Val_var1)

    Final_scoring_data = pd.concat(
        [data[idc.ID_cols], data_woe[Final_model_vars]], axis=1
    )
    Final_scoring_data.shape

    print(Val_var1.shape)
    print(Val_var1.head())

    pickled_model = pickle.load(
        open(
            f"/Users/vedang.bhardwaj/Desktop/work_mode/airflow_learn/UW_Airflow_Dags/KB_TXN_MODULE/models/Model_{j}.pkl",
            "rb",
        )
    )

    print(pickled_model.model)

    Val_scoring_data[f"Val_pred_{j}"] = pickled_model.predict(Final_scoring_data)
