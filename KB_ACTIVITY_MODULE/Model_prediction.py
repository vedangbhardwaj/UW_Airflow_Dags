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

    from snowflake.connector.pandas_tools import pd_writer
    from sqlalchemy import create_engine
    from snowflake.sqlalchemy import URL

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
        insecure_mode=True,
    )
    cur = conn.cursor()

    def get_data():
        sql_query = sq.get_raw_data.format(sd=idc.start_date, ed=idc.end_date)
        data = pd.read_sql(sql_query, con=conn)
        return data

    def get_data_woe():
        sql_query = sq.get_transformed_woe_data.format(
            sd=idc.start_date, ed=idc.end_date
        )
        data = pd.read_sql(sql_query, con=conn)
        return data

    def write_to_snowflake(data, module_name="kb_activity_module"):
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

    model_perf2 = pd.read_csv(f"{idc.data_path}Model_selected.csv")
    Top_models = [0]
    j = 0
    # Keep_cols=['CUSTOMER_ID','DECISION_DATE','IS_FAIL_FLAG','ACCEPT_REJECT','LOAN_ID','DISBURSED_DATE','BAD_FLAG','DAYS_SINCE_LAST_TXN']

    # for j in Top_models:
    Final_model_vars = list(model_perf2["variable"][model_perf2["Model_no"] == j])
    Final_model_vars = Final_model_vars[1 : len(Final_model_vars)]
    Final_model_vars = [str(x).upper() for x in Final_model_vars]

    Final_scoring_data = pd.concat(
        [data[idc.ID_cols], data_woe[Final_model_vars]], axis=1
    )
    pred_data = Final_scoring_data[Final_model_vars]
    pred_data = sm.add_constant(pred_data)

    pickled_model = pickle.load(
        open(
            f"{idc.model_path}Model_{j}.pkl",
            "rb",
        )
    )

    # Final model prediction
    Final_scoring_data["pred_train"] = pickled_model.predict(pred_data)

    write_to_snowflake(Final_scoring_data)
