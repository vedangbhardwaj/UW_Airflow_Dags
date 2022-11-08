if __name__ == "__main__":
    import pandas as pd
    import numpy as np
    import math
    import statsmodels.api as sm
    import snowflake.connector
    from snowflake.connector.pandas_tools import pd_writer
    from snowflake.sqlalchemy import URL
    from sqlalchemy import create_engine
    import yaml
    import pickle

    ### importing initial declaration ###
    import Initial_declaration as id

    ### importing sql queries declaration ##
    import Sql_queries as sq

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

    def get_data(query):
        sql_cmd = query
        cur.execute(sql_cmd)
        df = pd.DataFrame(cur.fetchall())
        colnames = [desc[0] for desc in cur.description]
        df.columns = [i for i in colnames]
        return df

    Transaction_module_data = get_data(sq.get_txn_data)
    Activity_module_data = get_data(sq.get_activity_data)
    Bureau_module_data = get_data(sq.get_bureau_data)

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

    ### combining the training data
    data_merge = Activity_module_data[
        ["USER_ID", "LOAN_ID", "DISBURSED_DATE", "BAD_FLAG", "act_logodds"]
    ].merge(
        Transaction_module_data[["LOAN_ID", "trx_logodds"]], on="LOAN_ID", how="left"
    )

    data_merge = data_merge.merge(
        Bureau_module_data[["LOAN_ID", "br_logodds"]], on="LOAN_ID", how="left"
    )

    print(data_merge.shape)
    data_merge.head()

    combination_train = data_merge.dropna()
    combination_train.shape

    combination_train["comb_score"] = (
        (41 / 100) * combination_train["trx_logodds"]
        + (40 / 100) * combination_train["br_logodds"]
        + (19 / 100) * combination_train["act_logodds"]
    )

    combination_train["PD_score"] = 1 / (1 + np.exp(-combination_train["comb_score"]))

    model_calib = pickle.load(open(f"{id.model_path}Model_LR_calibration_LR.pkl", "rb"))

    combination_train["Calib_PD"] = model_calib.predict(
        sm.add_constant(combination_train["comb_score"])
    )

    combination_train.to_csv("LR_combined_op.csv", index=False)
