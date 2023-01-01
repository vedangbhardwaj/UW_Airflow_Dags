import pandas as pd
import yaml

with open("config.yml") as config_file:
    config_var = yaml.full_load(config_file)["txn_module"]

missing_value_num = config_var["missing_value_num"]
missing_value_cat = config_var["missing_value_cat"]
IV_threshold = config_var["IV_threshold"]  ### threshold for IV (IV should be accepted
var_threshold = config_var["var_threshold"]
ID_cols = config_var["ID_cols"]
input_path = config_var["input_path"]
data_path = config_var["data_path"]
model_path = config_var["model_path"]

feature_list = pd.read_csv("KB_transaction_module_variables.csv")

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
            df[var] = pd.Categorical(df[var])
    return df


def generate_prediction(input):
    import pickle

    import numpy as np
    import statsmodels.api as sm

    pickled_model = pickle.load(
        open(
            f"Model_xgb.pkl",
            "rb",
        )
    )
    data = pd.read_json(input)
    data1 = data[keep_var_list]
    data["pred_train"] = pickled_model.predict_proba(data1)[:, 1]

    data["logodds_score"] = np.log(data["pred_train"] / (1 - data["pred_train"]))

    model_xgb_calib = pickle.load(
        open(
            f"Model_LR_calibration_xgb.pkl",
            "rb",
        )
    )

    data["pred_train_xgb"] = model_xgb_calib.predict(
        sm.add_constant(data["logodds_score"])
    )

    return data.to_json(orient='records')[1:-1].replace('},{', '} {')

data = pd.read_csv('example_input.csv')
data = missing_ind_convert_num(data)

XGB_keep_var_list = pd.read_csv("XGBoost_feature_list.csv")
keep_var_list = list(XGB_keep_var_list["variables"])
input_data = data.to_json(orient='records')
result = generate_prediction(input_data)
result

