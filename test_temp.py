import KB_ACTIVITY_MODULE as KB_ACTIVITY_MODULE
import KB_TXN_MODULE as KB_TXN_MODULE
import KB_BUREAU_MODULE as KB_BUREAU_MODULE
from airflow.models import Variable
import snowflake.connector
from sql_queries import Get_query
import pandas as pd
import pickle 
from s3fs.core import S3FileSystem
from xgboost import XGBClassifier
import boto3
import json


print(pickle.format_version)

globals()["KB_ACTIVITY_MODULE"].getting_data("KB_ACTIVITY_MODULE")
globals()["KB_ACTIVITY_MODULE"].woe_calculation("KB_ACTIVITY_MODULE")
globals()["KB_ACTIVITY_MODULE"].xgboost_model_prediction("KB_ACTIVITY_MODULE")
globals()["KB_ACTIVITY_MODULE"].model_prediction("KB_ACTIVITY_MODULE")


globals()["KB_TXN_MODULE"].getting_data("KB_TXN_MODULE")
globals()["KB_TXN_MODULE"].woe_calculation("KB_TXN_MODULE")
globals()["KB_TXN_MODULE"].xgboost_model_prediction("KB_TXN_MODULE")
globals()["KB_TXN_MODULE"].model_prediction("KB_TXN_MODULE")

globals()["KB_BUREAU_MODULE"].getting_data("KB_BUREAU_MODULE")
globals()["KB_BUREAU_MODULE"].woe_calculation("KB_BUREAU_MODULE")
globals()["KB_BUREAU_MODULE"].xgboost_model_prediction("KB_BUREAU_MODULE")
globals()["KB_BUREAU_MODULE"].model_prediction("KB_BUREAU_MODULE")

import COMBINATION_MODEL_LR as COMBINATION_MODEL_LR
globals()["COMBINATION_MODEL_LR"].predict("COMBINATION_MODEL_LR")

import COMBINATION_MODEL_XG as COMBINATION_MODEL_XG
globals()["COMBINATION_MODEL_XG"].predict("COMBINATION_MODEL_XG")



config = Variable.get("underwriting_dags", deserialize_json=True)


conn = snowflake.connector.connect(
    user=config["user"],
    password=config["password"],
    account=config["account"],
    role=config["role"],
    warehouse=config["warehouse"],
    database=config["database"],
)
cur = conn.cursor()

def get_data(module_name):
    sql_cmd = None
    if module_name == "KB_TXN_MODULE":
        sql_cmd = Get_query("COMBINATION_MODEL_LR").get_txn_data
    if module_name == "KB_ACTIVITY_MODULE":
        sql_cmd = Get_query("COMBINATION_MODEL_LR").get_activity_data
    if module_name == "KB_BUREAU_MODULE":
        sql_cmd = Get_query("COMBINATION_MODEL_LR").get_bureau_data
    cur.execute(sql_cmd)
    df = pd.DataFrame(cur.fetchall())
    colnames = [desc[0] for desc in cur.description]
    df.columns = [i for i in colnames]
    return df

Transaction_module_data = get_data("KB_TXN_MODULE")

config = Variable.get("underwriting_dags", deserialize_json=True)
s3 = boto3.resource("s3")
s3_bucket = config["s3_bucket"]

pickled_model = XGBClassifier()

s3_file = S3FileSystem()

s3_obj = s3_file.open('{}/{}'.format(s3_bucket,"underwriting_assets/activity_module/models/model_xgb_json.json")).read()
type(s3_obj)

json_object = json.loads(s3_obj)


type(json_object)

json.loads(s3_file.open('{}/{}'.format(s3_bucket,"underwriting_assets/activity_module/models/model_xgb_json.json")).read())

pickled_model.load_model(json.loads(s3_file.open('{}/{}'.format(s3_bucket,"underwriting_assets/activity_module/models/model_xgb_json.json")).read().decode('utf-8')))

pickled_model.load_model(json.loads(s3_file.open('{}/{}'.format(s3_bucket,"underwriting_assets/activity_module/models/model_xgb_json.json")).read()))




s3_client = boto3.client('s3')
s3.meta.client.download_file(s3_bucket,"underwriting_assets/activity_module/models/model_xgb_json.json",'s3_downloaded_json.json')

type(json_object)
pickled_model.load_model("s3_downloaded_json.json")




