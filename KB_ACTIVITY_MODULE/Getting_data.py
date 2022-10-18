import pandas as pd
import re

import snowflake.connector
from datetime import date, datetime, timedelta
from snowflake.connector.pandas_tools import pd_writer

from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine

import builtins
from termcolor import colored
from sklearn import metrics

import yaml

#### importing Initial declaration ###
import Initial_declaration as idc

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
)
cur = conn.cursor()


def get_data(start_date, end_date):
    sql_cmd = sq.get_raw_data.format(sd=idc.start_date, ed=idc.end_date)
    cur.execute(sql_cmd)
    df = pd.DataFrame(cur.fetchall())
    colnames = [desc[0] for desc in cur.description]
    df.columns = [i for i in colnames]
    return df


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


cat_col = list(idc.feature_list["variables"][idc.feature_list["Type"] == "Categorical"])
num_col = list(idc.feature_list["variables"][idc.feature_list["Type"] == "Numerical"])


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
            df[var] = df[var].fillna(idc.missing_value_num)
    for var in df.columns:
        if var_type(var) == "Categorical":
            df[var] = df[var].fillna(idc.missing_value_cat)
            df[var] = df[var].replace("--", idc.missing_value_cat)
            df[var] = pd.Categorical(df[var])
    return df


data = get_data(idc.start_date, idc.end_date)
data = missing_ind_convert_num(data)
write_to_snowflake(data)
