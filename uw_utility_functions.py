from snowflake.connector.pandas_tools import pd_writer
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from airflow.models import Variable
import os
import numpy as np
import copy
import pandas as pd 

config = Variable.get("underwriting_dags", deserialize_json=True)


def write_to_snowflake(data, identifier, dataset_name):
    data1 = data.copy()
    from sqlalchemy.types import (
        Boolean,
        Date,
        DateTime,
        Float,
        Integer,
        Interval,
        Text,
        Time,
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
        if dtype_dict[i] == "float32":
            dtype_dict[i] = Float
        if dtype_dict[i] == "int64":
            dtype_dict[i] = Integer
        if dtype_dict[i] == "bool":
            dtype_dict[i] = Boolean
    dtype_dict
    engine = create_engine(
        URL(
            account=config["account"],
            user=config["user"],
            password=config["password"],
            # user=os.environ.get('SNOWFLAKE_UNAME'),
            # password=os.environ.get('SNOWFLAKE_PASS'),
            # account=os.environ.get('SNOWFLAKE_ACCOUNT'),
            database=config["database"],
            schema=config["schema"],
            warehouse=config["warehouse"],
            role=config["role"],
        )
    )

    # con = engine.raw_connection()
    data1.columns = map(lambda x: str(x).upper(), data1.columns)
    name = f"airflow_demo_write_{identifier}_{dataset_name.lower()}"
    data1.to_sql(
        name=name,
        con=engine,
        if_exists="replace",
        index=False,
        index_label=None,
        dtype=dtype_dict,
        method=pd_writer,
    )
    return


def calculate_psi_num(Base, New, buckettype="quantiles", buckets=10, axis=0):
    """Calculate the PSI (population stability index) across all numerical variables
    Args:
       Base: numpy matrix of original values (Baseline)
       New: numpy matrix of new values, same size as Base
       buckettype: type of strategy for creating buckets, bins splits into even splits, quantiles splits into quantile buckets
       buckets: number of quantiles to use in bucketing variables
       axis: axis by which variables are defined, 0 for vertical, 1 for horizontal
    Returns:
       psi_values: ndarray of psi values for each variable
    """
    missing_value_num = -99999
    def psi(Base_array, New_array, buckets):
        """Calculate the PSI for a single variable
        Args:
           Base_array: numpy array of original values (Baseline)
           New_array: numpy array of new values, same size as Base
           buckets: number of percentile ranges to bucket the values into
        Returns:
           psi_value: calculated PSI value
        """

        def scale_range(input, min, max):
            input += -(np.min(input))
            input /= np.max(input) / (max - min)
            input += min
            return input

        """Considering missing value"""
        Base_array_WOM = Base_array[Base_array != missing_value_num]
        New_array_WOM = New_array[New_array != missing_value_num]
        Base_array_WM = Base_array[Base_array == missing_value_num]
        New_array_WM = New_array[New_array == missing_value_num]

        if len(Base_array_WOM) == 0:
            return None

        breakpoints = np.arange(0, buckets + 1) / (buckets) * 100

        if buckettype == "bins":
            breakpoints = scale_range(
                breakpoints, np.min(Base_array_WOM), np.max(Base_array_WOM)
            )
        elif buckettype == "quantiles":
            breakpoints = np.stack(
                [np.percentile(Base_array_WOM, b) for b in breakpoints]
            )

        Base_percents = np.histogram(Base_array_WOM, breakpoints)[0] / len(Base_array)
        Base_percents = np.append(Base_percents, len(Base_array_WM) / len(Base_array))
        New_percents = np.histogram(New_array_WOM, breakpoints)[0] / len(New_array)
        New_percents = np.append(New_percents, len(New_array_WM) / len(New_array))

        test_df = pd.concat(
            [pd.DataFrame(Base_percents), pd.DataFrame(New_percents)], axis=1
        )
        # print(Base_array.name)
        # print(test_df)
        def sub_psi(e_perc, a_perc):
            """Calculate the actual PSI value from comparing the values.
            Update the actual value to a very small number if equal to zero
            """
            if a_perc == 0:
                a_perc = 0.0001
            if e_perc == 0:
                e_perc = 0.0001

            value = (e_perc - a_perc) * np.log(e_perc / a_perc)
            return value

        psi_value = sum(
            sub_psi(Base_percents[i], New_percents[i])
            for i in range(0, len(Base_percents))
        )

        return psi_value

    if len(Base.shape) == 1:
        psi_values = np.empty(len(Base.shape))
    else:
        psi_values = np.empty(Base.shape[axis])

    for i in range(0, len(psi_values)):
        if len(psi_values) == 1:
            psi_values = psi(Base, New, buckets)
        elif axis == 0:
            psi_values[i] = psi(Base[:, i], New[:, i], buckets)
        elif axis == 1:
            psi_values[i] = psi(Base[i, :], New[i, :], buckets)

    return psi_values


### Calculate PSI for categorical features
def calculate_psi_cat(Base, New, axis=0):
    """Calculate the PSI (population stability index) across all numerical variables
    Args:
       Base: numpy matrix of original values (Baseline)
       New: numpy matrix of new values, same size as Base
       axis: axis by which variables are defined, 0 for vertical, 1 for horizontal
    Returns:
       psi_values: ndarray of psi values for each variable
    """

    def psi(Base_array, New_array):

        """Calculate the PSI for a single variable
        Args:
           Base_array: numpy array of original values (Baseline)
           New_array: numpy array of new values, same size as Base
        Returns:
           psi_value: calculated PSI value
        """
        Base_percent = pd.DataFrame(Base_array.value_counts() / Base_array.shape[0])
        Base_percent["v1"] = Base_percent.index
        Base_percent.columns = ["Dist1", "v1"]

        New_percent = pd.DataFrame(New_array.value_counts() / New_array.shape[0])
        New_percent["v1"] = New_percent.index
        New_percent.columns = ["Dist2", "v1"]

        Tab = Base_percent.merge(New_percent, on="v1", how="left")
        # print(Tab)

        def sub_psi(e_perc, a_perc):
            """Calculate the actual PSI value from comparing the values.
            Update the actual value to a very small number if equal to zero
            """
            if pd.isna(a_perc) == True or a_perc == 0:
                a_perc = 0.0001
            if pd.isna(e_perc) == True or e_perc == 0:
                e_perc = 0.0001

            value = (e_perc - a_perc) * np.log(e_perc / a_perc)
            # print(value)
            return value

        psi_value = sum(
            sub_psi(Tab["Dist1"][i], Tab["Dist2"][i]) for i in range(0, len(Tab))
        )

        return psi_value

    if len(Base.shape) == 1:
        psi_values = np.empty(len(Base.shape))
    else:
        psi_values = np.empty(Base.shape[axis])

    for i in range(0, len(psi_values)):
        if len(psi_values) == 1:
            psi_values = psi(Base, New)
        elif axis == 0:
            psi_values[i] = psi(Base[:, i], New[:, i])
        elif axis == 1:
            psi_values[i] = psi(Base[i, :], New[i, :])

    return psi_values

def ks(data=None, target=None, prob=None, model=None):
    print(f"**********{prob}**********")
    print(f"**********{model}**********")
    data1 = copy.deepcopy(data)
    data1 = data1.loc[data1["MODEL_TYPE"] == model, :]
    data1["target0"] = 1 - data[target]
    data1["bucket"] = pd.cut(data[prob], 10)
    grouped = data1.groupby("bucket", as_index=False)
    kstable = pd.DataFrame()
    kstable["Min_prob"] = grouped.min()[prob]
    kstable["Max_prob"] = grouped.max()[prob]
    kstable["Bads"] = grouped.sum()[target]
    kstable["Goods"] = grouped.sum()["target0"]
    kstable = kstable.sort_values(by="Min_prob", ascending=False).reset_index(drop=True)
    kstable["Distribution Goods"] = (kstable.Bads / data1[target].sum()).apply(
        "{0:.2%}".format
    )
    kstable["Distribution Bads"] = (kstable.Goods / data1["target0"].sum()).apply(
        "{0:.2%}".format
    )
    kstable["%Cum_bads"] = (kstable.Bads / data1[target].sum()).cumsum()
    kstable["%Cum_goods"] = (kstable.Goods / data1["target0"].sum()).cumsum()
    kstable["%Cum_difference"] = (
        np.round(kstable["%Cum_bads"] - kstable["%Cum_goods"], 3) * 100
    )

    # Formating
    kstable["%Cum_bads"] = kstable["%Cum_bads"].apply("{0:.2%}".format)
    kstable["%Cum_goods"] = kstable["%Cum_goods"].apply("{0:.2%}".format)
    kstable.index = range(1, 11)
    kstable.index.rename("Decile", inplace=True)
    pd.set_option("display.max_columns", 20)
    print(kstable)

    # Display KS
    from colorama import Fore

    print(
        Fore.RED
        + "KS is "
        + str(max(kstable["%Cum_difference"]))
        + "%"
        + " at decile "
        + str(
            (
                kstable.index[
                    kstable["%Cum_difference"] == max(kstable["%Cum_difference"])
                ][0]
            )
        )
    )
    data1 = None
    return kstable