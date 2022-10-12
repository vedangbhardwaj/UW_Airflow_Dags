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
    )
    cur = conn.cursor()

    print(sq.get_transformed_data)

    def get_data():
        sql_query = sq.get_transformed_data
        data = pd.read_sql(sql_query, con=conn)
        return data

    def monotonic_chk(bins2):
        bins2["counter"] = bins2.groupby("variable").cumcount() + 1
        bins2["badprob_lag"] = bins2["badprob"].shift(+1)
        bins2["variable_lag"] = bins2["variable"]
        bins2["missing_bin"] = [
            1 if x == y and (z == "missing" or z == "-99999.0") else 0
            for (x, y, z) in zip(
                bins2["variable_lag"], bins2["variable"], bins2["breaks"]
            )
        ]
        bins2["missing_bin_lag"] = bins2["missing_bin"].shift(+1)
        bins2["test"] = [
            "Inc"
            if (x >= 2 and y != 1 and z > a)
            else "Dec"
            if (x >= 2 and y != 1 and z < a)
            else "NA"
            for (x, y, z, a) in zip(
                bins2["counter"],
                bins2["missing_bin_lag"],
                bins2["badprob_lag"],
                bins2["badprob"],
            )
        ]
        bins2["Inc"] = [1 if x == "Inc" else 0 for x in bins2["test"]]
        bins2["Dec"] = [1 if x == "Dec" else 0 for x in bins2["test"]]
        bins2["Na"] = [1 if x == "NA" else 0 for x in bins2["test"]]
        bins2["Inc_cnt"] = bins2.groupby(by=["variable"])["Inc"].transform("sum")
        bins2["Dec_cnt"] = bins2.groupby(by=["variable"])["Dec"].transform("sum")
        bins2["Na_cnt"] = bins2.groupby(by=["variable"])["Na"].transform("sum")
        bins2["break_mis"] = [
            1 if (z == "missing" or z == "-99999.0") else 0 for z in bins2["breaks"]
        ]
        bins2["break_missing"] = bins2.groupby(by=["variable"])["break_mis"].transform(
            "sum"
        )
        bins2["Max_iv"] = bins2.groupby(by=["variable"])["total_iv"].transform("max")
        bins2["Prop_iv"] = bins2["bin_iv"] / bins2["total_iv"]
        bins2["num_bin"] = bins2.groupby(by=["variable"])["variable"].transform("count")
        return bins2

    binning_combo = [
        [0.02, 10],
        [0.05, 10],
        [0.1, 10],
        [0.02, 9],
        [0.05, 9],
        [0.1, 9],
        [0.02, 8],
        [0.05, 8],
        [0.1, 8],
        [0.02, 7],
        [0.05, 7],
        [0.1, 7],
        [0.02, 6],
        [0.05, 6],
        [0.1, 6],
        [0.02, 5],
        [0.05, 5],
        [0.1, 5],
        [0.02, 4],
        [0.05, 4],
        [0.1, 4],
        [0.02, 3],
        [0.05, 3],
        [0.1, 3],
        [0.02, 2],
        [0.05, 2],
        [0.1, 2],
    ]

    def Woe_binning(data):
        final_bin = pd.DataFrame()
        All_rejected_bin = pd.DataFrame()

        var_binning = list(data.columns)
        var_binning = [i for i in var_binning if i not in idc.ID_cols]
        # print(var_binning)
        j = 0
        for i in binning_combo:
            j = j + 1
            if len(np.unique(var_binning)) > 1:
                bins = sc.woebin(
                    data,
                    y="BAD_FLAG",
                    x=var_binning,
                    bin_num_limit=i[1],
                    count_distr_limit=i[0],
                    special_values=[-99999],
                )
                bins2 = pd.DataFrame()
                for k in list(bins.keys()):
                    bins2 = pd.concat([bins2, pd.DataFrame(bins[k])])
                bins3_1 = monotonic_chk(bins2)
                bins3_1["mtest"] = [
                    "NonMono" if x > 0 and y > 0 else "Mono"
                    for (x, y) in zip(bins3_1["Inc_cnt"], bins3_1["Dec_cnt"])
                ]
                bins3_1["IV_test"] = [
                    1 if x < 0.05 and y == 0 else 0
                    for (x, y) in zip(bins2["Prop_iv"], bins2["missing_bin"])
                ]
                bins3_1["Max_IV_test"] = bins3_1.groupby(by=["variable"])[
                    "IV_test"
                ].transform("max")

                ########bins with low iv < .02 --- should be rejected
                # print(bins3_1)
                bins_rejected = bins3_1[
                    (bins3_1["Max_iv"] < idc.IV_threshold)
                ]  ###### --------- vars dropped
                var_reject_list = list(bins_rejected["variable"].unique())
                print(
                    "In round ",
                    j,
                    " vars with low iv < ",
                    idc.IV_threshold,
                    " : ",
                    bins_rejected["variable"].nunique(),
                )

                ####### bins with high iv and monotoniticy ----- should be used as is####
                bins3_1["Accept_flag"] = [
                    1
                    if (x >= idc.IV_threshold and b == 0 and y == "Mono")
                    or (z == 2 and x >= idc.IV_threshold and b == 0)
                    or (a > 0 and z == 3 and x >= idc.IV_threshold and b == 0)
                    else 0
                    for (x, y, z, a, b) in zip(
                        bins3_1["Max_iv"],
                        bins3_1["mtest"],
                        bins3_1["num_bin"],
                        bins3_1["break_missing"],
                        bins3_1["Max_IV_test"],
                    )
                ]
                bins_accepted = bins3_1[bins3_1["Accept_flag"] == 1]
                var_accept_list = list(bins_accepted["variable"].unique())
                print(
                    "In round ",
                    j,
                    " vars with monotonic and high iv >=",
                    idc.IV_threshold,
                    "-- ",
                    bins_accepted["variable"].nunique(),
                )

                ####### bins with high iv and no monotoniticy ------ should be rebinned ---
                bins3_1["Issue_bin"] = [
                    0 if (x in var_reject_list or x in var_accept_list) else 1
                    for x in bins3_1["variable"]
                ]
                bins_issue = bins3_1[bins3_1["Issue_bin"] == 1]
                print(
                    "In round ",
                    j,
                    " vars with non monotonic and high iv >=",
                    idc.IV_threshold,
                    "-- ",
                    bins_issue["variable"].nunique(),
                )
                var_binning = list(bins_issue["variable"].unique())
                bins_accepted["round"] = j

                final_bin = pd.concat([final_bin, pd.DataFrame(bins_accepted)])
                bins_rejected["round"] = j
                All_rejected_bin = pd.concat(
                    [All_rejected_bin, pd.DataFrame(bins_rejected)]
                )

        print(colored("\n No of variables with final bins: ", "blue", attrs=["bold"]))
        print(final_bin["variable"].nunique())
        print(
            colored("\n No of variables with rejected bins: ", "blue", attrs=["bold"])
        )
        print(All_rejected_bin["variable"].nunique())
        print(colored("\n No of variables with bins issue: ", "blue", attrs=["bold"]))
        print(bins_issue["variable"].nunique())
        final_bin.to_csv(idc.interim_path + "final_bin.csv", index=False)
        All_rejected_bin.to_csv(idc.interim_path + "All_rejected_bin.csv", index=False)
        bins_issue.to_csv(idc.interim_path + "bins_issue.csv", index=False)

        return final_bin

    def woe_Apply(data, final_bin1):
        new_bin = final_bin1[final_bin1.columns[0:13]]
        data_w = sc.woebin_ply(data, new_bin)
        data_w_features = data_w.filter(regex="_woe$", axis=1)
        data_w_bad = data_w["BAD_FLAG"]

        data_woe = pd.concat([data_w_bad, data_w_features], axis=1)
        return data_woe

    def woe_univariate_gini(Train, Train_bad, OOT, OOT_bad):
        logisticRegr = LogisticRegression()
        feature = []
        gini_train = []
        gini_oot = []
        perf_metrics = {}
        for col in list(Train.columns):
            logisticRegr.fit(Train[col].values.reshape(-1, 1), Train_bad)
            pred_train = logisticRegr.predict_proba(Train[col].values.reshape(-1, 1))[
                :, 1
            ]
            pred_test = logisticRegr.predict_proba(OOT[col].values.reshape(-1, 1))[:, 1]
            fpr, tpr, thresholds = roc_curve(Train_bad, pred_train)
            roc_auc = auc(fpr, tpr)
            GINI_train = (2 * roc_auc) - 1
            fpr1, tpr1, thresholds1 = roc_curve(OOT_bad, pred_test)
            roc_auc1 = auc(fpr1, tpr1)
            GINI_oot = (2 * roc_auc1) - 1
            feature.append(col)
            gini_train.append(GINI_train)
            gini_oot.append(GINI_oot)
        perf_metrics["var"] = feature
        perf_metrics["Gini_train"] = gini_train
        perf_metrics["Gini_oot"] = gini_oot
        perf_metrics = pd.DataFrame(perf_metrics)
        perf_metrics["Relative_diff"] = abs(
            perf_metrics["Gini_train"] - perf_metrics["Gini_oot"]
        ) / perf_metrics["Gini_train"].round(4)
        return perf_metrics

    def woe_gini_trend(final_bin1, perf_metrics):
        perf_metrics["var1"] = perf_metrics["var"].str.rstrip("_woe")
        final_bin1 = pd.DataFrame(final_bin1)
        print(final_bin1.columns)
        Final_bin_gini = final_bin1.merge(
            perf_metrics, left_on="variable", right_on="var1", how="left"
        )
        print(Final_bin_gini.columns)
        Final_bin_gini["Trend_ind"] = [
            1 if x == "Dec" else 0 for x in Final_bin_gini["test"]
        ]
        Final_bin_gini["Trend_ind1"] = Final_bin_gini.groupby(["variable"])[
            "Trend_ind"
        ].transform("max")
        Final_bin_gini["Trend"] = [
            "Increasing" if x == 1 else "Decreasing"
            for x in Final_bin_gini["Trend_ind1"]
        ]
        Final_bin_gini = Final_bin_gini.merge(
            idc.feature_list, left_on="variable", right_on="variables", how="left"
        )
        Final_bin_gini = Final_bin_gini[
            [
                "variable",
                "var",
                "bin",
                "count",
                "count_distr",
                "good",
                "bad",
                "badprob",
                "woe",
                "bin_iv",
                "total_iv",
                "Prop_iv",
                "Max_IV_test",
                "IV_test",
                "breaks",
                "is_special_values",
                "counter",
                "num_bin",
                "Trend",
                "Trend_Business",
                "Gini_train",
                "Gini_oot",
                "Relative_diff",
            ]
        ]

        return Final_bin_gini

    def write_to_snowflake(data):
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

        con = engine.raw_connection()
        data1.columns = map(lambda x: str(x).upper(), data1.columns)
        data1.to_sql(
            "airflow_demo_write_transformed",
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
        "/Users/vedang.bhardwaj/Desktop/work_mode/airflow_learn/UW_Airflow_Dags/KB_TXN_MODULE/data/Final_bin_gini_performance.csv"
    )
    data_woe = woe_Apply(data, Final_bin_gini)

    write_to_snowflake(data_woe)
