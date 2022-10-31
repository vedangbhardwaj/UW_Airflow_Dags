import pandas as pd


class Get_query:
    def __init__(self, dataset_name):
        self.dataset_name = dataset_name
        self.get_raw_data = None
        self.get_transformed_data = None
        self.get_transformed_woe_data = None
        self.get_txn_data = None
        self.get_activity_data = None
        self.get_bureau_data = None

        if self.dataset_name.strip().upper() == "KB_TXN_MODULE":
            self.get_raw_data = """
                select  
                    user_id,loan_id,disbursed_date, ever11DPD_in_90days,
                    ever15DPD_in_90days as BAD_FLAG,
                    ever15DPD_in_60days,
                    TOTAL_KHATAS,
                    KB_AGE,
                    TOTAL_CUSTOMERS,
                    NO_KB_CUSTOMERS,
                    DAYS_SINCE_FIRST_CUST_ADDED,
                    DAYS_SINCE_LAST_CUST_ADDED,
                    TOTAL_TRXNS,
                    TOTAL_TXN_MONTHS,
                    ACTIVE_KHATAS,
                    ACTIVE_DAYS,
                    DEBIT_TRXNS,
                    CREDIT_TRXNS,
                    DEBIT_AMNT,
                    CREDIT_AMNT,
                    TOTAL_TXN_AMNT,
                    DAYS_SINCE_FIRST_TXN,
                    DAYS_SINCE_LAST_TXN,
                    MONTHS_SINCE_FIRST_TXN,
                    MONTHS_SINCE_LAST_TXN,
                    ACTIVATION_AGE,
                    D_1_TXNS,
                    D_30_TXNS,
                    AVG_TRXNS_CNT_L1MONTH_DAILY,
                    AVG_TRXNS_CNT_L2MONTH_DAILY,
                    AVG_TRXNS_CNT_L3MONTH_DAILY,
                    AVG_TRXNS_CNT_L6MONTH_DAILY,
                    AVG_TRXNS_CNT_L12MONTH_DAILY,
                    AVG_TRXNS_AMOUNT_L1MONTH_DAILY,
                    AVG_TRXNS_AMT_L2MONTH_DAILY,
                    AVG_TRXNS_AMT_L3MONTH_DAILY,
                    AVG_TRXNS_AMT_L6MONTH_DAILY,
                    AVG_TRXNS_AMT_L12MONTH_DAILY,
                    AVG_CREDIT_TRXNS_CNT_L1MONTH_DAILY,
                    AVG_CREDIT_TRXNS_CNT_L2MONTH_DAILY,
                    AVG_CREDIT_TRXNS_CNT_L3MONTH_DAILY,
                    AVG_CREDIT_TRXNS_CNT_L6MONTH_DAILY,
                    AVG_CREDIT_TRXNS_CNT_L12MONTH_DAILY,
                    AVG_DEBIT_TRXNS_CNT_L1MONTH_DAILY,
                    AVG_DEBIT_TRXNS_CNT_L2MONTH_DAILY,
                    AVG_DEBIT_TRXNS_CNT_L3MONTH_DAILY,
                    AVG_DEBIT_TRXNS_CNT_L6MONTH_DAILY,
                    AVG_DEBIT_TRXNS_CNT_L12MONTH_DAILY,
                    AVG_TRXNS_CNT_L1MONTH_MONTHLY,
                    AVG_TRXNS_CNT_L2MONTH_MONTHLY,
                    AVG_TRXNS_CNT_L3MONTH_MONTHLY,
                    AVG_TRXNS_CNT_L6MONTH_MONTHLY,
                    AVG_TRXNS_CNT_L12MONTH_MONTHLY,
                    AVG_TRXNS_AMOUNT_L1MONTH_MONTHLY,
                    AVG_TRXNS_AMT_L2MONTH_MONTHLY,
                    AVG_TRXNS_AMT_L3MONTH_MONTHLY,
                    AVG_TRXNS_AMT_L6MONTH_MONTHLY,
                    AVG_TRXNS_AMT_L12MONTH_MONTHLY,
                    AVG_CREDIT_TRXNS_CNT_L1MONTH_MONTHLY,
                    AVG_CREDIT_TRXNS_CNT_L2MONTH_MONTHLY,
                    AVG_CREDIT_TRXNS_CNT_L3MONTH_MONTHLY,
                    AVG_CREDIT_TRXNS_CNT_L6MONTH_MONTHLY,
                    AVG_CREDIT_TRXNS_CNT_L12MONTH_MONTHLY,
                    AVG_DEBIT_TRXNS_CNT_L1MONTH_MONTHLY,
                    AVG_DEBIT_TRXNS_CNT_L2MONTH_MONTHLY,
                    AVG_DEBIT_TRXNS_CNT_L3MONTH_MONTHLY,
                    AVG_DEBIT_TRXNS_CNT_L6MONTH_MONTHLY,
                    AVG_DEBIT_TRXNS_CNT_L12MONTH_MONTHLY,
                    TOTAL_TRXNS_CNT_3_BY_6,
                    TOTAL_TRXNS_CNT_3_BY_12,
                    round(RATIO_CNT_L1_L3_MNTH_DAILY,2) as RATIO_CNT_L1_L3_MNTH_DAILY,
                    round(RATIO_AMNT_L1_L3_MNTH_DAILY,2) as RATIO_AMNT_L1_L3_MNTH_DAILY,
                    RATIO_CNT_L1_L3_MNTH_MONTHLY,
                    RATIO_AMNT_L1_L3_MNTH_MONTHLY,
                    AVG_TRAN_SIZE_1_MNTH_DAILY,
                    AVG_TRAN_SIZE_2_MNTH_DAILY,
                    AVG_TRAN_SIZE_3_MNTH_DAILY,
                    AVG_TRAN_SIZE_1_MNTH_MONTHLY,
                    AVG_TRAN_SIZE_2_MNTH_MONTHLY,
                    AVG_TRAN_SIZE_3_MNTH_MONTHLY,
                    TOTAL_COUNTERPARTY_TRXNS,
                    TOTAL_COUNTERPARTY_TXN_AMT,
                    AVG_TXN_CNT_WITH_KB_COUNTERPARTY_USERS_1MNTH_DAILY,
                    AVG_TXN_CNT_WITH_KB_COUNTERPARTY_USERS_2MNTH_DAILY,
                    AVG_TXN_CNT_WITH_KB_COUNTERPARTY_USERS_3MNTH_DAILY,
                    AVG_TXN_AMT_WITH_KB_COUNTERPARTY_USERS_1MNTH_DAILY,
                    AVG_TXN_AMT_WITH_KB_COUNTERPARTY_USERS_2MNTH_DAILY,
                    AVG_TXN_AMT_WITH_KB_COUNTERPARTY_USERS_3MNTH_DAILY,
                    AVG_TXN_CNT_WITH_KB_COUNTERPARTY_USERS_1MNTH_MONTHLY,
                    AVG_TXN_CNT_WITH_KB_COUNTERPARTY_USERS_2MNTH_MONTHLY,
                    AVG_TXN_CNT_WITH_KB_COUNTERPARTY_USERS_3MNTH_MONTHLY,
                    AVG_TXN_AMT_WITH_KB_COUNTERPARTY_USERS_1MNTH_MONTHLY,
                    AVG_TXN_AMT_WITH_KB_COUNTERPARTY_USERS_2MNTH_MONTHLY,
                    AVG_TXN_AMT_WITH_KB_COUNTERPARTY_USERS_3MNTH_MONTHLY,
                    ACTIVE_KB_COUNTERPARTY_USERS,
                    CREDIT_EXTENDED_TOTAL,
                    CREDIT_RECOVERED,
                    AMT_RECOVERED_0TO30,
                    AMT_RECOVERED_31TO60,
                    AMT_RECOVERED_61TO90,
                    PARTIAL_SETTLED_TRXNS,
                    FULL_SETTLED_TRXNS,
                    UNSETTLED_TRXNS,
                    CREDIT_EXTENDED_3MNTH_AVG_DAILY,
                    CREDIT_EXTENDED_6MNTH_AVG_DAILY,
                    CREDIT_EXTENDED_12MNTH_AVG_DAILY,
                    CREDIT_EXTENDED_OVERALL_AVG_DAILY,
                    CREDIT_RECOVERED_3MNTH_AVG_DAILY,
                    CREDIT_RECOVERED_6MNTH_AVG_DAILY,
                    CREDIT_RECOVERED_12MNTH_AVG_DAILY,
                    CREDIT_RECOVERED_OVERALL_AVG_DAILY,
                    AMT_RECOVERED_0TO30_3MNTH_AVG_DAILY,
                    AMT_RECOVERED_0TO30_6MNTH_AVG_DAILY,
                    AMT_RECOVERED_0TO30_12MNTH_AVG_DAILY,
                    AMT_RECOVERED_0TO30_OVERALL_AVG_DAILY,
                    AMT_RECOVERED_31TO60_3MNTH_AVG_DAILY,
                    AMT_RECOVERED_31TO60_6MNTH_AVG_DAILY,
                    AMT_RECOVERED_31TO60_12MNTH_AVG_DAILY,
                    AMT_RECOVERED_31TO60_OVERALL_AVG_DAILY,
                    AMT_RECOVERED_61TO90_3MNTH_AVG_DAILY,
                    AMT_RECOVERED_61TO90_6MNTH_AVG_DAILY,
                    AMT_RECOVERED_61TO90_12MNTH_AVG_DAILY,
                    AMT_RECOVERED_61TO90_OVERALL_AVG_DAILY,
                    PARTIAL_SETTLED_TRXNS_3MNTH_AVG_DAILY,
                    PARTIAL_SETTLED_TRXNS_6MNTH_AVG_DAILY,
                    PARTIAL_SETTLED_TRXNS_12MNTH_AVG_DAILY,
                    PARTIAL_SETTLED_TRXNS_OVERALL_AVG_DAILY,
                    FULL_SETTLED_TRXNS_3MNTH_AVG_DAILY,
                    FULL_SETTLED_TRXNS_6MNTH_AVG_DAILY,
                    FULL_SETTLED_TRXNS_12MNTH_AVG_DAILY,
                    round(FULL_SETTLED_TRXNS_OVERALL_AVG_DAILY,2) as FULL_SETTLED_TRXNS_OVERALL_AVG_DAILY,
                    UNSETTLED_TRXNS_3MNTH_AVG_DAILY,
                    UNSETTLED_TRXNS_6MNTH_AVG_DAILY,
                    UNSETTLED_TRXNS_12MNTH_AVG_DAILY,
                    round(UNSETTLED_TRXNS_OVERALL_AVG_DAILY,2) as UNSETTLED_TRXNS_OVERALL_AVG_DAILY,
                    CREDIT_EXTENDED_3MNTH_AVG_MONTHLY,
                    CREDIT_EXTENDED_6MNTH_AVG_MONTHLY,
                    CREDIT_EXTENDED_12MNTH_AVG_MONTHLY,
                    CREDIT_EXTENDED_OVERALL_AVG_MONTHLY,
                    CREDIT_RECOVERED_3MNTH_AVG_MONTHLY,
                    CREDIT_RECOVERED_6MNTH_AVG_MONTHLY,
                    CREDIT_RECOVERED_12MNTH_AVG_MONTHLY,
                    CREDIT_RECOVERED_OVERALL_AVG_MONTHLY,
                    AMT_RECOVERED_0TO30_3MNTH_AVG_MONTHLY,
                    AMT_RECOVERED_0TO30_6MNTH_AVG_MONTHLY,
                    AMT_RECOVERED_0TO30_12MNTH_AVG_MONTHLY,
                    AMT_RECOVERED_0TO30_OVERALL_AVG_MONTHLY,
                    AMT_RECOVERED_31TO60_3MNTH_AVG_MONTHLY,
                    AMT_RECOVERED_31TO60_6MNTH_AVG_MONTHLY,
                    AMT_RECOVERED_31TO60_12MNTH_AVG_MONTHLY,
                    AMT_RECOVERED_31TO60_OVERALL_AVG_MONTHLY,
                    AMT_RECOVERED_61TO90_3MNTH_AVG_MONTHLY,
                    AMT_RECOVERED_61TO90_6MNTH_AVG_MONTHLY,
                    AMT_RECOVERED_61TO90_12MNTH_AVG_MONTHLY,
                    AMT_RECOVERED_61TO90_OVERALL_AVG_MONTHLY,
                    PARTIAL_SETTLED_TRXNS_3MNTH_AVG_MONTHLY,
                    PARTIAL_SETTLED_TRXNS_6MNTH_AVG_MONTHLY,
                    PARTIAL_SETTLED_TRXNS_12MNTH_AVG_MONTHLY,
                    round(PARTIAL_SETTLED_TRXNS_OVERALL_AVG_MONTHLY,2) as PARTIAL_SETTLED_TRXNS_OVERALL_AVG_MONTHLY,
                    FULL_SETTLED_TRXNS_3MNTH_AVG_MONTHLY,
                    FULL_SETTLED_TRXNS_6MNTH_AVG_MONTHLY,
                    FULL_SETTLED_TRXNS_12MNTH_AVG_MONTHLY,
                    FULL_SETTLED_TRXNS_OVERALL_AVG_MONTHLY,
                    UNSETTLED_TRXNS_3MNTH_AVG_MONTHLY,
                    UNSETTLED_TRXNS_6MNTH_AVG_MONTHLY,
                    UNSETTLED_TRXNS_12MNTH_AVG_MONTHLY,
                    UNSETTLED_TRXNS_OVERALL_AVG_MONTHLY,
                    CREDIT_EXTENDED_LAST_MONTH_VALUE,
                    CREDIT_EXTENDED_3_BY_6,
                    CREDIT_RECOVERED_3_BY_6,
                    AMT_RECOVERED_0TO30_3_BY_6,
                    AMT_RECOVERED_31TO60_3_BY_6,
                    AMT_RECOVERED_61TO90_3_BY_6,
                    PARTIAL_SETTLED_TRXNS_3_BY_6,
                    FULL_SETTLED_TRXNS_3_BY_6,
                    UNSETTLED_TRXNS_3_BY_6,
                    CREDIT_EXTENDED_3_BY_12,
                    CREDIT_RECOVERED_3_BY_12,
                    AMT_RECOVERED_0TO30_3_BY_12,
                    AMT_RECOVERED_31TO60_3_BY_12,
                    AMT_RECOVERED_61TO90_3_BY_12,
                    PARTIAL_SETTLED_TRXNS_3_BY_12,
                    FULL_SETTLED_TRXNS_3_BY_12,
                    UNSETTLED_TRXNS_3_BY_12,
                    CREDIT_RECOVERY_RATIO_3_MNTH_AVG_DAILY,
                    CREDIT_RECOVERY_RATIO_6_MNTH_AVG_DAILY,
                    CREDIT_RECOVERY_RATIO_12_MNTH_AVG_DAILY,
                    CREDIT_RECOVERY_RATIO_OVERALL_MNTH_AVG_DAILY,
                    CREDIT_RECOVERY_RATIO_3_MNTH_AVG_MONTHLY,
                    CREDIT_RECOVERY_RATIO_6_MNTH_AVG_MONTHLY,
                    CREDIT_RECOVERY_RATIO_12_MNTH_AVG_MONTHLY,
                    CREDIT_RECOVERY_RATIO_OVERALL_MNTH_AVG_MONTHLY,
                    CREDIT_EXTENDED_3MNTH_SUM,
                    CREDIT_RECOVERED_3MNTH_SUM,
                    CREDIT_RECOVERY_RATIO_3_MNTH,
                    CREDIT_EXTENDED_6MNTH_SUM,
                    CREDIT_RECOVERED_6MNTH_SUM,
                    CREDIT_RECOVERY_RATIO_6_MNTH,
                    CREDIT_EXTENDED_12MNTH_SUM,
                    CREDIT_RECOVERED_12MNTH_SUM,
                    CREDIT_RECOVERY_RATIO_12_MNTH,
                    CREDIT_RECOVERY_RATIO_3_BY_6,
                    CREDIT_RECOVERY_RATIO_3_BY_12
                from analytics.kb_analytics.A1_UW_model_lending_transaction_base_v1
                where disbursed_date>=date('{sd}')
                and date(disbursed_date)<=date('{ed}')
                and (DAYS_SINCE_LAST_TXN<=90 and DAYS_SINCE_LAST_TXN is not NULL)
                """
            # SQL Query - getting transformed data:
            self.get_transformed_data = """
                    SELECT * from ANALYTICS.KB_ANALYTICS.airflow_demo_write_transformed_kb_txn_module
                    """
            # SQL Query - getting transformed WOE applied data:
            self.get_transformed_woe_data = """
                    SELECT * from ANALYTICS.KB_ANALYTICS.airflow_demo_write_transformed_woe_kb_txn_module
                    """
        if self.dataset_name.strip().upper() == "KB_ACTIVITY_MODULE":

            self.get_raw_data = """
                select  
                    user_id,loan_id,disbursed_date, ever11DPD_in_90days,
                    ever11DPD_in_60days,
                    ever15DPD_in_90days as BAD_FLAG,
                    ever15DPD_in_60days
                    ,LAST_1_MONTH_CUSTOMERS
                    ,LAST_2_MONTH_CUSTOMERS
                    ,LAST_3_MONTH_CUSTOMERS
                    ,RATIO_OF_CUSTOMERS_ADDED_L1_L3_MNTH
                    ,DELETED_CUSTOMERS
                    ,LAST_1_MONTH_BOOKS
                    ,LAST_2_MONTH_BOOKS
                    ,LAST_3_MONTH_BOOKS
                    ,RATIO_OF_BOOKS_ADDED_L1_L3_MNTH
                    ,DELETED_BOOKS
                    ,TOTAL_APPS
                    ,BUSINESS_APPS
                    ,FINANCE_APPS
                    ,BUSINESS_TOTALAPPS_PROPORTION
                    ,FINANCE_TOTALAPPS_PROPORTION
                    ,DEVICE_BRAND_GD
                    ,DEVICE_CARRIER_GROUP
                    ,PRICE
                    ,Business_Card_Flag
                from ANALYTICS.KB_ANALYTICS.A4_UW_KB_Activity_data_v2
                where disbursed_date>=date('{sd}')
                and date(disbursed_date)<=date('{ed}')
                """
            # SQL Query - getting transformed data:
            self.get_transformed_data = """
                    SELECT * from ANALYTICS.KB_ANALYTICS.airflow_demo_write_transformed_kb_activity_module 
                    """
            # SQL Query - getting transformed WOE applied data:
            self.get_transformed_woe_data = """
                    SELECT * from ANALYTICS.KB_ANALYTICS.airflow_demo_write_transformed_woe_kb_activity_module 
                    """
        if self.dataset_name.strip().upper() == "KB_BUREAU_MODULE":
            # SQL Query - getting raw data:
            self.get_raw_data = """
                select  
                    user_id,loan_id,disbursed_date,
                    ever15DPD_in_60days,
                    ever15DPD_in_90days as BAD_FLAG,
                    BUREAUSCORE,
                    CREDITACCOUNTTOTAL,
                    CREDITACCOUNTACTIVE,
                    CREDITACCOUNTDEFAULT,
                    CREDITACCOUNTCLOSED,
                    CADSUITFILEDCURRENTBALANCE,
                    OUTSTANDING_BALANCE_SECURED,
                    OUTSTANDING_BALANCE_SECURED_PERCENTAGE,
                    OUTSTANDING_BALANCE_UNSECURED,
                    OUTSTANDING_BALANCE_UNSECURED_PERCENTAGE,
                    OUTSTANDING_BALANCE_ALL,
                    ACCOUNT_TYPE_COUNT,
                    CREDIT_CARDS_COUNT,
                    SECURED_ACCOUNT_COUNT,
                    UNSECURED_ACCOUNT_COUNT,
                    CREDIT_LIMIT_AMOUNT_MAX,
                    CREDIT_LIMIT_AMOUNT_AVG,
                    HIGHEST_CREDIT_OR_ORIGINAL_LOAN_AMOUNT_SUM,
                    HIGHEST_CREDIT_OR_ORIGINAL_LOAN_AMOUNT_AVG,
                    HIGHEST_CREDIT_OR_ORIGINAL_LOAN_AMOUNT_MAX,
                    TERMS_DURATION_AVG,
                    TERMS_DURATION_MAX,
                    SCHEDULED_MONTHLY_PAYMENT_AMOUNT_SUM,
                    SCHEDULED_MONTHLY_PAYMENT_AMOUNT_AVG,
                    SCHEDULED_MONTHLY_PAYMENT_AMOUNT_MAX,
                    ACCOUNT_STATUS_ACTIVE,
                    ACCOUNT_STATUS_CLOSED,
                    CURRENT_BALANCE_SUM,
                    CURRENT_BALANCE_AVG,
                    CURRENT_BALANCE_MAX,
                    CURRENT_BALANCE_MIN,
                    AMOUNT_PAST_DUE_SUM,
                    AMOUNT_PAST_DUE_AVG,
                    AMOUNT_PAST_DUE_MAX,
                    ORIGINAL_CHARGE_OFF_AMOUNT_SUM,
                    ORIGINAL_CHARGE_OFF_AMOUNT_AVG,
                    ORIGINAL_CHARGE_OFF_AMOUNT_MAX,
                    DAYS_SINCE_FIRST_DELINQUENCY,
                    DAYS_SINCE_LAST_PAYMENT,
                    SUITFILEDWILLFULDEFAULTWRITTENOFFSTATUS,
                    SUITFILED_WILFULDEFAULT,
                    VALUE_OF_CREDITS_LAST_MONTH_SUM,
                    VALUE_OF_CREDITS_LAST_MONTH_AVG,
                    VALUE_OF_CREDITS_LAST_MONTH_MAX,
                    SETTLEMENT_AMOUNT_SUM,
                    SETTLEMENT_AMOUNT_AVG,
                    SETTLEMENT_AMOUNT_MAX,
                    VALUE_OF_COLLATERAL_SUM,
                    VALUE_OF_COLLATERAL_AVG,
                    VALUE_OF_COLLATERAL_MAX,
                    TYPE_OF_COLLATERAL,
                    WRITTEN_OFF_AMT_TOTAL_SUM,
                    WRITTEN_OFF_AMT_TOTAL_AVG,
                    WRITTEN_OFF_AMT_TOTAL_MAX,
                    WRITTEN_OFF_AMT_PRINCIPAL_SUM,
                    WRITTEN_OFF_AMT_PRINCIPAL_AVG,
                    WRITTEN_OFF_AMT_PRINCIPAL_MAX,
                    RATE_OF_INTEREST_AVG,
                    RATE_OF_INTEREST_MAX,
                    RATE_OF_INTEREST_MIN,
                    REPAYMENT_TENURE_AVG,
                    REPAYMENT_TENURE_MAX,
                    DAYS_SINCE_LAST_DEFAULT_STATUS,
                    DAYS_SINCE_LAST_LITIGATION_STATUS,
                    DAYS_SINCE_LAST_WRITE_OFF_STATUS,
                    MAX_DPD_LAST_1_MONTH,
                    MAX_DPD_LAST_3_MONTHS,
                    MAX_DPD_LAST_6_MONTHS,
                    MAX_DPD_LAST_9_MONTHS,
                    MAX_DPD_LAST_12_MONTHS,
                    MAX_DPD_LAST_24_MONTHS,
                    MAX_DPD_LAST_1_MONTH_ACTIVE,
                    MAX_DPD_LAST_3_MONTHS_ACTIVE,
                    MAX_DPD_LAST_6_MONTHS_ACTIVE,
                    MAX_DPD_LAST_9_MONTHS_ACTIVE,
                    MAX_DPD_LAST_12_MONTHS_ACTIVE,
                    MAX_DPD_LAST_24_MONTHS_ACTIVE,
                    NO_OF_TIMES_30_DPD_3MONTHS,
                    NO_OF_TIMES_30_DPD_6MONTHS,
                    NO_OF_TIMES_30_DPD_9MONTHS,
                    NO_OF_TIMES_30_DPD_12MONTHS,
                    NO_OF_TIMES_30_DPD_3MONTHS_ACTIVE,
                    NO_OF_TIMES_30_DPD_6MONTHS_ACTIVE,
                    NO_OF_TIMES_30_DPD_9MONTHS_ACTIVE,
                    NO_OF_TIMES_30_DPD_12MONTHS_ACTIVE,
                    NO_OF_TIMES_60_DPD_3MONTHS,
                    NO_OF_TIMES_60_DPD_6MONTHS,
                    NO_OF_TIMES_60_DPD_9MONTHS,
                    NO_OF_TIMES_60_DPD_12MONTHS,
                    NO_OF_TIMES_60_DPD_3MONTHS_ACTIVE,
                    NO_OF_TIMES_60_DPD_6MONTHS_ACTIVE,
                    NO_OF_TIMES_60_DPD_9MONTHS_ACTIVE,
                    NO_OF_TIMES_60_DPD_12MONTHS_ACTIVE,
                    NO_OF_TIMES_90_DPD_3MONTHS,
                    NO_OF_TIMES_90_DPD_6MONTHS,
                    NO_OF_TIMES_90_DPD_9MONTHS,
                    NO_OF_TIMES_90_DPD_12MONTHS,
                    NO_OF_TIMES_90_DPD_3MONTHS_ACTIVE,
                    NO_OF_TIMES_90_DPD_6MONTHS_ACTIVE,
                    NO_OF_TIMES_90_DPD_9MONTHS_ACTIVE,
                    NO_OF_TIMES_90_DPD_12MONTHS_ACTIVE,
                    MONTHS_SINCE_30_DPD,
                    MONTHS_SINCE_60_DPD,
                    MONTHS_SINCE_90_DPD,
                    MONTHS_SINCE_30_DPD_ACTIVE,
                    MONTHS_SINCE_60_DPD_ACTIVE,
                    MONTHS_SINCE_90_DPD_ACTIVE,
                    TOTALCAPSLAST7DAYS,
                    TOTALCAPSLAST30DAYS,
                    TOTALCAPSLAST90DAYS,
                    TOTALCAPSLAST180DAYS,
                    CAPSLAST7DAYS,
                    CAPSLAST30DAYS,
                    CAPSLAST90DAYS,
                    CAPSLAST180DAYS,
                    RESIDENCE_CODE_NON_NORMALIZED,
                    AGE_OF_OLDEST_ACCOUNT_DAYS,
                    ACCOUNT_OPEN_1MONTH,
                    ACCOUNT_OPEN_2MONTHS,
                    ACCOUNT_OPEN_3MONTHS,
                    ACCOUNT_OPEN_6MONTHS,
                    ACCOUNT_OPEN_12MONTHS,
                    MAX_DPD_LAST_1_MONTH_SECURED,
                    MAX_DPD_LAST_3_MONTHS_SECURED,
                    MAX_DPD_LAST_6_MONTHS_SECURED,
                    MAX_DPD_LAST_9_MONTHS_SECURED,
                    MAX_DPD_LAST_12_MONTHS_SECURED,
                    MAX_DPD_LAST_24_MONTHS_SECURED,
                    MAX_DPD_LAST_1_MONTH_UNSECURED,
                    MAX_DPD_LAST_3_MONTHS_UNSECURED,
                    MAX_DPD_LAST_6_MONTHS_UNSECURED,
                    MAX_DPD_LAST_9_MONTHS_UNSECURED,
                    MAX_DPD_LAST_12_MONTHS_UNSECURED,
                    MAX_DPD_LAST_24_MONTHS_UNSECURED,
                    PAYMENT_WITHOUT_DPD_LAST_1_MONTH,
                    PAYMENT_WITHOUT_DPD_LAST_3_MONTHS,
                    PAYMENT_WITHOUT_DPD_LAST_6_MONTHS,
                    PAYMENT_WITHOUT_DPD_LAST_9_MONTHS,
                    PAYMENT_WITHOUT_DPD_LAST_12_MONTHS
                from  ANALYTICS.KB_ANALYTICS.A1_UW_model_lending_Bureau_base_v1
                where date(disbursed_date)>=date('{sd}')
                and date(disbursed_date)<=date('{ed}')
                """
            # SQL Query - getting transformed data:
            self.get_transformed_data = """
                    SELECT * from ANALYTICS.KB_ANALYTICS.airflow_demo_write_transformed_kb_bureau_module 
                    """
            # SQL Query - getting transformed WOE applied data:
            self.get_transformed_woe_data = """
                    SELECT * from ANALYTICS.KB_ANALYTICS.airflow_demo_write_transformed_woe_kb_bureau_module 
                    """
        if self.dataset_name == "COMBINATION_MODEL_LR":
            # SQL Query - getting txn module data:
            self.get_txn_data = """
                select * from ANALYTICS.KB_ANALYTICS.airflow_demo_write_result_kb_txn_module ; 
                """
            # SQL Query - getting activity module data:
            self.get_activity_data = """
                select * from ANALYTICS.KB_ANALYTICS.airflow_demo_write_result_kb_activity_module ; 
                """
            # SQL Query - getting bureau module data
            self.get_bureau_data = """
                select * from ANALYTICS.KB_ANALYTICS.airflow_demo_write_result_kb_bureau_module ; 
                """
        if self.dataset_name.strip().upper() == "COMBINATION_MODEL_XG":
            # SQL Query - getting txn module data:
            self.get_txn_data = """
                select * from ANALYTICS.KB_ANALYTICS.airflow_demo_write_result_xgb_kb_txn_module ; 
                """
            # SQL Query - getting activity module data:
            self.get_activity_data = """
                select * from ANALYTICS.KB_ANALYTICS.airflow_demo_write_result_xgb_kb_activity_module ; 
                """
            # SQL Query - getting bureau module data
            self.get_bureau_data = """
                select * from ANALYTICS.KB_ANALYTICS.airflow_demo_write_result_xgb_kb_bureau_module ; 
                """
