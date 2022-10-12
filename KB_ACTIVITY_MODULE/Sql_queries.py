import pandas as pd

# SQL Query - getting raw data:
get_raw_data = """
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
get_transformed_data = """
        SELECT * from ANALYTICS.KB_ANALYTICS.airflow_demo_write_kb_activity_module 
        """
