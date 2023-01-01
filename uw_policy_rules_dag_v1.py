import pandas as pd
import math


def apply_rules(data):
    def westerncap_policy(data, val):
        global score
        experian_score = (
            0
            if (
                data["BUREAUSCORE"] != "NULL"
                and data["BUREAUSCORE"] >= 650
                or data["BUREAUSCORE"] == ""
            )
            else 1
        )
        scoreXSell = (
            0 if data["SCORE_XSELL"] != "NULL" and data["SCORE_XSELL"] >= 550 else 1
        )  # this needs to be changed with val
        max_DPD_Unsec_loan_6_months = (
            0
            if data["MAX_DPD_6_MONTHS_UNSECURED_FOR_RULE"] != "NULL"
            and data["MAX_DPD_6_MONTHS_UNSECURED_FOR_RULE"] < 15
            else 1
        )
        new_acc_opened_gt_1lac = (
            0
            if data["ACCOUNTS_GT1L_OPEN_IN_60DAYS_FOR_RULE"] != "NULL"
            and data["ACCOUNTS_GT1L_OPEN_IN_60DAYS_FOR_RULE"] <= 3
            else 1
        )
        new_acc_opened_lt_1lac = (
            0
            if data["ACCOUNTS_LTE1L_OPEN_IN_60DAYS_FOR_RULE"] != "NULL"
            and data["ACCOUNTS_LTE1L_OPEN_IN_60DAYS_FOR_RULE"] <= 5
            else 1
        )
        max_DPD_Sec_loans_6_months = (
            0
            if data["MAX_DPD_6_MONTHS_SECURED_FOR_RULE"] != "NULL"
            and data["MAX_DPD_6_MONTHS_SECURED_FOR_RULE"] < 15
            else 1
        )
        active_accounts = (
            0
            if data["ACTIVE_ACCOUNTS_FOR_RULE"] != "NULL"
            and data["ACTIVE_ACCOUNTS_FOR_RULE"] < 15
            else 1
        )
        credit_enq_last_6_months = (
            0
            if data["REQUEST_180_DAYS_FOR_RULE"] != "NULL"
            and pd.to_numeric(data["REQUEST_180_DAYS_FOR_RULE"]) < 15
            else 1
        )
        max_overdues = (
            0
            if data["AMOUNT_PAST_DUE_FOR_RULE"] != "NULL"
            and data["AMOUNT_PAST_DUE_FOR_RULE"] <= 5000
            else 1
        )
        foll_account_status_code = (
            0
            if data["NEGATIVEACCOUNSTATUS_24MONTHS_FOR_RULE"] != "NULL"
            and data["NEGATIVEACCOUNSTATUS_24MONTHS_FOR_RULE"] == 0
            else 1
        )
        written_off_amt_total = (
            0
            if data["WRITTEN_OFF_AMT_TOTAL_FOR_RULE"] != "NULL"
            and data["WRITTEN_OFF_AMT_TOTAL_FOR_RULE"] <= 500
            else 1
        )
        written_off_settled = (
            0
            if data["WRITTEN_OFF_SETTLED_24MONTHS_CNT_FOR_RULE"] != "NULL"
            and data["WRITTEN_OFF_SETTLED_24MONTHS_CNT_FOR_RULE"] == 0
            else 1
        )
        debit_bounce = (
            1
            if (
                (
                    data["AUTO_DEBIT_BOUNCE_M0"] != "NULL"
                    and data["AUTO_DEBIT_BOUNCE_M0"] == "TRUE"
                )
                or (
                    data["AUTO_DEBIT_BOUNCE_M1"] == "TRUE"
                    and data["AUTO_DEBIT_BOUNCE_M1"] != "NULL"
                )
            )
            else 0
        )
        overall_bounces = (
            0
            if data["OVERALL_BOUNCE_M0123"] != "NULL"
            and data["OVERALL_BOUNCE_M0123"] < 5
            else 1
        )
        # risk_bucket_not_F = 0 if data["RISK_BUCKET"]!=F else 1 # not available
        cnt_deliquency_loan = (
            0
            if data["CNT_DELINQUNCY_LOAN_C30"] != "NULL"
            and data["CNT_DELINQUNCY_LOAN_C30"] < 3
            else 1
        )
        max_credit_amt = (
            0
            if data["TOTAL_CREDIT_FOR_RULE"] != "NULL"
            and data["TOTAL_CREDIT_FOR_RULE"] >= 5000
            else 1
        )
        willful_default = (
            0
            if data["SUITFILED_WILFULDEFAULT_24MONTHS_FOR_RULE"] != "NULL"
            and data["SUITFILED_WILFULDEFAULT_24MONTHS_FOR_RULE"] == 0
            else 1
        )
        willful_default_written_off = (
            0
            if data["SUITFILEDWILLFULDEFAULTWRITTENOFFSTATUS_24MONTHS_FOR_RULE"]
            != "NULL"
            and data["SUITFILEDWILLFULDEFAULTWRITTENOFFSTATUS_24MONTHS_FOR_RULE"] == 0
            else 1
        )
        payment_history_val = (
            0
            if data["PAYMENTHISTORYPROFILE_12MONTHS_FOR_RULE"] != "NULL"
            and data["PAYMENTHISTORYPROFILE_12MONTHS_FOR_RULE"] == 0
            else 1
        )

        if (
            experian_score
            or scoreXSell
            or max_DPD_Unsec_loan_6_months
            or new_acc_opened_gt_1lac
            or new_acc_opened_lt_1lac
            or max_DPD_Sec_loans_6_months
            or max_overdues
            or written_off_amt_total
            or written_off_settled
            or debit_bounce
            or overall_bounces
            or cnt_deliquency_loan
            # or max_credit_amt
            or active_accounts
            or credit_enq_last_6_months
            or foll_account_status_code
            or willful_default
            or willful_default_written_off
            or payment_history_val
        ) > 0:
            score = 1
        else:
            score = 0

        return score

    def credit_policy_1_9_b(data, val):
        global score
        max_credit_amt = (
            0
            if data["TOTAL_CREDIT_FOR_RULE"] != "NULL"
            and data["TOTAL_CREDIT_FOR_RULE"] >= 5000
            else 1
        )
        scoreXSell = (
            0 if data["SCORE_XSELL"] != "NULL" and data["SCORE_XSELL"] >= 550 else 1
        )  # this needs to be changed with val
        overall_bounces = (
            0
            if data["OVERALL_BOUNCE_M0123"] != "NULL"
            and data["OVERALL_BOUNCE_M0123"] < 5
            else 1
        )
        debit_bounce = (
            1
            if (
                data["AUTO_DEBIT_BOUNCE_M0"] != "NULL"
                and data["AUTO_DEBIT_BOUNCE_M0"] == "TRUE"
                or data["AUTO_DEBIT_BOUNCE_M1"] == "TRUE"
            )
            else 0
        )
        cnt_deliquency_loan = (
            0
            if data["CNT_DELINQUNCY_LOAN_C30"] != "NULL"
            and data["CNT_DELINQUNCY_LOAN_C30"] < 3
            else 1
        )
        auto_debit_bounce = (
            0
            if data["AUTO_DEBIT_BOUNCE_M0"] != "NULL"
            and data["AUTO_DEBIT_BOUNCE_M0"] == "FALSE"
            else 1
        )
        written_off_settled = (
            0
            if data["WRITTEN_OFF_SETTLED_24MONTHS_CNT_FOR_RULE"] != "NULL"
            and data["WRITTEN_OFF_SETTLED_24MONTHS_CNT_FOR_RULE"] == 0
            else 1
        )
        written_off_amt_total = (
            0
            if data["WRITTEN_OFF_AMT_TOTAL_FOR_RULE"] != "NULL"
            and data["WRITTEN_OFF_AMT_TOTAL_FOR_RULE"] <= 500
            else 1
        )
        willful_default = (
            0
            if data["SUITFILED_WILFULDEFAULT_24MONTHS_FOR_RULE"] != "NULL"
            and data["SUITFILED_WILFULDEFAULT_24MONTHS_FOR_RULE"] == 0
            else 1
        )
        willful_default_written_off = (
            0
            if data["SUITFILEDWILLFULDEFAULTWRITTENOFFSTATUS_24MONTHS_FOR_RULE"]
            != "NULL"
            and data["SUITFILEDWILLFULDEFAULTWRITTENOFFSTATUS_24MONTHS_FOR_RULE"] == 0
            else 1
        )
        new_acc_opened_gt_1lac = (
            0
            if data["ACCOUNTS_GT1L_OPEN_IN_60DAYS_FOR_RULE"] != "NULL"
            and data["ACCOUNTS_GT1L_OPEN_IN_60DAYS_FOR_RULE"] <= 3
            else 1
        )
        new_acc_opened_lt_1lac = (
            0
            if data["ACCOUNTS_LTE1L_OPEN_IN_60DAYS_FOR_RULE"] != "NULL"
            and data["ACCOUNTS_LTE1L_OPEN_IN_60DAYS_FOR_RULE"] <= 5
            else 1
        )
        max_DPD_Sec_loans_6_months = (
            0
            if data["MAX_DPD_6_MONTHS_SECURED_FOR_RULE"] != "NULL"
            and data["MAX_DPD_6_MONTHS_SECURED_FOR_RULE"] < 15
            else 1
        )
        max_DPD_Unsec_loan_6_months = (
            0
            if data["MAX_DPD_6_MONTHS_UNSECURED_FOR_RULE"] != "NULL"
            and data["MAX_DPD_6_MONTHS_UNSECURED_FOR_RULE"] < 15
            else 1
        )
        payment_history_val = (
            0
            if data["PAYMENTHISTORYPROFILE_12MONTHS_FOR_RULE"] != "NULL"
            and data["PAYMENTHISTORYPROFILE_12MONTHS_FOR_RULE"] == 0
            else 1
        )
        foll_account_status_code = (
            0
            if data["NEGATIVEACCOUNSTATUS_24MONTHS_FOR_RULE"] != "NULL"
            and data["NEGATIVEACCOUNSTATUS_24MONTHS_FOR_RULE"] == 0
            else 1
        )
        experian_score = (
            0
            if (
                data["BUREAUSCORE"] != "NULL"
                and data["BUREAUSCORE"] >= 650
                or data["BUREAUSCORE"] == ""
            )
            else 1
        )
        credit_enq_last_6_months = (
            0
            if data["REQUEST_180_DAYS_FOR_RULE"] != "NULL"
            and pd.to_numeric(data["REQUEST_180_DAYS_FOR_RULE"]) < 15
            else 1
        )
        active_accounts = (
            0
            if data["ACTIVE_ACCOUNTS_FOR_RULE"] != "NULL"
            and data["ACTIVE_ACCOUNTS_FOR_RULE"] < 15
            else 1
        )

        if (
            experian_score
            or scoreXSell
            or max_DPD_Unsec_loan_6_months
            or new_acc_opened_gt_1lac
            or new_acc_opened_lt_1lac
            or max_DPD_Sec_loans_6_months
            or written_off_amt_total
            or written_off_settled
            or debit_bounce
            or overall_bounces
            or cnt_deliquency_loan
            # or max_credit_amt
            or auto_debit_bounce
            or active_accounts
            or credit_enq_last_6_months
            or foll_account_status_code
            or willful_default
            or willful_default_written_off
            or payment_history_val
        ) > 0:
            score = 1
        else:
            score = 0

        return score

    def credit_policy_1_9_a(data):
        if (
            data["Rule2_OVERALL_BOUNCE_M0123_rule_decision"] == "FAIL"
            or data["Rule4_AUTO_DEBIT_BOUNCE_rule_decision"] == "FAIL"
            or data["Rule5_WRITTEN_OFF_SETTLED_rule_decision"] == "FAIL"
            or data["Rule6_WRITTEN_OFF_AMT_TOTAL_FOR_RULE_decision"] == "FAIL"
            or data["Rule7_No_foll_suit_filed_default_RULE_decision"] == "FAIL"
            or data["Rule8_No_foll_suit_filed_Written_OFF_RULE_decision"] == "FAIL"
            or data["Rule9_MAX_DPD_2_YEARS_FOR_RULE"] == "FAIL"
            or data["Rule10_No_foll_PAYMENTHISTORYPROFILE_12MONTHS_FOR_RULE"] == "FAIL"
            or data["Rule11_No_foll_status_in_24M_rule"] == "FAIL"
            or data["Rule12_Experian_score"] == "FAIL"
        ):
            return "FAIL"
        else:
            return "PASS"

    def credit_policy_1_8_1(data, val):
        global score
        scoreXSell = (
            0 if data["SCORE_XSELL"] != "NULL" and data["SCORE_XSELL"] >= 550 else 1
        )  # this needs to be changed with val
        overall_bounces = (
            0
            if data["OVERALL_BOUNCE_M0123"] != "NULL"
            and data["OVERALL_BOUNCE_M0123"] < 5
            else 1
        )
        max_credit_amt = (
            0
            if data["TOTAL_CREDIT_FOR_RULE"] != "NULL"
            and data["TOTAL_CREDIT_FOR_RULE"] >= 0
            else 1
        )
        cnt_deliquency_loan = (
            0
            if data["CNT_DELINQUNCY_LOAN_C30"] != "NULL"
            and data["CNT_DELINQUNCY_LOAN_C30"] < 3
            else 1
        )
        auto_debit_bounce_m0 = (
            0
            if data["AUTO_DEBIT_BOUNCE_M0"] != "NULL"
            and data["AUTO_DEBIT_BOUNCE_M0"] == "FALSE"
            else 1
        )
        written_off_settled = (
            0
            if data["WRITTEN_OFF_SETTLED_24MONTHS_CNT_FOR_RULE"] != "NULL"
            and data["WRITTEN_OFF_SETTLED_24MONTHS_CNT_FOR_RULE"] == 0
            else 1
        )
        written_off_amt_total = (
            0
            if data["WRITTEN_OFF_AMT_TOTAL_FOR_RULE"] != "NULL"
            and data["WRITTEN_OFF_AMT_TOTAL_FOR_RULE"] <= 500
            else 1
        )
        willful_default = (
            0
            if data["SUITFILED_WILFULDEFAULT_24MONTHS_FOR_RULE"] != "NULL"
            and data["SUITFILED_WILFULDEFAULT_24MONTHS_FOR_RULE"] == 0
            else 1
        )
        willful_default_written_off = (
            0
            if data["SUITFILEDWILLFULDEFAULTWRITTENOFFSTATUS_24MONTHS_FOR_RULE"]
            != "NULL"
            and data["SUITFILEDWILLFULDEFAULTWRITTENOFFSTATUS_24MONTHS_FOR_RULE"] == 0
            else 1
        )
        new_acc_opened_gt_1lac = (
            0
            if data["ACCOUNTS_GT1L_OPEN_IN_60DAYS_FOR_RULE"] != "NULL"
            and data["ACCOUNTS_GT1L_OPEN_IN_60DAYS_FOR_RULE"] <= 3
            else 1
        )
        new_acc_opened_lt_1lac = (
            0
            if data["ACCOUNTS_LTE1L_OPEN_IN_60DAYS_FOR_RULE"] != "NULL"
            and data["ACCOUNTS_LTE1L_OPEN_IN_60DAYS_FOR_RULE"] <= 5
            else 1
        )
        max_DPD_Sec_loans_6_months = (
            0
            if data["MAX_DPD_6_MONTHS_SECURED_FOR_RULE"] != "NULL"
            and data["MAX_DPD_6_MONTHS_SECURED_FOR_RULE"] < 15
            else 1
        )
        max_DPD_Unsec_loan_6_months = (
            0
            if data["MAX_DPD_6_MONTHS_UNSECURED_FOR_RULE"] != "NULL"
            and data["MAX_DPD_6_MONTHS_UNSECURED_FOR_RULE"] < 15
            else 1
        )
        payment_history_val = (
            0
            if data["PAYMENTHISTORYPROFILE_12MONTHS_FOR_RULE"] != "NULL"
            and data["PAYMENTHISTORYPROFILE_12MONTHS_FOR_RULE"] == 0
            else 1
        )
        foll_account_status_code = (
            0
            if data["NEGATIVEACCOUNSTATUS_24MONTHS_FOR_RULE"] != "NULL"
            and data["NEGATIVEACCOUNSTATUS_24MONTHS_FOR_RULE"] == 0
            else 1
        )
        credit_enq_last_6_months = (
            0
            if data["REQUEST_180_DAYS_FOR_RULE"] != "NULL"
            and pd.to_numeric(data["REQUEST_180_DAYS_FOR_RULE"]) < 15
            else 1
        )
        active_accounts = (
            0
            if data["ACTIVE_ACCOUNTS_FOR_RULE"] != "NULL"
            and data["ACTIVE_ACCOUNTS_FOR_RULE"] < 15
            else 1
        )
        if (
            scoreXSell
            or auto_debit_bounce_m0
            or max_DPD_Unsec_loan_6_months
            or new_acc_opened_gt_1lac
            or new_acc_opened_lt_1lac
            or max_DPD_Sec_loans_6_months
            or written_off_amt_total
            or written_off_settled
            or overall_bounces
            or cnt_deliquency_loan
            # or max_credit_amt
            or active_accounts
            or credit_enq_last_6_months
            or foll_account_status_code
            or willful_default
            or willful_default_written_off
            or payment_history_val
        ) > 0:
            score = 1
        else:
            score = 0

        return score

    def credit_policy_1_8_0(data, val):
        global score
        fis_credit_score = (
            0
            if (
                data["SCORE_CD_V2_EMSEMBLE_PROBABILITY"] != "NULL"
                and data["SCORE_CD_V2_EMSEMBLE_PROBABILITY"] <= 0.0284
                if (
                    data["BUREAUSCORE"] == "NULL"
                    or data["BUREAUSCORE"] >= 700
                    or data["BUREAUSCORE"] == ""
                    or data["BUREAUSCORE"] < 300
                )
                else data["SCORE_CD_V2_EMSEMBLE_PROBABILITY"] != "NULL"
                and data["SCORE_CD_V2_EMSEMBLE_PROBABILITY"] <= 0.0354
            )
            else 1
        )
        overall_bounces = (
            0
            if data["OVERALL_BOUNCE_M0123"] != "NULL"
            and data["OVERALL_BOUNCE_M0123"] < 5
            else 1
        )
        max_credit_amt = (
            0
            if data["TOTAL_CREDIT_FOR_RULE"] != "NULL"
            and data["TOTAL_CREDIT_FOR_RULE"] > 0
            else 1
        )
        # Device Connect Confidence should be HIGH - to be applied
        debit_bounce = (
            1
            if (
                (
                    data["AUTO_DEBIT_BOUNCE_M0"] != "NULL"
                    and data["AUTO_DEBIT_BOUNCE_M0"] == "TRUE"
                )
                or (
                    data["AUTO_DEBIT_BOUNCE_M1"] == "TRUE"
                    and data["AUTO_DEBIT_BOUNCE_M1"] != "NULL"
                )
            )
            else 0
        )
        written_off_settled = (
            0
            if data["WRITTEN_OFF_SETTLED_24MONTHS_CNT_FOR_RULE"] != "NULL"
            and data["WRITTEN_OFF_SETTLED_24MONTHS_CNT_FOR_RULE"] == 0
            else 1
        )
        written_off_amt_total = (
            0
            if data["WRITTEN_OFF_AMT_TOTAL_FOR_RULE"] != "NULL"
            and data["WRITTEN_OFF_AMT_TOTAL_FOR_RULE"] <= 500
            else 1
        )
        willful_default = (
            0
            if data["SUITFILED_WILFULDEFAULT_24MONTHS_FOR_RULE"] != "NULL"
            and data["SUITFILED_WILFULDEFAULT_24MONTHS_FOR_RULE"] == 0
            else 1
        )
        willful_default_written_off = (
            0
            if data["SUITFILEDWILLFULDEFAULTWRITTENOFFSTATUS_24MONTHS_FOR_RULE"]
            != "NULL"
            and data["SUITFILEDWILLFULDEFAULTWRITTENOFFSTATUS_24MONTHS_FOR_RULE"] == 0
            else 1
        )
        max_dpd_2_year = (
            0
            if data["MAX_DPD_2_YEARS_FOR_RULE"] != "NULL"
            and pd.to_numeric(data["MAX_DPD_2_YEARS_FOR_RULE"]) <= 30
            else 1
        )
        payment_history_val = (
            0
            if data["PAYMENTHISTORYPROFILE_12MONTHS_FOR_RULE"]
            and data["PAYMENTHISTORYPROFILE_12MONTHS_FOR_RULE"] == 0
            else 1
        )
        foll_account_status_code = (
            0
            if data["NEGATIVEACCOUNSTATUS_24MONTHS_FOR_RULE"] != "NULL"
            and data["NEGATIVEACCOUNSTATUS_24MONTHS_FOR_RULE"] == 0
            else 1
        )
        experian_score = (
            0
            if (
                data["BUREAUSCORE"] != "NULL"
                and data["BUREAUSCORE"] >= 700
                or data["BUREAUSCORE"] == ""
            )
            else 1
        )
        if (
            written_off_amt_total
            or written_off_settled
            or overall_bounces
            or debit_bounce
            or max_dpd_2_year
            or foll_account_status_code
            or experian_score
            # or max_credit_amt
            or foll_account_status_code
            or willful_default
            or willful_default_written_off
            or payment_history_val
            or fis_credit_score
        ) > 0:
            score = 1
        else:
            score = 0

        return score

    # if westerncap_policy(data, val) == 1:
    #     if credit_policy_1_9_a(data, val) == 1:
    #         if credit_policy_1_9_b(data, val) == 1:
    #             if credit_policy_1_8_0(data, val) == 1:
    #                 if credit_policy_1_8_1(data, val) == 1:
    #                     # return  ["all failed",score]
    #                     return "fail"
    #                 else:
    #                     # return ["credit_policy_1_8_1",score]
    #                     return "pass"
    #             else:
    #                 # return ["credit_policy_1_8_0",score]
    #                 return "pass"
    #         else:
    #             # return ["credit_policy_1_9_b",score]
    #             return "pass"
    #     else:
    #         # return ["credit_policy_1_9_a",score]
    #         return "pass"
    # else:
    #     # return ["westerncap_policy",score]
    #     return "pass"
    return credit_policy_1_9_a(data)


def policy_with_fb(data):
    verdict = [
        "FAIL" if (x == "FAIL" or y == "FAIL" or z == "FAIL") else "PASS"
        for (x, y, z) in zip(
            data["verdict_policy_alone"],
            data["Rule14_FIS_PD_score_rule_decision"],
            data["Rule13_NTC_FIS_PD_score_rule_decision"],
        )
    ]
    return verdict
