from airflow.models import DAG, Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import DagRun
import os
import sys
import imp
import logging

sys.path.append(
    "/Users/vedang.bhardwaj/Desktop/work_mode/airflow_learn/UW_Airflow_Dags"
)
# import kubernetes_resources as kubernetes
# imp.reload(kubernetes)

# from great_expectations_provider.operators.great_expectations import (
#     GreatExpectationsOperator,
# )
# from great_expectations.core.batch import BatchRequest
# from great_expectations.data_context.types.base import (
#     DataContextConfig,
#     CheckpointConfig,
# )


from datetime import datetime
from datetime import timedelta
import airflow
import yaml

# root_folder = os.path.abspath(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) ## In order to import clients package which is one directory above from dev directory or prod directory
# sys.path.append(root_folder) ## This could be removed when we start publishing packages and directly use them.


import uw_kb_txn_module_dag_v1 as KB_TXN_MODULE
import uw_kb_activity_module_dag_v1 as KB_ACTIVITY_MODULE
import uw_kb_bureau_module_dag_v1 as KB_BUREAU_MODULE
import uw_combination_model_lr_dag_v1 as COMBINATION_MODEL_LR
import uw_combination_model_xg_dag_v1 as COMBINATION_MODEL_XG
import uw_psi_gini_calculation as MASTER_TABLE_GENERATION

# def intimate_of_failure_on_slack(context):
#     slack_api_token = os.environ.get('SLACK_API_TOKEN')
#     slack = Slacker(slack_api_token)

#     message = """
#     Failure in execution of the DAG: %s.
#     """ % (context['dag'].dag_id)

#     slack.chat.post_message('airflow-jobs-slack-alerts', message ,username='Airflow',icon_emoji=':wind_blowing_face:')

config = Variable.get("underwriting_dags", deserialize_json=True)
config_resource = Variable.get("underwriting_dags", deserialize_json=True)[
    "kubernetes_executor"
]

resource_config = {
    "KubernetesExecutor": {
        "request_memory": "7000Mi",
        "limit_memory": "7000Mi",
        "request_cpu": "3000m",
        "limit_cpu": "3500m",
        "node_selectors": {"infra-node-group": "node-group-memory-intensive"},
    }
}

combined_resource_config = {
    "KubernetesExecutor": {
        "request_memory": config_resource["request_memory"],
        "limit_memory": config_resource["limit_memory"],
        "request_cpu": config_resource["request_cpu"],
        "limit_cpu": config_resource["limit_cpu"],
        "node_selectors": config_resource["node_selectors"],
    }
}

args = {
    "owner": config["owner"],
    "depends_on_past": False,
    "start_date": airflow.utils.dates.days_ago(1),
    "email": config["email"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    # 'on_failure_callback':intimate_of_failure_on_slack
}


def _get_start_date(**kwargs):
    user_provided_data_start_date = kwargs["dag_run"].conf.get("data_start_date")

    # If start date is provided as inputs while running the DAG
    if user_provided_data_start_date:
        data_start_date = datetime.strptime(
            user_provided_data_start_date, "%Y-%m-%d"
        ).date()
    elif kwargs["prev_start_date_success"]:
        prev_start_date_success = kwargs["prev_start_date_success"]
        data_start_date = prev_start_date_success.date()
    else:
        data_start_date = datetime.strptime(
            config["default_start_date"], "%Y-%m-%d"
        ).date()
    kwargs["ti"].xcom_push(key="start_date", value=str(data_start_date))
    logging.info("start_date: %s", data_start_date)


def generate_dag(dataset_name):
    with DAG(
        dag_id=f"uw_{dataset_name.lower()}_dag_v1",
        default_args=args,
        schedule_interval=None,
        max_active_runs=1,
        max_active_tasks=1,
        tags=["ds", "underwriting", "lending", "model"],
        catchup=False,
    ) as dag:
        start = DummyOperator(task_id=f"{dataset_name}", dag=dag)

        get_start_date = PythonOperator(
            task_id="get_start_date",
            provide_context=True,
            python_callable=_get_start_date,
            dag=dag,
        )

        getting_data = PythonOperator(
            task_id="get_data",
            execution_timeout=timedelta(minutes=60),
            provide_context=True,
            op_kwargs={"dataset_name": dataset_name},
            python_callable=globals()[dataset_name].getting_data,
            # executor_config=resource_config,
        )

        #         data_validation = BashOperator(
        #             task_id="Data_Validation",
        #             execution_timeout=timedelta(minutes=60),
        #             bash_command=f"cd /Users/vedang.bhardwaj/Desktop/work_mode/airflow_learn/UW_Airflow_Dags/{dataset_name}; great_expectations checkpoint run autogen_suite_checkpoint",
        #         )

        woe_calculation = PythonOperator(
            task_id="woe_transformation",
            execution_timeout=timedelta(minutes=60),
            provide_context=True,
            op_kwargs={"dataset_name": dataset_name},
            python_callable=globals()[dataset_name].woe_calculation,
            # executor_config=resource_config,
            trigger_rule="none_skipped",
        )

        model_prediction = PythonOperator(
            task_id="model_prediction",
            execution_timeout=timedelta(minutes=60),
            provide_context=True,
            op_kwargs={"dataset_name": dataset_name},
            python_callable=globals()[dataset_name].model_prediction,
            # executor_config=resource_config,
            trigger_rule="none_skipped",
        )

        xgboost_model = PythonOperator(
            task_id="xgboost_model_prediction",
            execution_timeout=timedelta(minutes=60),
            provide_context=True,
            op_kwargs={"dataset_name": dataset_name},
            python_callable=globals()[dataset_name].xgboost_model_prediction,
            # executor_config=resource_config,
            trigger_rule="none_failed",
        )

    start >> get_start_date >> getting_data
    #     getting_data >> [data_validation, xgboost_model]
    getting_data >> [woe_calculation, xgboost_model]
    #     data_validation >> WOE_calculation
    woe_calculation >> model_prediction
    return dag


def combined_prediction_dag(prediction_name, external_task_id):
    with DAG(
        dag_id=f"uw_{prediction_name.lower()}_dag_v1",
        default_args=args,
        schedule_interval=None,
        max_active_runs=1,
        max_active_tasks=1,
        tags=["ds", "underwriting", "lending", "model"],
        catchup=False,
    ) as dag:

        def get_most_recent_dag_run(dag_id):
            dag_runs = DagRun.find(dag_id=dag_id)
            dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
            if dag_runs:
                return dag_runs[0].execution_date

        start = DummyOperator(task_id=f"{prediction_name}", dag=dag)

        kb_txn_module_wait = ExternalTaskSensor(
            task_id="kb_txn_module_wait",
            # poke_interval=60,
            # timeout=180,
            # soft_fail=False,
            # retries=2,
            external_dag_id="uw_kb_txn_module_dag_v1",
            external_task_id=external_task_id,
            execution_date_fn=lambda dt: get_most_recent_dag_run(
                "uw_kb_txn_module_dag_v1"
            ),
            check_existence=True,
            mode="reschedule",
        )
        kb_activity_module_wait = ExternalTaskSensor(
            task_id="kb_activity_module_wait",
            external_dag_id="uw_kb_activity_module_dag_v1",
            external_task_id=external_task_id,
            execution_date_fn=lambda dt: get_most_recent_dag_run(
                "uw_kb_activity_module_dag_v1"
            ),
            mode="reschedule",
        )
        kb_bureau_module_wait = ExternalTaskSensor(
            task_id="kb_bureau_module_wait",
            external_dag_id="uw_kb_bureau_module_dag_v1",
            external_task_id=external_task_id,
            execution_date_fn=lambda dt: get_most_recent_dag_run(
                "uw_kb_bureau_module_dag_v1"
            ),
            mode="reschedule",
        )
        # combined_model_prediction = BashOperator(
        #     task_id="Combined_model_prediction",
        #     execution_timeout=timedelta(minutes=60),
        #     bash_command=f"cd /Users/vedang.bhardwaj/Desktop/work_mode/airflow_learn/UW_Airflow_Dags/{prediction_name}; python Model_prediction.py",
        # )
        combined_model_prediction = PythonOperator(
            task_id="combined_model_prediction",
            execution_timeout=timedelta(minutes=60),
            provide_context=True,
            op_kwargs={"dataset_name": prediction_name},
            python_callable=globals()[prediction_name].predict,
            # executor_config=resource_config,
        )
        generate_result = PythonOperator(
            task_id="generate_result",
            execution_timeout=timedelta(minutes=60),
            provide_context=True,
            op_kwargs={"dataset_name": prediction_name},
            python_callable=globals()[prediction_name].result_generation,
            # executor_config=combined_resource_config,
        )
        write_master_table = PythonOperator(
            task_id="write_data_master_table",
            execution_timeout=timedelta(minutes=60),
            provide_context=True,
            op_kwargs={"dataset_name": prediction_name},
            python_callable=globals()[prediction_name].merge_master_table,
            # executor_config=combined_resource_config,
        )
    start >> [kb_txn_module_wait, kb_activity_module_wait, kb_bureau_module_wait]
    [
        kb_txn_module_wait,
        kb_activity_module_wait,
        kb_bureau_module_wait,
    ] >> combined_model_prediction
    combined_model_prediction >> generate_result >> write_master_table
    return dag


def gini_psi_calculation(dataset_name, external_task_id):
    with DAG(
        dag_id=f"uw_{dataset_name.lower()}_dag_v1",
        default_args=args,
        schedule_interval=None,
        max_active_runs=1,
        max_active_tasks=1,
        tags=["ds", "underwriting", "lending", "model"],
        catchup=False,
    ) as dag:

        def get_most_recent_dag_run(dag_id):
            dag_runs = DagRun.find(dag_id=dag_id)
            dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
            if dag_runs:
                return dag_runs[0].execution_date

        start = DummyOperator(task_id=f"{dataset_name.lower()}", dag=dag)

        comb_lr_final_task_wait = ExternalTaskSensor(
            task_id="comb_lr_final_task_wait",
            external_dag_id="uw_combination_model_lr_dag_v1",
            external_task_id=external_task_id,
            execution_date_fn=lambda dt: get_most_recent_dag_run(
                "uw_combination_model_lr_dag_v1"
            ),
            check_existence=True,
            mode="reschedule",
        )
        comb_xg_final_task_wait = ExternalTaskSensor(
            task_id="comb_xg_final_task_wait",
            external_dag_id="uw_combination_model_xg_dag_v1",
            external_task_id=external_task_id,
            execution_date_fn=lambda dt: get_most_recent_dag_run(
                "uw_combination_model_xg_dag_v1"
            ),
            mode="reschedule",
        )
        merge_repayments_data = PythonOperator(
            task_id="merge_repayments_data",
            execution_timeout=timedelta(minutes=60),
            provide_context=True,
            op_kwargs={"dataset_name": dataset_name},
            python_callable=globals()[dataset_name].merge_repayments_data,
            # executor_config=combined_resource_config,
        )
        calculate_gini_score = PythonOperator(
            task_id="calculate_gini_score",
            execution_timeout=timedelta(minutes=60),
            provide_context=True,
            op_kwargs={"dataset_name": dataset_name},
            python_callable=globals()[dataset_name].calculate_gini_score,
            # executor_config=resource_config,
        )
        calculate_ks_score = PythonOperator(
            task_id="calculate_ks_score",
            execution_timeout=timedelta(minutes=60),
            provide_context=True,
            op_kwargs={"dataset_name": dataset_name},
            python_callable=globals()[dataset_name].calculate_ks_score,
            trigger_rule="none_failed",
            # executor_config=resource_config,
        )
        # write_master_table = PythonOperator(
        #     task_id="write_data_master_table",
        #     execution_timeout=timedelta(minutes=60),
        #     provide_context=True,
        #     op_kwargs={"dataset_name": prediction_name},
        #     python_callable=globals()[prediction_name].merge_master_table,
        #     #executor_config=combined_resource_config,
        # )
    start >> [comb_lr_final_task_wait, comb_xg_final_task_wait]
    [comb_lr_final_task_wait, comb_xg_final_task_wait] >> merge_repayments_data
    merge_repayments_data >> calculate_gini_score >> calculate_ks_score
    return dag


for dataset in ["KB_TXN_MODULE", "KB_ACTIVITY_MODULE", "KB_BUREAU_MODULE"]:
    globals()[f"uw_{dataset.lower()}_dag_v1"] = generate_dag(dataset_name=dataset)

for prediction in ["COMBINATION_MODEL_LR", "COMBINATION_MODEL_XG"]:
    if prediction == "COMBINATION_MODEL_LR":
        globals()[f"{prediction}_dag"] = combined_prediction_dag(
            prediction_name=prediction, external_task_id="model_prediction"
        )
    else:
        globals()[f"{prediction}_dag"] = combined_prediction_dag(
            prediction_name=prediction, external_task_id="xgboost_model_prediction"
        )
for dataset in ["MASTER_TABLE_GENERATION"]:
    globals()[f"{dataset.lower()}_dag_v1"] = gini_psi_calculation(
        dataset_name=dataset, external_task_id="write_data_master_table"
    )
