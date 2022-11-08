from airflow.models import DAG, Variable
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import DagRun
import os
import sys
import imp

# sys.path.append('/opt/airflow/dags/repo/dags/template/')
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

root_folder = os.path.abspath(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) ## In order to import clients package which is one directory above from dev directory or prod directory
sys.path.append(root_folder) ## This could be removed when we start publishing packages and directly use them.


import KB_TXN_MODULE as KB_TXN_MODULE
import KB_ACTIVITY_MODULE as KB_ACTIVITY_MODULE
import KB_BUREAU_MODULE as KB_BUREAU_MODULE
import COMBINATION_MODEL_LR as COMBINATION_MODEL_LR
import COMBINATION_MODEL_XG as COMBINATION_MODEL_XG

# def intimate_of_failure_on_slack(context):
#     slack_api_token = os.environ.get('SLACK_API_TOKEN')
#     slack = Slacker(slack_api_token)

#     message = """
#     Failure in execution of the DAG: %s.
#     """ % (context['dag'].dag_id)

#     slack.chat.post_message('airflow-jobs-slack-alerts', message ,username='Airflow',icon_emoji=':wind_blowing_face:')

config = Variable.get("underwriting_dags", deserialize_json=True)

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


def generate_dag(dataset_name):
    with DAG(
        dag_id=f"{dataset_name}_dag",
        default_args=args,
        schedule_interval=None,
        max_active_runs=1,
        max_active_tasks=1,
        tags=['ds', 'underwriting', 'lending', 'model'],
        catchup=False,
    ) as dag:
        start = DummyOperator(task_id=f"{dataset_name}", dag=dag)

        # def str_to_class(classname):
        #     return getattr(sys.modules[__name__], classname)

        # module_name = str_to_class(dataset_name)

        getting_data = PythonOperator(
            task_id="Getting_data",
            execution_timeout=timedelta(minutes=60),
            op_kwargs={"dataset_name": dataset_name},
            python_callable=globals()[dataset_name].getting_data,
        )

#         data_validation = BashOperator(
#             task_id="Data_Validation",
#             execution_timeout=timedelta(minutes=60),
#             bash_command=f"cd /Users/vedang.bhardwaj/Desktop/work_mode/airflow_learn/UW_Airflow_Dags/{dataset_name}; great_expectations checkpoint run autogen_suite_checkpoint",
#         )

        WOE_calculation = PythonOperator(
            task_id="WOE_Calculation",
            execution_timeout=timedelta(minutes=60),
            op_kwargs={"dataset_name": dataset_name},
            python_callable=globals()[dataset_name].woe_calculation,
            # executor_config=kubernetes.resources(memory=2, cpu=2),
            trigger_rule="none_skipped",
        )

        model_prediction = PythonOperator(
            task_id="Model_prediction",
            execution_timeout=timedelta(minutes=60),
            op_kwargs={"dataset_name": dataset_name},
            python_callable=globals()[dataset_name].model_prediction,
            # executor_config=kubernetes.resources(memory=2, cpu=2),
            trigger_rule="none_skipped",
        )

        xgboost_model = PythonOperator(
            task_id="XGBoost_Model_Prediction",
            execution_timeout=timedelta(minutes=60),
            op_kwargs={"dataset_name": dataset_name},
            python_callable=globals()[dataset_name].xgboost_model_prediction,
            # executor_config=kubernetes.resources(memory=2, cpu=2),
            trigger_rule="none_failed",
        )

    start >> getting_data
#     getting_data >> [data_validation, xgboost_model]
    getting_data >> [WOE_calculation, xgboost_model]
#     data_validation >> WOE_calculation
    WOE_calculation >> model_prediction
    return dag


def combined_prediction_dag(prediction_name, external_task_id):
    with DAG(
        dag_id=f"{prediction_name}_dag",
        default_args=args,
        schedule_interval=None,
        max_active_runs=1,
        max_active_tasks=1,
        tags=['ds', 'underwriting', 'lending', 'model'],
        catchup=False,
    ) as dag:

        def get_most_recent_dag_run(dag_id):
            dag_runs = DagRun.find(dag_id=dag_id)
            dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
            if dag_runs:
                return dag_runs[0].execution_date

        start = DummyOperator(task_id=f"{prediction_name}", dag=dag)

        kb_txn_module_wait = ExternalTaskSensor(
            task_id="KB_TXN_MODULE_wait",
            # poke_interval=60,
            # timeout=180,
            # soft_fail=False,
            # retries=2,
            external_dag_id="KB_TXN_MODULE_dag",
            external_task_id=external_task_id,
            execution_date_fn=lambda dt: get_most_recent_dag_run("KB_TXN_MODULE_dag"),
            check_existence=True,
            mode="reschedule",
        )
        kb_activity_module_wait = ExternalTaskSensor(
            task_id="KB_ACTIVITY_MODULE_wait",
            external_dag_id="KB_ACTIVITY_MODULE_dag",
            external_task_id=external_task_id,
            execution_date_fn=lambda dt: get_most_recent_dag_run(
                "KB_ACTIVITY_MODULE_dag"
            ),
            mode="reschedule",
        )
        kb_bureau_module_wait = ExternalTaskSensor(
            task_id="KB_BUREAU_MODULE_wait",
            external_dag_id="KB_BUREAU_MODULE_dag",
            external_task_id=external_task_id,
            execution_date_fn=lambda dt: get_most_recent_dag_run(
                "KB_BUREAU_MODULE_dag"
            ),
            mode="reschedule",
        )
        # combined_model_prediction = BashOperator(
        #     task_id="Combined_model_prediction",
        #     execution_timeout=timedelta(minutes=60),
        #     bash_command=f"cd /Users/vedang.bhardwaj/Desktop/work_mode/airflow_learn/UW_Airflow_Dags/{prediction_name}; python Model_prediction.py",
        # )
        combined_model_prediction = PythonOperator(
            task_id="Combined_model_prediction",
            execution_timeout=timedelta(minutes=60),
            op_kwargs={"dataset_name": prediction_name},
            python_callable=globals()[prediction_name].predict,
        )
    start >> [kb_txn_module_wait, kb_activity_module_wait, kb_bureau_module_wait]
    [
        kb_txn_module_wait,
        kb_activity_module_wait,
        kb_bureau_module_wait,
    ] >> combined_model_prediction
    return dag

for dataset in ["KB_TXN_MODULE", "KB_ACTIVITY_MODULE", "KB_BUREAU_MODULE"]:
    globals()[f"{dataset}_dag"] = generate_dag(dataset_name=dataset)

for prediction in ["COMBINATION_MODEL_LR", "COMBINATION_MODEL_XG"]:
    if prediction == "COMBINATION_MODEL_LR":
        globals()[f"{prediction}_dag"] = combined_prediction_dag(
            prediction_name=prediction, external_task_id="Model_prediction"
        )
    else:
        globals()[f"{prediction}_dag"] = combined_prediction_dag(
            prediction_name=prediction, external_task_id="XGBoost_Model_Prediction"
        )
