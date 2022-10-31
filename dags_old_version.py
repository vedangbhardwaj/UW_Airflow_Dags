from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import DagRun

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
import sys

sys.path.append(
    "/Users/vedang.bhardwaj/Desktop/work_mode/airflow_learn/UW_Airflow_Dags"
)

# def intimate_of_failure_on_slack(context):
#     slack_api_token = os.environ.get('SLACK_API_TOKEN')
#     slack = Slacker(slack_api_token)

#     message = """
#     Failure in execution of the DAG: %s.
#     """ % (context['dag'].dag_id)

#     slack.chat.post_message('airflow-jobs-slack-alerts', message ,username='Airflow',icon_emoji=':wind_blowing_face:')

with open("airflow_config.yml") as config_file:
    config = yaml.full_load(config_file)

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
        catchup=False,
    ) as dag:
        start = EmptyOperator(task_id=f"{dataset_name}", dag=dag)
        initial_declaration = BashOperator(
            task_id="Initial_Declaration",
            execution_timeout=timedelta(minutes=60),
            bash_command=f"cd /Users/vedang.bhardwaj/Desktop/work_mode/airflow_learn/UW_Airflow_Dags/{dataset_name}; python Initial_declaration.py",
        )

        getting_data = BashOperator(
            task_id="Getting_data",
            execution_timeout=timedelta(minutes=60),
            bash_command=f"cd /Users/vedang.bhardwaj/Desktop/work_mode/airflow_learn/UW_Airflow_Dags/{dataset_name}; python Getting_data.py",
        )

        data_validation = BashOperator(
            task_id="Data_Validation",
            execution_timeout=timedelta(minutes=60),
            bash_command=f"cd /Users/vedang.bhardwaj/Desktop/work_mode/airflow_learn/UW_Airflow_Dags/{dataset_name}; great_expectations checkpoint run autogen_suite_checkpoint",
        )

        WOE_calculation = BashOperator(
            task_id="WOE_Calculation",
            execution_timeout=timedelta(minutes=60),
            bash_command=f"cd /Users/vedang.bhardwaj/Desktop/work_mode/airflow_learn/UW_Airflow_Dags/{dataset_name}; python WOE_calculation.py",
            trigger_rule="none_skipped",
        )

        model_prediction = BashOperator(
            task_id="Model_prediction",
            execution_timeout=timedelta(minutes=60),
            bash_command=f"cd /Users/vedang.bhardwaj/Desktop/work_mode/airflow_learn/UW_Airflow_Dags/{dataset_name}; python Model_prediction.py",
        )

        xgboost_model = BashOperator(
            task_id="XGBoost_Model_Prediction",
            execution_timeout=timedelta(minutes=60),
            bash_command=f"cd /Users/vedang.bhardwaj/Desktop/work_mode/airflow_learn/UW_Airflow_Dags/{dataset_name}; python XGboost_prediction.py",
            trigger_rule="none_failed",
        )

    start >> initial_declaration >> getting_data
    getting_data >> [data_validation, xgboost_model]
    data_validation >> WOE_calculation
    WOE_calculation >> model_prediction
    return dag


def combined_prediction_dag(prediction_name, external_task_id):
    with DAG(
        dag_id=f"{prediction_name}_dag",
        default_args=args,
        schedule_interval=None,
        max_active_runs=1,
        max_active_tasks=1,
        catchup=False,
    ) as dag:

        def get_most_recent_dag_run(dag_id):
            dag_runs = DagRun.find(dag_id=dag_id)
            dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
            if dag_runs:
                return dag_runs[0].execution_date

        # def get_external_task_id(prediction_name):
        #     if prediction_name == "COMBINATION_MODEL_LR":
        #         return "Model_prediction"
        #     return "XGBoost_Model_Prediction"

        start = EmptyOperator(task_id=f"{prediction_name}", dag=dag)

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
            # external_task_id="Model_prediction",
            external_task_id=external_task_id,
            # external_task_id= lambda x : get_external_task_id(prediction_name),
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
        combined_model_prediction = BashOperator(
            task_id="Combined_model_prediction",
            execution_timeout=timedelta(minutes=60),
            bash_command=f"cd /Users/vedang.bhardwaj/Desktop/work_mode/airflow_learn/UW_Airflow_Dags/{prediction_name}; python Model_prediction.py",
        )

        start >> [kb_txn_module_wait, kb_activity_module_wait, kb_bureau_module_wait]
        [
            kb_txn_module_wait,
            kb_activity_module_wait,
            kb_bureau_module_wait,
        ] >> combined_model_prediction


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
