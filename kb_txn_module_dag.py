from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from great_expectations_provider.operators.great_expectations import (
    GreatExpectationsOperator,
)
from great_expectations.core.batch import BatchRequest
from great_expectations.data_context.types.base import (
    DataContextConfig,
    CheckpointConfig,
)

from datetime import datetime
from datetime import timedelta
import airflow
import yaml

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
            bash_command= f"cd /Users/vedang.bhardwaj/Desktop/work_mode/airflow_learn/UW_Airflow_Dags/{dataset_name}; python Initial_declaration.py",
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


for dataset in ["KB_TXN_MODULE","KB_ACTIVITY_MODULE"]:
    globals()[f"{dataset}_dag"] = generate_dag(dataset_name=dataset)
