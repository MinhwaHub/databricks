from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
# from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
import sys
from airflow.hooks.base_hook import BaseHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

def slack_alert(context):
    slack_webhook_token = BaseHook.get_connection('slack_conn').password
    slack_msg = f"""
        :red_circle: DAG Failed 
        *Task*: {context.get('task_instance').task_id}  
        *Dag*: {context.get('task_instance').dag_id}  
        *Execution Time*: {context.get('execution_date')}  
        *Log URL*: {context.get('task_instance').log_url}
        """

    alert = SlackWebhookOperator(
        task_id='slack_alert',
        http_conn_id='slack_conn',
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username='airflow'
    )

    return alert.execute(context=context)


default_args = {
    'owner': 'airflow',  # Owner of the DAG
    'depends_on_past': False,  # DAG should not depend on past runs
    'start_date': datetime(2024, 8, 9),  # Start date for the DAG
    'retries': 2,  # Number of retries if a task fails
    'retry_delay': timedelta(minutes=5),  # Delay between retries
    'on_failure_callback': slack_alert
}

with DAG('mina_test',
    default_args = default_args,
    description='A simple example of execute databricks job',
    schedule_interval='0 16 * * *',  
    catchup=False 
  ) as dag:

    # Define Databricks job parameters
    job_1_params = {
        'job_id': '800504763956431',  # Replace with your Databricks Job ID for job 1
    }

    job_2_params = {
        'job_id': '800504763956432',  # Replace with your Databricks Job ID for job 2
    }

    job_3_params = {
        'job_id': '800504763956433',  # Replace with your Databricks Job ID for job 3
    }

    # Define tasks
    start = DummyOperator(
        task_id='start',
        dag=dag
    )

    run_job_1 = DatabricksRunNowOperator(
        task_id='run_job_1',
        databricks_conn_id='databricks_default',  # Connection ID configured in Airflow
        job_id=job_1_params['job_id'],
        dag=dag
    )

    run_job_2 = DatabricksRunNowOperator(
        task_id='run_job_2',
        databricks_conn_id='databricks_default',
        job_id=job_2_params['job_id'],
        dag=dag
    )

    run_job_3 = DatabricksRunNowOperator(
        task_id='run_job_3',
        databricks_conn_id='databricks_default',
        job_id=job_3_params['job_id'],
        dag=dag
    )

    end = DummyOperator(
        task_id='end',
        dag=dag
    )

    # Define the dependencies between tasks
    start >> run_job_1 >> run_job_2 >> run_job_3 >> end