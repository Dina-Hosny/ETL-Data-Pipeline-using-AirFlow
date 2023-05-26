from airflow import DAG
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime
import sys

sys.path.append('/opt/airflow/includes')
import queries
from emp_dim_insert_update import join_and_detect_new_or_changed_rows

# Functions to check if there IDs to Insert or Update before insert and update

def check_ids_to_update(**context):
    ids_to_update = context['ti'].xcom_pull(task_ids="join_and_detect_new_or_changed_rows", key="ids_to_update")
    if ids_to_update == '':
        return 'check_rows_to_insert'
    else:
        return 'snowflake_update_task'


def check_rows_to_insert(**context):
    rows_to_insert = context['ti'].xcom_pull(task_ids="join_and_detect_new_or_changed_rows", key="rows_to_insert")
    if rows_to_insert is None:
        return 'skip_snowflake_insert_task'
    else:
        return 'snowflake_insert_task'
        


# Start the Airflow DAG


with DAG("ETL_Dag", start_date=datetime(2023, 5, 12), catchup=False, schedule='@hourly') as Dag:



    # Task to read Finance data
    
    extract_finance = SqlToS3Operator(
        task_id="extract_finance",
        sql_conn_id="PostgreSQL_conn",
        aws_conn_id="AWS_S3_conn",
        query= queries.SELECT_EMP_SAL,
        s3_bucket="staging.emp.data",
        s3_key="Dina_emp_data.csv",
        replace=True
    )
    
    
    # Task to read HR data

    extract_hr = SqlToS3Operator(
        task_id="extract_hr",
        sql_conn_id="PostgreSQL_conn",
        aws_conn_id="AWS_S3_conn",
        query= queries.SELECT_EMP_DETAIL,
        s3_bucket="staging.emp.data",
        s3_key="Dina_hr_sal.csv",
        replace=True
    )
    
    # Task to apply the python script to find the new records and updates

    join_and_detect_task = join_and_detect_new_or_changed_rows()
    
    
    # Task to insert the new records

    snowflake_insert_task = SnowflakeOperator(
        task_id='snowflake_insert_task',
        sql= queries.INSERT_INTO_DWH_EMP_DIM('{{ ti.xcom_pull(task_ids="join_and_detect_new_or_changed_rows", key="rows_to_insert") }}'),
        snowflake_conn_id="snowflake_conn",
        trigger_rule="none_failed"
    )
    
    
    # Task to update the changed records

    snowflake_update_task = SnowflakeOperator(
        task_id='snowflake_update_task',
        sql=queries.UPDATE_DWH_EMP_DIM('{{ti.xcom_pull(task_ids="join_and_detect_new_or_changed_rows", key="ids_to_update")}}'),
        snowflake_conn_id="snowflake_conn",
    )
    
    # Task to check if there IDs to update before perform the update

    check_ids_to_update_task = BranchPythonOperator(
        task_id='check_ids_to_update',
        python_callable=check_ids_to_update,
        provide_context=True
    )
    
    # Task to check if there IDs to Insert before perform the insert task

    check_rows_to_insert_task = BranchPythonOperator(
        task_id='check_rows_to_insert',
        python_callable=check_rows_to_insert,
        provide_context=True
    )


    # Tasks Flow

    [extract_finance, extract_hr] >> join_and_detect_task >> check_ids_to_update_task >> [snowflake_update_task, check_rows_to_insert_task]
    check_rows_to_insert_task >> snowflake_insert_task
    snowflake_update_task >> snowflake_insert_task
    