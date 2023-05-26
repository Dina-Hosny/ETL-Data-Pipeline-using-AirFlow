import boto3
import psycopg2
import os
import pandas as pd
from datetime import datetime
from snowflake.connector import connect
import queries
from airflow.decorators import task

@task(multiple_outputs=True)

def join_and_detect_new_or_changed_rows():
    ################################### Connect to S3 #####################################
    AWS_ACCESS_KEY=''
    AWS_SECRET_KEY=''

    os.environ['AWS_ACCESS_KEY_ID']=AWS_ACCESS_KEY
    os.environ['AWS_SECRET_ACCESS_KEY']=AWS_SECRET_KEY

    emp_detail = pd.read_csv('s3://staging.emp.data/Dina_hr_sal.csv')
    emp_detail.drop(columns='Unnamed: 0', inplace=True)
    emp_sal = pd.read_csv('s3://staging.emp.data/Dina_emp_data.csv')
    emp_sal.drop(columns='Unnamed: 0', inplace=True)
    print("Succesfully read data from AWS S3")
    
    # Join all employee data
    src_emp_df = pd.merge(emp_detail, emp_sal, how='outer', on='emp_id')

    # drop department column as it's not part of employee dimension
    src_emp_df.drop('dept_id', axis=1, inplace=True)

    ################################### Connect to DWH ###################################
    # Use your Snowfake user credentials to connect
    conn = connect(
            user='',
            password='',
            account=''
        )
    print("Connected to Snowflake DWH succesfully")
    
    
    
    
    
    # Create your SQL command
    sql_query = queries.SELECT_DWH_EMP_DIM
    
    # Create the cursor object with your SQL command
    cursor = conn.cursor()
    cursor.execute(sql_query)
    print("query executed")
    # Convert output to a dataframe
    tgt_emp_df = cursor.fetch_pandas_all()
    print("data fetched")
    cursor.close()
    print("cursor closed")
    
    # columns renaming
    src_emp_df.columns = ["src_"+col for col in src_emp_df.columns]
    tgt_emp_df.columns = ["tgt_"+col.lower() for col in tgt_emp_df.columns]
    print("columns renamed")
    
    # Join source & target data and add effective dates and active flag
    src_plus_tgt = pd.merge(src_emp_df, tgt_emp_df, how='left', left_on='src_emp_id', right_on='tgt_emp_id')
    src_plus_tgt['effective_start_date'] = datetime.now().date().strftime("%Y-%m-%d")
    src_plus_tgt['effective_end_date'] = "2999-12-31"
    src_plus_tgt['active'] = True

    # Get new rows only (i.e. rows that doesnt exist in DWH, all DWH data will be null)
    new_inserts = src_plus_tgt[src_plus_tgt.tgt_emp_id.isna()].copy()

    # Select only source columns and the effective dates and active flag
    cols_to_insert = src_emp_df.columns.tolist() + ['effective_start_date', 'effective_end_date', 'active']

    # Convert result to string
    result_list = new_inserts[cols_to_insert].values.tolist()
    result_tuple = [tuple(row) for row in result_list]
    new_rows_to_insert = str(result_tuple).lstrip('[').rstrip(']')
    print("Found {} new rows".format(len(result_list)))

    # Get changed rows only (i.e. rows that exist in DWH but with different salary)
    insert_updates = src_plus_tgt[(src_plus_tgt['src_salary'] != src_plus_tgt['tgt_salary']) & (~src_plus_tgt.tgt_emp_id.isna())]

    # Convert result to string
    insert_list = insert_updates[cols_to_insert].values.tolist()
    insert_tuples = [tuple(row) for row in insert_list]
    changed_rows_to_insert = str(insert_tuples).lstrip('[').rstrip(']')
    
    
    # The resulted string will be sent to the next task (SnowflakeOperator) to use in insert query
    if changed_rows_to_insert == '':
        rows_to_insert = new_rows_to_insert
    else:
        rows_to_insert = new_rows_to_insert + ", " + changed_rows_to_insert

    # The resulted string will be sent to the next task (SnowflakeOperator) to use in insert query
    #rows_to_insert = new_rows_to_insert + ", " + changed_rows_to_insert

    # Get emp_ids to upgrade
    ids_to_update = insert_updates.src_emp_id.tolist()
    print("Found {} changed rows".format(len(ids_to_update)))

    # This result will be sent to the next task (SnowflakeOperator) to use in update query
    ids_to_update = str(ids_to_update).lstrip('[').rstrip(']')
    
    return {"rows_to_insert": rows_to_insert, "ids_to_update": ids_to_update}
