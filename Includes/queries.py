from datetime import datetime

#################################### AWS RDS Queries ###################################
SELECT_EMP_SAL = """
select * from finance.emp_sal
"""

SELECT_EMP_DETAIL = """
select * from hr.emp_details
"""

################################## Snowflake DWH Queries ###############################
SELECT_DWH_EMP_DIM = """
select * from airflow_iti.dimensions.employee_dim
"""

def INSERT_INTO_DWH_EMP_DIM(rows_to_insert):
    print("rows_to_insert:",rows_to_insert)
    sql = f"""
    insert into airflow_iti.dimensions.employee_dim (emp_id, name, marital_status, num_children, address, phone_number, job, hire_date, salary, effective_start_date, effective_end_date, active)
    values {rows_to_insert}
    """
    return sql



    
    
    
def UPDATE_DWH_EMP_DIM(ids_to_update):
    print("ids_to_update",ids_to_update)
    sql = f"""
    update airflow_iti.dimensions.employee_dim
    set effective_end_date = '{datetime.now().date().strftime("%Y-%m-%d")}',
    active = false
    where emp_id in ({ids_to_update}) and active=true
    """
    return sql





