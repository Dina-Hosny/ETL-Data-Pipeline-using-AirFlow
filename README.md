
# ETL Data Pipeline using AirFlow



## Project Overview:

An ETL Data Pipelines Project that uses AirFlow DAGs to extract employees' data from PostgreSQL Schemas, load it in AWS Data Lake, Transform it with Python script, and Finally load it into SnowFlake Data warehouse using SCD type 2.


## Project Details:

![New Project](https://github.com/Dina-Hosny/ETL-Data-Pipeline-using-AirFlow/assets/46838441/32769201-4ecb-487a-b999-bbed1a851c2b)


The idea of the project is to use the AirFlow DAGs to extract the employees' data from HR and Finance PostgreSQL schemas and load it into a Snowflake data warehouse to store it and keep all salary change history.

The AirFlow DAG runs hourly to check and extract all new data from the PostgreSQL source, then load it into AWS S3 buckets used as a Data Lake containing all raw data as CSV files. After that, some Python functions will be applied to extract the new records that will be inserted and the records that will be updated to perform the Slowly Changing Dimension 'SCD' concept to keep all historical employees' salary changes in the Snowflake Data warehouse.


## Project Steps:

- 1- Implement An AirFlow DAG that runs hourly and used the TaskFlow approach to pass the outputs from each task to another.

- 2- Implement two tasks that use the ```SqlToS3Operator``` operation to extract the data from PostgreSQL schema to AWS S3 buckets in CSV file format. One of the tasks is for extracting HR data and the other one is for extracting Finance Data.

- 3- Implement two tasks that perform some Python functions on the extracted data to retrieve the IDs of the new records to insert them in the Data warehouse, and the IDs of the records that contain salary changes to update it and insert new records with new values to apply the SCD type 2 concept.

- 4- Load the data into the Snowflake Data warehouse table.

- 5- The Airflow DAG contains some Python functions using 'use the ```BranchPythonOperator``` operation to check if there are new records to insert or records to update before running the task to avoid errors. 

## Tools and Technologies:

- Apache Airflow
- Python
- Pandas
- PostgreSQL
- Snowflake
- AWS S3
- ETL
- Data Warehouse Concepts
- SCD


## Project Files:

- ```Dags```: Contains the AirFlow Dag.
- ```Includes```: Contains the SQL and Python scripts that uses in the AirFlow Dag.
- ```Output```: Contains screenshots from the AirFlow Dag Output. 

## Project Output:
![Screenshot 2023-05-14 191951](https://github.com/Dina-Hosny/ETL-Data-Pipeline-using-AirFlow/assets/46838441/eee98f52-db18-4022-bce0-6c38fb7dadd2)



