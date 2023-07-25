# import the libraries

from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago

#defining DAG arguments

# Task 1.1 - Define DAG arguments
default_args = {
    'owner': 'Ramesh Sannareddy',
    'start_date': days_ago(0),
    'email': ['ramesh@somemail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Task 1.2 - Define the DAG
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)

# define the tasks

# Task 1.3 - Create a task to unzip data
unzip_data = BashOperator(
    task_id='task1.3',
    bash_command='sudo tar -xzf tolldata.tgz',
    dag=dag,
)

# Task 1.4 - Create a task to extract data from csv file
extract_data_from_csv = BashOperator(
    task_id='task1.4',
    bash_command='sudo bash -c "cut -f 4 /home/project/airflow/dags/finalassignment/vehicle-data.csv > /home/project/airflow/dags/finalassignment/csv_data.csv"',
    dag=dag,
)

# Task 1.5 - Create a task to extract data from tsv file
extract_data_from_tsv = BashOperator(
    task_id='task1.5',
    bash_command='sudo bash -c "cut -f5-7 /home/project/airflow/dags/finalassignment/tollplaza-data.tsv > /home/project/airflow/dags/finalassignment/tsv_data.csv"',
    dag=dag,
)

# Task 1.6 - Create a task to extract data from fixed width file
extract_data_from_fixed_width = BashOperator(
    task_id='task1.6',
    bash_command='sudo bash -c "cut -c 59-67 /home/project/airflow/dags/finalassignment/payment-data.txt > /home/project/airflow/dags/finalassignment/fixed_width_data.csv"',
    dag=dag,
)

# Task 1.7 - Create a task to consolidate data extracted from previous tasks
consolidate_data = BashOperator(
    task_id='task1.7',
    bash_command='sudo bash -c "paste /home/project/airflow/dags/finalassignment/csv_data.csv '
    + '/home/project/airflow/dags/finalassignment/tsv_data.csv /home/project/airflow/dags/finalassignment/fixed_width_data.csv > '
    + '/home/project/airflow/dags/finalassignment/extracted_data.csv"',
    dag=dag,
)

# Task 1.8 - Transform and load the data
transform_data = BashOperator(
    task_id='task1.8',
    bash_command="sudo sed -i -E 's//^(([^,]+,){3})([^,]+)//\\1\\U\\3//' /home/project/airflow/dags/finalassignment/extracted_data.csv",
    dag=dag,
)

# Task 1.9 - Define the task pipeline
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data

