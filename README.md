# Demo-ETL-data-pipelines-using-Apache-Airflow

This is a demo project to create ETL data pipelines using Apache Airflow.

## Objectives

1. Extract data from a csv file

2. Extract data from a tsv file

3. Extract data from a fixed width file

4. Transform the data

5. Load the transformed data into the staging area

## Installation and Setup

1. Clone the repository or download the code to your local system.

2. Navigate to the project directory in your terminal.

3. Open a terminal and create a directory structure for staging area as follows:
```
sudo mkdir -p /home/project/airflow/dags/finalassignment/staging
```

4. Download the dataset from the source to the destination mentioned below using wget command.
```
cd /home/project/airflow/dags/finalassignment
sudo wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz
```
You may also extract the data from `/data/tolldata.tgz` instead of `wget` from the link.

## Running the Application

1. Start Apache Airflow.
```
start_airflow
```

2. Submit the DAG.
```
cp ETL_toll_data.py $AIRFLOW_HOME/dags
```

3. Verify that the DAG actually got submitted.
```
airflow dags list
airflow dags list|grep "ETL_toll_data"
airflow tasks list ETL_toll_data
```

4. Unpause the DAG to start.
```
airflow dags unpause ETL_toll_data
```

5. If you want to stop the DAG, pause the it.
```
airflow dags pause ETL_toll_data
```