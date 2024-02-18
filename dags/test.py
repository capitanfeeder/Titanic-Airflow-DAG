from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.sql_sensor import SqlSensor
from airflow.utils.task_group import TaskGroup

from datetime import datetime, timedelta
from pandas_profiling import ProfileReport
import pytz
import pandas as pd

TZ = pytz.timezone('America/Buenos_Aires')
TODAY = datetime.now(TZ).strftime('%Y-%m-%d')
URL = 'https://raw.githubusercontent.com/capitanfeeder/test/main/titanic.csv'
PATH = '/opt/airflow/dags/data/titanic.csv'
OUTPUT_DQ = '/opt/airflow/dags/data/data_quality_report_{}.html'.format(TODAY)
OUTPUT_SQL = '/opt/airflow/dags/sql/titanic_{}.sql'.format(TODAY)
OUTPUT = '/opt/airflow/dags/data/titanic_curated_{}.csv'.format(TODAY)
TARGET = 'sql/titanic_{}.sql'.format(TODAY)

DEFAULT_ARGS = {
    'owner' : 'Gabriel',
    'retries' : 2,
    'retry_delay' : timedelta(minutes=0.5)
}

def _profile():
    df = pd.read_csv(PATH)
    profile = ProfileReport(df, title = 'Data Quality Report')
    profile.to_file(OUTPUT_DQ)

def _curated():
    # Read the csv
    df = pd.read_csv(PATH)

    # Drop the columns
    df.drop(['Ticket', 'Cabin'], axis = 1, inplace = True)

    # Fill the missing values
    df['Age'].fillna(df['Age'].median(), inplace = True)
    df['Embarked'].fillna(df['Embarked'].mode()[0], inplace = True)

    # We modify the 'Name' column to obtain first and last name
    df['Full Name'] = df['Name'].apply(lambda x: ' '.join(x.split(',')[1].split('(')[0].strip().split(' ')[1:]) + ' ' + x.split(',')[0])

    df['Full Name'] = df['Full Name'].str.replace('["\']', '', regex = True)

    # We add a new column 'Title' from the column 'Name'
    df['Title'] = df['Name'].apply(lambda x: x.split(',')[1].split('.')[0].strip())

    # We remove the 'Name' column
    df.drop(['Name'], axis=1, inplace=True)
    
    # We simplify the titles
    tittle_dict = {
        'Capt': 'Officer',
        'Col': 'Officer',
        'Major': 'Officer',
        'Jonkheer': 'Royalty',
        'Don': 'Royalty',
        'Sir': 'Royalty',
        'Dr': 'Officer',
        'Rev': 'Officer',
        'the Countess': 'Royalty',
        'Dona': 'Royalty',
        'Mme': 'Mrs',
        'Mlle': 'Miss',
        'Ms': 'Mrs',
        'Mr': 'Mr',
        'Mrs': 'Mrs',
        'Miss': 'Miss',
        'Master': 'Master',
        'Lady': 'Royalty'
    }
    df['Title'] = df['Title'].map(tittle_dict)

    # We rearrange the columns in the desired order
    df = df[['PassengerId', 'Full Name', 'Title', 'Survived', 'Pclass', 'Sex', 'Age', 'SibSp', 'Parch', 'Fare', 'Embarked']]

    # Save the new csv
    df.to_csv(OUTPUT, index = False)

    # We iterate over the rows and create the inserts
    with open(OUTPUT_SQL, 'w') as file:
        for index, row in df.iterrows():
            values = f"({row['PassengerId']}, '{row['Full Name']}', '{row['Title']}', {row['Survived']}, {row['Pclass']}, '{row['Sex']}', {row['Age']}, {row['SibSp']}, {row['Parch']}, {row['Fare']}, '{row['Embarked']}')"
            insert = f"INSERT INTO raw_titanic VALUES {values};\n"
            file.write(insert)

with DAG(
    'dag_test',
    description = 'Data pipeline for test',
    default_args = DEFAULT_ARGS,
    catchup = False,
    start_date = datetime(2024,1,1),
    schedule_interval = '@daily',
    tags = ['proyecto48hs', 'sdc']

) as dag:
    download = BashOperator(
        task_id = 'download_csv',
        bash_command = 'curl -o {{ params.path }} {{ params.url }}',
        params = {'url' : URL, 'path' : PATH}
    )

    with TaskGroup('DataQuality', tooltip="Data Quality Engine") as dq:
        profiling = PythonOperator(
            task_id = 'profiling',
            python_callable = _profile
        )

        curated = PythonOperator(
            task_id = 'curated',
            python_callable = _curated
        )

        profiling >> curated

    with TaskGroup('RawLayer', tooltip="Data Engine") as raw_layer:
        create_raw = PostgresOperator(
            task_id = 'create_raw',
            postgres_conn_id = 'postgres_docker',
            sql = """
                CREATE TABLE IF NOT EXISTS raw_titanic (
                        passenger_id VARCHAR(50),
                        full_Name VARCHAR(50),
                        title VARCHAR(10),
                        survived INT,
                        pclass INT,
                        sex VARCHAR(10),
                        age FLOAT,
                        sibsp INT,
                        parch INT,
                        fare FLOAT,
                        embarked VARCHAR(10)
                )
            """
        )

        load_raw = PostgresOperator(
            task_id = 'load_raw',
            postgres_conn_id = 'postgres_docker',
            sql = TARGET
        )

        create_raw >> load_raw

    with TaskGroup('MasterLayer', tooltip="Data Engine") as master_layer:
        create_master = PostgresOperator(
            task_id = 'create_master',
            postgres_conn_id = 'postgres_docker',
            sql = """
                CREATE TABLE IF NOT EXISTS master_titanic (
                        id VARCHAR(50),
                        full_Name VARCHAR(50),
                        title VARCHAR(10),
                        survived VARCHAR(50),
                        age VARCHAR(10),
                        generation VARCHAR(20),
                        genre VARCHAR(10),
                        brot VARCHAR(10),
                        fat_son VARCHAR(10),
                        ticket_price VARCHAR(10),
                        port VARCHAR(20),
                        load_date VARCHAR(10),
                        load_datetime TIMESTAMP
                        )
                    """
            )

        load_master = PostgresOperator(
            task_id = 'load_master',
            postgres_conn_id = 'postgres_docker',
            sql = """
                INSERT INTO master_titanic (
                    id, full_name, title, survived, age, generation, genre, brot, fat_son,
                    ticket_price, port, load_date, load_datetime
                    )
                SELECT DISTINCT
                CAST(passenger_id AS VARCHAR) AS id,
                CAST(full_name AS VARCHAR) AS full_name,
                CAST(title AS VARCHAR) AS title,
                CASE
                    WHEN survived = 1 THEN 'Survived'
                    ELSE 'Died'
                END AS survived,
                CAST(age AS VARCHAR) AS age,
                CASE
                    WHEN age < 18 THEN 'Minor'
                    WHEN age < 35 THEN 'Young'
                    ELSE 'Old'
                END AS generation,
                CASE
                    WHEN sex = 'male' THEN 'MALE'
                    ELSE 'FEMALE'
                END AS genre,
                CAST(sibsp AS VARCHAR) AS brot,
                CAST(parch AS VARCHAR) AS fat_son,
                CAST(fare AS VARCHAR) AS ticket_price,
                CASE
                    WHEN embarked = 'C' THEN 'Cherbourg'
                    WHEN embarked = 'Q' THEN 'Queenstown'
                    ELSE 'Southampton'
                END AS port,
                CAST({{ params.today }} AS VARCHAR) AS load_date,
                now() AS load_datetime
                FROM public.raw_titanic t
                WHERE NOT EXISTS (
                    SELECT 1 FROM master_titanic lt
                    WHERE lt.id = CAST(t.passenger_id AS VARCHAR)
                )
                """,
            params = {'today' : TODAY}
        )

        create_master >> load_master

    validator = SqlSensor(
        task_id = 'validator',
        conn_id = 'postgres_docker',
        sql = "SELECT COUNT(*) FROM public.master_titanic WHERE load_date = CAST({{ params.today }} AS VARCHAR)",
        params = {'today' : TODAY},
        timeout = 30,
    )    

    download >> dq >> raw_layer >> master_layer >> validator