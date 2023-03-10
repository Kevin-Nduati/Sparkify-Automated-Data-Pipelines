from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries, CreateTableOperator

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'end_date': datetime(2019, 1, 15),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          max_active_runs=1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables = CreateTableOperator(
    task_id = 'Create_tables',
    dag = dag,
    redshift_conn_id = 'redshift')

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    provide_context=True,
    redshift_conn_id = 'redshift',
    aws_credentials_id='aws_credentials',
    table = 'staging_events',
    s3_bucket='kevbucket100',
    s3_key = "log_data/{execution_date.year}/{execution_date.month}/{execution_date.year}-{execution_date.month}-0{execution_date.day}-events.json",
    region='us-west-2',
    data_format = 'json',
    jsonpaths=""
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    provide_context = True,
    redshift_conn_id = 'redshift',
    aws_credentials_id='aws_credentials',
    table = 'staging_songs',
    s3_bucket='kevbucket100',
    s3_key = "song_data",
    region='us-west-2',
    ignore_headers=0,
    data_format = 'json',
    jsonpaths=""
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    sql = SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    table = 'users',
    sql = SqlQueries.user_table_insert,
    update_strategy = 'overwrite'
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    table = 'users',
    sql = SqlQueries.song_table_insert,
    update_strategy = 'overwrite'
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    table = 'users',
    sql = SqlQueries.artist_table_insert,
    update_strategy = 'overwrite'
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    table = 'users',
    sql = SqlQueries.time_table_insert,
    update_strategy = 'overwrite'
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id = 'redshift',
    tables = ['songplays', 'artists', 'songs', 'users', 'time']
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


# Tasks dependencies
start_operator >> create_tables >> [stage_events_to_redshift, stage_songs_to_redshift]

[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table]

[load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks

run_quality_checks >> end_operator
