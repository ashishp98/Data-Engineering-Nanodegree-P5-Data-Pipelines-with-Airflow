import datetime
from airflow.models import DAG
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator, DataQualityOperator, CreateTablesOperator)
from helpers import SqlQueries
from sparkify_subdag import load_dimensional_tables_dag


start_date = datetime.datetime(2022, 2, 1)

default_args = {
    'owner': 'Udacity DE ND',
    'start_date': start_date,
    'end_date': datetime.datetime(2022, 7, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': datetime.timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag_name='sparkify_dag'
dag = DAG(dag_name,
          default_args=default_args,
          description="Loads and transforms the data in Redshift with Airflow",
          schedule_interval='0 * * * *',
          max_active_runs=3
        )

start_operator = DummyOperator(task_id='start_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='stage_events',
    dag=dag,
    provide_context=True,
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    region="us-west-2",
    file_format="JSON",
    execution_date=start_date
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='stage_songs',
    dag=dag,
    provide_context=True,
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    region="us-west-2",
    data_format="JSON"
)

load_songplays_table = LoadFactOperator(
    task_id='load_songplays_fact_table',
    dag=dag,
    provide_context=True,
    redshift_conn_id="redshift",
    sql_query=SqlQueries.songplay_table_insert
)

load_user_dimension_table_task_id='load_user_dim_table'
load_user_dimension_table = SubDagOperator(
    subdag=load_dimensional_tables_dag(
        parent_dag_name=dag_name,
        task_id=load_user_dimension_table_task_id,
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        start_date=start_date,
        table="users",
        sql_query=SqlQueries.user_table_insert,
    ),
    task_id=load_user_dimension_table_task_id,
    dag=dag,
)

load_song_dimension_table_task_id='load_song_dim_table'
load_song_dimension_table = SubDagOperator(
    subdag=load_dimensional_tables_dag(
        parent_dag_name=dag_name,
        task_id=load_song_dimension_table_task_id,
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        start_date=start_date,
        table="songs",
        sql_query=SqlQueries.song_table_insert,
    ),
    task_id=load_song_dimension_table_task_id,
    dag=dag,
)

load_artist_dimension_table_task_id='load_artist_dim_table'
load_artist_dimension_table = SubDagOperator(
      subdag=load_dimensional_tables_dag(
        parent_dag_name=dag_name,
        task_id=load_artist_dimension_table_task_id,
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="artists",
        start_date=start_date,
        sql_query=SqlQueries.artist_table_insert,
    ),
    task_id=load_artist_dimension_table_task_id,
    dag=dag,
)

load_time_dimension_table_task_id='load_time_dim_table'
load_time_dimension_table = SubDagOperator(
    subdag=load_dimensional_tables_dag(
        parent_dag_name=dag_name,
        task_id=load_time_dimension_table_task_id,
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="times",
        start_date=start_date,
        sql_query=SqlQueries.time_table_insert,
    ),
    task_id=load_time_dimension_table_task_id,
    dag=dag,
)

run_quality_checks = DataQualityOperator(
    task_id='run_data_quality_checks',
    dag=dag,
    provide_context=True,
    redshift_conn_id='redshift',
    tables=["songplays", "users", "songs", "artists", "times"],
    sql_check_queries=["SELECT COUNT(*) FROM songs WHERE songid IS NULL","SELECT COUNT(*) FROM songs"],
    expected_results=[lambda num_records: num_records==0, lambda num_records: num_records==0]
)

end_operator = DummyOperator(task_id='stop_execution',  dag=dag)

start_operator >> [stage_songs_to_redshift, stage_events_to_redshift]
[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, 
                         load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks

run_quality_checks >> end_operator