from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from src.etl_process import ETLPipeline

@dag(start_date=datetime(2026, 1, 1), schedule_interval='@daily', catchup=False)
def finance_etl_dag():
    
    @task
    def run_incremental_load():
        # 1. Obtenemos las conexiones de forma segura desde Airflow
        # 'source_db_conn' y 'target_db_conn' se configuran en Airflow UI
        source_hook = PostgresHook(postgres_conn_id='source_db_conn')
        target_hook = PostgresHook(postgres_conn_id='target_db_conn')
        
        # 2. Obtenemos los engines (SQLAlchemy) desde los hooks
        source_engine = source_hook.get_sqlalchemy_engine()
        target_engine = target_hook.get_sqlalchemy_engine()
        
        # 3. Inyectamos los engines directamente al Pipeline
        pipeline = ETLPipeline(source_engine, target_engine)
        
        pipeline.run(last_timestamp="2026-04-01 00:00:00")

    run_incremental_load()

finance_etl_dag()