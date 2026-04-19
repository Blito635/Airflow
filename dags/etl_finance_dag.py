from airflow.decorators import dag, task
from datetime import datetime
from src.etl_process import ETLPipeline

@dag(start_date=datetime(2026, 1, 1), schedule_interval='@daily', catchup=False)
def finance_etl_dag():
    
    @task
    def run_incremental_load():
        pipeline = ETLPipeline()
        # En producción, obtendrías el last_timestamp de un XCom o tabla de control
        last_timestamp = "2026-04-01 00:00:00" 
        pipeline.run(last_timestamp)

    run_incremental_load()

finance_etl_dag()