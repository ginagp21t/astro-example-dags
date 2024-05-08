from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago
from datetime import  timedelta


## variables globales
location='US'
project_id='premium-guide-410714'
conexion_gcp='conexion_gcp_file'

## Definimos los argumentos para los dag's
default_args = {
    'owner': 'LuisM',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


    
def start_process():
    print(" INICIO EL PROCESO!")

def end_process():
    print(" FIN DEL PROCESO!")

def load_transaction():
    print(f" INICIO LOAD TRANSACTION")

with DAG(
    dag_id="load_project",
    schedule="20 04 * * *", 
    start_date=days_ago(1), 
    default_args=default_args
) as dag:
    step_start = PythonOperator(
        task_id='step_start_id',
        python_callable=start_process,
        dag=dag
    )


    step_raw_tipo_cambio =BigQueryInsertJobOperator(
        task_id= 'step_raw_tipo_cambio' ,
        #trigger_rule=TriggerRule.ALL_DONE,  
        configuration={"query": {"query": "CALL `raw_ventas.sp_load_ba_itc_audience_contact`()", "useLegacySql": False,}}, 
        gcp_conn_id=conexion_gcp,  
        location=location,
        force_rerun=True
        )
    step_end = PythonOperator(
        task_id='step_end_id',
        python_callable=end_process,
        dag=dag
    )
    step_start>>step_raw_tipo_cambio>>step_end