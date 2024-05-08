from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.providers.google.cloud.hooks.base import GoogleCloudBaseHook
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
    
    # Obtener el token de identidad de GCP
    gcp_hook = GoogleCloudBaseHook(gcp_conn_id='google_cloud_default')
    gcp_token = gcp_hook._get_credentials().token
    
    step_sunat_tip_cambio = SimpleHttpOperator(
        task_id='step_sunat_tip_cambio',
        http_conn_id='http_default',  # O el ID de tu conexión HTTP si has configurado una personalizada
        method='POST',
        endpoint='https://us-central1-premium-guide-410714.cloudfunctions.net/prd-load_tipo_cambio',
        headers={
            "Authorization": f"Bearer {gcp_token}",
            "Content-Type": "application/json"
        },
        data='{"load_type": "storage","target":"gs://test-nh/tipo_cambio.csv"}'
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
    step_start>>step_sunat_tip_cambio>>step_raw_tipo_cambio>>step_end