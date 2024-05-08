from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.providers.http.operators.http import HttpOperator
#from airflow.providers.google.cloud.hooks.base import GoogleCloudBaseHook
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import  timedelta
from google.auth import default
from google.auth.transport.requests import Request
from google.auth.transport import requests


## variables globales
location='US'
project_id='premium-guide-410714'
conexion_gcp='conexion_gcp_file'

## Definimos los argumentos para los dag's
default_args = {
    'owner': 'LuisM',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False
}

def get_gcp_token():
    credentials, _ = default()
    if credentials.expired:
        credentials.refresh(Request())
        
    return credentials.token
    
def start_process():
    print(" INICIO EL PROCESO!")
    # Obtener las credenciales predeterminadas de autenticación
    credentials, _ = default()

    # Crear una solicitud de autenticación
    auth_request = requests.Request()

    # Obtener información sobre el usuario actual
    userinfo = credentials.service_account_email

    # Imprimir el correo electrónico del usuario actual
    print("El usuario actual de Google Cloud Platform es:", userinfo)
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

#    Obtener el token de identidad de GCP
#    gcp_hook = GoogleCloudBaseHook(gcp_conn_id='google_cloud_default')
#    gcp_token = gcp_hook._get_credentials().token
    gcp_token = get_gcp_token()
    
    step_start = PythonOperator(
        task_id='step_start_id',
        python_callable=start_process,
        dag=dag
    )
   
    step_sunat_tip_cambio = BashOperator(
        task_id="step_sunat_tip_cambio",
        ##bash_command="ls -alh --color=always / && echo https://airflow.apache.org/  && echo 'some <code>html</code>'",
        bash_command='curl -m 70 -X POST https://us-central1-premium-guide-410714.cloudfunctions.net/prd-load_tipo_cambio -H "Authorization: bearer $(gcloud auth print-identity-token)" -H "Content-Type: application/json" '
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