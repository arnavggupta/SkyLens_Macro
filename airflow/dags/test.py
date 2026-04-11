from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
from datetime import datetime

#  In production will save these token in secrets 
HOST = "https://dbc-f32115ec-3d2b.cloud.databricks.com"
TOKEN = "dapi330bd381ab1904bf579262bb837f35d0"

def trigger_notebook(notebook_path):
    def _run():
        url = f"{HOST}/api/2.0/jobs/runs/submit"

        headers = {
            "Authorization": f"Bearer {TOKEN}"
        }

        payload = {
            "run_name": f"Run {notebook_path.split('/')[-1]}",
            "notebook_task": {
                "notebook_path": notebook_path
            }
        }

        response = requests.post(url, headers=headers, json=payload)

        print("STATUS:", response.status_code)
        print("RESPONSE:", response.text)

        if response.status_code != 200:
            raise Exception("Notebook execution failed")

    return _run


with DAG(
    dag_id="SkyLens_full_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    # ✅ Notebook paths
    bronze_path = "/Workspace/Users/202251023@iiitvadodara.ac.in/SkyLens_Macro/Bronze_Layer"
    silver_path = "/Workspace/Users/202251023@iiitvadodara.ac.in/SkyLens_Macro/Bronze_Layer"
    silver_dq_path = "/Workspace/Users/202251023@iiitvadodara.ac.in/SkyLens_Macro/Silver_Data_Quality_Checks"
    gold_path = "/Workspace/Users/202251023@iiitvadodara.ac.in/SkyLens_Macro/Gold_Layer"
    gold_dq_path = "/Workspace/Users/202251023@iiitvadodara.ac.in/SkyLens_Macro/Gold_Data_Quality_Checks"
    export_goldData_s3_path='/Workspace/Users/202251023@iiitvadodara.ac.in/SkyLens_Macro/export_Gold_To_S3'

    # ✅ Tasks
    bronze = PythonOperator(
        task_id="bronze_layer",
        python_callable=trigger_notebook(bronze_path)
    )

    silver = PythonOperator(
        task_id="silver_layer",
        python_callable=trigger_notebook(silver_path)
    )

    silver_dq = PythonOperator(
        task_id="silver_data_quality",
        python_callable=trigger_notebook(silver_dq_path)
    )

    gold = PythonOperator(
        task_id="gold_layer",
        python_callable=trigger_notebook(gold_path)
    )

    gold_dq = PythonOperator(
        task_id="gold_data_quality",
        python_callable=trigger_notebook(gold_dq_path)
    )

    export_goldData_s3=PythonOperator(
        task_id='export_data_in_s3',
        python_callable=trigger_notebook(export_goldData_s3_path)

    )

 
    bronze >> silver >> silver_dq >> gold >> gold_dq >> export_goldData_s3