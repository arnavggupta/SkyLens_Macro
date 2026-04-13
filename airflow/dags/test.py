# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
# import requests
# from datetime import datetime

# HOST = "https://dbc-f32115ec-3d2b.cloud.databricks.com"



# def trigger_notebook(notebook_path):
#     def _run():
#         url = f"{HOST}/api/2.0/jobs/runs/submit"

#         headers = {
#             "Authorization": f"Bearer {TOKEN}"
#         }

#         payload = {
#             "run_name": f"Run {notebook_path.split('/')[-1]}",
#             "tasks": [
#                 {
#                     "task_key": "task1",
#                     "notebook_task": {
#                         "notebook_path": notebook_path
#                     }
#                 }
#             ]
#         }

#         response = requests.post(url, headers=headers, json=payload)

#         print("RUNNING:", notebook_path)
#         print("STATUS:", response.status_code)
#         print("RESPONSE:", response.text)

#         if response.status_code != 200:
#             raise Exception(f"Notebook failed: {response.text}")

#     return _run


# with DAG(
#     dag_id="SkyLens_full_pipeline",
#     start_date=datetime(2024, 1, 1),
#     schedule=None,
#     catchup=False
# ) as dag:

#     bronze_path = "/Workspace/Users/202251023@iiitvadodara.ac.in/SkyLens_Macro/Bronze_Layer"
#     silver_path = "/Workspace/Users/202251023@iiitvadodara.ac.in/SkyLens_Macro/Silver_Layer"
#     silver_dq_path = "/Workspace/Users/202251023@iiitvadodara.ac.in/SkyLens_Macro/Silver_Data_Quality_Checks"
#     gold_path = "/Workspace/Users/202251023@iiitvadodara.ac.in/SkyLens_Macro/Gold_Layer"
#     gold_dq_path = "/Workspace/Users/202251023@iiitvadodara.ac.in/SkyLens_Macro/Gold_Data_Quality_Checks"
#     export_goldData_s3_path = "/Workspace/Users/202251023@iiitvadodara.ac.in/SkyLens_Macro/export_Gold_To_S3"

#     bronze = PythonOperator(
#         task_id="bronze_layer",
#         python_callable=trigger_notebook(bronze_path)
#     )

#     silver = PythonOperator(
#         task_id="silver_layer",
#         python_callable=trigger_notebook(silver_path)
#     )

#     silver_dq = PythonOperator(
#         task_id="silver_data_quality",
#         python_callable=trigger_notebook(silver_dq_path)
#     )

#     gold = PythonOperator(
#         task_id="gold_layer",
#         python_callable=trigger_notebook(gold_path)
#     )

#     gold_dq = PythonOperator(
#         task_id="gold_data_quality",
#         python_callable=trigger_notebook(gold_dq_path)
#     )

#     export_goldData_s3 = PythonOperator(
#         task_id="export_data_in_s3",
#         python_callable=trigger_notebook(export_goldData_s3_path)
#     )

#     snowflake_step = SQLExecuteQueryOperator(
#         task_id="snowflake_transformation",
#         conn_id="snowflake_default",
#         sql="sql/snowflake_transformations.sql"
#     )

#     bronze >> silver >> silver_dq >> gold >> gold_dq >> export_goldData_s3 >> snowflake_step



from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import requests
import time
from datetime import datetime

HOST = "https://dbc-f32115ec-3d2b.cloud.databricks.com"
TOKEN =OS.env('DATABRICKS_TOKEN)
JOB_ID = os.env('JOB_ID') 


def trigger_databricks_job():
    url = f"{HOST}/api/2.0/jobs/run-now"

    headers = {
        "Authorization": f"Bearer {TOKEN}"
    }

    response = requests.post(url, headers=headers, json={"job_id": JOB_ID})
    print("STATUS:", response.status_code)
    print("RESPONSE:", response.text)

    if response.status_code != 200:
        raise Exception(f"Job trigger failed: {response.text}")

    run_id = response.json()["run_id"]
    print(f"Job started with run_id: {run_id}")

    # ✅ Poll until all tasks inside the job complete
    while True:
        status_resp = requests.get(
            f"{HOST}/api/2.0/jobs/runs/get?run_id={run_id}",
            headers=headers
        )
        state = status_resp.json()["state"]
        life_cycle = state["life_cycle_state"]
        print(f"Job state: {life_cycle}")

        if life_cycle == "TERMINATED":
            result = state["result_state"]
            if result != "SUCCESS":
                raise Exception(f"Databricks job failed with result: {result}")
            print("Databricks job completed successfully!")
            break
        elif life_cycle == "INTERNAL_ERROR":
            raise Exception("Databricks job hit internal error")

        time.sleep(20)  # poll every 20 seconds


with DAG(
    dag_id="SkyLens_full_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    databricks_job = PythonOperator(
        task_id="run_databricks_pipeline",
        python_callable=trigger_databricks_job
    )

    snowflake_step = SQLExecuteQueryOperator(
        task_id="snowflake_transformation",
        conn_id="snowflake_default",
        sql="sql/snowflake_transformations.sql"
    )

    databricks_job >> snowflake_step
