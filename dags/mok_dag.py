
import json
from io import BytesIO
from pathlib import Path
from datetime import datetime
from datetime import timedelta
from typing import Any

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from minio import Minio


MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = Variable.get("minio_access_key")
MINIO_SECRET_KEY = Variable.get("minio_secret_key")

default_args = {
    "owner": "i.korsakov",
    "depends_on_past": True,
    "start_date": datetime(2025, 11, 14),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "catchup": True,
    "retry_delay": timedelta(minutes=5),
}


def get_storage_options() -> dict:
    """
    Получает параметры подключения для pandas S3 operations.

    :return: Словарь с параметрами подключения для pandas.
    """
    protocol = "http"  # Измените на "https" если используете HTTPS
    endpoint_url = f"{protocol}://{MINIO_ENDPOINT}"

    return {
        "key": MINIO_ACCESS_KEY,
        "secret": MINIO_SECRET_KEY,
        "client_kwargs": {"endpoint_url": endpoint_url},
    }
    
def get_minio_client():
    """
    Создает MinIO клиент с учетными данными из Variables.

    :return: Объект MinIO клиента.
    """
    return Minio(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,  # Установите True если используете HTTPS
    )
    
    
def extract_data_into_s3(**context: dict[str, Any]) -> str:
    """
    Получение данных и сохранение в S3.

    :param context: Контекст DAG, содержащий информацию о выполнении задачи.
    :return: S3 ключ сохраненного JSON файла.
    """
    # execution_date = context["data_interval_end"].format("YYYY-MM-DD")
    execution_date = datetime.now().strftime("%Y-%m-%d:%f")[:-3]

    # Подключение к MinIO
    minio_client = get_minio_client()
    bucket_name = "okx"
    object_name = f"data/data_{execution_date}.json"

    # Убеждаемся что bucket существует
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)

    data_path = Path(__file__).parent / "data.json"
    with open(data_path, "r", encoding="utf-8") as js:
        data = json.load(js)
    # Преобразуем данные в JSON строку
    json_data = json.dumps(data, ensure_ascii=False, indent=2)
    json_bytes = json_data.encode("utf-8")

    # Сохраняем в MinIO
    minio_client.put_object(
        bucket_name=bucket_name,
        object_name=object_name,
        data=BytesIO(json_bytes),
        length=len(json_bytes),
        content_type="application/json",
    )

    print(f"data saved to s3://{bucket_name}/{object_name}")
    return object_name

with DAG(
    dag_id="test_dag",
    start_date=datetime(2025, 11, 10),
    schedule_interval="@hourly",
    catchup=False,
    max_active_runs=3,
) as dag:
    
    task_start = EmptyOperator(
        task_id="start_dag",
    )
    
    task_extract_data_into_s3 = PythonOperator(
        task_id="extract_data_into_s3",
        python_callable=extract_data_into_s3,
    )
    
    task_end = EmptyOperator(
        task_id="end_dag",
    )
    
    task_start >> task_extract_data_into_s3 >> task_end