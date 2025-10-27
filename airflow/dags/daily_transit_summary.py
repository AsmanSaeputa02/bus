from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# 1. กำหนด default_args
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# 2. กำหนดฟังก์ชัน Python สำหรับ Tasks
def extract_from_elasticsearch():
    """Task 1: ดึงข้อมูลเมื่อวานจาก Elasticsearch"""
    print("Extracting data from ES...")
    # TODO: เขียนโค้ดดึงข้อมูล

def transform_with_spark():
    """Task 2: ประมวลผลด้วย Spark Batch"""
    print("Running Spark batch job...")
    # TODO: เรียก Spark batch script

def load_to_postgresql():
    """Task 3: โหลดข้อมูลเข้า PostgreSQL"""
    print("Loading to PostgreSQL...")
    # TODO: เขียนโค้ด insert ข้อมูล

def data_quality_check():
    """Task 4: ตรวจสอบคุณภาพข้อมูล"""
    print("Checking data quality...")
    # TODO: เช็คว่าข้อมูลถูกต้อง

# 3. ประกาศ DAG (ที่ระดับบนสุด)
with DAG(
    'daily_transit_summary',
    default_args=default_args,
    description='Daily batch processing for transit data',
    schedule_interval='0 0 * * *',  # รันทุกเที่ยงคืน
    catchup=False,
) as dag:

    # 4. กำหนด Tasks ภายใน context ของ DAG
    t1 = PythonOperator(
        task_id='extract_from_es',
        python_callable=extract_from_elasticsearch,
    )

    t2 = PythonOperator(
        task_id='transform_spark',
        python_callable=transform_with_spark,
    )

    t3 = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgresql,
    )

    t4 = PythonOperator(
        task_id='quality_check',
        python_callable=data_quality_check,
    )

    # 5. กำหนด Dependencies
    t1 >> t2 >> t3 >> t4