from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta , timezone
from elasticsearch import Elasticsearch
from airflow.providers.docker.operators.docker import DockerOperator

# 1. กำหนด default_args
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# 2. กำหนดฟังก์ชัน Python สำหรับ Tasks
def extract_from_elasticsearch(**context):
    """Task 1: ดึงข้อมูลเมื่อวานจาก Elasticsearch"""
    print("Extracting data from ES...")
    # เชื่อมต่อกับ Elasticsearch
    es_client = Elasticsearch(
        "http://elasticsearch:9200"
        )
    if not es_client.ping():
        raise ValueError("Connection to Elasticsearch failed")
    print("Connected to Elasticsearch")

    # 2. คำนวณช่วงเวลา "เมื่อวาน" จาก execution_date ของ Airflow
    # execution_date คือเวลา *เริ่มต้น* ของช่วงเวลาที่ DAG รัน (เช่น 2025-10-27 00:00:00)
    now_utc = datetime.now(timezone.utc)

    today_start_utc = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    current_time_utc = now_utc

   # Format เวลาให้เป็น ISO UTC String ()
    start_time_iso = today_start_utc.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + 'Z' 
    end_time_iso = current_time_utc.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + 'Z'   

    print(f"Querying data between {start_time_iso} and {end_time_iso}")

    # 3. สร้าง Elasticsearch Query (ใช้ Query DSL)
    # เราต้องการข้อมูลทั้งหมดในช่วงเวลาที่กำหนด

    query = {
        "size": 100, # ลดขนาดลงหน่อย แค่ทดสอบ
        "_source": ["bus_id", "route", "timestamp", "lat", "lon", "speed_kmh"],
        "query": {
            "range": {
                "timestamp": {
                    "gte": start_time_iso,
                    "lte": end_time_iso,
                    # ไม่ต้องระบุ format เพราะเราส่ง String ที่ตรงเป๊ะแล้ว
                }
            }
        },
        "sort": [ { "timestamp": "desc" } ] # เรียงตามเวลาล่าสุด
    }
    # 4. ส่ง Query และดึงข้อมูล
    try:
        response = es_client.search(index="bus-locations", body=query)
        hits = response['hits']['hits']
        extracted_data = [hit['_source'] for hit in hits]

        print(f"✅ Extracted {len(extracted_data)} records from Elasticsearch.")

        # พิมพ์ข้อมูลตัวอย่างออกมาดู (ถ้ามี)
        if extracted_data:
            print("Sample data:")
            for i, record in enumerate(extracted_data[:3]): # พิมพ์สูงสุด 3 records
                print(f"  Record {i+1}: {record}")

        # ลอง push จำนวน records ผ่าน XComs
        context['ti'].xcom_push(key='extracted_record_count', value=len(extracted_data))

    except Exception as e:
        print(f"❌ Error querying Elasticsearch: {e}")
        raise e

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