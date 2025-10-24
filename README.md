# 🚀 Hybrid Transit Data Platform

Real-time และ Batch Processing สำหรับระบบขนส่งมวลชน

## 🎯 เป้าหมาย
- แสดงตำแหน่งรถเมล์แบบ Real-time
- วิเคราะห์ประสิทธิภาพย้อนหลัง

## 🏗️ Architecture
- **Speed Layer:** Kafka → Spark Streaming → Elasticsearch → Kibana
- **Batch Layer:** Airflow → Spark Batch → PostgreSQL → BI Tools

## 🚀 Quick Start

### 1. เริ่มต้น Services
```bash
docker-compose up -d
```

### 2. สร้าง Kafka Topic
```bash
docker exec -it kafka kafka-topics --create \
  --topic bus-locations \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

### 3. รัน Producer
```bash
cd producer
python bus_producer.py
```

## 🌐 Web UIs
- Kibana: http://localhost:5601
- Spark: http://localhost:8080
- Airflow: http://localhost:8081 (admin/admin)

## 📚 Documentation
ดูรายละเอียดใน [docs/PROJECT_ROADMAP.md](docs/PROJECT_ROADMAP.md)