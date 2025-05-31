# ⚡️ GoIT DE Homework 6 — Spark Streaming IoT Monitoring

## 📘 Task Overview

This project simulates an IoT monitoring system using **Apache Kafka** and **Apache Spark Structured Streaming**. The system:

- Consumes sensor data from a Kafka topic.
- Aggregates average temperature and humidity using a sliding window.
- Applies alert thresholds read from a CSV file.
- Outputs alerts to another Kafka topic.

---

## 🧱 Architecture

- **Sensor producer** → sends JSON messages (sensor_id, timestamp, temperature, humidity) to Kafka.
- **Spark Structured Streaming** → reads messages from Kafka, aggregates data in real-time, filters by thresholds.
- **Alerts CSV** → defines thresholds for temperature and humidity.
- **Kafka alert producer** → writes filtered alerts back to Kafka.

---

## 📂 Files

| File                    | Purpose                                         |
| ----------------------- | ----------------------------------------------- |
| `sensor_producer.py`    | Generates and sends random sensor data to Kafka |
| `alerts_conditions.csv` | Thresholds for temperature and humidity alerts  |
| `streaming_alerts.py`   | Main Spark Structured Streaming application     |
| `docker-compose.yml`    | Kafka and Zookeeper services                    |

---

## 🚀 How to Run

> Requires Docker and WSL with Python and PySpark.

1. **Start Kafka:**
   ```bash
   docker compose up -d
   ```
2. **Create Kafka topics manually**
3. **Generate sensor data (in multiple terminals):**
   ```bash
   python sensor_producer.py
   ```
4. **Run Spark Streaming:**
   ```bash
   spark-submit streaming_alerts.py
   ```
5. **Check alerts in Kafka or in a consumer script.**

---

## 📝 Alerts CSV Example

| min_temperature | max_temperature | min_humidity | max_humidity | alert_code | alert_message             |
| --------------- | --------------- | ------------ | ------------ | ---------- | ------------------------- |
| 40              | -999            | -999         | -999         | T001       | "High temperature alert!" |
| -999            | -999            | -999         | 20           | H002       | "Low humidity alert!"     |

- Use **-999** for non-applicable thresholds.

---

## 📸 Screenshots

Screenshots are stored in the `screenshots/` folder and in the LMS `.docx` file.

---
