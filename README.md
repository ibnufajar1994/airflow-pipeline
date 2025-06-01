# **Designing Data Pipeline using Airflow**

## Content
[1. Project Description](#1-project-description)  
[2. Source of Dataset](#2-source-of-dataset)  
[3. Tech Stack](#3-tech-stack)  
[4. Pipeline Design](#4-pipeline-design)  
[5. Project Setup](#5-project-setup)  

---
# 1. Project Description
This project builds a simple and reliable data pipeline for a flight booking system using Apache Airflow. The goal is to automate the process of collecting, storing, processing, and organizing data. It starts by pulling raw booking and operations data from a PostgreSQL database, which is the main source. The data is then saved in MinIO, a storage system similar to Amazon S3, which helps store data in a flexible and scalable way. After that, the data is moved to a data warehouse, where it can be accessed quickly for analysis. In the final step, the data is cleaned, organized, and transformed into analytical tables that are easy to use for reporting and business insights. Apache Airflow manages the whole workflow, making sure each task runs in the right order, handles any problems, and sticks to the schedule. This makes the system dependable and useful for making better decisions.

---

# 2. Source of Dataset
The dataset used on this repo comes from airlines data management system that handle flight bookings of customers. you can access the repo on this [link](https://drive.google.com/file/d/1Zu5grD6mKuCcLagggE9R8ILjzvPIbXDQ/view?usp=sharing)

---

# 3. Tech Stack  
- Database: PostgresQL
- Containerization: Docker
- Object Storage: MinIO
- Orchestrator: Airflow

---

# 4. Pipeline Design
![image](https://github.com/user-attachments/assets/eb18c951-557a-4eb8-91f0-8925aa7f3814)

# 5. Project Setup
Clone this repository
```
https://github.com/ibnufajar1994/airflow-pipeline.git
```



create .env file on your project directory
```
AIRFLOW_DB_URI=postgresql+psycopg2://airflow:airflow@airflow_metadata/airflow
AIRFLOW_DB_USER=airflow
AIRFLOW_DB_PASSWORD=airflow
AIRFLOW_DB_NAME=airflow
AIRFLOW_FERNET_KEY=

AIRCRAFT_DB_USER='postgres'
AIRCRAFT_DB_PASSWORD='postgres'
AIRCRAFT_DB_NAME="aircraft_data"


WAREHOUSE_DB_USER='postgres'
WAREHOUSE_DB_PASSWORD='postgres'
WAREHOUSE_DB_NAME="warehouse"



MINIO_ROOT_USER='minio'
MINIO_ROOT_PASSWORD='minio123'
```

Generate fernet key, then copy it to your .env file
```
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

run the project
```
docker-compose up --build
```

Setup Airflow Connection:
- aircraft_db (postgresql) ---> adjust with your .env file
- warehouse_db ---> adjust with your .env file
- minIO ---> adjust with your .env file
   choose Amazon web service, in the "extra" box, copy and paste this:
```
{
  "endpoint_url":"minio:9000",
}
```
---

# Documentation

## Dag Run
![Dag Run](sample_image/dag%20run.png)

## Dag Graph
![Dag Graph](sample_image/dag_graph.png)

## Task Group
![Task Group](sample_image/task%20group.png)


## Minio Data
![Minio Data](sample_image/minio_data.png)

## Sample Query
![Sample Query](sample_image/sample%20query.png)



  

