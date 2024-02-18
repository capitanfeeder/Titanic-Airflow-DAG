# **Titanic DAG Project in Apache Airflow**

## Project Description

### This project aims to automate the data processing workflow for the Titanic dataset using Apache Airflow. The Directed Acyclic Graph (DAG) created in Airflow orchestrates the tasks of downloading, transforming, and loading the dataset into a PostgreSQL database.

## Workflow Overview:
- **Data Download:** Fetches the Titanic dataset from a public URL using the download_csv task.

- **Data Quality:** Generates a data quality report and performs transformations on the dataset to enhance its quality using the DataQuality/profiling and DataQuality/curated tasks.

- **Raw Data Layer:** Establishes a PostgreSQL table to store the raw dataset using the RawLayer/create_raw task, followed by loading the data into this table using the RawLayer/load_raw task.

- **Master Data Layer:** Sets up a master table in the PostgreSQL database to store the transformed data using the MasterLayer/create_master task. The transformed data is then loaded into this table using the MasterLayer/load_master task.

- **Data Validation:** Utilizes a SQL sensor to validate that the data has been correctly loaded into the master table using the validator task.

## Prerequisites
- Docker
- DBeaver or another PostgreSQL-compatible database management tool
- Environment Setup

## Docker Installation:

### Follow the installation instructions for Docker on your operating system from the official Docker documentation.
Airflow Setup with Docker:

Clone the Apache Airflow repository: