# Brazilian E-Commerce Data Engineering Project

## Overview

This project develops an end-to-end ETL process using the Brazilian E-Commerce dataset from Kaggle. It demonstrates the use of PostgreSQL, Docker, Docker Compose, Airflow, dbt, and BigQuery to ingest, process, and analyze e-commerce data. The goal is to provide insights into product category sales, delivery times, and order distribution across states.

### Data Architecture

![image](img/architecture.png)

## Prerequisites
- Python 3.12.4
- Docker and Docker Compose.
- Google Cloud Platform account with BigQuery enabled.
- Kaggle account to download the Brazilian E-Commerce dataset.

## Project Structure


## How to Run This Project

1. Clone the repository
    ```bash 
    git clone https://github.com/thevictoressien/alt-de-capstone.git
    ```
2. Set up the environment
    - Change directory to project directory
        ```bash 
        cd alt-de-capstone
        ```
    - Create a virtual environment and activate it
        ```bash 
        python -m venv 
        ```
    - Install requirements
        ```bash 
        pip install -r requirements.txt
        ```
    - Build airflow image
        ```bash
        docker-compose build
        ```
    - Run containers
        ```bash
        docker-compose up
        ```

##
    
