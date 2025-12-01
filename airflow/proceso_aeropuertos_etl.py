#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

# --- Argumentos Default ---
default_args = {
    'owner': 'hadoop',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='proceso_aeropuertos_etl_v3_simple',
    default_args=default_args,
    description='ETL Aeropuertos con Data Contracts y Catalog (Sin checks complejos)',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['spark', 'hive', 'etl', 'contracts'],
) as dag:

    inicio = DummyOperator(task_id='inicio')

    # Tarea 1: Ingesta
    # Mueve/Descarga los archivos a HDFS
    ingest = BashOperator(
        task_id='ingesta_datos',
        bash_command='bash /home/hadoop/landing/ingesta_vuelos_argentina.sh ',
    )

    # Tarea 2: Empaquetar Contratos
    package_contracts = BashOperator(
        task_id='empaquetar_contratos',
        bash_command='cd /home/hadoop && python3 -m zipfile -c contracts.zip data_contracts/',
    )

    # Tarea 3: Procesamiento Spark
    # Usa --py-files para enviar el contrato.
    # Ejecuta el script estándar de procesamiento (sin bloqueos por duplicados).
    process = BashOperator(
        task_id='procesa_spark_estandar',
        bash_command=(
            'spark-submit '
            '--master "local[*]" '
            '--py-files /home/hadoop/contracts.zip '
            '/home/hadoop/scripts/process_aeropuertos.py'
        ),
    )

    # Tarea 4: Verificacion simple
    # Solo cuenta filas para confirmar que hay datos
    verify = BashOperator(
        task_id='verifica_tabla_vuelos',
        bash_command='beeline -u jdbc:hive2://localhost:10000 -e "USE aeropuertos_db; SELECT COUNT(*) AS total_vuelos FROM vuelos;"',
    )

    fin = DummyOperator(task_id='fin_proceso')

    # --- Flujo Lineal Simple ---
    # inicio -> ingest -> zip -> spark -> verify -> fin
    inicio >> ingest >> package_contracts >> process >> verify >> fin
