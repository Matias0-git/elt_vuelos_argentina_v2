#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

# --- Argumentos Default del DAG ---
default_args = {
    'owner': 'hadoop',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='proceso_aeropuertos_etl',
    default_args=default_args,
    description='Orquesta la ingesta y procesamiento de informes de aeropuertos',
    schedule_interval=None,         # Se ejecuta manually
    start_date=days_ago(1),
    catchup=False,
    tags=['spark', 'hive', 'etl', 'aeropuertos'],
) as dag:

    inicio = DummyOperator(task_id='inicio')

    # Tarea 1: Ingesta de datos
    ingest = BashOperator(
        task_id='ingesta_datos',
        # Comando con el espacio al final para el error de Jinja
        bash_command='bash /home/hadoop/landing/ej_1_ingest_informes.sh ',
    )

    # Tarea 2: Procesa los datos con Spark
    process = BashOperator(
        task_id='procesa_spark',
        # --- AQUÍ ESTÁ LA CORRECCIÓN ---
        # Añadimos --master "local[*]" para que Spark se ejecute
        # localmente y no dependa de la configuración de YARN.
        bash_command='spark-submit --master "local[*]" /home/hadoop/scripts/process_aeropuertos.py',
    )

    # Tarea 3: Verifica la tabla final en Hive
    verify = BashOperator(
        task_id='verifica_tabla_vuelos',
        bash_command='beeline -u jdbc:hive2://localhost:10000 -e "USE aeropuertos_db; SELECT COUNT(*) AS total FROM vuelos;"',
    )

    fin = DummyOperator(task_id='fin_proceso')

    # --- Definiendo el Flujo ---
    inicio >> ingest >> process >> verify >> fin
