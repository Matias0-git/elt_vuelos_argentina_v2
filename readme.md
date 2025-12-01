# Proyecto: Pipeline ELT de Vuelos en Argentina

Este proyecto implementa un pipeline de datos completo para ingestar, procesar y analizar informes de vuelos domésticos en Argentina, cubriendo el período de Enero 2021 a Junio 2022.

El pipeline utiliza un stack de herramientas de Big Data, incluyendo Apache Airflow para la orquestación, Apache Spark para el procesamiento ETL, contratos de datos y esquemas para su debida governanza y Apache Hive como Data Warehouse para el análisis final.

---

## 📈 Arquitectura del Pipeline

El flujo de trabajo sigue este orden:

1.  **Orquestación (Apache Airflow):** Un DAG de Airflow (`proceso_aeropuertos_etl`) define y ejecuta las tareas en el orden correcto.
2.  **Ingesta (`ingesta_vuelos_argentina.sh`):** Un script de Bash descarga los archivos CSV de una fuente pública y los transfiere al Data Lake en HDFS.
3.  **Governanza:** Contratos de datos y esquemas (fuente de verdad).
4.  **Procesamiento (Apache Spark):** Un script de PySpark (`process_aeropuertos.py`) lee los CSVs "crudos" de HDFS, aplica transformaciones (limpieza, renombrado, filtrado) y los guarda en tablas de Hive optimizadas.
5.  **Almacenamiento (Apache Hive):** Los datos limpios residen en un Data Warehouse de Hive, listos para ser consultados vía SQL.

---

## 📂 Estructura del Proyecto

Este repositorio está organizado para documentar cada etapa del pipeline.

[Ingesta al Data Lake Hadoop](./bash/)

[Procesamiento Spark](./spark/)

[Orquestración Airflow](./airflow/)

[Governanza](./data_contracts)

[Hive Data Warehouse](./hive)


