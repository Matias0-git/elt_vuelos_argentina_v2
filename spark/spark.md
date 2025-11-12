# 3. Procesamiento ETL (Apache Spark)

Este es el cerebro del pipeline. Un script de PySpark lee los CSVs "crudos" de HDFS, aplica todas las transformaciones y limpieza de datos, y sobrescribe las tablas de Hive con los datos limpios.

### Script: `process_aeropuertos.py`

Este script se encuentra en `/home/hadoop/scripts/`.
