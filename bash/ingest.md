# 1. Proceso de Ingesta (Bash)

Esta etapa descarga los 3 archivos CSV fuente de S3 y los coloca en el directorio `/ingest` de HDFS.

### Script: `ej_1_ingest_informes.sh`

Este script se encuentra en `/home/hadoop/landing/` y debe tener permisos de ejecución (`chmod +x`).

# 2. Organización en HDFS
hadoop@88e4c6167f0c:/$ hdfs dfs -mkdir -p /ingest/vuelos
hadoop@88e4c6167f0c:/$ hdfs dfs -mkdir -p /ingest/aeropuertos_detalle

hadoop@88e4c6167f0c:/$ hdfs dfs -mv /ingest/2021-informe-ministerio.csv /ingest/vuelos/
hadoop@88e4c6167f0c:/$ hdfs dfs -mv /ingest/202206-informe-ministerio.csv /ingest/vuelos/
hadoop@88e4c6167f0c:/$ hdfs dfs -mv /ingest/aeropuertos_detalle.csv /ingest/aeropuertos_detalle/
