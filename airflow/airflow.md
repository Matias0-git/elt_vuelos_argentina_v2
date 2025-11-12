# 2. Orquestación (Apache Airflow)

Apache Airflow se utiliza para automatizar, programar y monitorear el pipeline completo.

### Configuración del Entorno

* **Directorio de DAGs:** `/home/hadoop/airflow/dags/`
* **Puerto del Webserver:** La interfaz web se configuró para ejecutarse en el puerto `8010`.
    ```bash
    hadoop@88e4c6167f0c:/$ cat ~/airflow/airflow.cfg | grep web_server_port
    web_server_port = 8010
    ```

### Script del DAG: `proceso_aeropuertos_etl.py`

*(Nota: en tus logs el archivo se llama `airport_trips_scheduler.py`, pero el `dag_id` es `proceso_aeropuertos_etl`)*
