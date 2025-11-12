11. Qué datos externos agregaría en este dataset que mejoraría el análisis de los datos
¡Gran pregunta! Los datos actuales nos dicen qué pasó, pero no por qué. Yo agregaría:

Datos Meteorológicos (Clima): 🌦️ Unir por fecha y aeropuerto (local). Esto nos permitiría responder preguntas como: "¿Cuántos vuelos se cancelaron o demoraron por baja visibilidad, viento o lluvia?".

Feriados y Eventos: 📅 Una tabla simple de fechas (fecha, descripcion_evento). Esto ayudaría a explicar los picos de pasajeros (ej. "Inicio de vacaciones de invierno", "Feriado de Carnaval").

Datos Económicos (Inflación/Dólar): 💸 Unir por fecha. Esto ayudaría a correlacionar la cantidad de pasajeros con el poder adquisitivo. ¿Sube el dólar y bajan los pasajeros?

Detalle de Aeronaves: ✈️ Unir por aeronave. Una tabla que diga la capacidad máxima de cada modelo de avión. Con esto, podríamos calcular el porcentaje de ocupación (pasajeros / capacidad_maxima), una métrica clave para las aerolíneas.

12. Elabore sus conclusiones y recomendaciones sobre este proyecto
Conclusiones:

El stack tecnológico (Airflow > Spark > Hive) fue validado y demostró ser robusto para un proceso ETL de Big Data.

La limpieza de datos (Spark) fue esencial: Los datos crudos (CSV) tenían problemas críticos que Hive no podía manejar solo (delimitadores incorrectos, fechas nulas, nombres de columna con espacios). El uso de Spark para transformar los datos fue un éxito.

Airflow es clave para la automatización: El DAG asegura que el proceso sea repetible, confiable y que cada paso se ejecute en el orden correcto. El historial de logs fue fundamental para la depuración.

Hive es una gran capa analítica: Una vez procesados los datos, Hive nos da el poder de usar SQL para análisis complejos (como los JOINs y GROUP BYs que hicimos) sobre archivos que viven en HDFS.

Recomendaciones:

Monitoreo de Calidad de Datos: Implementar "data checks" en el DAG de Airflow (ej. usando el SQLCheckOperator). Por ejemplo, una tarea que verifique que SUM(pasajeros) sea mayor a cero. Si falla, el DAG se detiene y avisa.

Estandarizar Nombres de Columnas: El mayor problema que tuvimos fue adivinar los nombres de las columnas (Clasificación Vuelo vs clasificacion_de_vuelo). Si es posible, se debe exigir que los archivos CSV siempre vengan con los mismos nombres estandarizados (ej. todo en minúscula y con _).

Pasar a Parquet: El script de Spark actualmente guarda en Hive en el formato por defecto (texto). Sería mucho más eficiente si Spark guardara los datos limpios en formato Parquet (df_vuelos_clean.write.format("parquet")...). Es más rápido para consultar y ocupa mucho menos espacio.

13. Proponer una arquitectura alternativa para este proceso (Cloud)
La arquitectura que usaste es un "stack" On-Premise clásico. Una alternativa moderna usando Cloud (Google Cloud - GCP) se vería así:

Ingesta (Reemplazo de ingest.sh):

Un Cloud Scheduler (un cron en la nube) ejecuta una Cloud Function (una mini-función sin servidor).

Esta función descarga los CSVs y los guarda en un "Data Lake" en Google Cloud Storage (GCS) (un bucket de almacenamiento, como HDFS pero más simple).

Procesamiento (Reemplazo de Spark-Submit):

Usar Dataproc Serverless. Es un servicio que ejecuta tu script de PySpark (process_aeropuertos.py) sin que tengas que configurar un clúster.

Dataproc lee los CSVs "sucios" de GCS, los procesa (aplicando las mismas reglas) y guarda los datos limpios (en formato Parquet) de nuevo en GCS.

Data Warehouse (Reemplazo de Hive):

Google BigQuery. Es el almacén de datos de Google.

Puedes crear una "tabla externa" en BigQuery que lea los archivos Parquet limpios directamente desde GCS.

Todas las consultas SQL que hicimos funcionarían igual (o más rápido) en BigQuery.

Orquestación (Reemplazo de Airflow):

Cloud Composer. ¡Es simplemente Airflow, pero administrado por Google! Usarías el mismo DAG que ya tienes.

Ventajas de esta arquitectura Cloud:

Serverless (Sin Servidores): No tienes que administrar máquinas virtuales, ni YARN, ni HDFS.

Escalabilidad: Si un día procesas 100 archivos en lugar de 3, el sistema escala solo.

Costo: Pagas solo por los segundos que tu script de Spark está corriendo, no por tener un clúster encendido 24/7.
