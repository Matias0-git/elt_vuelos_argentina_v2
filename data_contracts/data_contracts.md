## Contratos de Datos y Definición de Catálogo

Este directorio contiene la Fuente Única de Verdad para los esquemas de datos utilizados en la pipeline ETL.

# Propósito

Estos archivos Python definen los esquemas StructType explícitos para nuestros datasets. Cumplen dos roles críticos en nuestra arquitectura:

- Cumplimiento de Esquema (Schema-on-Read):

      Spark utiliza estas definiciones para leer archivos CSV crudos de manera segura.

      Esto previene la "deriva de esquema" (schema drift) y asegura que los tipos se manejen correctamente (por ejemplo, leyendo códigos numéricos como Strings para preservar los ceros iniciales).

- Catálogo de Datos Automatizado (Hive Metastore):

      Embebemos metadatos (descripciones) directamente en los campos del esquema usando un diccionario: {"comment": "Descripción..."}.

      Durante el proceso ETL, nuestro trabajo de Spark lee estos comentarios y los empuja al Hive Metastore automáticamente usando comandos ALTER TABLE.

      Resultado: La documentación en Hive/Hue/Beeline siempre está sincronizada con el código.

# Archivos de Contrato

schema_datos_vuelos.py: Define la estructura para los reportes de Vuelos (2021/2022/etc.), incluyendo campos críticos como Fecha, Hora UTC y Clasificación.

schema_detalles_aeropuerto.py: Define los datos maestros para los detalles de Aeropuertos, corrigiendo problemas históricos de nombres (ej. sna vs oana) y estandarizando coordenadas.

# Uso

Estos contratos son empaquetados en contracts.zip por Airflow y enviados al clúster de Spark a través del argumento --py-files.
