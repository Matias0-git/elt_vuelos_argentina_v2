#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, when, lit

# --- Constantes ---
HDFS_NAMENODE = "hdfs://172.17.0.2:9000"
HDFS_VUELOS_PATH = f"{HDFS_NAMENODE}/ingest/vuelos/"
HDFS_AEROPUERTOS_PATH = f"{HDFS_NAMENODE}/ingest/aeropuertos_detalle/"
HIVE_DB = "aeropuertos_db"
HIVE_VUELOS_TABLE = f"{HIVE_DB}.vuelos"
HIVE_AEROPUERTOS_TABLE = f"{HIVE_DB}.aeropuertos_detalle"


def process_vuelos(spark):
    """
    Procesa los CSVs de vuelos, aplica transformaciones y guarda en Hive.
    """
    print(f"Iniciando procesamiento de vuelos desde {HDFS_VUELOS_PATH}")

    # 1. Leer los datos 'sucios' (todos los CSV en la carpeta)
    df_vuelos = spark.read \
        .option("header", True) \
        .option("sep", ";") \
        .csv(HDFS_VUELOS_PATH)

    # --- Transformaciones de Vuelos ---

    # 2. Filtrar solo vuelos Domésticos
    df_vuelos_dom = df_vuelos.filter(
        col("Clasificación Vuelo") == "Domestico"
    )

    # 3. Renombrar, transformar y seleccionar columnas para Hive
    #    Esto soluciona el error "Hora UTC" y limpia todos los nombres.
    #    También corrige el nombre de 'Pasajeros' (mayúscula).
    df_vuelos_clean = df_vuelos_dom.select(
        to_date(col("Fecha"), "dd/MM/yyyy").alias("fecha"),
        col("Hora UTC").alias("horaUTC"),
        col("Clase de Vuelo (todos los vuelos)").alias("clase_de_vuelo"),
        col("Clasificación Vuelo").alias("clasificacion_de_vuelo"),
        col("Tipo de Movimiento").alias("tipo_de_movimiento"),
        col("Aeropuerto").alias("aeropuerto"),
        col("Origen / Destino").alias("origen_destino"),
        col("Aerolinea Nombre").alias("aerolinea_nombre"),
        col("Aeronave").alias("aeronave"),
        when(col("Pasajeros").isNull(), 0)
            .otherwise(col("Pasajeros").cast("int"))
            .alias("pasajeros")
    )
    # Las columnas que no seleccionamos (como 'Calidad dato') se eliminan automáticamente.

    # 4. Guardar la tabla limpia en Hive
    print(f"Guardando datos limpios en la tabla Hive: {HIVE_VUELOS_TABLE}")

    df_vuelos_clean.write \
        .mode("overwrite") \
        .saveAsTable(HIVE_VUELOS_TABLE)

    print("Procesamiento de vuelos completado.")


def process_aeropuertos(spark):
    """
    Procesa el CSV de aeropuertos, aplica transformaciones y guarda en Hive.
    (Esta función ya funcionaba bien)
    """
    print(f"Iniciando procesamiento de aeropuertos desde {HDFS_AEROPUERTOS_PATH}")

    # 1. Leer los datos
    df_aeropuertos = spark.read \
        .option("header", True) \
        .option("sep", ";") \
        .option("inferSchema", True) \
        .csv(HDFS_AEROPUERTOS_PATH)

    # --- Transformaciones de Aeropuertos ---

    # 2. Eliminar las columnas no deseadas
    cols_to_drop = ['inhab', 'fir', 'calidad del dato']
    actual_cols_to_drop = [c for c in cols_to_drop if c in df_aeropuertos.columns]

    if actual_cols_to_drop:
        print(f"Eliminando columnas: {actual_cols_to_drop}")
        df_aeropuertos_dropped = df_aeropuertos.drop(*actual_cols_to_drop)
    else:
        print("No se encontraron las columnas 'inhab', 'fir', 'calidad del dato' para eliminar.")
        df_aeropuertos_dropped = df_aeropuertos

    # 3. En distancia_ref, convertir NULLs a 0
    df_aeropuertos_clean = df_aeropuertos_dropped.na.fill(
        0, subset=["distancia_ref"]
    )

    # 4. Guardar la tabla limpia en Hive
    print(f"Guardando datos limpios en la tabla Hive: {HIVE_AEROPUERTOS_TABLE}")

    df_aeropuertos_clean.write \
        .mode("overwrite") \
        .saveAsTable(HIVE_AEROPUERTOS_TABLE)

    print("Procesamiento de aeropuertos completado.")


def main():
    """
    Función principal del script de Spark.
    """
    # Inicializar SparkSession con soporte para Hive
    spark = SparkSession.builder \
        .appName("ProcesoETLAeropuertos") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("--- Iniciando Proceso ETL de Aeropuertos ---")

    # Ejecutar ambos procesamientos
    process_aeropuertos(spark)
    process_vuelos(spark)

    print("--- Proceso ETL completado exitosamente ---")
    spark.stop()


# Punto de entrada del script
if __name__ == "__main__":
    main()
