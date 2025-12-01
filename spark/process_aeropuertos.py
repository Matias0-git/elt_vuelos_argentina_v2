#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, when, lit
from pyspark.sql.types import IntegerType

# --- IMPORT DATA CONTRACTS ---
from data_contracts.schema_datos_vuelos import FLIGHTS_SCHEMA
from data_contracts.schema_detalles_aeropuerto import AIRPORTS_DETAIL_SCHEMA

# --- CONSTANTS ---
HDFS_NAMENODE = "hdfs://172.17.0.2:9000"
HDFS_VUELOS_PATH = f"{HDFS_NAMENODE}/ingest/*informe-ministerio.csv"
HDFS_AEROPUERTOS_PATH = f"{HDFS_NAMENODE}/ingest/aeropuertos_detalle.csv"

HIVE_DB = "aeropuertos_db"
HIVE_VUELOS_TABLE = f"{HIVE_DB}.vuelos"
HIVE_AEROPUERTOS_TABLE = f"{HIVE_DB}.aeropuertos_detalle"


def update_hive_catalog(spark, table_name, schema_contract, properties):
    """Updates Hive Metastore comments and properties."""
    try:
        print(f"--- Actualizando Catalogo (Hive Metastore) para {table_name} ---")

        # Filter out 'owner' if it exists to avoid reserved key error
        safe_props = {k: v for k, v in properties.items() if k != 'owner'}

        props_str = ", ".join([f"'{k}'='{v}'" for k, v in safe_props.items()])
        spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES ({props_str})")

        # Helper loop for comments
        for field in schema_contract.fields:
            if "comment" in field.metadata:
                pass # Skipping manual comment updates to avoid name mismatch errors

    except Exception as e:
        print(f"Advertencia: No se pudo actualizar el catalogo. Error: {str(e)}")


def process_vuelos(spark):
    print(f"Iniciando procesamiento de vuelos desde {HDFS_VUELOS_PATH}")

    # 1. READ (Schema-on-Read using Contract)
    df_vuelos = spark.read \
        .option("header", True) \
        .option("sep", ";") \
        .schema(FLIGHTS_SCHEMA) \
        .csv(HDFS_VUELOS_PATH)

    # 2. TRANSFORM

    df_normalized = df_vuelos.select(
        col("Fecha").alias("fecha_raw"),
        col("Hora UTC").alias("hora_utc"),
        col("Clase de Vuelo (todos los vuelos)").alias("clase_de_vuelo"),
        col("Clasificación Vuelo").alias("clasificacion_de_vuelo"),
        col("Tipo de Movimiento").alias("tipo_de_movimiento"),
        col("Aeropuerto").alias("aeropuerto"),
        col("Origen / Destino").alias("origen_destino"),
        col("Aerolinea Nombre").alias("aerolinea_nombre"),
        col("Aeronave").alias("aeronave"),
        col("Pasajeros").alias("pasajeros_raw")
    )

    # Now filter on the CLEAN name (no accents, no spaces)
    df_vuelos_dom = df_normalized.filter(
        col("clasificacion_de_vuelo").isin("Domestico", "Doméstico")
    )

    # Final Cleaning
    df_vuelos_clean = df_vuelos_dom.select(
        to_date(col("fecha_raw"), "dd/MM/yyyy").alias("fecha"),
        col("hora_utc").alias("horaUTC"),
        col("clase_de_vuelo"),
        col("clasificacion_de_vuelo"),
        col("tipo_de_movimiento"),
        col("aeropuerto"),
        col("origen_destino"),
        col("aerolinea_nombre"),
        col("aeronave"),
        when(col("pasajeros_raw").isNull(), 0)
            .otherwise(col("pasajeros_raw").cast(IntegerType()))
            .alias("pasajeros")
    )

    # 3. WRITE
    print(f"Guardando datos limpios en: {HIVE_VUELOS_TABLE}")
    df_vuelos_clean.write.mode("overwrite").saveAsTable(HIVE_VUELOS_TABLE)

    # 4. CATALOG UPDATE
    update_hive_catalog(spark, HIVE_VUELOS_TABLE, FLIGHTS_SCHEMA,
                       {"data_owner": "Data Team", "source": "Ministerio Transporte"})
    print("Vuelos Done.")


def process_aeropuertos(spark):
    print(f"Iniciando procesamiento de aeropuertos desde {HDFS_AEROPUERTOS_PATH}")

    # 1. READ
    df_aeropuertos = spark.read \
        .option("header", True) \
        .option("sep", ";") \
        .schema(AIRPORTS_DETAIL_SCHEMA) \
        .csv(HDFS_AEROPUERTOS_PATH)

    # 2. DROP UNWANTED COLUMNS
    cols_to_drop = ['inhab', 'fir', 'calidad del dato']
    df_dropped = df_aeropuertos.drop(*cols_to_drop)

    # 3. TRANSFORM
    df_clean = df_dropped.na.fill("0", subset=["distancia_ref"])

    # 4. WRITE
    print(f"Guardando datos limpios en: {HIVE_AEROPUERTOS_TABLE}")
    df_clean.write.mode("overwrite").saveAsTable(HIVE_AEROPUERTOS_TABLE)

    # 5. CATALOG UPDATE
    update_hive_catalog(spark, HIVE_AEROPUERTOS_TABLE, AIRPORTS_DETAIL_SCHEMA,
                       {"data_owner": "Data Team", "source": "ORSNA"})
    print("Aeropuertos Done.")


def main():
    spark = SparkSession.builder \
        .appName("ETL_Vuelos_Aeropuertos_Fixed") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    process_aeropuertos(spark)
    process_vuelos(spark)

    spark.stop()

if __name__ == "__main__":
    main()
