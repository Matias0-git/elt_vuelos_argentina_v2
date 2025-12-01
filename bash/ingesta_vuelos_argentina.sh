#!/bin/bash

# Directorio temporal de destino
LANDING_DIR="/home/hadoop/landing"

# Directorio de HDFS de destino
HDFS_INGEST_DIR="/ingest"

# Array con las URLs de los archivos a descargar
URLS=(
  "https://data-engineer-edvai-public.s3.amazonaws.com/2021-informe-ministerio.csv"
  "https://data-engineer-edvai-public.s3.amazonaws.com/aeropuertos_detalle.csv"
  "https://data-engineer-edvai-public.s3.amazonaws.com/202206-informe-ministerio.csv"
)

# 1. Crear el directorio temporal si no existe
echo "Creando el directorio temporal $LANDING_DIR si no existe..."
mkdir -p "$LANDING_DIR"

# 2. Iterar sobre cada URL, descargar, subir a HDFS y borrar
for FILE_URL in "${URLS[@]}"; do
  # Obtener el nombre del archivo de la URL
  FILE_NAME=$(basename "$FILE_URL")

  echo "--- Procesando: $FILE_NAME ---"

  # 2a. Descargar el archivo al directorio temporal usando wget
  echo "Descargando $FILE_NAME al directorio $LANDING_DIR..."
  wget -O "$LANDING_DIR/$FILE_NAME" "$FILE_URL"

  # Verificar si la descarga fue exitosa
  if [ $? -ne 0 ]; then
    echo "Error: La descarga de $FILE_NAME falló. Saltando al siguiente archivo."
    continue # Continúa con el siguiente item del loop
  fi

  # 2b. Enviar el archivo a HDFS
  echo "Enviando $FILE_NAME a HDFS en el directorio $HDFS_INGEST_DIR..."
  # Utiliza -f para sobrescribir si el archivo ya existe en HDFS
  hdfs dfs -put -f "$LANDING_DIR/$FILE_NAME" "$HDFS_INGEST_DIR"

  # Verificar si la subida a HDFS fue exitosa
  if [ $? -ne 0 ]; then
    echo "Error: La subida de $FILE_NAME a HDFS falló."
    # Opcional: decidir si borrar el archivo local o no. Aquí no lo borramos si falla.
    continue # Continúa con el siguiente item del loop
  fi

  # 2c. Borrar el archivo del directorio temporal
  echo "Borrando el archivo local $FILE_NAME..."
  rm "$LANDING_DIR/$FILE_NAME"

  echo "--- $FILE_NAME procesado exitosamente ---"

done

echo "Script terminado. Todos los archivos han sido procesados."
