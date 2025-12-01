from pyspark.sql.types import StructType, StructField, StringType

# CONTRATO DE DATOS: VUELOS
# Basado en headers: Fecha;Hora UTC;Clase de Vuelo (todos los vuelos);...
FLIGHTS_SCHEMA = StructType([
    StructField("Fecha", StringType(), True,
                {"comment": "Fecha del vuelo (DD/MM/YYYY)"}),

    StructField("Hora UTC", StringType(), True,
                {"comment": "Hora UTC (HH:mm)"}),

    StructField("Clase de Vuelo (todos los vuelos)", StringType(), True,
                {"comment": "Ej: Regular, Vuelo Privado"}),

    StructField("Clasificación Vuelo", StringType(), True,
                {"comment": "Ej: Domestico, Internacional (Ojo: puede tener tildes)"}),

    StructField("Tipo de Movimiento", StringType(), True,
                {"comment": "Despegue o Aterrizaje"}),

    StructField("Aeropuerto", StringType(), True,
                {"comment": "Codigo IATA/OACI del aeropuerto"}),

    StructField("Origen / Destino", StringType(), True,
                {"comment": "Aeropuerto de origen o destino"}),

    StructField("Aerolinea Nombre", StringType(), True,
                {"comment": "Nombre de la aerolinea"}),

    StructField("Aeronave", StringType(), True,
                {"comment": "Modelo o matricula"}),

    StructField("Pasajeros", StringType(), True,
                {"comment": "Cantidad de pasajeros (String para evitar errores de lectura)"}),

    StructField("Calidad dato", StringType(), True,
                {"comment": "Ej: DEFINITIVO"})
])
