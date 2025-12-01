from pyspark.sql.types import StructType, StructField, StringType

# CONTRATO DE DATOS: AEROPUERTOS DETALLE
# Basado en headers: local;oaci;iata;tipo;denominacion;coordenadas;...
AIRPORTS_DETAIL_SCHEMA = StructType([
    StructField("local", StringType(), True, {"comment": "Codigo local"}),
    StructField("oaci", StringType(), True, {"comment": "Codigo OACI"}),
    StructField("iata", StringType(), True, {"comment": "Codigo IATA"}),
    StructField("tipo", StringType(), True, {"comment": "Tipo (Aerodromo, etc)"}),
    StructField("denominacion", StringType(), True, {"comment": "Nombre del aeropuerto"}),
    StructField("coordenadas", StringType(), True, {"comment": "Coordenadas originales"}),
    StructField("latitud", StringType(), True, {"comment": "Latitud decimal"}),
    StructField("longitud", StringType(), True, {"comment": "Longitud decimal"}),

    StructField("elev", StringType(), True, {"comment": "Elevacion"}),
    StructField("uom_elev", StringType(), True, {"comment": "Unidad de medida elevacion metros/Pies"}),

    StructField("ref", StringType(), True, {"comment": "Referencia"}),
    StructField("distancia_ref", StringType(), True, {"comment": "Distancia ref"}),
    StructField("direccion_ref", StringType(), True, {"comment": "Direccion ref"}),
    StructField("condicion", StringType(), True, {"comment": "Condicion publico/Privado)"}),
    StructField("control", StringType(), True, {"comment": "Tipo de control"}),
    StructField("region", StringType(), True, {"comment": "Region"}),
    StructField("fir", StringType(), True, {"comment": "FIR region"}),
    StructField("uso", StringType(), True, {"comment": "Uso (Civil/Militar)"}),
    StructField("trafico", StringType(), True, {"comment": "Trafico"}),
    StructField("sna", StringType(), True, {"comment": "Sistema Nacional de Aeropuertos (SI/NO)"}),
    StructField("concesionado", StringType(), True, {"comment": "Concesionado"}),
    StructField("provincia", StringType(), True, {"comment": "Provincia"}),
    StructField("inhab", StringType(), True, {"comment": "Inhabilitado"})
])
