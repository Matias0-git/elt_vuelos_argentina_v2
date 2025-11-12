Cantidad de pasajeros que viajaron en Aerolíneas Argentinas entre el 01/01/2021 y
30/06/2022. Mostrar consulta y Resultado de la query

SELECT SUM(pasajeros) AS total_pasajeros_aerolineas
FROM vuelos
WHERE aerolinea_nombre = 'AEROLINEAS ARGENTINAS SA'
  AND fecha >= '2021-01-01' AND fecha <= '2022-06-30';

![alt text](ej7.png)
