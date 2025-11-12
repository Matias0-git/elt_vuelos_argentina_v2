Determinar la cantidad de vuelos entre las fechas 01/12/2021 y 31/01/2022. Mostrar
consulta y Resultado de la query

SELECT COUNT(*) AS total_vuelos
FROM vuelos
WHERE fecha >= '2021-12-01' AND fecha <= '2022-01-31';

![alt text](ej6.png)
