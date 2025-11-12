8.​ Mostrar fecha, hora, código aeropuerto salida, ciudad de salida, código de aeropuerto
de arribo, ciudad de arribo, y cantidad de pasajeros de cada vuelo, entre el 01/01/2022 y el 30/06/2022 ordenados por fecha de manera descendiente. Mostrar consulta y
Resultado de la query

WITH vuelos_con_codigos AS (
    SELECT
        *,
        CASE
            WHEN tipo_de_movimiento = 'Despegue' THEN aeropuerto
            ELSE origen_destino
        END AS codigo_salida,
        CASE
            WHEN tipo_de_movimiento = 'Aterrizaje' THEN aeropuerto
            ELSE origen_destino
        END AS codigo_arribo
    FROM vuelos
    WHERE fecha >= '2022-01-01' AND fecha <= '2022-06-30'
)
SELECT
    v.fecha, v.horaUTC, v.codigo_salida,
    ad_salida.denominacion AS ciudad_salida,
    v.codigo_arribo,
    ad_arribo.denominacion AS ciudad_arribo,
    v.pasajeros
FROM vuelos_con_codigos v
LEFT JOIN aeropuertos_detalle ad_salida ON v.codigo_salida = ad_salida.`local`
LEFT JOIN aeropuertos_detalle ad_arribo ON v.codigo_arribo = ad_arribo.`local`
ORDER BY v.fecha DESC
LIMIT 50;
