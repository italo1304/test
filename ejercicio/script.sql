CREATE EXTERNAL TABLE IF NOT EXISTS paises_text(cod_pais STRING,pais STRING)
COMMENT 'Tabla de Países'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
LOCATION '/user/training/ejercicio/paises';

CREATE EXTERNAL TABLE IF NOT EXISTS vuelos_text(vuelo INT,origen STRING, destino STRING)
COMMENT 'Tabla de Vuelos'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION '/user/training/ejercicio/vuelos';


CREATE EXTERNAL TABLE IF NOT EXISTS retrasos_text(vuelo_retraso INT)
COMMENT 'Tabla de Retrasos'
ROW FORMAT DELIMITED
STORED AS TEXTFILE
LOCATION '/user/training/ejercicio/retrasos';

CREATE EXTERNAL TABLE IF NOT EXISTS fecha_text(vuelo INT, dia INT)
COMMENT 'Tabla de fechas'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '_'
STORED AS TEXTFILE
LOCATION '/user/training/ejercicio/fecha';

CREATE TABLE paises AS
SELECT * FROM paises_text;

CREATE TABLE vuelos as
SELECT * FROM vuelos_text;

CREATE TABLE retrasos as
SELECT cast(substring(vuelo_retraso, 1, 4) AS INT) vuelo, cast(substring(vuelo_retraso, 5,6) AS INT) retraso 
FROM retrasos_text;

CREATE TABLE fecha as
SELECT * FROM fecha_text;

-- select * from paises limit 5;
-- select * from vuelos limit 5;
-- select * from retrasos limit 5;
-- select * from fecha limit 5;