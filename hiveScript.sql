-- Trzy parametry:
-- input_dir3 - katalog wyniku przetwarzania etapu MapReduce - gs://pbd-24-lm/labs/hadoop/input/mapreduce
-- input_dir4 - katalog drugiego zbioru danych - gs://pbd-24-lm/labs/hadoop/input/datasource4
-- output_dir6 - katalog wyjściowy zawierający wynik przetwarzania - gs://pbd-24-lm/labs/hadoop/output


-- EXECUTION: beeline -n ${USER} -u jdbc:hive2://localhost:10000/default -f hiveScript.sql --hivevar input_dir3=<PATH> --hivevar input_dir4=<PATH> --hivevar output_dir6=<PATH>
-- MY_EXECUTION: beeline -n ${USER} -u jdbc:hive2://localhost:10000/default -f hiveScript.sql --hivevar input_dir3=gs://pbd-24-lm/labs/hadoop/input/mapreduce --hivevar input_dir4=gs://pbd-24-lm/labs/hadoop/input/datasource4 --hivevar output_dir6=gs://pbd-24-lm/labs/hadoop/output


DROP TABLE mapreduce;
DROP TABLE datasource;
DROP TABLE hiveresult;


CREATE EXTERNAL TABLE IF NOT EXISTS  mapreduce(
    id STRING, year INT, rating FLOAT, ratingCount INT, appNumber INT)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
    WITH SERDEPROPERTIES (
        "input.regex" = "([^,]+),([^,]+)\\t([^,]+),([^,]+),([^,]+)",
    "output.format.string" = "%1$s %2$s %3$s %4$s %5$s")
    STORED AS TEXTFILE
    location '${input_dir3}';


CREATE EXTERNAL TABLE IF NOT EXISTS datasource(
    name STRING, website STRING, email STRING, id STRING)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\u0001'
    STORED AS TEXTFILE
    location '${input_dir4}';


CREATE TABLE IF NOT EXISTS hiveresult(
    name STRING, year INT, avg_rate FLOAT, count_apps INT, count_rates int)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    STORED AS TEXTFILE
    location '${output_dir6}';


WITH cte AS
    (
    SELECT d.name, m.year, m.rating, m.appNumber, m.ratingCount,
                ROW_NUMBER() OVER (PARTITION BY m.year ORDER BY m.rating/m.appNumber DESC) AS rn
        FROM datasource d
            JOIN   
        mapreduce m ON m.id = d.id
    )
    INSERT INTO hiveresult
    SELECT name, year, rating, appNumber, ratingCount
    FROM cte 
    WHERE rn <= 3;
