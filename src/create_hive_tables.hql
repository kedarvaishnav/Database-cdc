CREATE DATABASE IF NOT EXISTS cdc_db;

USE cdc_db;

DROP TABLE IF EXISTS users_final_state;

CREATE EXTERNAL TABLE users_final_state (
    record_id   STRING,
    user_name   STRING,
    email       STRING,
    age         INT,
    status      STRING,
    balance     DOUBLE,
    `timestamp` TIMESTAMP
)
STORED AS PARQUET
LOCATION '/home/kedar/database-cdc/src/output/final_state.parquet';

DESCRIBE FORMATTED users_final_state;

SELECT COUNT(*) AS total_records FROM users_final_state;
