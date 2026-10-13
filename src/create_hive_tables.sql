-- Hive DDL for CDC Reconciled Final State Table
DROP TABLE IF EXISTS cdc_db.users_final_state;
CREATE DATABASE IF NOT EXISTS cdc_db;

CREATE EXTERNAL TABLE IF NOT EXISTS cdc_db.users_final_state (
    record_id STRING,
    user_name STRING,
    email STRING,
    age INT,
    status STRING,
    balance DOUBLE,
    timestamp TIMESTAMP
)
COMMENT 'Final state after CDC deduplication - 47,923 active records'
STORED AS PARQUET
LOCATION '/home/abel9/Database-cdc/src/output/final_state.parquet';

MSCK REPAIR TABLE cdc_db.users_final_state;