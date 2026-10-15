CREATE DATABASE IF NOT EXISTS cdc_db;

USE cdc_db;

DROP TABLE IF EXISTS stackoverflow_post_final_state;

CREATE EXTERNAL TABLE stackoverflow_post_final_state (
    record_id           STRING,
    author_display_name STRING,
    author_id           BIGINT,
    author_url          STRING,
    body                STRING,
    content_license     STRING,
    is_rollback         BOOLEAN,
    post_type           STRING,
    revision_comment    STRING,
    revision_number     BIGINT,
    revision_type       STRING,
    source_url          STRING,
    `timestamp`         TIMESTAMP
)
STORED AS PARQUET
LOCATION '/home/kedar/database-cdc/src/output/stackoverflow_final_state.parquet';

DESCRIBE FORMATTED stackoverflow_post_final_state;

SELECT COUNT(*) AS final_posts FROM stackoverflow_post_final_state;
