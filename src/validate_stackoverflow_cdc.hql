USE cdc_db;

-- Final state should have one row per active Stack Overflow post.
SELECT
    COUNT(*) AS final_posts,
    COUNT(DISTINCT record_id) AS unique_post_ids,
    COUNT(*) - COUNT(DISTINCT record_id) AS duplicate_post_ids
FROM stackoverflow_post_final_state;

-- Inspect the latest retained revision and its source attribution.
SELECT
    record_id,
    post_type,
    revision_number,
    author_display_name,
    content_license,
    `timestamp`,
    source_url
FROM stackoverflow_post_final_state;

-- Confirm which per-revision licenses were present in the final sample.
SELECT content_license, COUNT(*) AS post_count
FROM stackoverflow_post_final_state
GROUP BY content_license;
