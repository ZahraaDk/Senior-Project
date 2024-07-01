CREATE TABLE IF NOT EXISTS target_schema.agg_host
(
    host_id INTEGER, 
    avg_reviews_per_host NUMERIC,
    avg_scores_per_host NUMERIC, 
    superhost_count INTEGER, 
    non_superhost_count INTEGER
);
CREATE INDEX IF NOT EXISTS "idx_agg_host" ON target_schema.agg_host(host_id);
INSERT INTO target_schema.agg_host
SELECT 
    dim_host.host_id, 
    ROUND(AVG(fct_booking.number_of_reviews), 2) AS avg_reviews_per_host, 
    ROUND(AVG(fct_booking.review_scores_rating), 2) AS avg_scores_per_host, 
    SUM (CASE WHEN dim_host.host_is_superhost = 't' THEN 1 ELSE 0 END) AS superhost_count, 
    SUM (CASE WHEN dim_host.host_is_superhost = 'f' THEN 1 ELSE 0 END) AS non_superhost_count
FROM target_schema.dim_host
INNER JOIN target_schema.fct_booking
    ON dim_host.host_id = fct_booking.host_id
GROUP BY 
    dim_host.host_id
ORDER BY 
    dim_host.host_id;
    
UPDATE target_schema.agg_host
SET 
    host_id = subquery.host_id, 
    avg_reviews_per_host = subquery.avg_reviews_per_host, 
    avg_scores_per_host = subquery.avg_scores_per_host, 
    superhost_count = subquery.superhost_count, 
    non_superhost_count = subquery.non_superhost_count
FROM(
    SELECT 
    dim_host.host_id, 
    ROUND(AVG(fct_booking.number_of_reviews), 2) AS avg_reviews_per_host, 
    ROUND(AVG(fct_booking.review_scores_rating), 2) AS avg_scores_per_host, 
    SUM (CASE WHEN dim_host.host_is_superhost = 't' THEN 1 ELSE 0 END) AS superhost_count, 
    SUM (CASE WHEN dim_host.host_is_superhost = 'f' THEN 1 ELSE 0 END) AS non_superhost_count
    FROM target_schema.dim_host
    INNER JOIN target_schema.fct_booking
        ON dim_host.host_id = fct_booking.host_id
    GROUP BY 
        dim_host.host_id
) subquery
WHERE agg_host.host_id = subquery.host_id;