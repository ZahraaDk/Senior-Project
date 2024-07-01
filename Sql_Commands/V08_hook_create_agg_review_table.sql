CREATE TABLE IF NOT EXISTS target_schema.agg_review
(
    listing_id TEXT,
    avg_sentiment_score NUMERIC, 
    review_count NUMERIC,
    max_price NUMERIC, 
    min_price NUMERIC
);
CREATE INDEX IF NOT EXISTS "idx_agg_review" ON target_schema.agg_review(listing_id);
INSERT INTO target_schema.agg_review
SELECT 
    fct_booking.listing_id, 
    ROUND(AVG(dim_reviews.sentiment_score), 2) AS avg_sentiment_score, 
    COUNT(dim_reviews.review_id) AS review_count, 
    MAX(fct_booking.price) AS max_price, 
    MIN(price) AS min_price
FROM target_schema.dim_reviews
INNER JOIN target_schema.fct_booking
    ON dim_reviews.review_id = fct_booking.review_id
GROUP BY 
    fct_booking.listing_id
ORDER BY 
    fct_booking.listing_id;

UPDATE target_schema.agg_review 
SET
    listing_id = subquery.listing_id, 
    avg_sentiment_score = subquery.avg_sentiment_score, 
    review_count = subquery.review_count, 
    max_price = subquery.max_price, 
    min_price = subquery.min_price
FROM (
    SELECT 
    fct_booking.listing_id, 
    ROUND(AVG(dim_reviews.sentiment_score), 2) AS avg_sentiment_score, 
    COUNT(dim_reviews.review_id) AS review_count, 
    MAX(fct_booking.price) AS max_price, 
    MIN(price) AS min_price
    FROM target_schema.dim_reviews
    INNER JOIN target_schema.fct_booking
        ON dim_reviews.review_id = fct_booking.review_id
    GROUP BY 
        fct_booking.listing_id
) subquery
WHERE agg_review.listing_id = subquery.listing_id;