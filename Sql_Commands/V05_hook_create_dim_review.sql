CREATE TABLE IF NOT EXISTS target_schema.dim_reviews
(
    review_id bigint PRIMARY KEY, 
    booking_date TIMESTAMP, 
    reviewer_id INTEGER, 
    reviewer_name TEXT, 
    comments TEXT, 
    sentiment_score NUMERIC,
    sentiment TEXT
);
CREATE INDEX IF NOT EXISTS "idx_review_id" ON target_schema.dim_reviews(review_id);
INSERT INTO target_schema.dim_reviews
SELECT DISTINCT
    src_reviews.review_id, 
    src_reviews.booking_date, 
    src_reviews.reviewer_id, 
    src_reviews.reviewer_name, 
    src_reviews.comments, 
    src_reviews.sentiment_score, 
    src_reviews.sentiment
FROM target_schema.stg_cleaned_df2 AS src_reviews
ON CONFLICT(review_id)
DO UPDATE SET
    booking_date = EXCLUDED.booking_date, 
    reviewer_id = EXCLUDED.reviewer_id, 
    reviewer_name = EXCLUDED.reviewer_name, 
    comments = EXCLUDED.comments,
    sentiment_score = EXCLUDED.sentiment_score, 
    sentiment = EXCLUDED.sentiment;