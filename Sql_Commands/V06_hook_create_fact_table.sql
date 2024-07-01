CREATE TABLE IF NOT EXISTS target_schema.fct_booking 
(
    review_id BIGINT PRIMARY KEY,
    listing_id TEXT,
    booking_date TIMESTAMP, 
    reviewer_id INTEGER,
    host_id BIGINT, 
    neighbourhood_view TEXT, 
    price NUMERIC, 
    number_of_reviews NUMERIC, 
    review_scores_rating NUMERIC,
    sentiment_score NUMERIC, 
    longitude NUMERIC, 
    latitude NUMERIC
); 

CREATE INDEX IF NOT EXISTS "idx_fct_listing" ON target_schema.fct_booking(listing_id);

INSERT INTO target_schema.fct_booking
SELECT DISTINCT
    src_reviews.review_id, 
    src_listing.listing_id, 
    src_reviews.booking_date,
    src_reviews.reviewer_id,
    src_listing.host_id, 
    src_listing.neighbourhood_view, 
    src_listing.price, 
    src_listing.number_of_reviews, 
    src_listing.review_scores_rating,
    src_reviews.sentiment_score, 
    src_listing.longitude, 
    src_listing.latitude
FROM target_schema.stg_cleaned_df1 as src_listing
INNER JOIN target_schema.stg_cleaned_df2 as src_reviews
    ON src_listing.listing_id = src_reviews.listing_id

ON CONFLICT (review_id) DO UPDATE
SET 
    listing_id = EXCLUDED.listing_id,
    booking_date = EXCLUDED.booking_date, 
    reviewer_id = EXCLUDED.reviewer_id,
    host_id = EXCLUDED.host_id, 
    neighbourhood_view = EXCLUDED.neighbourhood_view, 
    price = EXCLUDED.price, 
    number_of_reviews = EXCLUDED.number_of_reviews, 
    review_scores_rating = EXCLUDED.review_scores_rating, 
    sentiment_score = EXCLUDED.sentiment_score, 
    longitude = EXCLUDED.longitude, 
    latitude = EXCLUDED.latitude;
