CREATE OR REPLACE VIEW target_schema.view_review AS 
SELECT DISTINCT
	fct_booking.listing_id,
	dim_listing.listing_name, 
	dim_property.property_type,
	fct_booking.neighbourhood_view,
	dim_property.accommodates,
	dim_property.has_availability,
	dim_reviews.booking_date,
	dim_reviews.review_id,
	dim_reviews.reviewer_id, 
	dim_reviews.reviewer_name, 
	dim_reviews.sentiment_score, 
	fct_booking.price
FROM target_schema.dim_reviews 
INNER JOIN target_schema.fct_booking
	ON fct_booking.review_id = dim_reviews.review_id
INNER JOIN target_schema.dim_listing
	ON dim_listing.listing_id = fct_booking.listing_id 
INNER JOIN target_schema.dim_property
	ON dim_property.listing_id = dim_listing.listing_id
ORDER BY fct_booking.listing_id;
