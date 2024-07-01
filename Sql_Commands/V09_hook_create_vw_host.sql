CREATE OR REPLACE VIEW dw_reporting.view_host AS 
SELECT DISTINCT
	dim_host.host_id, 
	dim_host.host_name,
	fct_booking.listing_id, 
	CAST(dim_host.host_since AS DATE) as host_since,
	dim_host.host_location, 
	dim_host.host_is_superhost,
	dim_host.host_total_listings_count,
	dim_host.host_response_time, 
	dim_host.host_response_rate,
	fct_booking.number_of_reviews,
	fct_booking.price,
	fct_booking.review_scores_rating, 
	agg_host.avg_reviews_per_host, 
	agg_host.avg_scores_per_host
FROM dw_reporting.dim_host 
INNER JOIN dw_reporting.fct_booking
	ON fct_booking.host_id = dim_host.host_id
INNER JOIN dw_reporting.agg_host 
	ON agg_host.host_id = fct_booking.host_id
ORDER BY dim_host.host_id;