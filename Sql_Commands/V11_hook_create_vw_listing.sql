CREATE OR REPLACE VIEW target_schema.view_listing AS 
SELECT DISTINCT
	dim_listing.listing_date,
	dim_listing.listing_id, 
	dim_listing.listing_name, 
	dim_property.property_type, 
	dim_property.accommodates, 
	dim_property.bathrooms, 
	dim_property.bedrooms, 
	dim_property.minimum_nights, 
	dim_property.maximum_nights, 
	dim_property.has_availability, 
	fct_booking.neighbourhood_view,
	fct_booking.price
FROM target_schema.dim_listing 
INNER JOIN target_schema.dim_property
	ON dim_property.listing_id = dim_listing.listing_id
INNER JOIN target_schema.fct_booking
	ON fct_booking.listing_id = dim_property.listing_id
ORDER BY dim_listing.listing_id;
