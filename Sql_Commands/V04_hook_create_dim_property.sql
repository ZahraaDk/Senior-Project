CREATE TABLE IF NOT EXISTS target_schema.dim_property
(
    listing_id TEXT PRIMARY KEY, 
    property_type TEXT, 
    accommodates INTEGER, 
    bathrooms VARCHAR, 
    bedrooms INTEGER, 
    beds INTEGER, 
    amenities TEXT, 
    minimum_nights INTEGER, 
    maximum_nights INTEGER, 
    has_availability TEXT
);
CREATE INDEX IF NOT EXISTS "idx_listing_id2" ON target_schema.dim_property(listing_id);
INSERT INTO target_schema.dim_property
SELECT DISTINCT
    src_listing.listing_id, 
    src_listing.property_type, 
    src_listing.accommodates, 
    src_listing.bathrooms,
    src_listing.bedrooms, 
    src_listing.beds, 
    src_listing.amenities, 
    src_listing.minimum_nights, 
    src_listing.maximum_nights, 
    src_listing.has_availability
FROM target_schema.stg_cleaned_df1 AS src_listing
ON CONFLICT(listing_id)
DO UPDATE SET  
    property_type = EXCLUDED.property_type, 
    accommodates = EXCLUDED.accommodates, 
    bathrooms = EXCLUDED.bathrooms, 
    bedrooms = EXCLUDED.bedrooms, 
    beds = EXCLUDED.beds, 
    amenities = EXCLUDED.amenities, 
    minimum_nights = EXCLUDED.minimum_nights, 
    maximum_nights = EXCLUDED.maximum_nights, 
    has_availability = EXCLUDED.has_availability;
