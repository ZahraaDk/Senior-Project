CREATE TABLE IF NOT EXISTS target_schema.dim_listing
(
    listing_id TEXT PRIMARY KEY, 
    listing_name TEXT,
    listing_date TIMESTAMP,
    listing_description TEXT,
    listing_url VARCHAR,
    picture_url VARCHAR
);
CREATE INDEX IF NOT EXISTS "idx_listing_id" ON target_schema.dim_listing(listing_id);
INSERT INTO target_schema.dim_listing
SELECT DISTINCT
    src_listing.listing_id, 
    src_listing.listing_name,
    src_listing.listing_date, 
    src_listing.listing_description, 
    src_listing.listing_url, 
    src_listing.picture_url
FROM target_schema.stg_cleaned_df1 as src_listing
ON CONFLICT(listing_id)
DO UPDATE SET 
    listing_name = EXCLUDED.listing_name, 
    listing_date = EXCLUDED.listing_date,
    listing_description = EXCLUDED.listing_description, 
    listing_url = EXCLUDED.listing_url, 
    picture_url = EXCLUDED.picture_url;
