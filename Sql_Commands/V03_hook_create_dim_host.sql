CREATE TABLE IF NOT EXISTS target_schema.dim_host
(
    host_id INTEGER PRIMARY KEY, 
    host_name TEXT, 
    host_url VARCHAR, 
    host_since TIMESTAMP, 
    host_location TEXT, 
    host_response_time TEXT, 
    host_response_rate TEXT, 
    host_neighbourhood TEXT, 
    host_identity_verified TEXT, 
    host_is_superhost TEXT, 
    host_total_listings_count FLOAT
);
CREATE INDEX IF NOT EXISTS "idx_host_id" ON target_schema.dim_host(host_id);
INSERT INTO target_schema.dim_host
SELECT DISTINCT
    src_listing.host_id, 
    src_listing.host_name, 
    src_listing.host_url, 
    src_listing.host_since, 
    src_listing.host_location, 
    src_listing.host_response_time, 
    src_listing.host_response_rate, 
    src_listing.host_neighbourhood, 
    src_listing.host_identity_verified, 
    src_listing.host_is_superhost, 
    CAST(src_listing.host_total_listings_count AS FLOAT)
FROM target_schema.stg_cleaned_df1 as src_listing
ON CONFLICT(host_id)
DO UPDATE SET 
    host_name = EXCLUDED.host_name, 
    host_url = EXCLUDED.host_url,
    host_since = EXCLUDED.host_since, 
    host_location = EXCLUDED.host_location, 
    host_response_time = EXCLUDED.host_response_time, 
    host_response_rate = EXCLUDED.host_response_rate, 
    host_neighbourhood = EXCLUDED.host_neighbourhood, 
    host_identity_verified = EXCLUDED.host_identity_verified,
    host_is_superhost = EXCLUDED.host_is_superhost, 
    host_total_listings_count = EXCLUDED.host_total_listings_count;