WITH 
listings as (
    SELECT * FROM {{ref('dim_listings_cleansed')}}
),
hosts as (
    SELECT * FROM {{ref('dim_hosts_cleansed')}}
)
SELECT
    l.listing_id,
    l.listing_name,
    l.room_type,
    l.minimum_nights,
    l.price,
    l.host_id,
    h.host_name,
    h.is_superhost AS host_is_superhost,
    l.created_at,
    GREATEST(l.updated_at, h.updated_at) AS updated_at
FROM listings l
LEFT JOIN hosts h ON l.host_id = h.host_id