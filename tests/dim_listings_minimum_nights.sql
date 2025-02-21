-- check if minimum nights is bigger than 0 or not (0 or less is not allowed)
SELECT * FROM {{ ref('dim_listings_cleansed') }}
WHERE minimum_nights < 1
LIMIT 10