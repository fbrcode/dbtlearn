{{
  config(
    materialized='table',
  )
}}

WITH 
fct_reviews AS (
  SELECT * FROM {{ref("fct_reviews")}}
),
full_moon_dates AS (
  SELECT * FROM {{ ref("seed_full_moon_dates")}}
)
SELECT 
  r.*, 
  CASE 
    WHEN m.full_moon_date IS NULL THEN false
    ELSE true
  END AS is_full_moon
FROM fct_reviews r
LEFT JOIN full_moon_dates m 
  ON (TO_DATE(r.review_date) = DATEADD(DAY, 1, m.full_moon_date))