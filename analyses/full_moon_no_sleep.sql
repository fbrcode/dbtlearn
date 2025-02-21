-- run an analysis in dbt context (without materialization)
-- to compare the number of reviews during full moon and non-full moon days
WITH mart_full_moon_reviews AS (
  SELECT * FROM {{ ref('mart_full_moon_reviews') }}
)
SELECT
  is_full_moon,
  review_sentiment,
  COUNT(*) as reviews
FROM mart_full_moon_reviews
GROUP BY is_full_moon, review_sentiment
ORDER BY is_full_moon, review_sentiment