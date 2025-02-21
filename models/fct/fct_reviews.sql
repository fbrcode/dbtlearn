-- the specific materialization incremental to append only table 
-- that fails if the schema changes
{{
  config(
    materialized='incremental',
    on_schema_change='fail'
  )
}}
WITH src_reviews AS (
    SELECT * FROM {{ref("src_reviews")}}
)
SELECT 
  {{ dbt_utils.generate_surrogate_key([
    'listing_id', 
    'review_date', 
    'reviewer_name', 
    'review_text']
    ) }} AS review_id,
  listing_id, 
  review_date, 
  reviewer_name, 
  review_text, 
  review_sentiment
FROM src_reviews
WHERE review_text IS NOT NULL

-- condition when using the incremental materialization strategy
-- to only select rows that are newer than the last run
-- {{ this }} refers to the current model fct_reviews
/* simple implementation without date range

{% if is_incremental() %}
  AND review_date > (SELECT MAX(review_date) FROM {{ this }})
{% endif %}

*/

/* implementation with date range variables defined at dbt_project.yml */
{% if is_incremental() %}
  {% if var("start_date", False) and var("end_date", False) %}
    {{ log('Loading ' ~ this ~ ' incrementally (start_date: ' ~ var("start_date") ~ ', end_date: ' ~ var("end_date") ~ ')', info=True) }}
    /* limit load to start and end dates (timestamp) */
    AND review_date >= '{{ var("start_date") }}'
    AND review_date < '{{ var("end_date") }}'
  {% else %}
    /* load all missing not already present in the model fct_reviews */
    AND review_date > (select max(review_date) from {{ this }})
    {{ log('Loading ' ~ this ~ ' incrementally (all missing dates)', info=True)}}
  {% endif %}
{% endif %}
