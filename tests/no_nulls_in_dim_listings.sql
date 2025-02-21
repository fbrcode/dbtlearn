-- use macro to ensure that there are no NULL values in the dim_listings_cleansed model
{{ no_nulls_in_column(ref("dim_listings_cleansed")) }}