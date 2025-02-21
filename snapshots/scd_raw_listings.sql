{% snapshot scd_raw_listings %}

{{
  config(
    target_schema='dev',
    unique_key='id',
    strategy='timestamp',
    updated_at='updated_at',
    invalidate_hard_deletes=True
  )
}}

/* 
invalidate_hard_deletes=True is used to invalidate the record 
if the record is deleted in the source system
*/

SELECT * FROM {{ source('airbnb', 'listings') }}

/*
dbt snapshot

snowsql -c dbt -q "SELECT id, minimum_nights, updated_at, dbt_scd_id, dbt_updated_at, dbt_valid_from, dbt_valid_to FROM AIRBNB.DEV.SCD_RAW_LISTINGS WHERE id=3176"

+------+----------------+-------------------------+----------------------------------+-------------------------+-------------------------+--------------+
|   ID | MINIMUM_NIGHTS | UPDATED_AT              | DBT_SCD_ID                       | DBT_UPDATED_AT          | DBT_VALID_FROM          | DBT_VALID_TO |
|------+----------------+-------------------------+----------------------------------+-------------------------+-------------------------+--------------|
| 3176 |             62 | 2009-06-05 21:34:42.000 | c9e3bc0b5eb3a808ee31530eccdfa503 | 2009-06-05 21:34:42.000 | 2009-06-05 21:34:42.000 | NULL         |
+------+----------------+-------------------------+----------------------------------+-------------------------+-------------------------+--------------+

Update an entry to observe snapshot changes:

snowsql -c dbt -q "UPDATE AIRBNB.RAW.RAW_LISTINGS SET minimum_nights=30, updated_at=CURRENT_TIMESTAMP() WHERE id=3176"

dbt snapshot

+------+----------------+-------------------------+----------------------------------+-------------------------+-------------------------+-------------------------+
|   ID | MINIMUM_NIGHTS | UPDATED_AT              | DBT_SCD_ID                       | DBT_UPDATED_AT          | DBT_VALID_FROM          | DBT_VALID_TO            |
|------+----------------+-------------------------+----------------------------------+-------------------------+-------------------------+-------------------------|
| 3176 |             30 | 2025-01-27 05:43:33.071 | 3587086ff906791ad2206bfb3ab6c618 | 2025-01-27 05:43:33.071 | 2025-01-27 05:43:33.071 | NULL                    |
| 3176 |             62 | 2009-06-05 21:34:42.000 | c9e3bc0b5eb3a808ee31530eccdfa503 | 2009-06-05 21:34:42.000 | 2009-06-05 21:34:42.000 | 2025-01-27 05:43:33.071 |
+------+----------------+-------------------------+----------------------------------+-------------------------+-------------------------+-------------------------+

*/

{% endsnapshot %}