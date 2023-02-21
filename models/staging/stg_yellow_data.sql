{{ config(materialized='view') }}

with tripdata as
(
  select *,
    row_number() over(partition by vendorid, tpep_pickup_datetime) as rn
  from {{source('taxi_data_all', 'yellow_data') }}
  where vendorid is not null
)
select
   -- identifiers
    {{ dbt_utils.surrogate_key(['vendorid', 'tpep_pickup_datetime']) }} as tripid,
    cast(vendorid as integer) as vendorid,
    cast(ratecodeid as integer) as ratecodeid,
    cast(pulocationid as integer) as  pickup_locationid,
    cast(dolocationid as integer) as dropoff_locationid,

    -- timestamps
    cast(tpep_pickup_datetime as timestamp) as pickup_datetime,
    cast(tpep_dropoff_datetime as timestamp) as dropoff_datetime,

    -- trip info
    SR_Flag,
    Affiliated_base_number,
from tripdata
where rn = 1
