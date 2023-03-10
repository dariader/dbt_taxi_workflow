{{ config(materialized='view') }}

with tripdata as
(
  select *,
    row_number() over(partition by dispatching_base_num, pickup_datetime) as rn
  from {{source('taxi_data_us', 'fhv_data') }}
  where dispatching_base_num is not null
)
select
   -- identifiers
    {{ dbt_utils.surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid,
    cast(pulocationid as integer) as  pickup_locationid,
    cast(dolocationid as integer) as dropoff_locationid,

    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,

    -- trip info
    SR_Flag,
    Affiliated_base_number,
from tripdata
where rn = 1
