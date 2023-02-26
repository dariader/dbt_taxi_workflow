{{ config(materialized='table') }}
"""
-- Similar to what we've done in fact_trips,
-- keep only records with known pickup and dropoff locations entries for pickup and dropoff locations.
-- cli: filter records with pickup time in year 2019.
"""
with trips_unioned as (
    select *,
        'fhv' as service_type
    from {{ ref('stg_fhv_data') }}
    where pickup_locationid is not NULL and dropoff_locationid is not NULL
),

dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select
    trips_unioned.tripid,
    trips_unioned.service_type,
    trips_unioned.pickup_locationid,
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone,
    trips_unioned.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough,
    dropoff_zone.zone as dropoff_zone,
    trips_unioned.pickup_datetime,
    trips_unioned.dropoff_datetime,
from trips_unioned
inner join dim_zones as pickup_zone
on trips_unioned.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on trips_unioned.dropoff_locationid = dropoff_zone.locationid
