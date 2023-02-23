{{ config(materialized='table') }}

with 
trips_unioned as (
    select * from {{ ref('stg_fhv_tripdata') }}
)

select 
    trips_unioned.tripid, 
    trips_unioned.dispatching_base_num, 
    trips_unioned.pickup_locationid, 
    trips_unioned.dropoff_locationid,
    trips_unioned.pickup_datetime, 
    trips_unioned.dropoff_datetime, 
    trips_unioned.affiliated_base_num, 
    trips_unioned.SR_Flag
from trips_unioned