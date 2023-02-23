{{ config(materialized="view") }}

select
    -- identifiers
    {{ dbt_utils.surrogate_key(['pickup_datetime', 'PUlocationID']) }} as tripid,

    dispatching_base_num as dispatching_base_num,
    cast(PUlocationID as integer) as pickup_locationid,
    Affiliated_base_number as affiliated_base_num,
    cast(DOlocationID as integer) as dropoff_locationid,

    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,

    -- trip info
    SR_Flag

from {{ source("staging", "fhv_tripdata") }}



-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
