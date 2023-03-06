{{ config(materialized = 'table') }}

with
green_data as (
    select
        *
        , 'Green' as service_type
    from {{ ref('stg_trips_data_all__green_trips') }}
)

, yellow_data as (
    select
        *
        , 'Yellow' as service_type
    from {{ ref('stg_trips_data_all__yellow_trips') }}
)

, trips_unioned as (
    select * from green_data
    union all
    select * from yellow_data
)

, dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

, final as (
    select
        tu.tripid
        , tu.vendorid
        , tu.service_type
        , tu.ratecodeid
        , tu.pickup_locationid
        , pickup_zone.borough as pickup_borough
        , pickup_zone.zone as pickup_zone
        , tu.dropoff_locationid
        , dropoff_zone.borough as dropoff_borough
        , dropoff_zone.zone as dropoff_zone
        , tu.pickup_datetime
        , tu.dropoff_datetime
        , tu.store_and_fwd_flag
        , tu.passenger_count
        , tu.trip_distance
        , tu.trip_type
        , tu.fare_amount
        , tu.extra
        , tu.mta_tax
        , tu.tip_amount
        , tu.tolls_amount
        , tu.ehail_fee
        , tu.improvement_surcharge
        , tu.total_amount
        , tu.payment_type
        , tu.payment_type_description
        , tu.congestion_surcharge
    from trips_unioned as tu

    inner join dim_zones as pickup_zone
        on tu.pickup_locationid = pickup_zone.locationid

    inner join dim_zones as dropoff_zone
        on tu.dropoff_locationid = dropoff_zone.locationid
)

select *
from final
