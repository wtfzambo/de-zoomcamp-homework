{{ config(materialized = 'table') }}

with
fhv_trips as (
    select * from {{ ref('stg_trips_data_all__fhv_data') }}
)

, zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

, final as (
    select
        fhv.tripid
        , fhv.dispatching_base_num
        , fhv.pickup_datetime
        , fhv.dropoff_datetime
        , fhv.pickup_locationid
        , pickup_zone.borough as pickup_borough
        , pickup_zone.zone as pickup_zone
        , fhv.dropoff_locationid
        , dropoff_zone.borough as dropoff_borough
        , dropoff_zone.zone as dropoff_zone
        , fhv.sr_flag
        , fhv.affiliated_base_number

    from fhv_trips as fhv

    inner join zones as pickup_zone
        on fhv.pickup_locationid = pickup_zone.locationid

    inner join zones as dropoff_zone
        on fhv.dropoff_locationid = dropoff_zone.locationid
)

select * from final
