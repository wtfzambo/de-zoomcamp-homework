with

source as (
    select * from {{ source("trips_data_all", "fhv_data") }}
)

, dedupe_rows as (
    select
        *
        , row_number() over(partition by pickup_datetime, dropoff_datetime, pulocationid, dolocationid) as rn

    from source
    where
        pulocationid is not null
        and pulocationid != 0
        and dolocationid is not null
        and dolocationid != 0
)

, renamed as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'pickup_datetime',
            'dropOff_datetime',
            'PUlocationID',
            'DOlocationID']) }} as tripid
        , {{ adapter.quote("dispatching_base_num") }}
        , {{ adapter.quote("pickup_datetime") }}
        , {{ adapter.quote("dropOff_datetime") }} as dropoff_datetime
        , {{ adapter.quote("PUlocationID") }} as pickup_locationid
        , {{ adapter.quote("DOlocationID") }} as dropoff_locationid
        , {{ adapter.quote("SR_Flag") }} as sr_flag
        , {{ adapter.quote("Affiliated_base_number") }} as affiliated_base_number

    from dedupe_rows
    where rn = 1
)

select * from renamed
{{ limit_10000() }}
