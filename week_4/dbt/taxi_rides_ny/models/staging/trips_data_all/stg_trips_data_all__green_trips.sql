with
source as (
    select * from {{ source('trips_data_all', 'green_trips') }}
)

, dedupe_rows as (
    select
        *
        , row_number() over(partition by vendorid, lpep_pickup_datetime) as rn

    from source
    where vendorid is not null
)

, renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['vendorid', 'lpep_pickup_datetime']) }} as tripid
        , cast({{ adapter.quote("VendorID") }} as integer) as vendorid
        , cast({{ adapter.quote("RatecodeID") }} as integer) as ratecodeid
        , cast({{ adapter.quote("PULocationID") }} as integer) as pickup_locationid
        , cast({{ adapter.quote("DOLocationID") }} as integer) as dropoff_locationid
        , cast({{ adapter.quote("lpep_pickup_datetime") }} as timestamp) as pickup_datetime
        , cast({{ adapter.quote("lpep_dropoff_datetime") }} as timestamp) as dropoff_datetime
        , {{ adapter.quote("store_and_fwd_flag") }}
        , cast({{ adapter.quote("passenger_count") }} as integer) as passenger_count
        , cast({{ adapter.quote("trip_distance") }} as numeric) as trip_distance
        , cast({{ adapter.quote("trip_type") }} as integer) as trip_type
        , cast({{ adapter.quote("fare_amount") }} as numeric) as fare_amount
        , cast({{ adapter.quote("extra") }} as numeric) as extra
        , cast({{ adapter.quote("mta_tax") }} as numeric) as mta_tax
        , cast({{ adapter.quote("tip_amount") }} as numeric) as tip_amount
        , cast({{ adapter.quote("tolls_amount") }} as numeric) as tolls_amount
        , cast({{ adapter.quote("ehail_fee") }} as integer) as ehail_fee
        , cast({{ adapter.quote("improvement_surcharge") }} as numeric) as improvement_surcharge
        , cast({{ adapter.quote("total_amount") }} as numeric) as total_amount
        , cast({{ adapter.quote("payment_type") }} as integer) as payment_type
        , {{ get_payment_type_description('payment_type') }} as payment_type_description
        , cast({{ adapter.quote("congestion_surcharge") }} as numeric) as congestion_surcharge

    from dedupe_rows
    where rn = 1
)

select *
from renamed
{{ limit_10000() }}
