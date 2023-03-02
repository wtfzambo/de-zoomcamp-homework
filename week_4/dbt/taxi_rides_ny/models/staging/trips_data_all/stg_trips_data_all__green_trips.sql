with
    source as (select * from {{ source("trips_data_all", "green_trips") }}),
    renamed as (
        select
            {{ adapter.quote("VendorID") }},
            {{ adapter.quote("lpep_pickup_datetime") }},
            {{ adapter.quote("lpep_dropoff_datetime") }},
            {{ adapter.quote("store_and_fwd_flag") }},
            {{ adapter.quote("RatecodeID") }},
            {{ adapter.quote("PULocationID") }},
            {{ adapter.quote("DOLocationID") }},
            {{ adapter.quote("passenger_count") }},
            {{ adapter.quote("trip_distance") }},
            {{ adapter.quote("fare_amount") }},
            {{ adapter.quote("extra") }},
            {{ adapter.quote("mta_tax") }},
            {{ adapter.quote("tip_amount") }},
            {{ adapter.quote("tolls_amount") }},
            {{ adapter.quote("ehail_fee") }},
            {{ adapter.quote("improvement_surcharge") }},
            {{ adapter.quote("total_amount") }},
            {{ adapter.quote("payment_type") }},
            {{ adapter.quote("trip_type") }},
            {{ adapter.quote("congestion_surcharge") }}

        from source
    )
select *
from renamed
