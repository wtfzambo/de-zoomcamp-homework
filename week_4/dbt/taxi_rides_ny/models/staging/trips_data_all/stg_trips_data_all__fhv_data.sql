with
    source as (select * from {{ source("trips_data_all", "fhv_data") }}),
    renamed as (
        select
            {{ adapter.quote("dispatching_base_num") }},
            {{ adapter.quote("pickup_datetime") }},
            {{ adapter.quote("dropOff_datetime") }},
            {{ adapter.quote("PUlocationID") }},
            {{ adapter.quote("DOlocationID") }},
            {{ adapter.quote("SR_Flag") }},
            {{ adapter.quote("Affiliated_base_number") }}

        from source
    )
select *
from renamed
