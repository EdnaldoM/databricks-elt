with
    source as (
        select *
        from {{ source('api','api_users') }}
    )

select *
from source