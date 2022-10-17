{{ config(
    tags=["population"]
) }}

with source as (
    select * from {{ source('world_bank_global_population', 'health_nutrition_population') }}
),


with max_val as (

    select
        year,
        country_name,
        country_code,
        indicator_code,
        indicator_name,
        max(value) over (partition by year) as highest_value,
        value
    from source
    where
        indicator_code = 'SH.XPD.CHEX.PP.CD' and
        year = 2018
    order by 1 desc

)

select
    *,
    (highest_value - value) as gap
from max_val;