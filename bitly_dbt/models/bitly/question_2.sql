/* 

If I were to run this model in dbt Cloud, I would use tags to run this model. For example, I would run this model (and any associated models) using the following  command:
`dbt run --select tag:population` 

*/


{{ config(
    tags=["population"]
) }}

with source as (
    select * from {{ source('world_bank_global_population', 'population_by_country') }}
),

orig as (

    select  
        country,
        country_code,
        year_2010,
        year_2011,
        year_2012,
        year_2013,
        year_2014,
        year_2015,
        year_2016,
        year_2017,
        year_2018,
        year_2019
    from source

),

final as (

    select * from orig
    unpivot(population for year in (
        year_2010,
        year_2011,
        year_2012,
        year_2013,
        year_2014,
        year_2015,
        year_2016,
        year_2017,
        year_2018,
        year_2019)
      )
)

select
    country,
    country_code,
    year as as_of_year,
    population
from final
order by 1, 2, 3;
