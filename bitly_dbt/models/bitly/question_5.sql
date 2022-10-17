{{ config(
    tags=["population"]
) }}

with joined as (
    select
        g_pop.*,
        lag(g_pop.population) over (partition by g_pop.country_code order by g_pop.year) as prev_pop,
        n_pop.value as health_exp,
        lag(n_pop.value) over (partition by g_pop.country_code order by g_pop.year) as prev_exp,    
    from  {{ref('question_2')}} as g_pop
    inner join bitly-technical.world_bank_health_population.health_nutrition_population as n_pop 
      on g_pop.country_code = n_pop.country_code and
      g_pop.year = concat('year_',n_pop.year)
    where
        n_pop.indicator_code = 'SH.XPD.CHEX.PC.CD'
    order by 1, 2, 3

)

select
    country,
    country_code,
    year,
    population,
    round(((population-prev_pop)/prev_pop)*100,2) as change_in_pop,
    health_exp,
    round(((health_exp-prev_exp)/prev_exp)*100,2) as change_in_exp
from joined