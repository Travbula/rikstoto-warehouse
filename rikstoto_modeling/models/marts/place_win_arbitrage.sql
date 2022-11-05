with bet_opportunities as (
    select * from {{ ref('odds_results') }}
),

final as (
    select distinct
        right(left(raceday_key, char_length(raceday_key) - 2), 10) as date,
        *
    
    from bet_opportunities
    where bet_size > 0
)

select * from final