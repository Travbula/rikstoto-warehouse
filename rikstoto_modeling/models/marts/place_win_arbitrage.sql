with bet_opportunities as (
    select * from {{ ref('odds_results') }}
),

final as (
    select * from bet_opportunities
    where min_place_odds >= 0.9 * win_odds
    and place_investment >= 5000
)

select * from final