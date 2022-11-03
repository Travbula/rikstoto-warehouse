with bet_opportunities as (
    select * from {{ ref('odds_results') }}
),

final as (
    select * from bet_opportunities
    where min_place_odds >= 1 * win_odds
)

select * from final