with arbitrage_bets as (
    select * from {{ ref('place_win_arbitrage') }}
)

select 
    sum(top3_profits),
    count(1) as bets,
    sum(top3_profits) / count(1) as roi,
    avg(place_investment) as avg_investment,
    max(place_investment) as max_investment

from arbitrage_bets