with win_odds as (
    select * from {{ ref('stg_race_win_odds') }}
),

place_odds as (
    select * from {{ ref('stg_race_place_odds') }}
),

results as (
    select * from {{ ref('stg_complete_results') }}
),

place_odds_payouts as (
    select * from {{ ref('stg_place_odds_payout') }}
),

investment as (
    select * from {{ ref('stg_total_investment') }}
),

final as (
    select
        win.raceday_key,
        win.start_number,
        win.win_odds,
        place.min_place_odds,
        place.max_place_odds,
        results.place,
        case when results.place = 1 then 1 else 0 end as win,
        case when results.place = 1 then win_odds - 1 else -1 end as win_profits,
        case when results.place in (1, 2, 3) then 1 else 0 end as top3,
        case when results.place in (1, 2, 3) then min_place_odds - 1 else -1 end as top3_min_profits,
        place_odds_payouts.place_odds_payout as top3_payout,
        case when results.place in (1, 2, 3) then place_odds_payouts.place_odds_payout - 1 else -1 end as top3_profits,
        investment.win_investment,
        investment.place_investment
    
    from win_odds win
    inner join place_odds place on win.raceday_key = place.raceday_key and win.start_number = place.start_number
    inner join results on win.raceday_key = results.raceday_key and win.start_number = results.start_number
    left outer join place_odds_payouts on win.raceday_key = place_odds_payouts.raceday_key and win.start_number = place_odds_payouts.start_number
    left outer join investment on win.raceday_key = investment.raceday_key
)

select * from final