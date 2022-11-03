with win_odds as (
    select * from {{ ref('stg_race_win_odds') }}
),

place_odds as (
    select * from {{ ref('stg_race_place_odds') }}
),

results as (
    select * from {{ ref('stg_race_results') }}
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
        case when results.place in (1, 2, 3) then min_place_odds - 1 else -1 end as top3_min_profits
    
    from win_odds win
    inner join place_odds place on win.raceday_key = place.raceday_key and win.start_number = place.start_number
    inner join results on win.raceday_key = results.raceday_key and win.start_number = results.start_number
)

select * from final