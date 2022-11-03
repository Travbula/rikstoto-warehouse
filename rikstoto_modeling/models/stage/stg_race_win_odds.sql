with win_odds_race as (
    select * from {{ source('rikstoto', 'win_odds_race') }}
),

win_odds_race_result as (
    select * from {{ source('rikstoto', 'win_odds_race_result') }}
),

final as (
    select
        race.raceday_key,
        result.startnumber as start_number,
        result.odds as win_odds
    
    from win_odds_race race
    inner join win_odds_race_result result on race._airbyte_ab_id = result._airbyte_ab_id
    group by 1, 2, 3
)

select * from final