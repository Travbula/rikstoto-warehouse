with place_odds_race as (
    select * from {{ source('rikstoto', 'place_odds_race') }}
),

place_odds_race_result as (
    select * from {{ source('rikstoto', 'place_odds_race_result') }}
),

final as (
    select
        race.raceday_key,
        result.startnumber as start_number,
        result.minodds as min_place_odds,
        result.maxodds as max_place_odds
    
    from place_odds_race race
    inner join place_odds_race_result result on race._airbyte_ab_id = result._airbyte_ab_id
    group by 1, 2, 3, 4
)

select * from final