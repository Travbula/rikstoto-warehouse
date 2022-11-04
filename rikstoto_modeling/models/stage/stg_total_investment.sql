with raw_response as (
    select * from {{ source('rikstoto', '_airbyte_raw_total_investment') }}
),

request_response as (
    select 
        jsonb_array_elements(_airbyte_data->'result') as result

    from raw_response
),

normalized_response as (
    select
        result->>'raceDay' as raceday,
        result->>'raceNumber' as race_number,
        result->>'product' as product,
        cast(result->>'totalInvestment' as bigint) / 100 as total_investment
    
    from request_response
),

final as (
    select
        win.raceday || '/' || win.race_number as raceday_key,
        win.total_investment as win_investment,
        place.total_investment as place_investment

    from normalized_response win
    inner join normalized_response place on win.raceday = place.raceday and win.race_number = place.race_number
    where win.product = 'V' and place.product = 'P'
)

select * from final