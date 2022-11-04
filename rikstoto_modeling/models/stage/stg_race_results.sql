with raw_results as (
    select * from {{ source('rikstoto', '_airbyte_raw_race_results') }}
),

request_response as (
    select 
        _airbyte_data->'result' as result,
        _airbyte_data->>'raceday_key' as raceday_key

    from raw_results
),

normalized_response as (
    select
        raceday_key,
        result->>'product' as product,
        result->>'totalInvestment' as total_investment
    
    from request_response
),

final as (
    select * from normalized_response
    where product in ('V', 'P')
)

select * from final