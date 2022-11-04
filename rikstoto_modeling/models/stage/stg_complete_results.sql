with complete_results as (
    select * from {{ source('rikstoto', 'complete_results') }}
),

complete_results_result as (
    select * from {{ source('rikstoto', 'complete_results_result') }}
),

complete_results_result_results as (
    select * from {{ source('rikstoto', 'complete_results_result_results') }}
),

final as (
    select
        main.raceday_key,
        result.startnumber as start_number,
        result.order,
        result.place
    
    from complete_results main
    inner join complete_results_result_results result on main._airbyte_ab_id = result._airbyte_ab_id
    group by 1, 2, 3, 4
)

select * from final