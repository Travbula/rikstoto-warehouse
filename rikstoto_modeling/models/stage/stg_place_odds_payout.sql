with raw_stuff as (
	select
		arrr."_airbyte_data"->'result' as result

	from raw_data."_airbyte_raw_race_results" arrr 
),

odds_jsons as (
	select
		result->>'raceDay' as raceday,
		result->'finalOdds'->'winOdds' as win_odds_json,
		result->'finalOdds'->'placeOdds' as place_odds_json

	from raw_stuff
),

place_odds_step1 as (
	select
		left(raceday, char_length(raceday) - 2) as raceday,
		s.*

	from odds_jsons
	cross join jsonb_each(odds_jsons.place_odds_json) as s(racenumber, data)
),

final as (
	select
		raceday || '/' || racenumber as raceday_key,
		racenumber,
		cast(t.start_number as bigint),
		cast(t.data->>'odds' as decimal) as place_odds_payout
	
	from place_odds_step1 raw
	cross join jsonb_each(raw.data) as t(start_number, data)
	group by 1, 2, 3, 4
)

select * from final