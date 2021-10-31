with combinations as (

	with numbers as (
		select generate_series(1, 50) as number
	)

	select 
		"id" as series_id, 

		null as movie_id,

		case 
			when nullif(split_part("cast", ',', numbers.number), '') like ' %' then right( nullif(split_part("cast", ',', numbers.number), ''), position(' %' in nullif(split_part("cast", ',', numbers.number), '')) -1 )
			else nullif(split_part("cast", ',', numbers.number), '')
		end as actor_name

	from {{ ref('series') }}

	cross join numbers

	where "cast" is not null

	union all

	select 
		null as series_id,

		"id" as movie_id,

		case 
			when nullif(split_part("cast", ',', numbers.number), '') like ' %' then right( nullif(split_part("cast", ',', numbers.number), ''), position(' %' in nullif(split_part("cast", ',', numbers.number), '')) -1 )
			else nullif(split_part("cast", ',', numbers.number), '')
		end as actor_name

	from {{ ref('movies') }}

	cross join numbers

	where "cast" is not null
)

select 
	*

from combinations 

where combinations.actor_name is not null 



