with numbers as (
	select generate_series(1, 50) as number
)

select 
	"id" as show_id, 

	null as movie_id,

	split_part("cast", ',', numbers.number) as actor_name 

from {{ ref('series') }}

cross join numbers

where "cast" is not null

union all

select 
	null as show_id,

	"id" as movie_id,

	split_part("cast", ',', numbers.number) as actor_name

from {{ ref('movies') }}

cross join numbers

where "cast" is not null

