with ranked_genre_frequency as (

	with titles_genre_frequency as (
		select 
			actor_name, 
			genre, 
			count(*) as frequency

		from {{ ref('title_actor') }} 

		join {{ ref('series') }} on title_actor.series_id = series.id 

		group by actor_name, genre

		union all 
		
		select 
			actor_name, 
			genre, 
			count(*) as frequency

		from {{ ref('title_actor') }} 
		
		join {{ ref('movies') }} on title_actor.movie_id = movies.id 

		group by actor_name, genre 
		
	) 
	select 
		actor_name, 
		genre, 
		
		row_number() over ( 
			partition by actor_name 
			order by frequency desc 
		), 

		frequency 
		
	from titles_genre_frequency 

) 
select
	actor_name as "name", 
	genre as "most_common_genre", 
	frequency as "most_common_genre_count" 

from ranked_genre_frequency 

where "row_number" = 1 