with ranked_genre_frequency as (

	with series_genre_freq as (
		select 
			actor_name, 
			genre as series_genre, 
			count(*) as series_frequency

		from {{ ref('title_actor') }} 

		join {{ ref('series') }} on title_actor.series_id = series.id 

		group by actor_name, genre
	)
	select 
		actor_name, 
		series_genre, 
		null as movie_genre, 
		
		row_number() over ( 
			partition by actor_name 
			order by series_frequency desc 
		), 

		series_frequency, 
		null as movie_frequency
		
	from series_genre_freq 
	
	union all

	(
		with movie_genre_frequency as (
			select 
				actor_name, 
				genre as movie_genre, 
				count(*) as movie_frequency

			from {{ ref('title_actor') }} 
			
			join {{ ref('movies') }} on title_actor.movie_id = movies.id 

			group by actor_name, genre 
		)

		select 
			actor_name, 
			null as series_genre, 
			movie_genre, 
			
			row_number() over ( 
				partition by actor_name 
				order by movie_frequency desc 
			), 
				
			null as series_frequency, 
			movie_frequency
			
		from movie_genre_frequency 
	)

) 
select
	actor_name as "name", 
	coalesce(max(series_frequency), 0)	as series_frequency,  
	coalesce(max(movie_frequency), 0) as movie_frequency, 

	case 
		when coalesce(max(series_frequency), 0) >= coalesce(max(movie_frequency), 0) then max(series_genre) 
		else max(movie_genre) 
	end as most_common_genre, 

	coalesce(greatest(max(series_frequency), max(movie_frequency)), 0) as most_common_genre_titles_count 	

from ranked_genre_frequency 

where "row_number" = 1 

group by actor_name

order by most_common_genre_titles_count desc 