with ranked_genre_frequency as (

	with series_genre_freq as (
		select 
			actor_name, 
			genre as series_genre, 
			count(*) as series_frequency, 
			count(series.id) as series_count 

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
		null as movie_frequency, 
		series_count, 
		null as movie_count 
		
	from series_genre_freq 
	
	union all

	(
		with movie_genre_frequency as (
			select 
				actor_name, 
				genre as movie_genre, 
				count(*) as movie_frequency, 
				count(movies.id) as movie_count 

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
			movie_frequency, 
			null as series_count, 
			movie_count 
			
		from movie_genre_frequency 
	)

) 
select
	actor_name as "name", 

	case 
		when max(series_count) is not null then max(series_count)
		else 0
	end	as series_count,  

	case 
		when max(movie_count) is not null then max(movie_count) 
		else 0
	end as movie_count, 

	case 
		when max(series_genre) is not null and max(movie_genre) is not null and max(series_genre) >= max(movie_genre) then max(series_genre) 
		when max(series_genre) is not null and max(movie_genre) is not null and max(movie_genre) > max(series_genre) then max(movie_genre) 
		when max(series_genre) is not null and max(movie_genre) is null then max(series_genre) 
		when max(movie_genre) is not null then max(movie_genre) 
	end as most_common_genre, 

	case 
		when max(series_frequency) is not null and max(movie_frequency) is not null and max(series_frequency) >= max(movie_frequency) then max(series_frequency) 
		when max(series_frequency) is not null and max(movie_frequency) is not null and max(movie_frequency) > max(series_frequency) then max(movie_frequency)
		when max(series_frequency) is not null and max(movie_frequency) is null then max(series_frequency)
		else max(movie_frequency)
	end as most_common_genre_titles_count

from ranked_genre_frequency 

where "row_number" = 1 

group by actor_name 

order by most_common_genre_titles_count desc 