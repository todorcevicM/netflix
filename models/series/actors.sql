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
	movie_count, 
	series_count, 
	genre as "most_common_genre", 
	frequency as "most_common_genre_count" 

from ranked_genre_frequency 

join (
	select 
		coalesce(movies_counts.actor_name, series_counts.actor_name) as "name", 
		coalesce(movie_count, 0) as movie_count, 
		coalesce(series_count, 0) as series_count

	from (
		select 
			actor_name, 
			count(*) as movie_count 
			
		from {{ ref('title_actor') }} 
		
		where movie_id is not null

		group by actor_name
	) movies_counts

	full outer join (
		select 
			actor_name, 
			count(*) as series_count 
			
		from {{ ref('title_actor') }} 
		
		where series_id is not null

		group by actor_name
	) series_counts
	on movies_counts.actor_name = series_counts.actor_name 
	
	order by movie_count desc
) titles_count 

on titles_count.name = ranked_genre_frequency.actor_name

where "row_number" = 1