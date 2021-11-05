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

), titles_count as (
	select 
		actor_name, 
		sum(case when movie_id is not null then 1 else 0 end) as movie_count,
		sum(case when series_id is not null then 1 else 0 end) as series_count
		
	from {{ ref('title_actor') }} 

	group by actor_name
)

select
	ranked_genre_frequency.actor_name as "name", 
	movie_count, 
	series_count, 
	genre as "most_common_genre", 
	frequency as "most_common_genre_count" 

from ranked_genre_frequency 

join titles_count

on ranked_genre_frequency.actor_name = titles_count.actor_name

where "row_number" = 1