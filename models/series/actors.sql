select
	actor_name as "name", 

	count(series_id) as series_count, 

	count(movie_id) as movie_count, 

	case 
		when max(sgenre) is not null and max(mgenre) is not null and max(sgenre) >= max(mgenre) then max(sgenre) 
		when max(sgenre) is not null and max(mgenre) is not null and max(mgenre) > max(sgenre) then max(mgenre)
		when max(sgenre) is not null and max(mgenre) is null then max(sgenre)
		else max(mgenre)
	end as most_common_genre, 

	case
		when max(sfreq) is not null then max(sfreq)
		else 0
	end as most_common_genre_series_count,

	case 
		when max(mfreq) is not null then max(mfreq)
		else 0
	end as most_common_genre_movie_count

	from (
		select 
			actor_name, 
			
			sgenre, 
			
			null as mgenre, 
			
			row_number() 
				over (
					partition by actor_name 
					
					order by sfreq desc
					
				) as rn, 
				
			sfreq, 
				
			null as mfreq,

			series_id, 
			
			null as movie_id
			
			from (
				select 
				
				actor_name, 
				
				genre as sgenre, 
				
				count('x') as sfreq, 
				
				series.id as series_id 

				from {{ ref('title_actor') }}

				join {{ ref('series') }} on title_actor.series_id = series.id

				group by actor_name, genre, series.id

			) sgenre_freq 
		
		union 

		select 
			actor_name, 
			
			null as sgenre, 
			
			mgenre, 
			
			row_number() 
				over (
					partition by actor_name 
					
					order by mfreq desc
				
				) as rn, 
				
			null as sfreq, 
			
			mfreq, 

			null as series_id,

			movie_id
			
			from (
				select 
					actor_name, 
					
					genre as mgenre, 
					
					count('x') as mfreq,  

					movies.id as movie_id

				from {{ ref('title_actor') }}
				
				join {{ ref('movies') }} on title_actor.movie_id = movies.id

				group by actor_name, genre, movies.id

			) mgenre_freq 

	) sranked_genre_freq 

where rn = 1

group by actor_name 

order by series_count desc


