with series_genre_freq as (
    select 
        actor_name, 
        null as series_genre, 
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