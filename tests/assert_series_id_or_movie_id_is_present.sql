select 
    series_id,

    movie_id

from {{ ref('title_actor')}}

where not(series_id is not null and movie_id is null or series_id is null and movie_id is not null)