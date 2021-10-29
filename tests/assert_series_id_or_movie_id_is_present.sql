select 
    series_id,

    movie_id

from {{ ref('title_actor')}}

where (series_id is null and movie_id is null)