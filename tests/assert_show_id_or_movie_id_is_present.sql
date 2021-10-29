select 
    show_id,

    movie_id

from {{ ref('titles_actor')}}

where not(show_id is null or movie_id is null)