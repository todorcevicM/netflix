select 
    number_of_seasons
from {{ ref('series') }}
where not(number_of_seasons between 1 and 50)