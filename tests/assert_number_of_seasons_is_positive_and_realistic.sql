select 
    number_of_seasons
from {{ ref('series') }}
group by 1
having not(number_of_seasons between 1 and 50)