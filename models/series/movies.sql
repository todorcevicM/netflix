select
	cast(replace(show_id, 's', '') as integer) as "id",

	title,

	case 
		when director like '%,%' then left(director, position(',' in director) - 1)
		else director
	end as director, 

	"cast", 

	case 
		when country like '%,%' then left(country, position(',' in country) - 1)
		else country
	end as country, 

	to_date(date_added, 'Month DD, YYYY') as date_added, 

	cast(release_year as integer) as release_year, 

	rating,

	case 
		when duration like '%min%' then cast(left(duration, position('m' in duration) - 1) as integer)
	end as duration_minutes,

	case 
		when listed_in like '%,%' then left(listed_in, position(',' in listed_in) - 1) 
		else listed_in
	end as genre, 

	"description"

from {{ source('raw_data', 'titles') }} 

where "type" = 'Movie'
