
select 

cast(replace(show_id, 's', '') as integer) as "id", -- id (based on id, but drop the 's'; type: integer)

title, 

case 
    when director like '%,%' then left(director, position(',' in director) - 1)
    else director
end, -- director (take the first if there are multiple)

"cast", 

case 
	when country like '%,%' then left(country, position(',' in country) - 1)
	else country
end, -- country (take the first if there are multiple)

to_date(date_added, 'Month DD, YYYY'), -- (type: date)

cast(release_year as integer), -- (type: int)

rating, 

case 
	when duration like '%Season%' then cast(left(duration, position('S' in duration) - 1) as integer)
end as number_of_seasons, -- number_of_seasons (type: integer)

case 
	when listed_in like '%,%' then left(listed_in, position(',' in listed_in) - 1) 
	else listed_in
end as genre, -- genre (based on listed_in, take the first if there are multiple)

"description"

from {{ source('raw_data', 'titles') }}
where "type" like 'TV Show'