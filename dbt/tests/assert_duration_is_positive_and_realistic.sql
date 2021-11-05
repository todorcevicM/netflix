select 
	duration_minutes

from {{ ref('movies') }}

where not(duration_minutes between 1 and 600)