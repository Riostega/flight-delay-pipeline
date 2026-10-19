-- Every file listed in excluded_source_files must exist in stg_flights.
-- A typo in the seed would otherwise exclude nothing and raise no error.

select e.source_file
from {{ ref('excluded_source_files') }} as e
left join (select distinct source_file from {{ ref('stg_flights') }}) as s
    on e.source_file = s.source_file
where s.source_file is null
