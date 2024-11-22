with oos_events as (
    select * from {{ ref('out_of_stock') }}
)
select 
    event_date
    , upc
    , hours_recouped
from oos_events
where hours_recouped<0