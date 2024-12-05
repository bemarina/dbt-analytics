with oos_events as (
    select * from {{ ref('out_of_stock') }}
)
select 
    event_date
    , upc
    , hours_recouped
    , expected_out_of_stock_duration
from oos_events
where expected_out_of_stock_duration<0 
