with oos_events as (
    select * from {{ ref('out_of_stock') }}
)
select 
    event_date
    , upc
    , hours_recouped
    , workable
from oos_events
where workable=true and hours_recouped<0 

-- This test fails because of the line below
-- 2019-01-04
-- 4389795548
-- -0.77
-- true