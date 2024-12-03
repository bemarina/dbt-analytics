with stage_data as 
    (
        select distinct
            event_date
            , upc
            , COUNT(*) Over (partition by event_date, upc) as cnt
            , dsd
            , workable
            , worked
            , out_of_stock_duration
            , hours_recouped
            , cast(sales_recouped_dollars as float64) as sales_recouped_dollars
            , cast(expected_workable_loss_dollars as float64) as expected_workable_loss_dollars
            , cast(expected_loss_dollars as float64) as expected_loss_dollars
            , cast(sales_per_hour_dollars as float64) as sales_per_hour_dollars
        from {{ ref("out_of_stock") }}
    )
select distinct
    event_date
    , upc
    , dsd
    , workable
    , worked
   
from stage_data
-- where upc = 3530100664
-- where cnt = 1
-- where expected_workable_loss_dollars = 0
-- where sales_recouped_dollars <= 0
-- where workable=false
-- order by actual_loss desc




-- not using the one that has two events for the same day 
-- 2019-01-20
-- 3530100664

-- sales recouped is always positive

-- in either case, calculate total recouped/gain per day+product (this is what
-- I'll plot)
-- calculate actual_loss


-- compare actual_loss when workable=true VS a metric when workable was false
-- , expected_loss_dollars-sales_recouped_dollars as actual_loss