with stage_data as 
    (
        select distinct
            event_date
            , cast(upc as string) as upc
            , COUNT(*) Over (partition by event_date, upc) as cnt
            , dsd
            , workable
            , worked
            , cast(hours_recouped as float64) as hours_recouped
            , cast(out_of_stock_duration as float64) as out_of_stock_duration            
            , case 
                when contains_substr(expected_workable_loss_dollars, "-") then 0.0
                else cast(expected_workable_loss_dollars as float64) 
                end as expected_workable_loss_dollars
            , cast(expected_loss_dollars as float64) as expected_loss_dollars
            , cast(sales_per_hour_dollars as float64) as sales_per_hour_dollars
            , case
                when regexp_contains(sales_recouped_dollars, r'[()-]') then 0.0
                else cast(sales_recouped_dollars as float64) 
                end as sales_recouped_dollars

        from {{ ref("out_of_stock") }}
    )
select *
    -- cnt
    -- , event_date
    -- , upc
    -- , dsd
    -- , workable
    -- , worked
    -- , expected_loss_dollars
    -- , expected_workable_loss_dollars
    -- , sales_per_hour_dollars
    -- , sales_recouped_dollars
    -- , hours_recouped
    -- , out_of_stock_duration
from stage_data
order by event_date, upc

-- code to make sure all data types are correct
-- removing the column expected_out_of_stock_duration
