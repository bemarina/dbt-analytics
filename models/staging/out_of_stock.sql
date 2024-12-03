with
    raw_data as (
        select
            date as event_date,
            upc as upc,
            dsd as dsd,
            workable as workable,
            worked as worked,
            `Out of Stock Duration` as out_of_stock_duration,
            `Expected Out of Stock Duration` as expected_out_of_stock_duration,
            -- ` Expected Lost Sales ` as expected_lost_sales,
            replace(` Expected Lost Sales `,"$","") as expected_loss_dollars,
            -- ` Expected Workable Lost Sales ` as expected_workable_lost_sales,
            replace(` Expected Workable Lost Sales `,"$","") as expected_workable_loss_dollars,
            `Hours Recouped` as hours_recouped,
            -- ` Sales Per Hour ` as sales_per_hour,
            replace(` Sales Per Hour `,"$","") as sales_per_hour_dollars,
            -- ` Recouped Sales ` as sales_recouped,
            replace(` Recouped Sales `,"$","") as sales_recouped_dollars,
        from {{ source("raw_data", "focal_systems_raw1") }}
    ),
    final as (select * from raw_data)
select *
from final
order by
    event_date,
    upc

    -- each model (sql file) will match with a table or view in db
    -- just configure in the top of the file/separate yml file how we want to
    -- materialize the model
    -- to run just this model
    -- dbt run --select recouped_sales
    -- the default materialization is a view
    -- this is going to be the first pass at the raw data
    -- now we have all the columns we wanted for the analysis
    -- add test that hours recouped is a above 0 -> this test fails: there's one row
    -- with negative hours recouped:
    -- out of stock duration was LONGER than expected
    -- not sure what negative hours recouped means: it's just one row, we'll remove it
    -- to do from Monday 12/2
    -- other singular tests: 
    
    -- is expected_workable_hours always positive? YES
    -- is expected workable 0 when workable is false? No, expected_workable_hours is never 0.
    -- can there be 2 rows for the same product on the same day? YES, but one UPC only
    
    -- plot sales_recouped_dollars per day+product 
    
    -- is sales recouped always the same as hours recouped*sales per hour?
    -- expected_lost sales is expected hours*sales_per_hour?

    -- 12 columns
    -- add audit columns (macro) !!

    -- If hours_recouped=0 then it is NOT workable!
    
