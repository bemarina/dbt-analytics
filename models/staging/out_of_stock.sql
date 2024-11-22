with
    all_data as (

        select
            date as event_date,
            upc as upc,
            dsd as dsd,
            workable as workable,
            worked as worked,
            `Out of Stock Duration` as out_of_stock_duration,
            `Expected Out of Stock Duration` as expected_out_of_stock_duration,
            ` Expected Lost Sales ` as expected_lost_sales,
            ` Expected Workable Lost Sales ` as expected_workable_lost_sales,
            `Hours Recouped` as hours_recouped,
            ` Sales Per Hour ` as sales_per_hour,
            ` Recouped Sales ` as sales_recouped

        from {{ source("raw_data", "focal_systems_raw1") }}
        where `Hours Recouped` > 0
    ),

    final as (select * from all_data)

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
    -- write python function/macros to remove the dollar sign of the others
    
