{{ config(materialized="table") }}
with
    all_data as (

        select 
        Date as event_date,
        UPC as upc,
        DSD as dsd,
        Workable as workable,
        Worked as worked,
        `Out of Stock Duration` as out_of_stock_duration,
        `Expected Out of Stock Duration` as expected_out_of_stock_duration,
        `Hours Recouped` as hours_recouped,
        ` Sales Per Hour ` as sales_per_hour,
        ` Recouped Sales ` as sales_recouped
        from `raw-data-442223`.focal_systems.focal_systems_raw
    -- from {{  ref('my_first_dbt_model')   }}
    -- macro ref that takes as input the name of the model
    ),

    final as (select * from all_data)

select *
from
    final

    -- each model (sql file) will match with a table or view in db
    -- just configure in the top of the file/separate yml file how we want to
    -- materialize the model
    -- to run just this model
    -- dbt run --select recouped_sales
    -- the default materialization is a view
    
