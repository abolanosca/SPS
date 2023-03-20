--This table can ONLY have one date per run. If you have more than one date in the same execution, the process will fail

create or replace table {{ params.ds_stg }}.sps_week_ending_date as

select   max(fiscal_wk_no) as fiscal_wk_no,
         max(fiscal_yr_no) as fiscal_yr_no,
         max(date(end_of_week,'America/Denver')) as end_of_week,
         max(date(begin_of_week,'America/Denver'))  as begin_of_week
from {{ params.ds_elcap }}.dim_dates
where date(calendar_date,'America/Denver') = current_Date('America/Denver')-{{ params.first_day }};