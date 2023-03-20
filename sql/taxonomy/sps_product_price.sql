
CREATE or replace TABLE {{ params.ds_stg }}.sps_chnnl_products_prices AS

SELECT DISTINCT a.sku
    , a.regular_price
    , a.wholesale_price
    , case when coalesce(a.sale_price,0) <> 0 then a.sale_price else a.regular_price end current_price
   , cast(NULL as float64) as regular_price_history
   , cast(NULL as float64) as wholesale_price_history
FROM {{ params.ds_elcap }}.sb_products a
    INNER JOIN (SELECT DISTINCT sku FROM {{ params.ds_stg }}.sps_chnnl_summary) s ON a.sku = s.sku;


MERGE INTO {{ params.ds_stg }}.sps_chnnl_products_prices target using
(select b.END_OF_WEEK,
a.sku,
a.regular_price,
a.wholesale_price
from {{ params.ds_elcap }}.sb_products_scd  a
  inner join {{ params.ds_elcap }}.dim_dates  b
    on date(b.calendar_date) >= effective_date and date(b.calendar_date)  < expiration_date

where date(b.calendar_date) = (select max(end_of_week) as end_of_week from {{ params.ds_stg }}.sps_week_ending_date )
) source
ON (target.sku = source.sku)
WHEN MATCHED THEN
update set target.regular_price_history = source.regular_price
       , target.wholesale_price_history = source.wholesale_price;
