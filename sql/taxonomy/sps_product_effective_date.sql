create or replace table {{ params.ds_stg }}.products_scd_eff_date as

select b.END_OF_WEEK, b.YEAR_ID, sku, max(effective_date) as effective_date
from {{ params.ds_elcap }}.vi_sb_products_scd_no_loyalty_sku a
inner join {{ params.ds_elcap }}.dim_dates b
on date(b.calendar_date) >= effective_date and date(b.calendar_date)  < expiration_date
where date(b.calendar_date) = (select date(max(a.calendar_date)) from {{ params.ds_elcap }}.dim_dates a INNER JOIN
                                                                        {{ params.ds_stg }}.sps_week_ending_date b
                                                                            ON a.fiscal_yr_no=b.fiscal_yr_no and a.fiscal_wk_no=b.fiscal_wk_no)
group by 1,2,3 order by 1;

CREATE  or replace TABLE {{ params.ds_stg }}.sps_chnnl_upc AS
SELECT distinct
    dim_products.sku  AS sku,
	dim_products_scd.effective_date as effective_date,
    coalesce(dim_products_scd.upc, dim_products.upc) AS upc
FROM
{{ params.ds_elcap }}.vi_nested_sb_customer_transactions
   AS Transactions_Nested
   LEFT JOIN unnest(details) as nested_sb_customer_transactions_daily__details


LEFT JOIN
      {{ params.ds_elcap }}.vi_sb_products_no_loyalty_sku
      AS dim_products ON nested_sb_customer_transactions_daily__details.sku = dim_products.sku
join {{ params.ds_stg }}.sps_chnnl_summary chn on chn.sku=dim_products.sku
JOIN
(select eff.effective_date as effective_date,upc,eff.sku from
      {{ params.ds_elcap }}.vi_sb_products_scd_no_loyalty_sku scd join elcap_stg_dev.products_scd_eff_date eff on  (eff.sku=scd.sku and scd.effective_date=eff.effective_date))
      AS dim_products_scd ON  nested_sb_customer_transactions_daily__details.sku = dim_products_scd.sku
                and  (DATE(Transactions_Nested.sb_event_date , 'America/Denver')) = dim_products_scd.effective_date

LEFT JOIN {{ params.ds_elcap }}.dim_dates  AS partition_dim_dates ON (DATE(Transactions_Nested.sb_event_date , 'America/Denver')) = (DATE(partition_dim_dates.calendar_date , 'America/Denver'))

where
DATE(sb_event_date) >= "2001-01-01" and
DATE(Transactions_Nested.sb_event_date , 'America/Denver')>=(select distinct DATE(a.begin_of_week , 'America/Denver')
                                                             from  {{ params.ds_elcap }}.dim_dates a INNER JOIN
                                                                        {{ params.ds_stg }}.sps_week_ending_date b
                                                                            ON a.fiscal_yr_no=b.fiscal_yr_no and a.fiscal_wk_no=b.fiscal_wk_no) and

DATE(Transactions_Nested.sb_event_date , 'America/Denver')<=(select distinct DATE(a.end_of_week , 'America/Denver')
                                                             from {{ params.ds_elcap }}.dim_dates a INNER JOIN
                                                                        {{ params.ds_stg }}.sps_week_ending_date b
                                                                            ON a.fiscal_yr_no=b.fiscal_yr_no and a.fiscal_wk_no=b.fiscal_wk_no) and
((nested_sb_customer_transactions_daily__details.sku ) <> 'BCCZ2F1-ONECOL-ONESIZ' OR (nested_sb_customer_transactions_daily__details.sku ) IS NULL)
and partition_dim_dates.fiscal_yr_no = (select fiscal_yr_no from {{ params.ds_stg }}.sps_week_ending_date)
and partition_dim_dates.fiscal_wk_no = (select fiscal_wk_no from {{ params.ds_stg }}.sps_week_ending_date)
GROUP BY
    1,
    2,
	3;