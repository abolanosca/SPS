CREATE or replace TABLE {{ params.ds_stg }}.sps_chnnl_first_last_receipt as
 select s.sku as Sku,
     rec.FirstReceiptDate,
     rec.LastReceiptDate,
	 Age
FROM {{ params.ds_stg }}.sps_chnnl_skus s
    INNER JOIN (
SELECT

dim_products.sku  AS Sku,
 (DATE(dim_products.first_received_date_ts , 'America/Denver')) AS FirstReceiptDate,
    (DATE(dim_products.last_receipt_date_ts , 'America/Denver')) AS LastReceiptDate,

cast((DATE_DIFF(( (DATE(current_timestamp() , 'America/Denver')) ),( (DATE(min((DATE(dim_products.first_received_date_ts , 'America/Denver'))) )) ),day))/7 as integer) as Age


FROM {{ params.ds_elcap }}.vi_nested_sb_customer_transactions
   AS Transactions_Nested
LEFT JOIN unnest(details) as nested_sb_customer_transactions_daily__details
LEFT JOIN
      {{ params.ds_elcap }}.vi_sb_products_no_loyalty_sku
      AS dim_products ON nested_sb_customer_transactions_daily__details.sku = dim_products.sku
LEFT JOIN {{ params.ds_elcap }}.dim_dates  AS partition_dim_dates ON (DATE(Transactions_Nested.sb_event_date , 'America/Denver')) = (DATE(partition_dim_dates.calendar_date , 'America/Denver'))
 where DATE(sb_event_date) > '2001-01-01'
 and ((nested_sb_customer_transactions_daily__details.sku ) <> 'BCCZ2F1-ONECOL-ONESIZ' OR (nested_sb_customer_transactions_daily__details.sku ) IS NULL)

 group by 1,2,3)rec on s.sku = rec.sku;