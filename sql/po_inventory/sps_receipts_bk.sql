
create or replace table {{ params.ds_stg }}.sps_chnnl_receipts AS

SELECT
  replace(cast( cast(dates.fiscal_end_of_wk as date) as string),'-','') as WeekEndingDate,
  dim_vendors.vendor_id AS vendor_id,
  dim_vendors.vendor_name AS vendor_name,
  dim_products_transactions.brandname AS brand_name,
  dim_products_transactions.sku AS SKU,
  15 as website_id,
  '15-DC/Warehouse' as website_nm,
  COALESCE(SUM(base.po_received_quantity ), 0) AS received_quantity,
  COALESCE(SUM(base.po_received_cost ), 0) AS received_cost,
  COALESCE(SUM(base.po_received_retail_value ), 0) AS received_retail
FROM (
  SELECT
    (TIMESTAMP_TRUNC(Transactions_Nested.sb_event_date, DAY, 'America/Denver')) AS sb_event_date_date,
    nested_sb_customer_transactions_daily__details.sku AS sku,
    (TIMESTAMP_TRUNC(CASE
          WHEN Transactions_Nested.transactions.transaction_event_type = 'on order' THEN nested_sb_customer_transactions_daily__details.transaction_expected_arrival_date
      END
        , DAY, 'America/Denver')) AS purchase_expected_arrival_date_date,
    Transactions_Nested.transactions.transaction_event_type AS transaction_event_type,
    CASE
      WHEN facilities.po_category = 'Closeout' THEN 'Closeout'
      WHEN facilities.po_category = 'Close-Out' THEN 'Closeout'
      WHEN facilities.po_category = 'close-out' THEN 'Closeout'
    ELSE
    'Retail'
  END
    AS po_category,
    CASE
      WHEN facilities.po_category = 'Closeout' THEN 'Closeout'
      WHEN facilities.po_category = 'Close-Out' THEN 'Closeout'
      WHEN facilities.po_category = 'close-out' THEN 'Closeout'
      WHEN facilities.po_category = 'Retail' THEN 'Retail'
    ELSE
    'Unknown'
  END
    AS po_category_org,
    dim_purchase_order.po_season AS po_season,
    dim_purchase_order.po_year AS po_year,
    dim_purchase_order.po_type AS po_type,
    Transactions_Nested.facilities.facility AS facility,
    Transactions_Nested.site_channels.website_name AS website_name,
    COALESCE(SUM(CASE
          WHEN Transactions_Nested.transactions.transaction_event_type = 'receipts' THEN nested_sb_customer_transactions_daily__details.transaction_line_estimated_cost
        ELSE
        0
      END
        ), 0) AS po_received_cost,
    COALESCE(SUM(CASE
          WHEN Transactions_Nested.transactions.transaction_event_type = 'receipts' THEN nested_sb_customer_transactions_daily__details.transaction_line_quantity
        ELSE
        0
      END
        ), 0) AS po_received_quantity,
    COALESCE(SUM((CASE
            WHEN Transactions_Nested.transactions.transaction_event_type = 'receipts' THEN nested_sb_customer_transactions_daily__details.transaction_line_quantity
          ELSE
          0
        END
          )* dim_products.regular_price ), 0) AS po_received_retail_value,
    COALESCE(SUM(CASE
          WHEN Transactions_Nested.transactions.transaction_event_type = 'on order' THEN nested_sb_customer_transactions_daily__details.order_cost
        ELSE
        0
      END
        ), 0) AS on_orders_cost,
    COALESCE(SUM(CASE
          WHEN Transactions_Nested.transactions.transaction_event_type = 'on order' THEN nested_sb_customer_transactions_daily__details.order_quantity
        ELSE
        0
      END
        ), 0) AS on_orders_quantity,
    COALESCE(SUM(nested_sb_customer_transactions_daily__details.order_cost ), 0) AS order_cost,
    COALESCE(SUM(CASE
          WHEN Transactions_Nested.transactions.transaction_event_type = 'on order current' THEN nested_sb_customer_transactions_daily__details.order_quantity
        ELSE
        0
      END
        ), 0) AS current_on_orders_quantity,
    COALESCE(SUM(CASE
          WHEN Transactions_Nested.transactions.transaction_event_type = 'on order current' THEN nested_sb_customer_transactions_daily__details.order_cost
        ELSE
        0
      END
        ), 0) AS current_on_orders_cost,
    COALESCE(SUM(nested_sb_customer_transactions_daily__details.order_quantity ), 0) AS order_quantity,
    COALESCE(SUM((CASE
            WHEN Transactions_Nested.transactions.transaction_event_type = 'on order' THEN nested_sb_customer_transactions_daily__details.order_quantity
          ELSE
          0
        END
          )* dim_products.regular_price ), 0) AS on_order_retail_value
  FROM
    {{ params.ds_elcap }}.vi_nested_sb_customer_transactions AS Transactions_Nested
  LEFT JOIN
    UNNEST(details) AS nested_sb_customer_transactions_daily__details
  LEFT JOIN
     {{ params.ds_elcap }}.vi_sb_products_no_loyalty_sku AS dim_products
  ON
    nested_sb_customer_transactions_daily__details.sku = dim_products.sku
  LEFT JOIN
    {{ params.ds_elcap }}.dim_dates AS dates
  ON
    (DATE(Transactions_Nested.sb_event_date, 'America/Denver')) = (DATE(dates.calendar_date, 'America/Denver'))
  LEFT JOIN
    {{ params.ds_elcap }}.dim_purchase_order AS dim_purchase_order
  ON
    Transactions_Nested.transactions.id = dim_purchase_order.po_number
    AND nested_sb_customer_transactions_daily__details.sku = dim_purchase_order.sku
    AND nested_sb_customer_transactions_daily__details.transaction_line_item_id = dim_purchase_order.po_purchase_order_item_id
  WHERE
    Transactions_Nested.sb_event_date >= (TIMESTAMP('2021-01-01 00:00:00', 'America/Denver'))
    AND ((nested_sb_customer_transactions_daily__details.sku ) <> 'BCCZ2F1-ONECOL-ONESIZ'
      OR (nested_sb_customer_transactions_daily__details.sku ) IS NULL)
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11) AS base
INNER JOIN
   {{ params.ds_elcap }}.dim_dates AS dates
ON
  (DATE(base.sb_event_date_date, 'America/Denver')) = (DATE(dates.calendar_date, 'America/Denver'))
INNER JOIN
   {{ params.ds_elcap }}.vi_sb_products_no_loyalty_sku AS dim_products_transactions
ON
  base.sku=dim_products_transactions.sku
inner JOIN  {{ params.ds_stg }}.sps_brand_vendor_map dim_vendors
    ON dim_products_transactions.brandid = dim_vendors.brand_id
WHERE
  DATE(base.sb_event_date_date) IN (
  SELECT
    DISTINCT DATE(END_OF_WEEK)
  FROM
      {{ params.ds_stg }}.sps_week_ending_date)
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6,
  7
  ;