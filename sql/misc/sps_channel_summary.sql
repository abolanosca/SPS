

CREATE OR REPLACE TABLE {{ params.ds_stg }}.sps_chnnl_summary (
    vendor_id                      INT64,
    vendor_name                    STRING,
    brand_name                     STRING,
    sku                            STRING NOT NULL,
    website_id                     INT64 NOT NULL,
    website_nm                     STRING NOT NULL,
    gross_sales                    FLOAT64,
    gross_quantity                 FLOAT64,
    gross_cogs                     FLOAT64,
    gross_gm                       FLOAT64,
    gross_markdown                 FLOAT64,
    return_sales                   FLOAT64,
    return_quantity                FLOAT64,
    return_cogs                    FLOAT64,
    return_gm                      FLOAT64,
    inventory_quantity             FLOAT64,
    inventory_cost                 FLOAT64,
    inventory_retail               FLOAT64,
    received_quantity              FLOAT64,
    received_cost                  FLOAT64,
    received_retail                FLOAT64,
    on_order_quantity              FLOAT64,
    on_order_cost                  FLOAT64,
    on_order_retail                FLOAT64,
    week_ending_date               STRING
);
