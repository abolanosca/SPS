
DELETE FROM {{ params.ds_stg }}.sps_chnnl_summary
WHERE sku IN
    (
        SELECT DISTINCT sku FROM
        (
            SELECT sku,week_ending_date, COUNT(1) FROM
            (
                SELECT DISTINCT sku, vendor_id, week_ending_date
                FROM {{ params.ds_stg }}.sps_chnnl_summary
            )
            GROUP BY sku, week_ending_date
            HAVING COUNT(1) > 1
        )
    )
;
