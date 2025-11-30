WITH
params AS (
  SELECT
    CURRENT_TIMESTAMP() AS now_ts,
    TIMESTAMP(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), MONTH)) AS current_month_start,
    TIMESTAMP(TIMESTAMP_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH)) AS last_month_start,
    TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)) AS same_day_last_month,
    TIMESTAMP_SUB(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), MONTH), INTERVAL 1 SECOND) AS last_month_end
),
lamr1 AS (
  SELECT loan_application_id_ref
  FROM mv-dw-wi.lending.loan_application_metadata
  WHERE DATE(date_created) >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
    AND UPPER(tag_name) IN (
      'PREMIUMLOWNGROWREPEAT',
      'REGULARLOWNGROWREPEAT',
      'LOW_AND_GROW_PC',
      'LOW_AND_GROW'
    )
),

-- Pre-filtered metadata for Starter
lamr2 AS (
  SELECT loan_application_id_ref
  FROM mv-dw-wi.lending.loan_application_metadata
  WHERE DATE(date_created) >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
    AND UPPER(tag_name) IN ('STARTER_REPEAT', 'STARTER_RELAXED_BUREAU')
),
base_data AS (
  SELECT
    loan_product_id,
    TIMESTAMP(date_created) AS date_created,
    TIMESTAMP(disbursal_date) AS disbursal_date,
    TIMESTAMP_TRUNC(date_created, MONTH) AS created_month,
    TIMESTAMP_TRUNC(disbursal_date, MONTH) AS disbursed_month,
    loan_amount,

    -- programme logic
    CASE
     
      WHEN loan_product_id = '6' 
           AND app_name IN ('com.whizdm.moneyview.loans','com.ios.moneyview') 
        THEN '02.MVL REPEAT'
     
     
      WHEN loan_product_id = '6' 
        THEN '04.DP REPEAT'
      ELSE '06.Others'
    END AS programme,

    -- partner bucket logic
     CASE
      WHEN lamr1.loan_application_id_ref IS NOT NULL
           AND la.loan_product_id = '6'
           AND la.app_name <> 'com.whizdm.moneyview.loans.topup'
        THEN 'LowNGrow'
      WHEN lamr2.loan_application_id_ref IS NOT NULL
           AND la.loan_product_id = '6'
           AND la.app_name <> 'com.whizdm.moneyview.loans.topup'
        THEN 'Starter'
      WHEN la.loan_product_id = '6'
           AND la.app_name <> 'com.whizdm.moneyview.loans.topup'
        THEN 'Regular'
      ELSE 'Others'
    end as repeat_bucket
  FROM `mv-dw-wi.lending.loan_application` la
  LEFT JOIN lamr1 ON lamr1.loan_application_id_ref = la.id
  LEFT JOIN lamr2 ON lamr2.loan_application_id_ref = la.id
  WHERE DATE(la.date_created) >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
     OR DATE(la.disbursal_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
),
  

aggregated AS (
  SELECT
    bd.loan_product_id,
    bd.date_created,
    bd.programme,
    bd.repeat_bucket,
    bd.disbursal_date,
    COUNT(*) AS count,
    SUM(CASE WHEN bd.disbursed_month IS NOT NULL THEN bd.loan_amount ELSE 0 END) AS amount
  FROM base_data bd
  GROUP BY 1, 2, 3, 4, 5
),

pivoted AS (
  SELECT
    programme,
    repeat_bucket,

    -- STARTS
    SUM(CASE 
      WHEN ag.date_created >= p.last_month_start AND ag.date_created <= p.same_day_last_month 
      THEN ag.count ELSE 0 END) AS last_starts,

    SUM(CASE 
      WHEN ag.date_created >= p.current_month_start AND ag.date_created <= p.now_ts 
      THEN ag.count ELSE 0 END) AS current_starts,

    -- DISBURSED
    SUM(CASE 
      WHEN ag.disbursal_date >= p.last_month_start AND ag.disbursal_date <= p.same_day_last_month 
      THEN ag.count ELSE 0 END) AS last_disbursed,

    SUM(CASE 
      WHEN ag.disbursal_date >= p.current_month_start AND ag.disbursal_date <= p.now_ts 
      THEN ag.count ELSE 0 END) AS current_disbursed,

    -- DISBURSED AMOUNT
    SUM(CASE 
      WHEN ag.disbursal_date >= p.last_month_start AND ag.disbursal_date <= p.same_day_last_month
      THEN ag.amount ELSE 0 END) AS last_disbursed_amt,

    SUM(CASE 
      WHEN ag.disbursal_date >= p.current_month_start AND ag.disbursal_date <= p.now_ts 
      THEN ag.amount ELSE 0 END) AS current_disbursed_amt

  FROM aggregated ag
  CROSS JOIN params p
  GROUP BY 1, 2
),

final AS (
  SELECT *,
    ROUND((current_starts - last_starts) / NULLIF(last_starts, 0) * 100, 2) AS change_in_starts_pct,
    ROUND(last_disbursed / NULLIF(last_starts, 0), 4) AS last_conversion,
    ROUND(current_disbursed / NULLIF(current_starts, 0), 4) AS current_conversion,
    ROUND(
      (
        (current_disbursed / NULLIF(current_starts, 0)) -
        (last_disbursed / NULLIF(last_starts, 0))
      ) / NULLIF((last_disbursed / NULLIF(last_starts, 0)), 0) * 100, 2
    ) AS change_in_conversion_pct,
    ROUND(last_disbursed_amt / NULLIF(last_disbursed, 0), 2) AS last_ats,
    ROUND(current_disbursed_amt / NULLIF(current_disbursed, 0), 2) AS current_ats,
    ROUND(
      (
        (current_disbursed_amt / NULLIF(current_disbursed, 0)) -
        (last_disbursed_amt / NULLIF(last_disbursed, 0))
      ) / NULLIF((last_disbursed_amt / NULLIF(last_disbursed, 0)), 0) * 100, 2
    ) AS change_in_ats_pct
  FROM pivoted
)

SELECT 
  
  repeat_bucket,
  last_ats,
  current_ats

  

  
FROM final
WHERE programme IN ('02.MVL REPEAT','04.DP REPEAT')
  AND repeat_bucket IN ('LowNGrow','Starter','Regular')
ORDER BY repeat_bucket ASC;
