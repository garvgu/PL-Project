WITH
params AS (
  SELECT
    CURRENT_DATE() AS now_dt,
    DATE_TRUNC(CURRENT_DATE(), MONTH) AS current_month_start,
    DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH) AS last_month_start,
    DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH) AS same_day_last_month
),

-- Scan loan_application_metadata ONCE
lamr AS (
  SELECT
    loan_application_id_ref,
    CASE
      WHEN UPPER(tag_name) IN ('PREMIUMLOWNGROWREPEAT','REGULARLOWNGROWREPEAT','LOW_AND_GROW_PC','LOW_AND_GROW')
        THEN 'LowNGrow'
      WHEN UPPER(tag_name) IN ('STARTER_REPEAT','STARTER_RELAXED_BUREAU')
        THEN 'Starter'
    END AS bucket_type
  FROM `mv-dw-wi.lending.loan_application_metadata`
  WHERE DATE(date_created) >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
    AND UPPER(tag_name) IN (
      'PREMIUMLOWNGROWREPEAT','REGULARLOWNGROWREPEAT','LOW_AND_GROW_PC','LOW_AND_GROW',
      'STARTER_REPEAT','STARTER_RELAXED_BUREAU'
    )
),

-- Split OR into two UNION ALL queries to allow partition pruning
base_data AS (
  SELECT
    la.id,
    la.loan_product_id,
    la.app_name,
    DATE(la.date_created) AS created_date,
    DATE(la.disbursal_date) AS disbursal_date,
    la.loan_amount,

    CASE
      WHEN la.loan_product_id = '6' 
           AND la.app_name IN ('com.whizdm.moneyview.loans','com.ios.moneyview')
        THEN '02.MVL REPEAT'
      WHEN la.loan_product_id = '6'
        THEN '04.DP REPEAT'
      ELSE '06.Others'
    END AS programme,

    COALESCE(lamr.bucket_type,
      CASE 
        WHEN la.loan_product_id = '6'
             AND la.app_name <> 'com.whizdm.moneyview.loans.topup'
          THEN 'Regular'
        ELSE 'Others'
      END
    ) AS repeat_bucket

  FROM `mv-dw-wi.lending.loan_application` la
  LEFT JOIN lamr ON lamr.loan_application_id_ref = la.id
  WHERE DATE(la.date_created) >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)

  UNION ALL

  SELECT
    la.id,
    la.loan_product_id,
    la.app_name,
    DATE(la.date_created) AS created_date,
    DATE(la.disbursal_date) AS disbursal_date,
    la.loan_amount,

    CASE
      WHEN la.loan_product_id = '6' 
           AND la.app_name IN ('com.whizdm.moneyview.loans','com.ios.moneyview')
        THEN '02.MVL REPEAT'
      WHEN la.loan_product_id = '6'
        THEN '04.DP REPEAT'
      ELSE '06.Others'
    END AS programme,

    COALESCE(lamr.bucket_type,
      CASE 
        WHEN la.loan_product_id = '6'
             AND la.app_name <> 'com.whizdm.moneyview.loans.topup'
          THEN 'Regular'
        ELSE 'Others'
      END
    ) AS repeat_bucket

  FROM `mv-dw-wi.lending.loan_application` la
  LEFT JOIN lamr ON lamr.loan_application_id_ref = la.id
  WHERE DATE(la.disbursal_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
),

-- Aggregate counts
aggregated AS (
  SELECT
    programme,
    repeat_bucket,
    created_date,
    disbursal_date,
    COUNT(*) AS count,
    SUM(CASE WHEN disbursal_date IS NOT NULL THEN loan_amount ELSE 0 END) AS amount
  FROM base_data
  GROUP BY 1,2,3,4
),

-- Pivot metrics
pivoted AS (
  SELECT
    ag.programme,
    ag.repeat_bucket,

    SUM(CASE 
      WHEN ag.created_date BETWEEN p.last_month_start AND p.same_day_last_month
      THEN ag.count ELSE 0 END) AS last_starts,

    SUM(CASE 
      WHEN ag.created_date BETWEEN p.current_month_start AND p.now_dt
      THEN ag.count ELSE 0 END) AS current_starts

  FROM aggregated ag
  CROSS JOIN params p
  GROUP BY 1,2
)

-- Final Output
SELECT
  repeat_bucket,
  last_starts,
  current_starts,
  ROUND((current_starts - last_starts) / NULLIF(last_starts, 0) * 100, 2) AS change_in_starts_pct
FROM pivoted
WHERE programme IN ('02.MVL REPEAT','04.DP REPEAT')
  AND repeat_bucket IN ('LowNGrow','Starter','Regular')
ORDER BY repeat_bucket ASC;
