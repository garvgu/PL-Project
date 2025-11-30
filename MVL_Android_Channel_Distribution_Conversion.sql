WITH
params AS (
  SELECT
    CURRENT_TIMESTAMP() AS now_ts,
    TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), MONTH) AS current_month_start,
    TIMESTAMP_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH) AS last_month_start,
    TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)) AS same_day_last_month,
    TIMESTAMP_SUB(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), MONTH), INTERVAL 1 SECOND) AS last_month_end
),

base_data AS (
  SELECT
    la.loan_product_id,
    la.date_created,
    la.disbursal_date,
    la.loan_amount,

    -- inline campaign_type
    COALESCE(LOWER(ia.campaign_type), 'organic') AS campaign_type,

    CASE
      WHEN (la.loan_product_id IN ('1','5') OR la.loan_product_id IS NULL) 
           AND la.app_name = 'com.whizdm.moneyview.loans' 
           THEN '01.MVL NEW + RETURN'
      WHEN la.loan_product_id = '6' 
           AND la.app_name = 'com.whizdm.moneyview.loans' 
           THEN '02.MVL REPEAT'
      WHEN la.loan_product_id = '6' 
           AND la.app_name = 'com.whizdm.moneyview.loans.topup' 
           THEN '05.TOPUP'
      WHEN (la.loan_product_id IN ('1','5') OR la.loan_product_id IS NULL) 
           THEN '03.DP NEW'
      WHEN la.loan_product_id = '6' 
           THEN '04.DP REPEAT'
      ELSE '06.Others'
    END AS programme

  FROM `mv-dw-wi.lending.loan_application` la
  LEFT JOIN `mv-dw-wi.uis_whizdb.app_user_history` auh
    ON la.user_id_ref = auh.user_id_ref
   AND la.date_created > auh.date_created - INTERVAL 1 HOUR
  LEFT JOIN `mv-dw-wi.cp_analytics.appsflyer_install_event` ia
    ON auh.attribution_id = ia.attribution_id
   AND auh.date_created > ia.date_created - INTERVAL 1 HOUR
  WHERE la.date_created >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 150 DAY)
    AND auh.date_created >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 150 DAY)
    AND ia.date_created >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 150 DAY)
),

aggregated AS (
  SELECT
    bd.loan_product_id,
    bd.programme,
    bd.campaign_type,
    DATE(bd.date_created) AS created_date,
    DATE(bd.disbursal_date) AS disbursal_date,
    COUNT(*) AS count,
    SUM(CASE WHEN bd.disbursal_date IS NOT NULL THEN bd.loan_amount ELSE 0 END) AS amount
  FROM base_data bd
  GROUP BY 1, 2, 3, 4, 5
),

pivoted AS (
  SELECT
    programme,
    campaign_type,

    -- STARTS
    SUM(CASE 
      WHEN ag.created_date >= DATE(p.last_month_start) AND ag.created_date <= DATE(p.same_day_last_month)
      THEN ag.count ELSE 0 END) AS last_starts,

    SUM(CASE 
      WHEN ag.created_date >= DATE(p.current_month_start) AND ag.created_date <= DATE(p.now_ts)
      THEN ag.count ELSE 0 END) AS current_starts,

    -- DISBURSED
    SUM(CASE 
      WHEN ag.disbursal_date >= DATE(p.last_month_start) AND ag.disbursal_date <= DATE(p.same_day_last_month)
      THEN ag.count ELSE 0 END) AS last_disbursed,

    SUM(CASE 
      WHEN ag.disbursal_date >= DATE(p.current_month_start) AND ag.disbursal_date <= DATE(p.now_ts)
      THEN ag.count ELSE 0 END) AS current_disbursed,

    -- DISBURSED AMOUNT
    SUM(CASE 
      WHEN ag.disbursal_date >= DATE(p.last_month_start) AND ag.disbursal_date <= DATE(p.same_day_last_month) 
      THEN ag.amount ELSE 0 END) AS last_disbursed_amt,

    SUM(CASE 
      WHEN ag.disbursal_date >= DATE(p.current_month_start) AND ag.disbursal_date <= DATE(p.now_ts) 
      THEN ag.amount ELSE 0 END) AS current_disbursed_amt

  FROM aggregated ag
  CROSS JOIN params p
  GROUP BY 1, 2
),

final AS (
  SELECT
    programme,
    campaign_type,
    last_starts,
    current_starts,
    last_disbursed,
    current_disbursed,
    last_disbursed_amt,
    current_disbursed_amt,

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

SELECT *
FROM final
WHERE programme = '01.MVL NEW + RETURN'
ORDER BY programme ASC, campaign_type ASC;
