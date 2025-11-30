WITH
params AS (
  SELECT
    CURRENT_TIMESTAMP() AS now_ts,
    TIMESTAMP(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), MONTH)) AS current_month_start,
    TIMESTAMP(TIMESTAMP_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH)) AS last_month_start,
    TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)) AS same_day_last_month,
    TIMESTAMP_SUB(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), MONTH), INTERVAL 1 SECOND) AS last_month_end
),
base_data AS (
  SELECT
    loan_product_id,
    timestamp(date_created) AS date_created,
    timestamp(disbursal_date) AS disbursal_date,
    TIMESTAMP_TRUNC(date_created, MONTH) AS created_month,
    TIMESTAMP_TRUNC(disbursal_date, MONTH) AS disbursed_month,
    loan_amount,
    CASE
      WHEN (loan_product_id IN ('1','5') OR loan_product_id IS NULL) AND app_name in ('com.whizdm.moneyview.loans','com.ios.moneyview') THEN '01.MVL NEW + RETURN'
      WHEN loan_product_id = '6' AND app_name in ('com.whizdm.moneyview.loans','com.ios.moneyview') THEN '02.MVL REPEAT'
      WHEN loan_product_id = '6' AND app_name = 'com.whizdm.moneyview.loans.topup' THEN '05.TOPUP'
      WHEN (loan_product_id IN ('1','5') OR loan_product_id IS NULL) THEN '03.DP NEW'
      WHEN loan_product_id = '6' THEN '04.DP REPEAT'
      ELSE '06.Others'
    END AS programme,
    CASE
      WHEN app_name LIKE '%.ios%' THEN 'iOS'
      ELSE 'Android'
    END AS platform
  FROM mv-dw-wi.lending.loan_application
  WHERE date_created >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 150 DAY)
),
aggregated AS (
  SELECT
    bd.loan_product_id,
    bd.date_created,
    bd.platform,
    bd.programme,
    bd.disbursal_date,
    COUNT(*) AS count,
    SUM(CASE WHEN bd.disbursed_month IS NOT NULL THEN bd.loan_amount ELSE 0 END) AS amount
  FROM base_data bd
  GROUP BY 1, 2, 3, 4, 5
),
pivoted AS (
  SELECT
    programme,
    platform,

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
SELECT *
FROM final
WHERE programme in('01.MVL NEW + RETURN')
ORDER BY programme ASC, platform ASC;
