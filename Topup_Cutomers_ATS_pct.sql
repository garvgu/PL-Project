WITH
params AS (
  SELECT
    CURRENT_TIMESTAMP() AS now_ts,
    timestamp(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), MONTH)) AS current_month_start,
    timestamp(TIMESTAMP_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH)) AS last_month_start,
    timestamp(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)) AS same_day_last_month,
    TIMESTAMP_SUB(timestamp(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), MONTH)), INTERVAL 1 SECOND) AS last_month_end
),
base_data AS (
  SELECT
    la.loan_product_id,
    TIMESTAMP(la.date_created) AS date_created,
    TIMESTAMP(la.disbursal_date) AS disbursal_date,
    TIMESTAMP_TRUNC(la.date_created, MONTH) AS created_month,
    TIMESTAMP_TRUNC(la.disbursal_date, MONTH) AS disbursed_month,
    la.loan_amount,

    -- Programme classification
    CASE
      WHEN (la.loan_product_id IN ('1','5') OR la.loan_product_id IS NULL) 
           AND la.app_name IN ('com.whizdm.moneyview.loans','com.ios.moneyview')
        THEN '01.MVL NEW + RETURN'
      WHEN la.loan_product_id = '6' 
           AND la.app_name IN ('com.whizdm.moneyview.loans','com.ios.moneyview')
        THEN '02.MVL REPEAT'
      WHEN la.loan_product_id = '6' 
           AND la.app_name = 'com.whizdm.moneyview.loans.topup'
        THEN '05.TOPUP'
      WHEN (la.loan_product_id IN ('1','5') OR la.loan_product_id IS NULL) 
        THEN '03.DP NEW'
      WHEN la.loan_product_id = '6' 
        THEN '04.DP REPEAT'
      ELSE '06.Others'
    END AS programme,

    -- Ticket size classification
    CASE
      WHEN la.loan_tenure < 6 THEN 'Mini'
      WHEN la.loan_tenure = 6 THEN 'Micro'
      WHEN la.loan_tenure > 6 THEN 'Regular'
    END AS ticket_size

  FROM mv-dw-wi.lending.loan_application la
  WHERE la.date_created >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 150 DAY)
),
aggregated AS (
  SELECT
    bd.programme,
    bd.ticket_size,
    bd.date_created,
    bd.disbursal_date,
    COUNT(*) AS count,
    SUM(CASE WHEN bd.disbursed_month IS NOT NULL THEN bd.loan_amount ELSE 0 END) AS amount
  FROM base_data bd
  GROUP BY 1,2,3,4
),
pivoted AS (
  SELECT
    ag.programme,
    ag.ticket_size,

    -- STARTS
    SUM(CASE WHEN ag.date_created >= p.last_month_start 
                  AND ag.date_created <= p.same_day_last_month
             THEN ag.count ELSE 0 END) AS last_starts,
    SUM(CASE WHEN ag.date_created >= p.current_month_start 
                  AND ag.date_created <= p.now_ts 
             THEN ag.count ELSE 0 END) AS current_starts,

    -- DISBURSED
    SUM(CASE WHEN ag.disbursal_date >= p.last_month_start 
                  AND ag.disbursal_date <= p.same_day_last_month
             THEN ag.count ELSE 0 END) AS last_disbursed,
    SUM(CASE WHEN ag.disbursal_date >= p.current_month_start 
                  AND ag.disbursal_date <= p.now_ts 
             THEN ag.count ELSE 0 END) AS current_disbursed,

    -- DISBURSED AMOUNT
    SUM(CASE WHEN ag.disbursal_date >= p.last_month_start 
                  AND ag.disbursal_date <= p.same_day_last_month
             THEN ag.amount ELSE 0 END) AS last_disbursed_amt,
    SUM(CASE WHEN ag.disbursal_date >= p.current_month_start 
                  AND ag.disbursal_date <= p.now_ts 
             THEN ag.amount ELSE 0 END) AS current_disbursed_amt

  FROM aggregated ag
  CROSS JOIN params p
  GROUP BY 1,2
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
WHERE programme = '05.TOPUP'
ORDER BY programme ASC, ticket_size ASC;
