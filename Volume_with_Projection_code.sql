SELECT
  CASE
    WHEN loan_product_id IN ('1','5') AND app_name = 'com.whizdm.moneyview.loans'
      THEN '01.MVL NEW + RETURN'
    WHEN loan_product_id = '6' AND app_name = 'com.whizdm.moneyview.loans'
      THEN '02.MVL REPEAT'
    WHEN loan_product_id = '6' AND app_name = 'com.whizdm.moneyview.loans.topup'
      THEN '05.TOPUP'
    WHEN loan_product_id IN ('1','5')
      THEN '03.DP NEW'
    WHEN loan_product_id = '6'
      THEN '04.DP REPEAT'
    ELSE '06.Others'
  END AS programme,

  
  SUM(
    CASE
      WHEN DATE(disbursal_date) >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH)
       AND DATE(disbursal_date) < DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)
      THEN loan_amount
      ELSE 0
    END
  )   AS prev_month_mtd,

  SUM(
    CASE
      WHEN DATE(disbursal_date) >= DATE_TRUNC(CURRENT_DATE(), MONTH)
       AND DATE(disbursal_date) < CURRENT_DATE()
      THEN loan_amount
      ELSE 0
    END
  )   AS current_month_mtd,

 
   ROUND(
  SUM(
    CASE
      WHEN DATE(disbursal_date) >= DATE_TRUNC(CURRENT_DATE(), MONTH)
       AND DATE(disbursal_date) < CURRENT_DATE()
      THEN loan_amount
      ELSE 0
    END
  )
  * (EXTRACT(DAY FROM LAST_DAY(CURRENT_DATE())) / 
     EXTRACT(DAY FROM DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
    )
, 2) AS current_month_total,




  
  SUM(
    CASE
      WHEN DATE(disbursal_date) >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH)
       AND DATE(disbursal_date) <= LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH))
      THEN loan_amount
      ELSE 0
    END
  )  AS last_month_total

FROM `mv-dw-wi.lending.loan_application`
WHERE loan_product_id IN ('1','5','6')
  AND DATE(disbursal_date) >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH)
GROUP BY programme
ORDER BY programme ASC;
