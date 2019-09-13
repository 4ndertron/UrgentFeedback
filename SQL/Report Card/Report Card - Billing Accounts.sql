WITH BILLING_TABLE AS (
    SELECT B.PROJECT_ID
         , P.SERVICE_STATE
         , DATE_TRUNC('MM', TO_DATE(B.METER_READ_DATE)) + DAY(CURRENT_DATE) - 1 AS METER_READ_MONTH
         , B.BILLED_EST_FLAG
         , B.ZERO_BILLED_FLAG
         , IFF(B.ZERO_BILLED_FLAG = FALSE AND
               B.BILLED_EST_FLAG = FALSE,
               TRUE, FALSE)                                                     AS ACTUAL_BILLED_FLAG
         , CASE
               WHEN ACTUAL_BILLED_FLAG = TRUE
                   THEN 'Actual'
               WHEN B.ZERO_BILLED_FLAG = TRUE
                   THEN 'Zero'
               WHEN B.BILLED_EST_FLAG = TRUE
                   THEN 'Estimate'
        END                                                                     AS BILLING_BUCKET
    FROM BILLING.T_MONTHLY_BILLING_BY_SYSTEM AS B
             LEFT OUTER JOIN RPT.T_PROJECT AS P
                             ON P.PROJECT_ID = B.PROJECT_ID
)

   , TEST_CTE AS (
    SELECT *
    FROM BILLING_TABLE AS BT
)

   , MAIN_TEST_1 AS (
    SELECT D.DT        AS MONTH
         , YEAR(MONTH) AS YEAR
         , BT.*
    FROM RPT.T_DATES AS D
             INNER JOIN BILLING_TABLE AS BT
                        ON BT.METER_READ_MONTH = D.DT
    WHERE D.DT BETWEEN
        DATE_TRUNC('Y', DATEADD('Y', -1, CURRENT_DATE)) AND
        CURRENT_DATE
      AND DAY(D.DT) = DAY(CURRENT_DATE)
)

SELECT *
FROM BILLING_TABLE