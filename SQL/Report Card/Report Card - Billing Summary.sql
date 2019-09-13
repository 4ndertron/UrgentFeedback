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

   , BILLING_WIP AS (
    SELECT DATE_TRUNC('MM', TO_DATE(D.DT)) + DAY(CURRENT_DATE) - 1 AS MONTH
         , YEAR(MONTH)                                             AS YEAR
         , B.SERVICE_STATE
         , B.BILLING_BUCKET
         , COUNT(CASE
                     WHEN B.METER_READ_MONTH = D.DT
                         THEN 1 END)                               AS BILLING_VOLUME
    FROM RPT.T_DATES AS D
       , BILLING_TABLE AS B
    WHERE D.DT BETWEEN
        DATE_TRUNC('Y', DATEADD('Y', -1, CURRENT_DATE)) AND
        CURRENT_DATE
      AND DAY(D.DT) = DAY(CURRENT_DATE)
    GROUP BY MONTH
           , B.BILLING_BUCKET
           , B.SERVICE_STATE
    ORDER BY B.BILLING_BUCKET
           , B.SERVICE_STATE
           , MONTH
)

   , TEST_CTE AS (
    SELECT *
    FROM BILLING_TABLE AS BT
)

   , METRIC_MERGE AS (
    SELECT *
    FROM BILLING_WIP
)

SELECT *
FROM METRIC_MERGE