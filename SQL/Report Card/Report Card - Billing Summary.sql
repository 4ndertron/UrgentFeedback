WITH BILLING_TABLE AS (
    SELECT B.PROJECT_ID
         , P.SERVICE_STATE
         , TO_DATE(B.METER_READ_DATE) AS METER_READ_DATE
         , B.BILLED_EST_FLAG
         , B.ZERO_BILLED_FLAG
         , IFF(B.ZERO_BILLED_FLAG = FALSE AND
               B.BILLED_EST_FLAG = FALSE,
               TRUE, FALSE)           AS ACTUAL_BILLED_FLAG
         , CASE
               WHEN ACTUAL_BILLED_FLAG = TRUE
                   THEN 'Actual'
               WHEN B.ZERO_BILLED_FLAG = TRUE
                   THEN 'Zero'
               WHEN B.BILLED_EST_FLAG = TRUE
                   THEN 'Estimate'
        END                           AS BILLING_BUCKET
    FROM BILLING.T_MONTHLY_BILLING_BY_SYSTEM AS B
             LEFT OUTER JOIN RPT.T_PROJECT AS P
                             ON P.PROJECT_ID = B.PROJECT_ID
)

   , BILLING_WIP AS (
    SELECT D.DT
         , YEAR(D.DT)                                                       AS YEAR
         , B.SERVICE_STATE
         , B.BILLING_BUCKET
         , SUM(COUNT(CASE
                         WHEN B.METER_READ_DATE = D.DT
                             THEN 1 END)) OVER
                   (PARTITION BY B.SERVICE_STATE, B.BILLING_BUCKET
                   ORDER BY D.DT ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING) AS BILLING_VOLUME
    FROM RPT.T_DATES AS D
       , BILLING_TABLE AS B
    WHERE D.DT BETWEEN
              DATE_TRUNC('Y', DATEADD('Y', -1, CURRENT_DATE)) AND
              CURRENT_DATE
    GROUP BY D.DT
           , B.BILLING_BUCKET
           , B.SERVICE_STATE
    ORDER BY B.BILLING_BUCKET
           , B.SERVICE_STATE
           , D.DT
)

   , TEST_CTE AS (
    SELECT *
    FROM BILLING_TABLE AS BT
)

   , METRIC_MERGE AS (
    SELECT *
    FROM BILLING_WIP
    WHERE DAY(DT) = DAY(CURRENT_DATE)
)

SELECT *
FROM METRIC_MERGE