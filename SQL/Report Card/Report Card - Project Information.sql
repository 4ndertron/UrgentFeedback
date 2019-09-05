WITH PROJECT_DAILY_STATUS AS (
    /*
     With the three table joins, the last query run took:
     2 m 34s
     (´。＿。｀)
     */
    SELECT ST.PROJECT_ID
         , P.SERVICE_STATE
         , NVL(ST.STATUS, 'UNKNOWN')                                                            AS STATUS
         , CASE
               WHEN ST.STATUS IN ('NORMAL')
                   THEN 'NORMAL'
               WHEN ST.STATUS IN ('DATACOMM_FAILURE', 'MLE_FAILURE', 'DATA_BACKLOG', 'ALERTS')
                   THEN 'COMMUNICATION'
               WHEN ST.STATUS IN ('ARC_FAULT', 'POWER_FAILURE', 'PRODUCTION_ISSUE', 'RGM_FAILURE')
                   THEN 'GENERATION'
               ELSE 'UNKNOWN' END                                                               AS STATUS_BUCKET
         , ST.FIRST_DAY_IN_STATUS
         , ST.LAST_DAY_IN_STATUS
         , ST.DAYS_IN_STATUS
         , D.DT
         , ROW_NUMBER() OVER (PARTITION BY ST.PROJECT_ID, ST.FIRST_DAY_IN_STATUS ORDER BY D.DT) AS STATUS_AGE
    FROM FLEET.T_STATUS_TIMELINE AS ST
             INNER JOIN RPT.T_DATES AS D
                        ON D.DT BETWEEN ST.FIRST_DAY_IN_STATUS AND ST.LAST_DAY_IN_STATUS
             LEFT OUTER JOIN RPT.T_PROJECT AS P
                             ON P.PROJECT_ID = ST.PROJECT_ID
    ORDER BY ST.PROJECT_ID DESC
           , D.DT
)

   , BILLING AS (
    SELECT B.PROJECT_ID
         , DATE_TRUNC('MM', TO_DATE(B.METER_READ_DATE)) + DAY(CURRENT_DATE) - 1 AS METER_READ_MONTH
         , B.BILLED_EST_FLAG
         , B.ZERO_BILLED_FLAG
         , IFF(B.ZERO_BILLED_FLAG = FALSE AND
               B.BILLED_EST_FLAG = FALSE,
               TRUE, FALSE)                                                     AS ACTUAL_BILLED_FLAG
    FROM BILLING.T_MONTHLY_BILLING_BY_SYSTEM AS B
)

   , PROJECT_COMBO AS (
    /*
     Even with obtaining results from the larger tables, this query last ran for:
     31 s 975 ms
     o(*^＠^*)o
     */
    SELECT B.METER_READ_MONTH
         , B.PROJECT_ID
         , S.SERVICE_STATE
         , CASE
               WHEN B.ACTUAL_BILLED_FLAG = TRUE
                   THEN 'Actual'
               WHEN B.ZERO_BILLED_FLAG = TRUE
                   THEN 'Zero'
               WHEN B.BILLED_EST_FLAG = TRUE
                   THEN 'Estimate'
        END AS BILLING_BUCKET
         , S.STATUS_BUCKET
--          , S.STATUS_AGE
    FROM BILLING AS B
       , PROJECT_DAILY_STATUS AS S
    WHERE S.DT = B.METER_READ_MONTH
      AND S.PROJECT_ID = B.PROJECT_ID
)

   , PROJECT_METRICS AS (
       /*
        I lost focus on the scope of the metrics.
        TODO: Create separate WIP and ION Tables to represent the system status and billing buckets.
        */
    SELECT METER_READ_MONTH
         , SERVICE_STATE
         , STATUS_BUCKET
         , BILLING_BUCKET
         , COUNT(STATUS_BUCKET)  STATUS_BUCKET_COUNT
         , COUNT(BILLING_BUCKET) BILLING_BUCKET_COUNT
    FROM PROJECT_COMBO
    GROUP BY METER_READ_MONTH
           , SERVICE_STATE
           , STATUS_BUCKET
           , BILLING_BUCKET
    ORDER BY SERVICE_STATE
           , STATUS_BUCKET
           , BILLING_BUCKET
           , METER_READ_MONTH
)

SELECT *
FROM PROJECT_METRICS