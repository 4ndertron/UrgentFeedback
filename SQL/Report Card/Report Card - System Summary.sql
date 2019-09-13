WITH SYSTEM_DAILY_STATUS AS (
    /*
     With the three table joins, the last query run took:
     2 m 34s
     (´。＿。｀)
     */
    SELECT ST.PROJECT_ID
         , P.SERVICE_STATE
         , CASE
               WHEN ST.STATUS IN ('NORMAL')
                   THEN 'NORMAL'
               WHEN ST.STATUS IN ('DATACOMM_FAILURE', 'MLE_FAILURE', 'DATA_BACKLOG', 'ALERTS')
                   THEN 'COMMUNICATION'
               WHEN ST.STATUS IN ('ARC_FAULT', 'POWER_FAILURE', 'PRODUCTION_ISSUE', 'RGM_FAILURE')
                   THEN 'GENERATION'
               ELSE 'UNKNOWN' END                                                 AS STATUS_BUCKET
         , ST.FIRST_DAY_IN_STATUS
         , IFF(ST.LAST_DAY_IN_STATUS = CURRENT_DATE, NULL, ST.LAST_DAY_IN_STATUS) AS LAST_DAY_IN_STATUS
         , ST.DAYS_IN_STATUS
    FROM FLEET.T_STATUS_TIMELINE AS ST
             LEFT OUTER JOIN RPT.T_PROJECT AS P
                             ON P.PROJECT_ID = ST.PROJECT_ID
    ORDER BY ST.PROJECT_ID DESC
           , ST.FIRST_DAY_IN_STATUS
)

   , SYSTEM_DAY_WIP AS (
    SELECT D.DT
         , S.STATUS_BUCKET
         , S.SERVICE_STATE
         , COUNT(CASE
                     WHEN S.FIRST_DAY_IN_STATUS <= D.DT AND
                          (S.LAST_DAY_IN_STATUS > D.DT OR S.LAST_DAY_IN_STATUS IS NULL)
                         THEN 1 END) AS ACTIVE_SYSTEM_STATUS
    FROM RPT.T_DATES AS D
       , SYSTEM_DAILY_STATUS S
    WHERE D.DT BETWEEN
              DATE_TRUNC('Y', DATEADD('Y', -1, CURRENT_DATE)) AND
              CURRENT_DATE
    GROUP BY S.STATUS_BUCKET
           , D.DT
           , S.SERVICE_STATE
    ORDER BY S.SERVICE_STATE
           , S.STATUS_BUCKET
           , D.DT
)

   , SYSTEM_MONTH_WIP AS (
    SELECT *
    FROM SYSTEM_DAY_WIP
    WHERE DAY(DT) = DAY(CURRENT_DATE)
)

   , SYSTEM_ION AS (
       /*
        Last run:
        1 m 17 s
        (#｀-_ゝ-)
        */
    SELECT DATE_TRUNC('MM', TO_DATE(D.DT)) + DAY(CURRENT_DATE) - 1 AS MONTH
         , YEAR(MONTH)                                             AS YEAR
         , S.STATUS_BUCKET
         , S.SERVICE_STATE
         , COUNT(CASE
                     WHEN S.FIRST_DAY_IN_STATUS = D.DT
                         THEN 1 END)                               AS SYSTEM_INFLOW
         , COUNT(CASE
                     WHEN S.LAST_DAY_IN_STATUS = D.DT
                         THEN 1 END)                               AS SYSTEM_OUTFLOW
         , SYSTEM_INFLOW - SYSTEM_OUTFLOW                          AS SYSTEM_NET
    FROM RPT.T_DATES AS D
       , SYSTEM_DAILY_STATUS S
    WHERE D.DT BETWEEN
              DATE_TRUNC('Y', DATEADD('Y', -1, CURRENT_DATE)) AND
              CURRENT_DATE
    GROUP BY S.STATUS_BUCKET
           , MONTH
           , S.SERVICE_STATE
    ORDER BY S.SERVICE_STATE
           , S.STATUS_BUCKET
           , MONTH
)

   , METRIC_MERGE AS (
    /*
     Last run time:
     33 s 435 ms
     */
    SELECT ION.MONTH
         , ION.SERVICE_STATE
         , ION.STATUS_BUCKET
         , ION.SYSTEM_INFLOW
         , ION.SYSTEM_OUTFLOW
         , ION.SYSTEM_NET
         , WIP.ACTIVE_SYSTEM_STATUS
    FROM SYSTEM_ION AS ION
             INNER JOIN SYSTEM_DAY_WIP AS WIP
                        ON WIP.DT = ION.MONTH AND
                           WIP.STATUS_BUCKET = ION.STATUS_BUCKET AND
                           WIP.SERVICE_STATE = ION.SERVICE_STATE
)

   , TEST_CTE AS (
    SELECT *
    FROM SYSTEM_ION
    ORDER BY MONTH DESC
    LIMIT 100
)

SELECT *
FROM METRIC_MERGE