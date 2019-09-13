WITH SYSTEM_DAILY_STATUS AS (
    /*
     With the three table joins, the last query run took:
     2 m 34s
     (´。＿。｀)
     */
    SELECT ST.PROJECT_ID
         , P.PROJECT_NAME                                                         AS PROJECT_NUMBER
         , P.SERVICE_NAME                                                         AS SERVICE_NUMBER
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

   , MAIN_TEST_1 AS (
    SELECT D.DT        AS MONTH
         , YEAR(MONTH) AS YEAR
         , S.*
    FROM RPT.T_DATES AS D
             INNER JOIN SYSTEM_DAILY_STATUS AS S
                        ON S.FIRST_DAY_IN_STATUS <= D.DT AND
                           NVL(S.LAST_DAY_IN_STATUS, CURRENT_DATE + 1) > D.DT
    WHERE D.DT BETWEEN
        DATE_TRUNC('Y', DATEADD('Y', -1, CURRENT_DATE)) AND
        CURRENT_DATE
      AND DAY(D.DT) = DAY(CURRENT_DATE)
)

   , TEST_CTE AS (
    SELECT *
    FROM SYSTEM_DAILY_STATUS
)

SELECT *
FROM SYSTEM_DAILY_STATUS