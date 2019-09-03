WITH PROJECTS_RAW AS (
    SELECT PROJECT_ID
         , NVL(SERVICE_STATE, '[blank]')  AS STATE_NAME
         , TO_DATE(INSTALLATION_COMPLETE) AS INSTALL_DATE
         , TO_DATE(CANCELLATION_DATE)     AS CANCELLATION_DATE
    FROM RPT.T_PROJECT
    WHERE INSTALLATION_COMPLETE IS NOT NULL
)

   , CASES_SERVICE AS (
    SELECT PR.STATE_NAME
         , PR.PROJECT_ID
         , CASE
               WHEN CA.CREATED_DATE IS NOT NULL
                   THEN 'Service' END AS CASE_BUCKET
         , TO_DATE(CA.CREATED_DATE)   AS CREATED_DATE
         , TO_DATE(CA.CLOSED_DATE)    AS CLOSED_DATE
    FROM RPT.T_CASE CA
             INNER JOIN PROJECTS_RAW AS PR
                        ON CA.PROJECT_ID = PR.PROJECT_ID
    WHERE CA.RECORD_TYPE = 'Solar - Service'
      AND CA.SOLAR_QUEUE IN ('Outbound', 'Tier II')
)

   , CASES_REMOVAL_REINSTALL AS (
    SELECT PR.STATE_NAME
         , PR.PROJECT_ID
         , CASE
               WHEN CA.CREATED_DATE IS NOT NULL
                   THEN 'Panel Removal' END AS CASE_BUCKET
         , TO_DATE(CA.CREATED_DATE)         AS CREATED_DATE
         , TO_DATE(CA.CLOSED_DATE)          AS CLOSED_DATE
    FROM RPT.T_CASE CA
             INNER JOIN PROJECTS_RAW AS PR
                        ON CA.PROJECT_ID = PR.PROJECT_ID
    WHERE CA.RECORD_TYPE = 'Solar - Panel Removal'
)

   , CASES_TROUBLESHOOTING AS (
    SELECT PR.STATE_NAME
         , PR.PROJECT_ID
         , CASE
               WHEN CA.CREATED_DATE IS NOT NULL
                   THEN 'Troubleshooting' END AS CASE_BUCKET
         , TO_DATE(CA.CREATED_DATE)           AS CREATED_DATE
         , TO_DATE(CA.CLOSED_DATE)            AS CLOSED_DATE
    FROM RPT.T_CASE CA
             INNER JOIN PROJECTS_RAW AS PR
                        ON CA.PROJECT_ID = PR.PROJECT_ID
    WHERE CA.RECORD_TYPE = 'Solar - Troubleshooting'
)

   , CASES_DAMAGE AS (
    SELECT PR.STATE_NAME
         , PR.PROJECT_ID
         , CASE
               WHEN CA.CREATED_DATE IS NOT NULL
                   THEN 'Damage' END AS CASE_BUCKET
         , TO_DATE(CA.CREATED_DATE)  AS CREATED_DATE
         , TO_DATE(CA.CLOSED_DATE)   AS CLOSED_DATE
    FROM RPT.T_CASE CA
             INNER JOIN PROJECTS_RAW AS PR
                        ON CA.PROJECT_ID = PR.PROJECT_ID
    WHERE CA.RECORD_TYPE IN ('Solar Damage Resolutions', 'Home Damage')
)

   , CASES_ESCALATION AS (
    SELECT PR.STATE_NAME
         , PR.PROJECT_ID
         , CASE
               WHEN CA.CREATED_DATE IS NOT NULL
                   AND CA.EXECUTIVE_RESOLUTIONS_ACCEPTED IS NOT NULL
                   AND CA.ORIGIN IN ('Legal', 'News Media')
                   THEN 'Legal, News Media'
               WHEN CA.CREATED_DATE IS NOT NULL
                   AND CA.EXECUTIVE_RESOLUTIONS_ACCEPTED IS NOT NULL
                   AND CA.ORIGIN IN ('BBB')
                   THEN 'BBB'
               WHEN CA.CREATED_DATE IS NOT NULL
                   AND CA.EXECUTIVE_RESOLUTIONS_ACCEPTED IS NOT NULL
                   AND CA.ORIGIN NOT IN ('BBB', 'Legal', 'News Media')
                   THEN 'ERT'
        END                         AS CASE_BUCKET
         , TO_DATE(CA.CREATED_DATE) AS CREATED_DATE
         , TO_DATE(CA.CLOSED_DATE)  AS CLOSED_DATE
    FROM RPT.T_CASE CA
             INNER JOIN PROJECTS_RAW AS PR
                        ON CA.PROJECT_ID = PR.PROJECT_ID
    WHERE CA.RECORD_TYPE = 'Solar - Customer Escalation'
      AND CA.SUBJECT NOT ILIKE '%VIP%'
      AND CA.SUBJECT NOT ILIKE '%NPS%'
      AND CA.SUBJECT NOT ILIKE '%COMP%'
)

   , CASES_DEFAULT AS (
    SELECT PR.STATE_NAME
         , PR.PROJECT_ID
         , CASE
               WHEN CA.CREATED_DATE IS NOT NULL
                   THEN 'Default' END AS CASE_BUCKET
         , TO_DATE(CA.CREATED_DATE)   AS CREATED_DATE
         , TO_DATE(CA.CLOSED_DATE)    AS CLOSED_DATE
    FROM RPT.T_CASE CA
             INNER JOIN PROJECTS_RAW AS PR
                        ON CA.PROJECT_ID = PR.PROJECT_ID
    WHERE CA.RECORD_TYPE = 'Solar - Customer Default'
)

   , CASES_OVERALL AS (
    SELECT STATE_NAME
         , PROJECT_ID
         , CASE_BUCKET
         , CREATED_DATE
         , CLOSED_DATE
         , COUNT(DISTINCT PROJECT_ID)
                 OVER (PARTITION BY MONTH(CREATED_DATE), STATE_NAME, CASE_BUCKET
                     ORDER BY CREATED_DATE
                     --                      RANGE BETWEEN
--                          UNBOUNDED PRECEDING AND
--                          CURRENT ROW
                     ) AS UNIQUE_ACCOUNTS
    FROM (
             SELECT STATE_NAME
                  , PROJECT_ID
                  , CREATED_DATE
                  , CLOSED_DATE
                  , CASE_BUCKET
             FROM CASES_SERVICE
             UNION ALL
             SELECT STATE_NAME
                  , PROJECT_ID
                  , CREATED_DATE
                  , CLOSED_DATE
                  , CASE_BUCKET
             FROM CASES_REMOVAL_REINSTALL
             UNION ALL
             SELECT STATE_NAME
                  , PROJECT_ID
                  , CREATED_DATE
                  , CLOSED_DATE
                  , CASE_BUCKET
             FROM CASES_TROUBLESHOOTING
             UNION ALL
             SELECT STATE_NAME
                  , PROJECT_ID
                  , CREATED_DATE
                  , CLOSED_DATE
                  , CASE_BUCKET
             FROM CASES_DAMAGE
             UNION ALL
             SELECT STATE_NAME
                  , PROJECT_ID
                  , CREATED_DATE
                  , CLOSED_DATE
                  , CASE_BUCKET
             FROM CASES_ESCALATION
             UNION ALL
             SELECT STATE_NAME
                  , PROJECT_ID
                  , CREATED_DATE
                  , CLOSED_DATE
                  , CASE_BUCKET
             FROM CASES_DEFAULT
         )
)

   , DAY_WIP_TABLE AS (
    SELECT D.DT
         , C.STATE_NAME
         , C.CASE_BUCKET
         , COUNT(CASE
                     WHEN C.CREATED_DATE <= D.DT AND
                          (C.CLOSED_DATE > D.DT OR
                           C.CLOSED_DATE IS NULL)
                         THEN 1 END) AS ACTIVE_CASES
    FROM CASES_OVERALL AS C
       , RPT.T_DATES AS D
    WHERE D.DT BETWEEN
              DATE_TRUNC('Y', DATEADD('Y', -1, CURRENT_DATE)) AND
              CURRENT_DATE
    GROUP BY D.DT
           , C.STATE_NAME
           , C.CASE_BUCKET
    ORDER BY C.STATE_NAME
           , C.CASE_BUCKET
           , D.DT
)

   , MONTH_WIP_TABLE AS (
    SELECT DWT.DT      AS MONTH
         , YEAR(MONTH) AS YEAR
         , DWT.STATE_NAME
         , DWT.CASE_BUCKET
         , DWT.ACTIVE_CASES
    FROM DAY_WIP_TABLE AS DWT
    WHERE DWT.DT = LAST_DAY(DWT.DT)
       OR DWT.DT = CURRENT_DATE
    ORDER BY DWT.STATE_NAME
           , DWT.CASE_BUCKET
           , MONTH
)

   , CASE_ION AS (
    SELECT LAST_DAY(D.DT)                                  AS MONTH
         , YEAR(MONTH)                                     AS YEAR
         , CO.STATE_NAME
         , CO.CASE_BUCKET
         , CO.UNIQUE_ACCOUNTS
         , COUNT(CASE WHEN CREATED_DATE = D.DT THEN 1 END) AS CASE_INFLOW
         , COUNT(CASE WHEN CLOSED_DATE = D.DT THEN 1 END)  AS CASE_OUTFLOW
         , CASE_INFLOW - CASE_OUTFLOW                      AS CASE_NET
    FROM CASES_OVERALL AS CO
       , RPT.T_DATES AS D
    WHERE D.DT BETWEEN
              DATE_TRUNC('Y', DATEADD('Y', -1, CURRENT_DATE)) AND
              CURRENT_DATE
    GROUP BY MONTH
           , CO.STATE_NAME
           , CO.CASE_BUCKET
           , CO.UNIQUE_ACCOUNTS
    ORDER BY CO.STATE_NAME
           , CO.CASE_BUCKET
           , MONTH
)

   , METRIC_MERGE AS (
    SELECT ION.MONTH
         , ION.YEAR
         , ION.STATE_NAME
         , ION.CASE_BUCKET
         , ION.CASE_INFLOW
         , ION.CASE_OUTFLOW
         , ION.CASE_NET
         , WIP.ACTIVE_CASES
    FROM CASE_ION AS ION
             INNER JOIN MONTH_WIP_TABLE AS WIP
                        ON WIP.MONTH = ION.MONTH AND
                           WIP.STATE_NAME = ION.STATE_NAME AND
                           WIP.CASE_BUCKET = ION.CASE_BUCKET AND
                           WIP.YEAR = ION.YEAR
    ORDER BY ION.STATE_NAME
           , ION.CASE_BUCKET
           , ION.MONTH
)

   , TEST_RESULTS AS (
    SELECT *
    FROM CASES_OVERALL
)

SELECT *
FROM TEST_RESULTS
;