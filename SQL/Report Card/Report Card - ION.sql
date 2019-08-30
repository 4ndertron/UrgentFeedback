-- v3 | Landon | 2018-10-01: As per Tyler Anderson, exclude any Escalation case with "VIP" in the subject
-- v2: Added Removal/Reinstall cases.
-- v2> Fixed overall_case_tally to use NVL for each addend
-- v2> Changed final sort to Overall Case Tally descending
-- v2> Added unique project counts; adjusted ratio calculations to use unique project count instead of case count
WITH PROJECTS_RAW AS (
    SELECT PROJECT_ID
         , NVL(SERVICE_STATE, '[blank]')  AS STATE_NAME
         , TO_DATE(INSTALLATION_COMPLETE) AS INSTALL_DATE
    FROM RPT.T_PROJECT
    WHERE INSTALLATION_COMPLETE IS NOT NULL
      AND CANCELLATION_DATE IS NULL
)

   , INSTALLS_BY_ROC AS (
    SELECT DISTINCT STATE_NAME
                  , COUNT(PROJECT_ID) OVER (PARTITION BY STATE_NAME) AS INSTALL_TALLY
                  ,
            TO_CHAR(100 * COUNT(PROJECT_ID) OVER (PARTITION BY STATE_NAME) / COUNT(PROJECT_ID) OVER (), '90.00') ||
            '%'                                                      AS INSTALL_RATIO
    FROM PROJECTS_RAW
)

   , CASES_SERVICE AS (
    SELECT PR.STATE_NAME
         , PR.PROJECT_ID
         , CASE
               WHEN CA.CREATED_DATE IS NOT NULL
                   THEN 'CX' END    AS CASE_BUCKET
         , TO_DATE(CA.CREATED_DATE) AS CREATED_DATE
         , TO_DATE(CA.CLOSED_DATE)  AS CLOSED_DATE
    FROM RPT.T_CASE CA
             INNER JOIN
         PROJECTS_RAW PR
         ON
             CA.PROJECT_ID = PR.PROJECT_ID
    WHERE CA.RECORD_TYPE = 'Solar - Service'
      AND CA.SOLAR_QUEUE IN ('Outbound', 'Tier II')
)

   , G_CASES_SERVICE AS (
    SELECT STATE_NAME
         , COUNT(STATE_NAME)          AS CASE_TALLY
         , COUNT(DISTINCT PROJECT_ID) AS PROJECT_TALLY
    FROM CASES_SERVICE
    GROUP BY STATE_NAME
)

   , CASES_REMOVAL_REINSTALL AS (
    SELECT PR.STATE_NAME
         , PR.PROJECT_ID
         , CASE
               WHEN CA.CREATED_DATE IS NOT NULL
                   AND CA.ACTUAL_UNINSTALL_DATE IS NULL
                   THEN 'Pre-Temporary Removal'
               WHEN CA.CREATED_DATE IS NOT NULL
                   AND CA.ACTUAL_UNINSTALL_DATE IS NOT NULL
                   THEN 'Post-Temporary Removal' END AS CASE_BUCKET
         , TO_DATE(CA.CREATED_DATE)                  AS CREATED_DATE
         , TO_DATE(CA.CLOSED_DATE)                   AS CLOSED_DATE
    FROM RPT.T_CASE CA
             INNER JOIN
         PROJECTS_RAW PR
         ON
             CA.PROJECT_ID = PR.PROJECT_ID
    WHERE CA.RECORD_TYPE = 'Solar - Panel Removal'
)

   , G_CASES_REMOVAL_REINSTALL AS (
    SELECT STATE_NAME
         , COUNT(STATE_NAME)          AS CASE_TALLY
         , COUNT(DISTINCT PROJECT_ID) AS PROJECT_TALLY
    FROM CASES_REMOVAL_REINSTALL
    GROUP BY STATE_NAME
)

   , CASES_TROUBLESHOOTING AS (
    SELECT PR.STATE_NAME
         , PR.PROJECT_ID
         , CASE
               WHEN CA.CREATED_DATE IS NOT NULL
                   AND CA.SOLAR_QUEUE = 'SPC'
                   THEN 'SPC'
               WHEN CA.CREATED_DATE IS NOT NULL
                   AND CA.SOLAR_QUEUE != 'SPC'
                   THEN 'CX'
        END                         AS CASE_BUCKET
         , TO_DATE(CA.CREATED_DATE) AS CREATED_DATE
         , TO_DATE(CA.CLOSED_DATE)  AS CLOSED_DATE
    FROM RPT.T_CASE CA
             INNER JOIN
         PROJECTS_RAW PR
         ON
             CA.PROJECT_ID = PR.PROJECT_ID
    WHERE CA.RECORD_TYPE = 'Solar - Troubleshooting'
)

   , G_CASES_TROUBLESHOOTING AS (
    SELECT STATE_NAME
         , COUNT(STATE_NAME)          AS CASE_TALLY
         , COUNT(DISTINCT PROJECT_ID) AS PROJECT_TALLY
    FROM CASES_TROUBLESHOOTING
    GROUP BY STATE_NAME
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
             INNER JOIN
         PROJECTS_RAW PR
         ON
             CA.PROJECT_ID = PR.PROJECT_ID
    WHERE CA.RECORD_TYPE IN ('Solar Damage Resolutions', 'Home Damage')
)

   , G_CASES_DAMAGE AS (
    SELECT STATE_NAME
         , COUNT(STATE_NAME)          AS CASE_TALLY
         , COUNT(DISTINCT PROJECT_ID) AS PROJECT_TALLY
    FROM CASES_DAMAGE
    GROUP BY STATE_NAME
)

   , CASES_ESCALATION AS (
    SELECT PR.STATE_NAME
         , PR.PROJECT_ID
         , CASE
               WHEN CA.CREATED_DATE IS NOT NULL
                   AND CA.EXECUTIVE_RESOLUTIONS_ACCEPTED IS NOT NULL
                   THEN 'Executive Resolutions'
               WHEN CA.CREATED_DATE IS NOT NULL
                   AND CA.EXECUTIVE_RESOLUTIONS_ACCEPTED IS NULL
                   THEN 'General Escalation' END AS CASE_BUCKET
         , TO_DATE(CA.CREATED_DATE)              AS CREATED_DATE
         , TO_DATE(CA.CLOSED_DATE)               AS CLOSED_DATE
    FROM RPT.T_CASE CA
             INNER JOIN
         PROJECTS_RAW PR
         ON
             CA.PROJECT_ID = PR.PROJECT_ID
    WHERE CA.RECORD_TYPE = 'Solar - Customer Escalation'
      AND CA.SUBJECT NOT ILIKE '%VIP%'
)

   , G_CASES_ESCALATION AS (
    SELECT STATE_NAME
         , COUNT(STATE_NAME)          AS CASE_TALLY
         , COUNT(DISTINCT PROJECT_ID) AS PROJECT_TALLY
    FROM CASES_ESCALATION
    GROUP BY STATE_NAME
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
             INNER JOIN
         PROJECTS_RAW PR
         ON
             CA.PROJECT_ID = PR.PROJECT_ID
    WHERE CA.RECORD_TYPE = 'Solar - Customer Default'
)

   , G_CASES_DEFAULT AS (
    SELECT STATE_NAME
         , COUNT(STATE_NAME)          AS CASE_TALLY
         , COUNT(DISTINCT PROJECT_ID) AS PROJECT_TALLY
    FROM CASES_DEFAULT
    GROUP BY STATE_NAME
)

   , CASES_OVERALL AS (
    SELECT STATE_NAME
         , PROJECT_ID
         , CASE_BUCKET
         , CREATED_DATE
         , CLOSED_DATE
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

   , I_DAY_WIP AS (
    SELECT D.DT
         , STATE_NAME
         , COUNT(CASE WHEN INSTALL_DATE <= D.DT THEN 1 END) AS ACTIVE_INSTALLS
    FROM PROJECTS_RAW AS PR
       , RPT.T_DATES AS D
    WHERE D.DT BETWEEN
              DATEADD('Y', -1, DATE_TRUNC('MM', CURRENT_DATE())) AND
              LAST_DAY(DATEADD('MM', -1, CURRENT_DATE))
    GROUP BY D.DT
           , STATE_NAME
    ORDER BY STATE_NAME
           , D.DT
)

   , I_MONTH_WIP AS (
    SELECT *
    FROM I_DAY_WIP
    WHERE DT = LAST_DAY(DT)
       OR DT = CURRENT_DATE()
    ORDER BY STATE_NAME, DT
)

   , G_ION_TABLE AS (
    SELECT LAST_DAY(D.DT)                                  AS HEAT_MONTH
         , YEAR(HEAT_MONTH)                                AS HEAT_YEAR
         , CO.STATE_NAME
         , CO.CASE_BUCKET
         , COUNT(CASE WHEN CREATED_DATE = D.DT THEN 1 END) AS HEAT_INFLOW
         , COUNT(CASE WHEN CLOSED_DATE = D.DT THEN 1 END)  AS HEAT_OUTFLOW
         , HEAT_INFLOW - HEAT_OUTFLOW                      AS HEAT_NET
    FROM CASES_OVERALL AS CO
       , RPT.T_DATES AS D
    WHERE D.DT BETWEEN DATEADD('y', -2, DATE_TRUNC('MM', CURRENT_DATE())) AND CURRENT_DATE()
    GROUP BY HEAT_MONTH
           , CO.STATE_NAME
           , CO.CASE_BUCKET
    ORDER BY CO.STATE_NAME
           , CO.CASE_BUCKET
           , HEAT_MONTH
)

   , COMBINED_TABLE AS (
    SELECT ION.HEAT_MONTH
         , ION.HEAT_YEAR
         , ION.STATE_NAME
         , WIP.ACTIVE_INSTALLS
         , ION.CASE_BUCKET
         , ION.HEAT_INFLOW
         , ION.HEAT_OUTFLOW
         , ION.HEAT_NET
    FROM G_ION_TABLE AS ION
             LEFT JOIN I_MONTH_WIP AS WIP
                       ON WIP.DT = ION.HEAT_MONTH AND WIP.STATE_NAME = WIP.STATE_NAME
    ORDER BY ION.STATE_NAME
           , ION.CASE_BUCKET
           , ION.HEAT_MONTH
)

SELECT *
FROM I_MONTH_WIP
;