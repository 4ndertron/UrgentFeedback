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
                  , COUNT(PROJECT_ID) OVER(PARTITION BY STATE_NAME) AS INSTALL_TALLY
                  ,
            TO_CHAR(100 * COUNT(PROJECT_ID) OVER(PARTITION BY STATE_NAME) / COUNT(PROJECT_ID) OVER(), '90.00') ||
            '%'                                                     AS INSTALL_RATIO
    FROM PROJECTS_RAW
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
             INNER JOIN
         PROJECTS_RAW PR
         ON
             CA.PROJECT_ID = PR.PROJECT_ID
    WHERE CA.RECORD_TYPE = 'Solar - Service'
      AND UPPER(CA.SUBJECT) LIKE '%NF%'
      AND CA.SOLAR_QUEUE IN ('Outbound', 'Tier II')
--       AND CA.CLOSED_DATE IS NULL
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
                   THEN 'Removal Reinstall' END AS CASE_BUCKET
         , TO_DATE(CA.CREATED_DATE)             AS CREATED_DATE
         , TO_DATE(CA.CLOSED_DATE)              AS CLOSED_DATE
    FROM RPT.T_CASE CA
             INNER JOIN
         PROJECTS_RAW PR
         ON
             CA.PROJECT_ID = PR.PROJECT_ID
    WHERE CA.RECORD_TYPE = 'Solar - Panel Removal'
--       AND CA.CLOSED_DATE IS NULL
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
                   THEN 'Troubleshooting' END AS CASE_BUCKET
         , TO_DATE(CA.CREATED_DATE)           AS CREATED_DATE
         , TO_DATE(CA.CLOSED_DATE)            AS CLOSED_DATE
    FROM RPT.T_CASE CA
             INNER JOIN
         PROJECTS_RAW PR
         ON
             CA.PROJECT_ID = PR.PROJECT_ID
    WHERE CA.RECORD_TYPE = 'Solar - Troubleshooting'
      AND UPPER(CA.SUBJECT) LIKE '%NF%'
--       AND ca.closed_date IS NULL
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
--       AND CA.CLOSED_DATE IS NULL
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
                   THEN 'Escalation' END AS CASE_BUCKET
         , TO_DATE(CA.CREATED_DATE)      AS CREATED_DATE
         , TO_DATE(CA.CLOSED_DATE)       AS CLOSED_DATE
    FROM RPT.T_CASE CA
             INNER JOIN
         PROJECTS_RAW PR
         ON
             CA.PROJECT_ID = PR.PROJECT_ID
    WHERE CA.RECORD_TYPE = 'Solar - Customer Escalation'
      AND CA.SUBJECT NOT ILIKE '%VIP%'
--       AND CA.CLOSED_DATE IS NULL
)

   , G_CASES_ESCALATION AS (
    SELECT STATE_NAME
         , COUNT(STATE_NAME)          AS CASE_TALLY
         , COUNT(DISTINCT PROJECT_ID) AS PROJECT_TALLY
    FROM CASES_ESCALATION
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
         )
)

   , G_WIP_TABLE AS (
    SELECT D.DT
         , C.STATE_NAME
         , C.CASE_BUCKET
         , COUNT(
            CASE
                WHEN C.CREATED_DATE <= D.DT AND (C.CLOSED_DATE >= D.DT OR C.CLOSED_DATE IS NULL)
                    THEN 1 END) AS ACTIVE_WIP
    FROM CASES_OVERALL AS C
       , RPT.T_DATES AS D
    WHERE D.DT BETWEEN DATEADD('Y', -1, DATE_TRUNC('MM', CURRENT_DATE())) AND CURRENT_DATE()
    GROUP BY D.DT
           , C.STATE_NAME
           , C.CASE_BUCKET
    ORDER BY C.STATE_NAME
           , C.CASE_BUCKET
           , D.DT
)

   , G_ION_TABLE AS (
    SELECT LAST_DAY(D.DT)                                  AS HEAT_MONTH1
         , CO.STATE_NAME
         , CO.CASE_BUCKET
         , COUNT(CASE WHEN CREATED_DATE = D.DT THEN 1 END) AS HEAT_INFLOW
         , COUNT(CASE WHEN CLOSED_DATE = D.DT THEN 1 END)  AS HEAT_OUTFLOW
         , HEAT_INFLOW - HEAT_OUTFLOW                      AS HEAT_NET
    FROM CASES_OVERALL AS CO
       , RPT.T_DATES AS D
    WHERE D.DT BETWEEN DATEADD('y', -1, DATE_TRUNC('MM', CURRENT_DATE())) AND CURRENT_DATE()
    GROUP BY HEAT_MONTH1
           , CO.STATE_NAME
           , CO.CASE_BUCKET
    ORDER BY CO.STATE_NAME
           , CO.CASE_BUCKET
           , HEAT_MONTH1
)

   , FINAL AS (
    SELECT GWT.DT AS HEAT_DT
         , GWT.STATE_NAME
         , GWT.CASE_BUCKET
         , GWT.ACTIVE_WIP
    FROM G_WIP_TABLE AS GWT
    WHERE GWT.DT = LAST_DAY(GWT.DT)
       OR GWT.DT = CURRENT_DATE()
    ORDER BY GWT.STATE_NAME
           , HEAT_DT
)

   , I_WIP AS (
    SELECT D.DT
         , STATE_NAME
         , COUNT(CASE WHEN INSTALL_DATE <= D.DT THEN 1 END) AS ACTIVE_INSTALLS
    FROM PROJECTS_RAW AS PR
       , RPT.T_DATES AS D
    WHERE D.DT BETWEEN DATEADD('Y', -1, DATE_TRUNC('MM', CURRENT_DATE())) AND CURRENT_DATE()
    GROUP BY D.DT
           , STATE_NAME
    ORDER BY STATE_NAME
           , D.DT
)

   , WIP_RATIO AS (
    SELECT IW.DT
         , IW.STATE_NAME
         , F.ACTIVE_WIP
         , IW.ACTIVE_INSTALLS
    FROM (SELECT *
          FROM I_WIP
          WHERE DT = LAST_DAY(DT)
             OR DT = CURRENT_DATE()
          ORDER BY STATE_NAME, DT
         ) AS IW
             INNER JOIN
         FINAL AS F
         ON F.HEAT_DT = IW.DT
    WHERE IW.DT = LAST_DAY(IW.DT)
       OR IW.DT = CURRENT_DATE()
    ORDER BY IW.STATE_NAME, IW.DT
)

SELECT *
FROM WIP_RATIO


/*
 TODO: Setup the case volumes against the active install total for the month, and stack that ratio.
 */
;