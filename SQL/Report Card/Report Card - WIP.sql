WITH PROJECTS_RAW AS (
    SELECT PROJECT_ID
         , NVL(SERVICE_STATE, '[blank]')  AS STATE_NAME
         , TO_DATE(INSTALLATION_COMPLETE) AS INSTALL_DATE
    FROM RPT.T_PROJECT
    WHERE INSTALLATION_COMPLETE IS NOT NULL
      AND CANCELLATION_DATE IS NULL
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

   , DAY_WIP_TABLE AS (
    SELECT D.DT
         , C.STATE_NAME
         , C.CASE_BUCKET
         , COUNT(
            CASE
                WHEN C.CREATED_DATE <= D.DT AND (C.CLOSED_DATE >= D.DT OR C.CLOSED_DATE IS NULL)
                    THEN 1 END) AS ACTIVE_WIP
    FROM CASES_OVERALL AS C
       , RPT.T_DATES AS D
    WHERE D.DT BETWEEN DATEADD('Y', -2, DATE_TRUNC('MM', CURRENT_DATE())) AND CURRENT_DATE()
    GROUP BY D.DT
           , C.STATE_NAME
           , C.CASE_BUCKET
    ORDER BY C.STATE_NAME
           , C.CASE_BUCKET
           , D.DT
)

   , MONTH_WIP_TABLE AS (
    SELECT DWT.DT        AS HEAT_DT
         , YEAR(HEAT_DT) AS HEAT_YEAR
         , DWT.STATE_NAME
         , DWT.CASE_BUCKET
         , DWT.ACTIVE_WIP
    FROM DAY_WIP_TABLE AS DWT
    WHERE DWT.DT = LAST_DAY(DWT.DT)
    ORDER BY DWT.STATE_NAME
           , HEAT_DT
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

   , STATE_CASE_WIP AS (
    SELECT F.HEAT_DT
         , F.STATE_NAME
         , SUM(F.ACTIVE_WIP) AS FINAL_WIP
    FROM MONTH_WIP_TABLE AS F
    GROUP BY F.HEAT_DT, F.STATE_NAME
    ORDER BY F.STATE_NAME, F.HEAT_DT
)

   , I_MONTH_WIP AS (
    SELECT *
    FROM I_DAY_WIP
    WHERE DT = LAST_DAY(DT)
       OR DT = CURRENT_DATE()
    ORDER BY STATE_NAME, DT
)

   , MAP_WIDGET AS (
       /*
        Group requests currently stand with the following:
        City
        case_bucket
        */
    SELECT SCW.HEAT_DT
         , SCW.STATE_NAME
         , SCW.FINAL_WIP
         , IDW.ACTIVE_INSTALLS
         , IFF(IDW.ACTIVE_INSTALLS = 0, NULL, ROUND(SCW.FINAL_WIP / IDW.ACTIVE_INSTALLS, 2)) AS WIP_RATIO
    FROM STATE_CASE_WIP AS SCW
             INNER JOIN
         I_DAY_WIP AS IDW
         ON SCW.HEAT_DT = IDW.DT AND SCW.STATE_NAME = IDW.STATE_NAME
)

, WIP_RATIO AS (
    SELECT *
    FROM MONTH_WIP_TABLE AS WT
    , I_MONTH_WIP AS IW
    WHERE IW.DT = WT.HEAT_DT
)

SELECT *
FROM MAP_WIDGET
-- WHERE HEAT_DT = LAST_DAY(DATEADD('MM', -1, CURRENT_DATE))
;