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

   , UNIQUE_TABLE AS (
    SELECT DISTINCT CO.PROJECT_ID
                  , CO.STATE_NAME
    FROM CASES_OVERALL AS CO
)

SELECT *
FROM UNIQUE_TABLE