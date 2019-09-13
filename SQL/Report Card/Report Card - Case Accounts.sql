WITH PROJECTS_RAW AS (
    SELECT PROJECT_ID
         , PROJECT_NAME                   AS PROJECT_NUMBER
         , SERVICE_NAME                   AS SERVICE_NUMBER
         , NVL(SERVICE_STATE, '[blank]')  AS STATE_NAME
         , TO_DATE(INSTALLATION_COMPLETE) AS INSTALL_DATE
         , TO_DATE(CANCELLATION_DATE)     AS CANCELLATION_DATE
    FROM RPT.T_PROJECT
    WHERE INSTALLATION_COMPLETE IS NOT NULL
)

   , CASES_SERVICE AS (
    SELECT PR.STATE_NAME
         , PR.PROJECT_ID
         , PR.PROJECT_NUMBER
         , PR.SERVICE_NUMBER
         , CA.CASE_NUMBER
         , CA.OWNER                   AS CASE_OWNER
         , CT.FULL_NAME               AS CUSTOMER_NAME
         , CASE
               WHEN CA.CREATED_DATE IS NOT NULL
                   THEN 'Service' END AS CASE_BUCKET
         , CASE
               WHEN CA.CREATED_DATE IS NOT NULL
                   THEN 'CX' END      AS ORG_BUCKET
         , TO_DATE(CA.CREATED_DATE)   AS CREATED_DATE
         , TO_DATE(CA.CLOSED_DATE)    AS CLOSED_DATE
    FROM RPT.T_CASE CA
             INNER JOIN PROJECTS_RAW AS PR
                        ON CA.PROJECT_ID = PR.PROJECT_ID
             LEFT JOIN RPT.T_CONTACT AS CT
                       ON CT.CONTACT_ID = CA.CONTACT_ID
    WHERE CA.RECORD_TYPE = 'Solar - Service'
      AND CA.SOLAR_QUEUE IN ('Outbound', 'Tier II')
)

   , CASES_REMOVAL_REINSTALL AS (
    SELECT PR.STATE_NAME
         , PR.PROJECT_ID
         , PR.PROJECT_NUMBER
         , PR.SERVICE_NUMBER
         , CA.CASE_NUMBER
         , CA.OWNER                         AS CASE_OWNER
         , CT.FULL_NAME                     AS CUSTOMER_NAME
         , CASE
               WHEN CA.CREATED_DATE IS NOT NULL
                   THEN 'Panel Removal' END AS CASE_BUCKET

         , CASE
               WHEN CA.CREATED_DATE IS NOT NULL
                   THEN 'CX' END            AS ORG_BUCKET
         , TO_DATE(CA.CREATED_DATE)         AS CREATED_DATE
         , TO_DATE(CA.CLOSED_DATE)          AS CLOSED_DATE
    FROM RPT.T_CASE CA
             INNER JOIN PROJECTS_RAW AS PR
                        ON CA.PROJECT_ID = PR.PROJECT_ID
             LEFT JOIN RPT.T_CONTACT AS CT
                       ON CT.CONTACT_ID = CA.CONTACT_ID
    WHERE CA.RECORD_TYPE = 'Solar - Panel Removal'
)

   , CASES_TROUBLESHOOTING AS (
    SELECT PR.STATE_NAME
         , PR.PROJECT_ID
         , PR.PROJECT_NUMBER
         , PR.SERVICE_NUMBER
         , CA.CASE_NUMBER
         , CA.OWNER                           AS CASE_OWNER
         , CT.FULL_NAME                       AS CUSTOMER_NAME
         , CASE
               WHEN CA.CREATED_DATE IS NOT NULL
                   THEN 'Troubleshooting' END AS CASE_BUCKET
         , CASE
               WHEN CA.CREATED_DATE IS NOT NULL
                   AND CA.SOLAR_QUEUE IN ('Outbound', 'Tier II')
                   THEN 'CX'
               WHEN CA.CREATED_DATE IS NOT NULL
                   AND CA.SOLAR_QUEUE NOT IN ('Outbound', 'Tier II')
                   THEN 'SPC' END             AS ORG_BUCKET
         , TO_DATE(CA.CREATED_DATE)           AS CREATED_DATE
         , TO_DATE(CA.CLOSED_DATE)            AS CLOSED_DATE
    FROM RPT.T_CASE CA
             INNER JOIN PROJECTS_RAW AS PR
                        ON CA.PROJECT_ID = PR.PROJECT_ID
             LEFT JOIN RPT.T_CONTACT AS CT
                       ON CT.CONTACT_ID = CA.CONTACT_ID
    WHERE CA.RECORD_TYPE = 'Solar - Troubleshooting'
      AND CA.SOLAR_QUEUE IN ('Outbound', 'Tier II')
)

   , CASES_DAMAGE AS (
    SELECT PR.STATE_NAME
         , PR.PROJECT_ID
         , PR.PROJECT_NUMBER
         , PR.SERVICE_NUMBER
         , CA.CASE_NUMBER
         , CA.OWNER                  AS CASE_OWNER
         , CT.FULL_NAME              AS CUSTOMER_NAME
         , CASE
               WHEN CA.CREATED_DATE IS NOT NULL
                   THEN 'Damage' END AS CASE_BUCKET
         , CASE
               WHEN CA.CREATED_DATE IS NOT NULL
                   THEN 'Damage' END AS ORG_BUCKET
         , TO_DATE(CA.CREATED_DATE)  AS CREATED_DATE
         , TO_DATE(CA.CLOSED_DATE)   AS CLOSED_DATE
    FROM RPT.T_CASE CA
             INNER JOIN PROJECTS_RAW AS PR
                        ON CA.PROJECT_ID = PR.PROJECT_ID
             LEFT JOIN RPT.T_CONTACT AS CT
                       ON CT.CONTACT_ID = CA.CONTACT_ID
    WHERE CA.RECORD_TYPE IN ('Solar Damage Resolutions', 'Home Damage')
)

   , CASES_ESCALATION AS (
    SELECT PR.STATE_NAME
         , PR.PROJECT_ID
         , PR.PROJECT_NUMBER
         , PR.SERVICE_NUMBER
         , CA.CASE_NUMBER
         , CA.OWNER                 AS CASE_OWNER
         , CT.FULL_NAME             AS CUSTOMER_NAME
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
               WHEN CA.CREATED_DATE IS NOT NULL
                   AND CA.EXECUTIVE_RESOLUTIONS_ACCEPTED IS NULL
                   THEN 'General Escalation'
        END                         AS CASE_BUCKET
         , CASE
               WHEN CA.CREATED_DATE IS NOT NULL
                   AND CA.EXECUTIVE_RESOLUTIONS_ACCEPTED IS NOT NULL
                   THEN 'ERT'
               WHEN CA.CREATED_DATE IS NOT NULL
                   AND CA.EXECUTIVE_RESOLUTIONS_ACCEPTED IS NULL
                   THEN 'CX'
        END                         AS ORG_BUCKET
         , TO_DATE(CA.CREATED_DATE) AS CREATED_DATE
         , TO_DATE(CA.CLOSED_DATE)  AS CLOSED_DATE
    FROM RPT.T_CASE CA
             INNER JOIN PROJECTS_RAW AS PR
                        ON CA.PROJECT_ID = PR.PROJECT_ID
             LEFT JOIN RPT.T_CONTACT AS CT
                       ON CT.CONTACT_ID = CA.CONTACT_ID
    WHERE CA.RECORD_TYPE = 'Solar - Customer Escalation'
      AND CA.SUBJECT NOT ILIKE '%VIP%'
      AND CA.SUBJECT NOT ILIKE '%NPS%'
      AND CA.SUBJECT NOT ILIKE '%COMP%'
)

   , CASES_DEFAULT AS (
    SELECT PR.STATE_NAME
         , PR.PROJECT_ID
         , PR.PROJECT_NUMBER
         , PR.SERVICE_NUMBER
         , CA.CASE_NUMBER
         , CA.OWNER                   AS CASE_OWNER
         , CT.FULL_NAME               AS CUSTOMER_NAME
         , CASE
               WHEN CA.CREATED_DATE IS NOT NULL
                   THEN 'Default' END AS CASE_BUCKET
         , CASE
               WHEN CA.CREATED_DATE IS NOT NULL
                   AND CA.SUBJECT ILIKE '%D3%'
                   THEN 'Collections'
               WHEN CA.CREATED_DATE IS NOT NULL
                   AND CA.SUBJECT NOT ILIKE '%D3%'
                   THEN 'Default' END AS ORG_BUCKET
         , TO_DATE(CA.CREATED_DATE)   AS CREATED_DATE
         , TO_DATE(CA.CLOSED_DATE)    AS CLOSED_DATE
    FROM RPT.T_CASE CA
             INNER JOIN PROJECTS_RAW AS PR
                        ON CA.PROJECT_ID = PR.PROJECT_ID
             LEFT JOIN RPT.T_CONTACT AS CT
                       ON CT.CONTACT_ID = CA.CONTACT_ID
    WHERE CA.RECORD_TYPE = 'Solar - Customer Default'
)

   , CASES_OVERALL AS (
    SELECT *
    FROM (
             SELECT *
             FROM CASES_SERVICE
             UNION ALL
             SELECT *
             FROM CASES_REMOVAL_REINSTALL
             UNION ALL
             SELECT *
             FROM CASES_TROUBLESHOOTING
             UNION ALL
             SELECT *
             FROM CASES_DAMAGE
             UNION ALL
             SELECT *
             FROM CASES_ESCALATION
             UNION ALL
             SELECT *
             FROM CASES_DEFAULT
         )
)

   , MAIN_TEST_1 AS (
    SELECT D.DT        AS MONTH
         , YEAR(MONTH) AS YEAR
         , CO.*
    FROM RPT.T_DATES AS D
             INNER JOIN CASES_OVERALL AS CO
                        ON CO.CREATED_DATE <= D.DT AND
                           NVL(CO.CLOSED_DATE, CURRENT_DATE + 1) > D.DT
    WHERE D.DT BETWEEN
        DATE_TRUNC('Y', DATEADD('Y', -1, CURRENT_DATE)) AND
        CURRENT_DATE
      AND DAY(D.DT) = DAY(CURRENT_DATE)
)

   , TEST_RESULTS AS (
    SELECT *
    FROM MAIN_TEST_1
--     GROUP BY DT
    ORDER BY MONTH DESC
)

SELECT *
FROM CASES_OVERALL
;