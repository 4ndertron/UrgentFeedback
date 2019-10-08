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
         , CA.OWNER                 AS CASE_OWNER
         , CT.FULL_NAME             AS CUSTOMER_NAME
         , CASE
               WHEN CA.CREATED_DATE IS NOT NULL
                   THEN 'Troubleshooting'
        END                         AS CASE_BUCKET
         , CASE
               WHEN CA.CREATED_DATE IS NOT NULL
                   AND CA.SOLAR_QUEUE IN ('Outbound', 'Tier II')
                   THEN 'CX'
               WHEN CA.CREATED_DATE IS NOT NULL
                   AND (CA.SOLAR_QUEUE NOT IN ('Outbound', 'Tier II') OR
                        CA.SOLAR_QUEUE IS NULL)
                   THEN 'SPC'
        END                         AS ORG_BUCKET
         , TO_DATE(CA.CREATED_DATE) AS CREATED_DATE
         , TO_DATE(CA.CLOSED_DATE)  AS CLOSED_DATE
    FROM RPT.T_CASE CA
             INNER JOIN PROJECTS_RAW AS PR
                        ON CA.PROJECT_ID = PR.PROJECT_ID
             LEFT JOIN RPT.T_CONTACT AS CT
                       ON CT.CONTACT_ID = CA.CONTACT_ID
    WHERE CA.RECORD_TYPE = 'Solar - Troubleshooting'
--       AND CA.SOLAR_QUEUE IN ('Outbound', 'Tier II')
)

   , CASES_DAMAGE AS (
    /*
     TODO: Split the total bucket by Origin...?
     TODO: And how many of them also have an ERT Case.
     TODO: Talk with Landon on how the Damage Case subcategories are reported.
     */
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
                   AND CA.ORIGIN IN ('Legal')
                   THEN 'Legal'
               WHEN CA.CREATED_DATE IS NOT NULL
                   AND CA.EXECUTIVE_RESOLUTIONS_ACCEPTED IS NOT NULL
                   AND CA.ORIGIN IN ('News Media')
                   THEN 'News Media'
               WHEN CA.CREATED_DATE IS NOT NULL
                   AND CA.EXECUTIVE_RESOLUTIONS_ACCEPTED IS NOT NULL
                   AND CA.ORIGIN IN ('Social Media')
                   THEN 'Social Media'
               WHEN CA.CREATED_DATE IS NOT NULL
                   AND CA.EXECUTIVE_RESOLUTIONS_ACCEPTED IS NOT NULL
                   AND CA.ORIGIN IN ('Online Review')
                   THEN 'Online Review'
               WHEN CA.CREATED_DATE IS NOT NULL
                   AND CA.EXECUTIVE_RESOLUTIONS_ACCEPTED IS NOT NULL
                   AND CA.ORIGIN IN ('BBB')
                   THEN 'BBB'
               WHEN CA.CREATED_DATE IS NOT NULL
                   AND CA.EXECUTIVE_RESOLUTIONS_ACCEPTED IS NOT NULL
                   AND CA.ORIGIN NOT IN ('BBB', 'Legal', 'News Media', 'Online Review', 'Social Media')
                   THEN 'ERT'
               WHEN CA.CREATED_DATE IS NOT NULL
                   AND CA.EXECUTIVE_RESOLUTIONS_ACCEPTED IS NULL
                   AND CA.SOLAR_QUEUE != 'Advocate Response'
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
    /*
     TODO: Breakout by Foreclosure and Default, and I guess the D1,2,4,5, Deceased...
     TODO: Use the standard breakpoints in the other reports... CXBR?
     */
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

   , CASE_DAY_WIP AS (
    SELECT D.DT
         , C.STATE_NAME
         , C.ORG_BUCKET
         , C.CASE_BUCKET
         , COUNT(CASE
                     WHEN C.CREATED_DATE <= D.DT AND
                          (C.CLOSED_DATE > D.DT OR
                           C.CLOSED_DATE IS NULL)
                         THEN 1 END)                       AS ACTIVE_CASES
         , COUNT(DISTINCT (CASE
                               WHEN C.CREATED_DATE <= D.DT AND
                                    (C.CLOSED_DATE > D.DT OR
                                     C.CLOSED_DATE IS NULL)
                                   THEN C.PROJECT_ID END)) AS DISTINCT_ACTIVE_ACCOUNTS
    FROM CASES_OVERALL AS C
       , RPT.T_DATES AS D
    WHERE D.DT BETWEEN
              DATE_TRUNC('Y', DATEADD('Y', -1, CURRENT_DATE)) AND
              CURRENT_DATE
    GROUP BY D.DT
           , C.ORG_BUCKET
           , C.STATE_NAME
           , C.CASE_BUCKET
    ORDER BY C.STATE_NAME
           , C.ORG_BUCKET
           , C.CASE_BUCKET
           , D.DT
)

   , INSTALL_DAY_WIP AS (
    SELECT D.DT
         , P.STATE_NAME
         , COUNT(CASE
                     WHEN P.INSTALL_DATE <= D.DT AND
                          (P.CANCELLATION_DATE > D.DT OR
                           P.CANCELLATION_DATE IS NULL)
                         THEN 1 END) AS CASE_ACTIVE_INSTALLS
    FROM PROJECTS_RAW AS P
       , RPT.T_DATES AS D
    WHERE D.DT BETWEEN
              DATE_TRUNC('Y', DATEADD('Y', -1, CURRENT_DATE)) AND
              CURRENT_DATE
    GROUP BY D.DT
           , P.STATE_NAME
    ORDER BY P.STATE_NAME
           , D.DT
)

   , MONTH_WIP_TABLE AS (
    SELECT CDW.DT      AS MONTH
         , YEAR(MONTH) AS YEAR
         , CDW.STATE_NAME
         , CDW.ORG_BUCKET
         , CDW.CASE_BUCKET
         , CDW.ACTIVE_CASES
         , CDW.DISTINCT_ACTIVE_ACCOUNTS
         , IDW.CASE_ACTIVE_INSTALLS
--          , CDW.ACTIVE_CASES / IDW.CASE_ACTIVE_INSTALLS AS CASE_WIP_RATIO
    FROM CASE_DAY_WIP AS CDW
             INNER JOIN INSTALL_DAY_WIP AS IDW
                        ON IDW.DT = CDW.DT AND IDW.STATE_NAME = CDW.STATE_NAME
    WHERE CDW.DT = LAST_DAY(CDW.DT)
       OR CDW.DT = CURRENT_DATE
    ORDER BY CDW.STATE_NAME
           , CDW.ORG_BUCKET
           , CDW.CASE_BUCKET
           , MONTH
)

   , CASE_ION AS (
    SELECT IFF(LAST_DAY(DT) >= CURRENT_DATE, CURRENT_DATE, LAST_DAY(DT)) AS MONTH
         , YEAR(MONTH)                                                   AS YEAR
         , CO.STATE_NAME
         , CO.ORG_BUCKET
         , CO.CASE_BUCKET
         , COUNT(CASE
                     WHEN CREATED_DATE = D.DT
                         THEN 1 END)                                     AS CASE_INFLOW
         , COUNT(CASE WHEN CLOSED_DATE = D.DT THEN 1 END)                AS CASE_OUTFLOW
         , CASE_INFLOW - CASE_OUTFLOW                                    AS CASE_NET
         , COUNT(DISTINCT (CASE
                               WHEN CREATED_DATE = D.DT
                                   THEN CO.PROJECT_ID END))              AS DISTINCT_ACCOUNT_INFLOW
         , COUNT(DISTINCT (CASE
                               WHEN CLOSED_DATE = D.DT
                                   THEN CO.PROJECT_ID END))              AS DISTINCT_ACCOUNT_OUTFLOW
         , DISTINCT_ACCOUNT_INFLOW - DISTINCT_ACCOUNT_OUTFLOW            AS DISTINCT_ACCOUNT_NET
    FROM CASES_OVERALL AS CO
       , RPT.T_DATES AS D
    WHERE D.DT BETWEEN
              DATE_TRUNC('Y', DATEADD('Y', -1, CURRENT_DATE)) AND
              CURRENT_DATE
    GROUP BY MONTH
           , CO.STATE_NAME
           , CO.ORG_BUCKET
           , CO.CASE_BUCKET
    ORDER BY CO.STATE_NAME
           , CO.ORG_BUCKET
           , CO.CASE_BUCKET
           , MONTH
)

   , METRIC_MERGE AS (
    SELECT ION.MONTH
         , ION.YEAR
         , ION.STATE_NAME
         , ION.ORG_BUCKET
         , ION.CASE_BUCKET
         , ION.CASE_INFLOW
         , ION.CASE_OUTFLOW
         , ION.CASE_NET
         , ION.DISTINCT_ACCOUNT_INFLOW
         , ION.DISTINCT_ACCOUNT_OUTFLOW
         , ION.DISTINCT_ACCOUNT_NET
         , WIP.ACTIVE_CASES
         , WIP.DISTINCT_ACTIVE_ACCOUNTS
         , WIP.CASE_ACTIVE_INSTALLS
    FROM CASE_ION AS ION
             INNER JOIN MONTH_WIP_TABLE AS WIP
                        ON WIP.MONTH = ION.MONTH AND
                           WIP.STATE_NAME = ION.STATE_NAME AND
                           WIP.CASE_BUCKET = ION.CASE_BUCKET AND
                           WIP.ORG_BUCKET = ION.ORG_BUCKET AND
                           WIP.YEAR = ION.YEAR
    ORDER BY ION.STATE_NAME
           , ION.CASE_BUCKET
           , ION.MONTH
)

   , TEST_RESULTS AS (
    SELECT DISTINCT CASE_BUCKET, ORG_BUCKET
    FROM METRIC_MERGE
    WHERE STATE_NAME = 'NY'
--     LIMIT 100
)

SELECT *
FROM METRIC_MERGE
;