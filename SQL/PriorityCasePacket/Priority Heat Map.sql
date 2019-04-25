-- v3 | Landon | 2018-10-01: As per Tyler Anderson, exclude any Escalation case with "VIP" in the subject
-- v2: Added Removal/Reinstall cases.
-- v2> Fixed overall_case_tally to use NVL for each addend  
-- v2> Changed final sort to Overall Case Tally descending
-- v2> Added unique project counts; adjusted ratio calculations to use unique project count instead of case count
WITH PROJECTS_RAW AS (
    SELECT PROJECT_ID
         , NVL(SERVICE_STATE, '[blank]') AS ROC_NAME
    FROM RPT.T_PROJECT
    WHERE INSTALLATION_COMPLETE IS NOT NULL
      AND CANCELLATION_DATE IS NULL
)

   , INSTALLS_BY_ROC AS (
    SELECT DISTINCT ROC_NAME
                  , COUNT(PROJECT_ID) OVER(PARTITION BY ROC_NAME) AS INSTALL_TALLY
                  ,
            TO_CHAR(100 * COUNT(PROJECT_ID) OVER(PARTITION BY ROC_NAME) / COUNT(PROJECT_ID) OVER(), '90.00') ||
            '%'                                                   AS INSTALL_RATIO
    FROM PROJECTS_RAW
)

   , CASES_SERVICE AS (
    SELECT PR.ROC_NAME
         , PR.PROJECT_ID
    FROM RPT.T_CASE CA
             INNER JOIN
         PROJECTS_RAW PR
         ON
             CA.PROJECT_ID = PR.PROJECT_ID
    WHERE CA.RECORD_TYPE = 'Solar - Service'
      AND UPPER(CA.SUBJECT) LIKE '%NF%'
      AND CA.SOLAR_QUEUE IN ('Outbound', 'Tier II')
      AND CA.CLOSED_DATE IS NULL
)

   , G_CASES_SERVICE AS (
    SELECT ROC_NAME
         , COUNT(ROC_NAME)            AS CASE_TALLY
         , COUNT(DISTINCT PROJECT_ID) AS PROJECT_TALLY
    FROM CASES_SERVICE
    GROUP BY ROC_NAME
)

   , CASES_REMOVAL_REINSTALL AS (
    SELECT PR.ROC_NAME
         , PR.PROJECT_ID
    FROM RPT.T_CASE CA
             INNER JOIN
         PROJECTS_RAW PR
         ON
             CA.PROJECT_ID = PR.PROJECT_ID
    WHERE CA.RECORD_TYPE = 'Solar - Panel Removal'
      AND CA.CLOSED_DATE IS NULL
)

   , G_CASES_REMOVAL_REINSTALL AS (
    SELECT ROC_NAME
         , COUNT(ROC_NAME)            AS CASE_TALLY
         , COUNT(DISTINCT PROJECT_ID) AS PROJECT_TALLY
    FROM CASES_REMOVAL_REINSTALL
    GROUP BY ROC_NAME
)

   , CASES_TROUBLESHOOTING AS (
    SELECT PR.ROC_NAME
         , PR.PROJECT_ID
    FROM RPT.T_CASE CA
             INNER JOIN
         PROJECTS_RAW PR
         ON
             CA.PROJECT_ID = PR.PROJECT_ID
    WHERE CA.RECORD_TYPE = 'Solar - Troubleshooting'
      AND UPPER(CA.SUBJECT) LIKE '%NF%'
      AND ca.closed_date IS NULL
)

   , G_CASES_TROUBLESHOOTING AS (
    SELECT ROC_NAME
         , COUNT(ROC_NAME)            AS CASE_TALLY
         , COUNT(DISTINCT PROJECT_ID) AS PROJECT_TALLY
    FROM CASES_TROUBLESHOOTING
    GROUP BY ROC_NAME
)

   , CASES_DAMAGE AS (
    SELECT PR.ROC_NAME
         , PR.PROJECT_ID
    FROM RPT.T_CASE CA
             INNER JOIN
         PROJECTS_RAW PR
         ON
             CA.PROJECT_ID = PR.PROJECT_ID
    WHERE CA.RECORD_TYPE IN ('Solar Damage Resolutions', 'Home Damage')
      AND CA.CLOSED_DATE IS NULL
)

   , G_CASES_DAMAGE AS (
    SELECT ROC_NAME
         , COUNT(ROC_NAME)            AS CASE_TALLY
         , COUNT(DISTINCT PROJECT_ID) AS PROJECT_TALLY
    FROM CASES_DAMAGE
    GROUP BY ROC_NAME
)

   , CASES_ESCALATION AS (
    SELECT PR.ROC_NAME
         , PR.PROJECT_ID
    FROM RPT.T_CASE CA
             INNER JOIN
         PROJECTS_RAW PR
         ON
             CA.PROJECT_ID = PR.PROJECT_ID
    WHERE CA.RECORD_TYPE = 'Solar - Customer Escalation'
      AND CA.SUBJECT NOT ILIKE '%VIP%'
      AND CA.CLOSED_DATE IS NULL
)

   , G_CASES_ESCALATION AS (
    SELECT ROC_NAME
         , COUNT(ROC_NAME)            AS CASE_TALLY
         , COUNT(DISTINCT PROJECT_ID) AS PROJECT_TALLY
    FROM CASES_ESCALATION
    GROUP BY ROC_NAME
)

   , G_CASES_OVERALL AS (
    SELECT ROC_NAME
         , COUNT(ROC_NAME)            AS CASE_TALLY
         , COUNT(DISTINCT PROJECT_ID) AS PROJECT_TALLY
    FROM (
             SELECT ROC_NAME
                  , PROJECT_ID
             FROM CASES_SERVICE
             UNION ALL
             SELECT ROC_NAME
                  , PROJECT_ID
             FROM CASES_REMOVAL_REINSTALL
             UNION ALL
             SELECT ROC_NAME
                  , PROJECT_ID
             FROM CASES_TROUBLESHOOTING
             UNION ALL
             SELECT ROC_NAME
                  , PROJECT_ID
             FROM CASES_DAMAGE
             UNION ALL
             SELECT ROC_NAME
                  , PROJECT_ID
             FROM CASES_ESCALATION
         )
    GROUP BY ROC_NAME
)

SELECT IBR.ROC_NAME
     , TO_CHAR(IBR.INSTALL_TALLY, '999,990')                                        AS INSTALL_TALLY
     , IBR.INSTALL_RATIO
     , NVL(CS.CASE_TALLY, 0)                                                        AS SERVICE_CASE_TALLY
     , NVL(CS.PROJECT_TALLY, 0)                                                     AS SERVICE_PROJECT_TALLY
     , TO_CHAR(100 * NVL(CS.PROJECT_TALLY, 0) / IBR.INSTALL_TALLY, '990.00') || '%' AS SERVICE_PROJECT_RATIO
     , NVL(CR.CASE_TALLY, 0)                                                        AS REMOVAL_CASE_TALLY
     , NVL(CR.PROJECT_TALLY, 0)                                                     AS REMOVAL_PROJECT_TALLY
     , TO_CHAR(100 * NVL(CR.PROJECT_TALLY, 0) / IBR.INSTALL_TALLY, '990.00') || '%' AS REMOVAL_PROJECT_RATIO
     , NVL(CT.CASE_TALLY, 0)                                                        AS TROUBLESHOOTING_CASE_TALLY
     , NVL(CT.PROJECT_TALLY, 0)                                                     AS TROUBLESHOOTING_PROJECT_TALLY
     , TO_CHAR(100 * NVL(CT.PROJECT_TALLY, 0) / IBR.INSTALL_TALLY, '990.00') || '%' AS TROUBLESHOOTING_PROJECT_RATIO
     , NVL(CD.CASE_TALLY, 0)                                                        AS DAMAGE_CASE_TALLY
     , NVL(CD.PROJECT_TALLY, 0)                                                     AS DAMAGE_PROJECT_TALLY
     , TO_CHAR(100 * NVL(CD.PROJECT_TALLY, 0) / IBR.INSTALL_TALLY, '990.00') || '%' AS DAMAGE_PROJECT_RATIO
     , NVL(CE.CASE_TALLY, 0)                                                        AS ESCALATION_CASE_TALLY
     , NVL(CE.PROJECT_TALLY, 0)                                                     AS ESCALATION_PROJECT_TALLY
     , TO_CHAR(100 * NVL(CE.PROJECT_TALLY, 0) / IBR.INSTALL_TALLY, '990.00') || '%' AS ESCALATION_PROJECT_RATIO
     , NVL(CO.CASE_TALLY, 0)                                                        AS OVERALL_CASE_TALLY
     , NVL(CO.PROJECT_TALLY, 0)                                                     AS OVERALL_PROJECT_TALLY
     , TO_CHAR(100 * NVL(CO.PROJECT_TALLY, 0) / IBR.INSTALL_TALLY, '990.00') || '%' AS OVERALL_PROJECT_RATIO
FROM INSTALLS_BY_ROC IBR
         LEFT OUTER JOIN
     G_CASES_SERVICE CS
     ON IBR.ROC_NAME = CS.ROC_NAME
         LEFT OUTER JOIN
     G_CASES_REMOVAL_REINSTALL CR
     ON IBR.ROC_NAME = CR.ROC_NAME
         LEFT OUTER JOIN
     G_CASES_TROUBLESHOOTING CT
     ON IBR.ROC_NAME = CT.ROC_NAME
         LEFT OUTER JOIN
     G_CASES_DAMAGE CD
     ON IBR.ROC_NAME = CD.ROC_NAME
         LEFT OUTER JOIN
     G_CASES_ESCALATION CE
     ON IBR.ROC_NAME = CE.ROC_NAME
         LEFT OUTER JOIN
     G_CASES_OVERALL CO
     ON IBR.ROC_NAME = CO.ROC_NAME
ORDER BY OVERALL_CASE_TALLY DESC
;