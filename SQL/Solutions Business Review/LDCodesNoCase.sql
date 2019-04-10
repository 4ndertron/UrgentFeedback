-------------------- Start of the core query --------------------
WITH CASE_TABLE AS (
    -- Collect all the open Default 1,2,4,5 and Escalation Cases in Salesforce --
    SELECT C.SERVICE_ID
         , C.PROJECT_ID
         , C.SUBJECT
         , C.STATUS
         , C.ORIGIN
         , C.CREATED_DATE
         , C.CLOSED_DATE
         , C.RECORD_TYPE
         , S.SOLAR_BILLING_ACCOUNT_NUMBER
         , CASE
               WHEN
                   C.SUBJECT LIKE '%MBW%'
                   THEN
                   'MBW'
               WHEN
                   C.SUBJECT LIKE '%D1%'
                   THEN
                   'D1'
               WHEN
                   C.SUBJECT LIKE '%D2%'
                   THEN
                   'D2'
               WHEN
                   C.SUBJECT LIKE '%D4%'
                   THEN
                   'D4'
               WHEN
                   C.SUBJECT LIKE '%D5%'
                   THEN
                   'D5'
        END AS DEFAULT_BUCKET
    FROM RPT.T_CASE AS C
             LEFT JOIN
         RPT.T_SERVICE AS S
         ON S.SERVICE_ID = C.SERVICE_ID
    WHERE C.CREATED_DATE >= DATEADD('Y', -1, DATE_TRUNC('M', CURRENT_DATE()))
--	C.STATUS NOT LIKE '%Close%'
      AND ((
                   C.SUBJECT LIKE '%D1%'
                   OR C.SUBJECT LIKE '%D2%'
                   OR C.SUBJECT LIKE '%D4%'
                   OR C.SUBJECT LIKE '%D5%'
                   OR C.SUBJECT LIKE '%MBW%'
--	AND C.STATUS != 'Completed'
               ) OR (
                   C.RECORD_TYPE = 'Solar - Customer Escalation'
                   AND EXECUTIVE_RESOLUTIONS_ACCEPTED IS NOT NULL
                   AND C.ASSIGNED_DEPARTMENT IN ('Advocate Response', 'Executive Resolutions')
                   AND C.STATUS NOT LIKE 'In Dispute'
               ) OR (
               C.RECORD_TYPE IN ('Solar Damage Resolutions')
               ) OR (
               C.RECORD_TYPE IN ('Solar - Service', 'Solar - Troubleshooting')
               ))
),

     LD_TABLE AS (
-- Collect all of the CORP coded accounts from LD --
         SELECT LD.BILLING_ACCOUNT
              , LD.COLLECTION_CODE
              , LD.COLLECTION_DATE
         FROM LD.T_DAILY_DATA_EXTRACT AS LD
                  LEFT JOIN
              RPT.T_SERVICE AS S
              ON LD.BILLING_ACCOUNT = S.SOLAR_BILLING_ACCOUNT_NUMBER
         WHERE LD.COLLECTION_CODE IN ('CORP', 'FORE', 'BK07', 'BK13', 'ATTY')
     ),

/*
 * Fields needed:
 * --------------
 * O CORP_DEFAULT (Case Check)
 * O CORP_ESCALATION (Case Check)
 * O CORP_DR (Case Check)
 * O CORP_CX (Case Check)
 * O BK(07|13)
 * O FORE
 * O ATTY
 */

     CORP_MERGE_TABLE AS (
         SELECT LD.BILLING_ACCOUNT
              , C.RECORD_TYPE
              , CASE
                    WHEN
                        C.RECORD_TYPE = NULL
                        THEN
                        NULL
                    WHEN
                        C.RECORD_TYPE = 'Solar - Customer Default'
                        THEN
                        1
                    WHEN
                        C.RECORD_TYPE = 'Solar - Customer Escalation'
                        THEN
                        2
                    WHEN
                        C.RECORD_TYPE = 'Solar Damage Resolutions'
                        THEN
                        3
                    WHEN
                        C.RECORD_TYPE = 'Solar - Service'
                        THEN
                        4
                    WHEN
                        C.RECORD_TYPE = 'Solar - Troubleshooting'
                        THEN
                        5
             END AS RECORD_PRIORITY
              , LD.COLLECTION_CODE
              , CASE
                    WHEN
                            C.RECORD_TYPE = 'Solar - Customer Default' AND LD.COLLECTION_CODE = 'CORP'
                        THEN
                        'CORP-Default'
                    WHEN
                            C.RECORD_TYPE = 'Solar - Customer Escalation' AND LD.COLLECTION_CODE = 'CORP'
                        THEN
                        'CORP-Escalation'
                    WHEN
                            C.RECORD_TYPE = 'Solar Damage Resolutions' AND LD.COLLECTION_CODE = 'CORP'
                        THEN
                        'CORP-Damage'
                    WHEN
                            C.RECORD_TYPE IN ('Solar - Service', 'Solar - Troubleshooting') AND
                            LD.COLLECTION_CODE = 'CORP'
                        THEN
                        'CORP-CX'
                    ELSE
                        LD.COLLECTION_CODE
             END AS CORP_BUCKET
              , LD.COLLECTION_DATE
         FROM LD_TABLE AS LD
                  LEFT JOIN
              CASE_TABLE AS C
              ON
                  C.SOLAR_BILLING_ACCOUNT_NUMBER = LD.BILLING_ACCOUNT
         ORDER BY LD.BILLING_ACCOUNT
     ),

     CORP_FIX AS (
         SELECT BILLING_ACCOUNT
              , CASE
                    WHEN MIN(RECORD_PRIORITY) = 1
                        THEN 'CORP-Default'
                    WHEN MIN(RECORD_PRIORITY) = 2
                        THEN 'CORP-Escalation'
                    WHEN MIN(RECORD_PRIORITY) = 3
                        THEN 'CORP-Damage'
                    WHEN MIN(RECORD_PRIORITY) >= 4
                        THEN 'CORP-CX'
                    ELSE
                        'CORP'
             END AS CORP_BUCKET
         FROM CORP_MERGE_TABLE
         GROUP BY BILLING_ACCOUNT
         ORDER BY BILLING_ACCOUNT
     ),

     SOLUTIONS_WORKLOAD_MERGE AS (
         SELECT LD.BILLING_ACCOUNT
              , LD.COLLECTION_DATE
              , CASE
                    WHEN
                        LD.COLLECTION_CODE = 'CORP'
                        THEN
                        C.CORP_BUCKET
                    ELSE
                        LD.COLLECTION_CODE
             END AS COLLECTION_BUCKET
         FROM LD_TABLE AS LD
                  LEFT JOIN
              CORP_FIX AS C
              ON
                  C.BILLING_ACCOUNT = LD.BILLING_ACCOUNT
     ),

     HOMELESS AS (
         SELECT *
         FROM SOLUTIONS_WORKLOAD_MERGE
         WHERE COLLECTION_BUCKET = 'CORP'
     )

SELECT DATE_TRUNC('MM', D.DT)                               AS MONTH3
     , COUNT(CASE WHEN H.COLLECTION_DATE = D.DT THEN 1 END) AS HOMELESS_INFLOW
FROM HOMELESS AS H
   , RPT.T_DATES AS D
WHERE D.DT BETWEEN DATEADD('y', -1, DATE_TRUNC('MM', CURRENT_DATE())) AND CURRENT_DATE()
GROUP BY MONTH3
ORDER BY MONTH3
