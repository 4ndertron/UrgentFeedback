WITH PROJECTS_RAW AS (
    SELECT PROJECT_ID
         , SERVICE_NAME                   AS SERVICE_NUMBER
         , NVL(SERVICE_STATE, '[blank]')  AS STATE_NAME
         , TO_DATE(INSTALLATION_COMPLETE) AS INSTALL_DATE
         , TO_DATE(CANCELLATION_DATE)     AS CANCELLATION_DATE
    FROM RPT.T_PROJECT
    WHERE INSTALLATION_COMPLETE IS NOT NULL
)

   , CASE_TABLE AS (
    SELECT PR.STATE_NAME
         , PR.PROJECT_ID
         , CA.CASE_NUMBER
         , PR.SERVICE_NUMBER
         , CA.OWNER
         , CA.ORIGIN
         , TO_DATE(CA.CREATED_DATE) AS CREATED_DATE
         , TO_DATE(CA.CLOSED_DATE)  AS CLOSED_DATE
         , CASE
               WHEN CA.STATUS = 'Escalated' AND CA.RECORD_TYPE = 'Solar - Customer Default' THEN 'Pending Legal'
               WHEN CA.RECORD_TYPE = 'Solar - Customer Default' AND CA.DESCRIPTION ILIKE '%MBW%' THEN 'MBW'
               WHEN CA.RECORD_TYPE = 'Solar - Customer Default' AND CA.SUBJECT ILIKE '%D1%' THEN 'D1'
               WHEN CA.RECORD_TYPE = 'Solar - Customer Default' AND CA.SUBJECT ILIKE '%D2%' THEN 'D2'
               WHEN CA.RECORD_TYPE = 'Solar - Customer Default' AND CA.SUBJECT ILIKE '%D3%' THEN 'D3'
               WHEN CA.RECORD_TYPE = 'Solar - Customer Default' AND CA.SUBJECT ILIKE '%D4%' THEN 'D4'
               WHEN CA.RECORD_TYPE = 'Solar - Customer Default' AND CA.SUBJECT ILIKE '%D5%' THEN 'D5'
               WHEN CA.RECORD_TYPE = 'Solar - Customer Default' AND CA.SUBJECT ILIKE '%CORP%' THEN 'CORP-Default'
        END                         AS MASTER_BUCKET
    FROM RPT.T_CASE CA
             INNER JOIN PROJECTS_RAW AS PR
                        ON CA.PROJECT_ID = PR.PROJECT_ID
    WHERE CA.RECORD_TYPE = 'Solar - Customer Default'
)

   , MAIN AS (
    SELECT ANY_VALUE(CREATED_DATE)   AS CREATED_DATE
         , CASE_NUMBER
         , ANY_VALUE(SERVICE_NUMBER) AS SERVICE_NUMBER
         , ANY_VALUE(STATE_NAME)     AS SERVICE_STATE
         , ANY_VALUE(OWNER)          AS OWNER
         , ANY_VALUE(ORIGIN)         AS ORIGIN
         , ANY_VALUE(MASTER_BUCKET)  AS MASTER_BUCKET
    FROM CASE_TABLE
    WHERE CASE_NUMBER IS NOT NULL
--       AND CREATED_DATE BETWEEN DATE_TRUNC('Y', CURRENT_DATE) AND '2019-08-26' -- Used to find the backlog
      AND CREATED_DATE >= '2019-08-26'
    GROUP BY CASE_NUMBER
    ORDER BY 1
)

SELECT *
FROM MAIN