-------------------- Start of the core query --------------------
WITH CASE_TABLE AS (
    -- Collect all the open Default 1,2,4,5 and Escalation Cases in Salesforce --
    SELECT S.SERVICE_ID
         , C.PROJECT_ID
         , C.CASE_NUMBER
         , C.CASE_ID
         , C.SUBJECT
         , CAD.SYSTEM_SIZE
         , C.STATUS
         , C.ORIGIN
         , C.CREATED_DATE
         , C.CLOSED_DATE
         , NVL2(C.CLOSED_DATE, 0, 1)                                                                    AS WIP_KPI
         , C.RECORD_TYPE
         , S.SOLAR_BILLING_ACCOUNT_NUMBER                                                               AS SOLAR_BILLING_ACCOUNT_NUMBER1
         , CASE
               WHEN RECORD_TYPE = 'Solar Damage Resolutions' THEN 1
               WHEN RECORD_TYPE = 'Solar - Customer Escalation' THEN 2
               WHEN RECORD_TYPE = 'Solar - Cancellation Request' THEN 3
               WHEN RECORD_TYPE = 'Solar - Panel Removal' THEN 4
               WHEN RECORD_TYPE = 'Solar - Transfer' THEN 5
               WHEN RECORD_TYPE = 'Solar - Service' THEN 6
               WHEN RECORD_TYPE = 'Solar - Troubleshooting' THEN 7
               WHEN RECORD_TYPE = 'Solar - Panel Removal' THEN 8
               WHEN RECORD_TYPE = 'Solar - Customer Default' THEN 9
        END                                                                                             AS RECORD_PRIORITY1
         , ROW_NUMBER() OVER(PARTITION BY S.SOLAR_BILLING_ACCOUNT_NUMBER ORDER BY RECORD_PRIORITY1 ASC) AS RN
         , CASE
               WHEN
                       C.STATUS ILIKE '%Escalated%' AND C.RECORD_TYPE = 'Solar - Customer Default'
                   THEN
                   'Pending Legal'
               WHEN
                   C.SUBJECT LIKE '%MBW%'
                   THEN
                   'MBW'
               WHEN
                   C.DESCRIPTION ILIKE '%MBW%'
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
        END                                                                                             AS DEFAULT_BUCKET
    FROM RPT.T_CASE AS C
             LEFT OUTER JOIN
         RPT.T_SERVICE AS S
         ON C.SERVICE_ID = S.SERVICE_ID

             LEFT OUTER JOIN
         RPT.T_SYSTEM_DETAILS_SNAP AS CAD
         ON CAD.SERVICE_ID = C.SERVICE_ID
    WHERE C.STATUS NOT LIKE '%Close%'
      AND ((
                   C.SUBJECT LIKE '%D1%'
                   OR C.SUBJECT LIKE '%D2%'
                   OR C.SUBJECT LIKE '%D4%'
                   OR C.SUBJECT LIKE '%D5%'
                   OR C.DESCRIPTION ILIKE '%MBW%'
                   OR C.SUBJECT ILIKE '%MBW%'
                   OR C.RECORD_TYPE = 'Solar - Customer Default'
               ) OR (
                   C.RECORD_TYPE IN ('Solar - Customer Escalation', 'Solar - Cancellation Request')
               ) OR (
               C.RECORD_TYPE IN ('Solar Damage Resolutions')
               ) OR (
                   C.RECORD_TYPE IN ('Solar - Service', 'Solar - Troubleshooting', 'Solar - Transfer')
               ) OR (
               C.RECORD_TYPE IN ('Solar - Panel Removal') --CORP-RTS
               ))
    ORDER BY 1
),

     CASE_HISTORY_TABLE AS (
         SELECT CT.*
              , CH.CREATEDDATE
              , DATEDIFF(S, CREATEDDATE,
                         NVL(LEAD(CREATEDDATE) OVER(PARTITION BY CT.CASE_NUMBER ORDER BY CH.CREATEDDATE),
                             CURRENT_TIMESTAMP())) / (24 * 60 * 60)                    AS GAP
              , ROW_NUMBER() OVER(PARTITION BY CT.CASE_NUMBER ORDER BY CH.CREATEDDATE) AS COVERAGE
              , IFF(CH.CREATEDDATE >= DATEADD('D', -30, CURRENT_DATE()),
                    DATEDIFF(S,
                             CREATEDDATE,
                             NVL(LEAD(CREATEDDATE) OVER(PARTITION BY CT.CASE_NUMBER
                                      ORDER BY CH.CREATEDDATE),
                                 CURRENT_TIMESTAMP())) / (24 * 60 * 60), NULL)
                                                                                       AS LAST_30_DAY_GAP
              , IFF(CH.CREATEDDATE >= DATEADD('D', -30, CURRENT_DATE()),
                    1,
                    NULL)
                                                                                       AS LAST_30_DAY_COVERAGE_TALLY
         FROM CASE_TABLE CT
                  LEFT OUTER JOIN
              RPT.V_SF_CASEHISTORY AS CH
              ON CH.CASEID = CT.CASE_ID
         WHERE RN = 1
         ORDER BY CASE_NUMBER, CREATEDDATE
     ),

     FULL_CASE AS (
         SELECT CASE_NUMBER
              , ANY_VALUE(SUBJECT)                                                                         AS SUBJECT
              , ANY_VALUE(SYSTEM_SIZE)                                                                     AS SYSTEM_SIZE
              , ANY_VALUE(STATUS)                                                                          AS STATUS1
              , ANY_VALUE(ORIGIN)                                                                          AS ORIGIN
              , ANY_VALUE(CREATED_DATE)                                                                    AS CREATED_DATE
              , ANY_VALUE(CLOSED_DATE)                                                                     AS CLOSED_DATE
              , ANY_VALUE(WIP_KPI)                                                                         AS WIP_KPI
              , ANY_VALUE(RECORD_TYPE)                                                                     AS RECORD_TYPE1
              , SOLAR_BILLING_ACCOUNT_NUMBER1
              , ANY_VALUE(RECORD_PRIORITY1)                                                                AS RECORD_PRIORITY
              , ANY_VALUE(DEFAULT_BUCKET)                                                                  AS DEFAULT_BUCKET1
              , CASE
                    WHEN STATUS1 IN ('In Progress') AND DEFAULT_BUCKET1 NOT IN ('MBW', 'Pending Legal')
                        THEN 'P4/P5/DRA Letters'
                    WHEN STATUS1 IN ('Pending Corporate Action', 'pending Customer Action') AND
                         DEFAULT_BUCKET1 NOT IN ('MBW', 'Pending Legal') THEN 'Working with Customer'
             END                                                                                           AS STATUS_BUCKET
              , AVG(GAP)                                                                                   AS AVERAGE_GAP
              , MAX(COVERAGE)                                                                              AS COVERAGE
              , AVG(LAST_30_DAY_GAP)                                                                       AS LAST_30_DAY_GAP
              , SUM(LAST_30_DAY_COVERAGE_TALLY)                                                            AS LAST_30_DAY_COVERAGE
              , ROW_NUMBER() OVER(PARTITION BY SOLAR_BILLING_ACCOUNT_NUMBER1 ORDER BY RECORD_PRIORITY ASC) AS RN
         FROM CASE_HISTORY_TABLE
         GROUP BY SOLAR_BILLING_ACCOUNT_NUMBER1
                , CASE_NUMBER
         ORDER BY SOLAR_BILLING_ACCOUNT_NUMBER1
     ),

     CASE_ACCOUNT_TABLE AS (
         SELECT *
         FROM FULL_CASE
         WHERE RN = 1
         ORDER BY 1
     )


SELECT STATUS_BUCKET
     , CASE
           WHEN STATUS_BUCKET = 'P4/P5/DRA Letters' THEN 1
           WHEN STATUS_BUCKET = 'Working with Customer' THEN 3
           ELSE 3
    END                          AS COVERAGE_GOAL
     , AVG(LAST_30_DAY_COVERAGE) AS COVERAGE
     , CASE
           WHEN STATUS_BUCKET = 'P4/P5/DRA Letters' THEN 10
           WHEN STATUS_BUCKET = 'Working with Customer' THEN 10
           ELSE 10
    END                          AS GAP_GOAL
     , AVG(LAST_30_DAY_GAP)      AS GAP_DAYS
FROM CASE_ACCOUNT_TABLE
GROUP BY STATUS_BUCKET
ORDER BY 1