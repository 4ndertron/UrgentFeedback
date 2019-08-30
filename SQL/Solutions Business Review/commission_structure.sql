/*
 If we were to give a bonus/pay of 0.5% for total amount saved. How much would that be for each agent? What would that
 look like for the entire group? By month.

 Play with the rate
 .005
 .0025
 .001


 Back to May, if not January.



 */

WITH CASE_TABLE AS (
    -- Collect all the open Default 1,2,4,5 and Escalation Cases in Salesforce --
    SELECT C.CASE_NUMBER
         , ANY_VALUE(C.CASE_ID)                            AS CASE_ID
         , C.PROJECT_ID
         , ANY_VALUE(C.OWNER)                              AS CASE_OWNER
         , ANY_VALUE(LDD.AGE)                              AS AGE_FOR_CASE
         , ANY_VALUE(C.SUBJECT)                            AS SUBJECT1
         , ANY_VALUE(CAD.SYSTEM_SIZE)                      AS SYSTEM_SIZE
         , ROUND(ANY_VALUE(CAD.SYSTEM_SIZE) * 1000 * 4, 2) AS SYSTEM_VALUE
         , ANY_VALUE(C.STATUS)                             AS STATUS2
         , ANY_VALUE(C.CREATED_DATE)                       AS CREATED_DATE1
         , ANY_VALUE(C.CLOSED_DATE)                        AS CLOSED_DATE1
         , ANY_VALUE(C.RECORD_TYPE)                        AS RECORD_TYPE1
         , ANY_VALUE(S.SOLAR_BILLING_ACCOUNT_NUMBER)       AS SOLAR_BILLING_ACCOUNT_NUMBER1
         , CASE
               WHEN STATUS2 = 'Escalated' AND RECORD_TYPE1 = 'Solar - Customer Default' THEN 'Pending Legal'
               WHEN RECORD_TYPE1 = 'Solar - Customer Default' AND ANY_VALUE(C.DESCRIPTION) ILIKE '%MBW%' THEN 'MBW'
               WHEN RECORD_TYPE1 = 'Solar - Customer Default' AND SUBJECT1 ILIKE '%D1%' THEN 'D1'
               WHEN RECORD_TYPE1 = 'Solar - Customer Default' AND SUBJECT1 ILIKE '%D2%' THEN 'D2'
               WHEN RECORD_TYPE1 = 'Solar - Customer Default' AND SUBJECT1 ILIKE '%D4%' THEN 'D4'
               WHEN RECORD_TYPE1 = 'Solar - Customer Default' AND SUBJECT1 ILIKE '%D5%' THEN 'D5'
               WHEN RECORD_TYPE1 = 'Solar - Customer Default' AND SUBJECT1 ILIKE '%CORP%' THEN 'CORP-Default'
        END                                                AS DEFAULT_BUCKET
         , CASE WHEN CLOSED_DATE1 IS NULL THEN 1 END       AS CASE_WIP_KPI
    FROM RPT.T_CASE AS C
             LEFT JOIN
         RPT.T_SERVICE AS S
         ON C.SERVICE_ID = S.SERVICE_ID

             LEFT JOIN
         RPT.T_SYSTEM_DETAILS_SNAP AS CAD
         ON CAD.SERVICE_ID = C.SERVICE_ID

             LEFT JOIN
         LD.T_DAILY_DATA_EXTRACT AS LDD
         ON LDD.BILLING_ACCOUNT = S.SOLAR_BILLING_ACCOUNT_NUMBER
    WHERE C.RECORD_TYPE = 'Solar - Customer Default'
      AND C.SUBJECT NOT LIKE '%D3%'
    GROUP BY C.PROJECT_ID
           , C.CASE_NUMBER
    ORDER BY C.PROJECT_ID
)

   , DEFAULT_AGENTS AS (
    SELECT e.EMPLOYEE_ID
         , e.FULL_NAME
         , e.BUSINESS_TITLE
         , e.SUPERVISOR_NAME_1 || ' (' || e.SUPERVISOR_BADGE_ID_1 || ')' AS direct_manager
         , e.SUPERVISORY_ORG
         , TO_DATE(NVL(ea.max_dt, e.HIRE_DATE))                          AS team_start_date
    FROM hr.T_EMPLOYEE e
             LEFT OUTER JOIN -- Determine last time each employee WASN'T on target manager's team
        (SELECT EMPLOYEE_ID
              , MAX(EXPIRY_DATE) AS max_dt
         FROM hr.T_EMPLOYEE_ALL
         WHERE NOT TERMINATED
           AND MGR_ID_6 <> '67600'
           -- Placeholder Manager (Tyler Anderson)
         GROUP BY EMPLOYEE_ID) ea
                             ON e.employee_id = ea.employee_id
    WHERE NOT e.TERMINATED
      AND e.PAY_RATE_TYPE = 'Hourly'
      AND e.MGR_ID_6 = '67600'
)

   , CASE_HISTORY_TABLE AS (
    SELECT CT.*
         , CH.CREATEDDATE
         , DATEDIFF(S, CREATEDDATE, NVL(LEAD(CREATEDDATE) OVER (PARTITION BY CT.CASE_NUMBER ORDER BY CH.CREATEDDATE),
                                        CURRENT_TIMESTAMP())) / (24 * 60 * 60)     AS GAP
         , ROW_NUMBER() OVER (PARTITION BY CT.CASE_NUMBER ORDER BY CH.CREATEDDATE) AS COVERAGE
         , IFF(CH.CREATEDDATE >= DATEADD('D', -30, CURRENT_DATE()),
               DATEDIFF(S,
                        CREATEDDATE,
                        NVL(LEAD(CREATEDDATE) OVER (PARTITION BY CT.CASE_NUMBER
                            ORDER BY CH.CREATEDDATE),
                            CURRENT_TIMESTAMP())) / (24 * 60 * 60), NULL)
                                                                                   AS LAST_30_DAY_GAP
         , IFF(CH.CREATEDDATE >= DATEADD('D', -30, CURRENT_DATE()),
               1,
               NULL)                                                               AS LAST_30_DAY_COVERAGE_TALLY
    FROM CASE_TABLE CT
             LEFT OUTER JOIN
         RPT.V_SF_CASEHISTORY AS CH
         ON CH.CASEID = CT.CASE_ID
    ORDER BY CASE_NUMBER, CREATEDDATE
)

   , FULL_CASE AS (
    SELECT CASE_NUMBER
         , ANY_VALUE(CASE_OWNER)             AS CASE_OWNER
         , ANY_VALUE(SUBJECT1)               AS SUBJECT
         , ANY_VALUE(SYSTEM_SIZE)            AS SYSTEM_SIZE
         , ANY_VALUE(SYSTEM_VALUE)           AS SYSTEM_VALUE
         , ANY_VALUE(STATUS2)                AS STATUS1
         , ANY_VALUE(AGE_FOR_CASE)           AS AGE_FOR_CASE
         , TO_DATE(ANY_VALUE(CREATED_DATE1)) AS CREATED_DATE
         , TO_DATE(ANY_VALUE(CLOSED_DATE1))  AS CLOSED_DATE
         , DATE_TRUNC('MM', CLOSED_DATE)     AS CLOSED_MONTH
         , ANY_VALUE(RECORD_TYPE1)           AS RECORD_TYPE1
         , SOLAR_BILLING_ACCOUNT_NUMBER1
         , ANY_VALUE(DEFAULT_BUCKET)         AS DEFAULT_BUCKET1
    FROM CASE_HISTORY_TABLE
    GROUP BY SOLAR_BILLING_ACCOUNT_NUMBER1
           , CASE_NUMBER
    ORDER BY SOLAR_BILLING_ACCOUNT_NUMBER1
           , CASE_NUMBER
)

/*
 Adjust Filings Query
 Adam
Business Review - Report Card
 */

SELECT *
FROM FULL_CASE AS FC
WHERE STATUS1 = 'Closed - Saved'
AND CLOSED_DATE BETWEEN
    DATE_TRUNC('Y', CURRENT_DATE) AND
    LAST_DAY(DATEADD('MM', -1, CURRENT_DATE))