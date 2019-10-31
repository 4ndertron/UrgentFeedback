WITH CASE_TABLE AS (
    -- Collect all the open Default 1,2,4,5 and Escalation Cases in Salesforce --
    SELECT C.CASE_NUMBER
         , ANY_VALUE(C.CASE_ID)                            AS CASE_ID
         , C.PROJECT_ID
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

   , QA AS (
    SELECT DISTINCT p.EMPLOYEE_ID             AS agent_badge
                  , rc.AGENT_DISPLAY_ID       AS agent_evaluated
                  , rc.TEAM_NAME
                  , rc.EVALUATION_EVALUATED   AS evaluation_date
                  , rc.RECORDING_CONTACT_ID   AS contact_id
                  , rc.EVALUATION_TOTAL_SCORE AS qa_score
                  , rc.EVALUATOR_DISPLAY_ID   AS evaluator
                  , rc.EVALUATOR_USER_NAME    AS evaluator_email
    FROM CALABRIO.T_RECORDING_CONTACT rc
             LEFT JOIN CALABRIO.T_PERSONS p
                       ON p.ACD_ID = rc.AGENT_ACD_ID
    WHERE rc.EVALUATION_EVALUATED BETWEEN
              DATE_TRUNC('MM', DATEADD('MM', -1, CURRENT_DATE)) AND
              LAST_DAY(DATEADD('MM', -1, CURRENT_DATE))
)

   , QA_METRICS AS (
    SELECT AVG(QA.qa_score) AS AVG_QA
    FROM DEFAULT_AGENTS AS D
             LEFT JOIN QA
                       ON QA.agent_badge = D.EMPLOYEE_ID
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
               NULL)
                                                                                   AS LAST_30_DAY_COVERAGE_TALLY
    FROM CASE_TABLE CT
             LEFT OUTER JOIN
         RPT.V_SF_CASEHISTORY AS CH
         ON CH.CASEID = CT.CASE_ID
    ORDER BY CASE_NUMBER, CREATEDDATE
)

   , FULL_CASE AS (
    SELECT CASE_NUMBER
         , ANY_VALUE(SUBJECT1)             AS SUBJECT
         , ANY_VALUE(SYSTEM_SIZE)          AS SYSTEM_SIZE
         , ANY_VALUE(SYSTEM_VALUE)         AS SYSTEM_VALUE
         , ANY_VALUE(STATUS2)              AS STATUS1
         , ANY_VALUE(AGE_FOR_CASE)         AS AGE_FOR_CASE
         , ANY_VALUE(CREATED_DATE1)        AS CREATED_DATE
         , ANY_VALUE(CLOSED_DATE1)         AS CLOSED_DATE
         , ANY_VALUE(RECORD_TYPE1)         AS RECORD_TYPE1
         , SOLAR_BILLING_ACCOUNT_NUMBER1
         , ANY_VALUE(DEFAULT_BUCKET)       AS DEFAULT_BUCKET1
         , ANY_VALUE(CASE_WIP_KPI)         AS WIP_KPI
         , CASE
               WHEN STATUS1 IN ('In Progress') AND DEFAULT_BUCKET1 NOT IN ('MBW', 'Pending Legal')
                   THEN 'P4/P5/DRA Letters'
               WHEN STATUS1 IN ('Pending Corporate Action', 'Pending Customer Action') AND
                    DEFAULT_BUCKET1 NOT IN ('MBW', 'Pending Legal') THEN 'Working with Customer'
        END                                AS STATUS_BUCKET
         , AVG(GAP)                        AS AVERAGE_GAP
         , MAX(COVERAGE)                   AS COVERAGE
         , AVG(LAST_30_DAY_GAP)            AS LAST_30_DAY_GAP
         , SUM(LAST_30_DAY_COVERAGE_TALLY) AS LAST_30_DAY_COVERAGE
    FROM CASE_HISTORY_TABLE
    GROUP BY SOLAR_BILLING_ACCOUNT_NUMBER1
           , CASE_NUMBER
    ORDER BY SOLAR_BILLING_ACCOUNT_NUMBER1
           , CASE_NUMBER
)


   , MONTHLY_WIP AS (
    SELECT DATE_TRUNC('MM', D.DT)                                        AS MONTH1
         , SUM(CASE
                   WHEN TO_DATE(FC.CREATED_DATE) <= D.DT AND
                        (TO_DATE(FC.CLOSED_DATE) >= D.DT OR FC.CLOSED_DATE IS NULL OR WIP_KPI = 1)
                       THEN 1 END)                                       AS ALL_WIP
         , SUM(CASE
                   WHEN TO_DATE(FC.CREATED_DATE) <= D.DT AND
                        (TO_DATE(FC.CLOSED_DATE) >= D.DT OR FC.CLOSED_DATE IS NULL OR WIP_KPI = 1) AND
                        FC.DEFAULT_BUCKET1 = 'Pending Legal' THEN 1 END) AS Pending_Legal_WIP
         , SUM(CASE
                   WHEN TO_DATE(FC.CREATED_DATE) <= D.DT AND
                        (TO_DATE(FC.CLOSED_DATE) >= D.DT OR FC.CLOSED_DATE IS NULL OR WIP_KPI = 1) AND
                        FC.DEFAULT_BUCKET1 = 'D1' THEN 1 END)            AS D1_WIP
         , SUM(CASE
                   WHEN TO_DATE(FC.CREATED_DATE) <= D.DT AND
                        (TO_DATE(FC.CLOSED_DATE) >= D.DT OR FC.CLOSED_DATE IS NULL OR WIP_KPI = 1) AND
                        FC.DEFAULT_BUCKET1 = 'D2' THEN 1 END)            AS D2_WIP
         , SUM(CASE
                   WHEN TO_DATE(FC.CREATED_DATE) <= D.DT AND
                        (TO_DATE(FC.CLOSED_DATE) >= D.DT OR FC.CLOSED_DATE IS NULL OR WIP_KPI = 1) AND
                        FC.DEFAULT_BUCKET1 = 'D4' THEN 1 END)            AS D4_WIP
         , SUM(CASE
                   WHEN TO_DATE(FC.CREATED_DATE) <= D.DT AND
                        (TO_DATE(FC.CLOSED_DATE) >= D.DT OR FC.CLOSED_DATE IS NULL OR WIP_KPI = 1) AND
                        FC.DEFAULT_BUCKET1 = 'D5' THEN 1 END)            AS D5_WIP
         , SUM(CASE
                   WHEN TO_DATE(FC.CREATED_DATE) <= D.DT AND
                        (TO_DATE(FC.CLOSED_DATE) >= D.DT OR FC.CLOSED_DATE IS NULL OR WIP_KPI = 1) AND
                        FC.DEFAULT_BUCKET1 = 'CORP-Default' THEN 1 END)  AS CORP_Default_WIP
    FROM FULL_CASE AS FC
       , RPT.T_DATES AS D
    WHERE D.DT BETWEEN
              DATE_TRUNC('MM', DATEADD('MM', -13, CURRENT_DATE)) AND
              LAST_DAY(DATEADD('MM', -1, CURRENT_DATE))
    GROUP BY MONTH1
    ORDER BY MONTH1
)

   , ION AS (
    SELECT DATE_TRUNC('MM', D.DT)                                   AS MONTH1
         , COUNT(CASE WHEN TO_DATE(CREATED_DATE) = D.DT THEN 1 END) AS INFLOW
         , COUNT(CASE WHEN TO_DATE(CLOSED_DATE) = D.DT THEN 1 END)  AS OUTFLOW
         , INFLOW - OUTFLOW                                         AS NET
    FROM FULL_CASE AS SW
       , RPT.T_DATES AS D
    WHERE D.DT BETWEEN DATEADD('Y', -1, DATE_TRUNC('MM', CURRENT_DATE())) AND CURRENT_DATE()
    GROUP BY MONTH1
    ORDER BY MONTH1
)

   , CLOSED_CASES AS (
    SELECT ROUND(AVG(OUTFLOW), 0) AS AVERAGE_MONTHLY_CLOSED
    FROM ION
    WHERE MONTH1 BETWEEN DATEADD('MM', -3, DATE_TRUNC('MM', CURRENT_DATE))
              AND DATE_TRUNC('MM', CURRENT_DATE)
)

   , GAP AS (
    SELECT ROUND(AVG(LAST_30_DAY_GAP), 2) AS AVG_GAP
    FROM FULL_CASE
    WHERE CLOSED_DATE IS NULL
)

   , SAVED AS (
    SELECT SUM(SYSTEM_VALUE)   AS SAVED
         , COUNT(SYSTEM_VALUE) AS SAVED_CT
    FROM FULL_CASE
    WHERE STATUS1 = 'Closed - Saved'
      AND CLOSED_DATE BETWEEN DATEADD('MM', -1, DATE_TRUNC('MM', CURRENT_DATE())) AND DATE_TRUNC('MM', CURRENT_DATE())
)

   , LOST AS (
    SELECT SUM(SYSTEM_VALUE)   AS LOST
         , COUNT(SYSTEM_VALUE) AS LOST_CT
    FROM FULL_CASE
    WHERE STATUS1 = 'Closed'
      AND CLOSED_DATE BETWEEN DATEADD('MM', -1, DATE_TRUNC('MM', CURRENT_DATE())) AND DATE_TRUNC('MM', CURRENT_DATE())
)
   , FINAL AS (
    SELECT *
    FROM CLOSED_CASES,
         GAP,
         SAVED,
         LOST,
         QA_METRICS
)

SELECT MONTH1
     , ALL_WIP
FROM MONTHLY_WIP