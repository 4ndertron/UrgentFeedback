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

   , CORE_TABLE AS (
    SELECT DATE_TRUNC('MM', D.DT)                                   AS MONTH1
         , DEFAULT_BUCKET1
         , COUNT(CASE WHEN TO_DATE(CREATED_DATE) = D.DT THEN 1 END) AS INFLOW
         , COUNT(CASE WHEN TO_DATE(CLOSED_DATE) = D.DT THEN 1 END)  AS OUTFLOW
         , INFLOW - OUTFLOW                                         AS NET
    FROM FULL_CASE AS FC
       , RPT.T_DATES AS D
    WHERE D.DT BETWEEN DATE_TRUNC('Y', DATEADD('Y', -1, DATE_TRUNC('MM', CURRENT_DATE()))) AND
              LAST_DAY(DATEADD('MM', -1, CURRENT_DATE()))
    GROUP BY MONTH1
           , DEFAULT_BUCKET1
    ORDER BY DEFAULT_BUCKET1
           , MONTH1
)

   , I_TABLE AS (
    SELECT MONTH1
         , YEAR(MONTH1)                                                    AS YEAR1
         , SUM(CASE WHEN DEFAULT_BUCKET1 = 'CORP-Default' THEN INFLOW END) AS DEFAULT_INFLOW
         , SUM(CASE WHEN DEFAULT_BUCKET1 = 'D1' THEN INFLOW END)           AS D1_INFLOW
         , SUM(CASE WHEN DEFAULT_BUCKET1 = 'D2' THEN INFLOW END)           AS D2_INFLOW
         , NVL(SUM(CASE WHEN DEFAULT_BUCKET1 = 'D4' THEN INFLOW END), 0)   AS D4_INFLOW
         , NVL(SUM(CASE WHEN DEFAULT_BUCKET1 = 'D5' THEN INFLOW END), 0)   AS D5_INFLOW
         , DEFAULT_INFLOW + D1_INFLOW + D2_INFLOW + D4_INFLOW + D5_INFLOW  AS TOTAL_INFLOW
    FROM CORE_TABLE AS CT
    WHERE DEFAULT_BUCKET1 IN ('BK', 'CORP-Default', 'CORP-No Case', 'D1', 'D2', 'D4', 'D5', 'FORE')
    GROUP BY MONTH1
    ORDER BY MONTH1
)

SELECT *
FROM I_TABLE