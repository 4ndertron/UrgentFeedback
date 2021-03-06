WITH WORK_TABLE AS (
    SELECT DISTINCT U.NAME                          NAMES
                  , C.PROJECT_ID
                  , P.SOLAR_BILLING_ACCOUNT_NUMBER
                  , LD.AGE
                  , DATE_TRUNC(DAY, S.CREATEDATE)   CREATED_DATE
                  , DATE_TRUNC(D, C.CLOSED_DATE) AS CLOSED_DATE
    FROM RPT.V_SF_CASECOMMENT S
             INNER JOIN RPT.V_SF_USER U
                        ON U.ID = S.CREATEDBYID
             INNER JOIN RPT.T_CASE C
                        ON S.PARENTID = C.CASE_ID
             INNER JOIN RPT.T_PROJECT AS P
                        ON P.PROJECT_ID = C.PROJECT_ID
             INNER JOIN LD.T_DAILY_DATA_EXTRACT AS LD
                        ON LD.BILLING_ACCOUNT = P.SOLAR_BILLING_ACCOUNT_NUMBER
    where C.RECORD_TYPE = 'Solar - Customer Default'
    AND C.STATUS = 'Pending Customer Action'
    AND C.SUBJECT NOT ILIKE '%D3%'
)

   , RM_AGENTS AS (
    SELECT FULL_NAME
         , EMPLOYEE_ID
         , SUPERVISOR_NAME_1
         , SUPERVISORY_ORG
    FROM HR.T_EMPLOYEE AS HR
    WHERE HR.MGR_ID_5 = 101769
      AND NOT TERMINATED
)

   , T1 AS (
    SELECT *
    FROM WORK_TABLE AS WT
             LEFT JOIN
         RM_AGENTS AS A
         ON A.FULL_NAME = WT.names
)

   , AGENT_COVERAGE AS (
    SELECT D.DT
         , FULL_NAME
         , COUNT(CASE WHEN created_date = D.DT THEN 1 END) AS COVERAGE
    FROM T1
       , RPT.T_DATES AS D
    WHERE SUPERVISOR_NAME_1 = 'Brittany Percival'
      AND D.DT BETWEEN DATEADD('Y', -1, CURRENT_DATE) AND CURRENT_DATE
    GROUP BY D.DT
           , FULL_NAME
    ORDER BY D.DT DESC
)

   , AGENT_DELINQUENT_COVERAGE AS (
    SELECT D.DT
         , FULL_NAME
         , COUNT(CASE WHEN created_date = D.DT THEN 1 END) AS UPDATES
    FROM T1
       , RPT.T_DATES AS D
    WHERE SUPERVISOR_NAME_1 = 'Brittany Percival'
      AND D.DT BETWEEN DATE_TRUNC('Y', CURRENT_DATE) AND LAST_DAY(DATEADD('MM', -1, CURRENT_DATE))
--       AND AGE > 30
    GROUP BY D.DT
           , FULL_NAME
    ORDER BY D.DT DESC
)

   , CASE_WIP AS (
--        Broken
    SELECT D.DT
         , COUNT(CASE
                     WHEN TO_DATE(WT.created_date) < D.DT AND
                          (TO_DATE(WT.CLOSED_DATE) >= D.DT OR WT.CLOSED_DATE IS NULL) THEN 1 END) AS TOTAL_CASES
    FROM WORK_TABLE AS WT
       , RPT.T_DATES AS D
    WHERE D.DT BETWEEN DATEADD('Y', -1, CURRENT_DATE) AND CURRENT_DATE
    GROUP BY D.DT
    ORDER BY D.DT
)

   , COMPLETE_TABLE AS (
    SELECT LAST_DAY(DT)  AS MONTH
         , SUM(UPDATES) AS CASES_WORKED
    FROM AGENT_DELINQUENT_COVERAGE
    GROUP BY LAST_DAY(DT)
    ORDER BY MONTH
)

SELECT *
FROM COMPLETE_TABLE