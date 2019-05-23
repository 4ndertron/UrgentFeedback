WITH WORK_TABLE AS (
    select distinct u.name                        names
                  , c.project_id
                  , P.SOLAR_BILLING_ACCOUNT_NUMBER
                  , LD.AGE
                  , date_trunc(day, s.createdate) created_date
    from rpt.v_sf_casecomment s
             inner join rpt.v_sf_user u
                        on u.id = s.createdbyid
             inner join rpt.t_case c
                        on s.parentid = c.case_id
             INNER JOIN RPT.T_PROJECT AS P
                        ON P.PROJECT_ID = C.PROJECT_ID
             INNER JOIN LD.T_DAILY_DATA_EXTRACT AS LD
                        ON LD.BILLING_ACCOUNT = P.SOLAR_BILLING_ACCOUNT_NUMBER
    where (c.subject ilike 'D3%'
        or c.subject ilike 'corp%')
      and date_trunc(day, s.createdate) between
        to_date(dateadd('Y', -1, current_date)) and current_date
)

   , RM_AGENTS AS (
    SELECT FULL_NAME
         , EMPLOYEE_ID
         , SUPERVISOR_NAME_1
         , SUPERVISORY_ORG
    FROM HR.T_EMPLOYEE AS HR
    WHERE HR.MGR_ID_4 = 101769
      AND NOT TERMINATED
)

   , DELINQUENT_ACCOUNTS AS (
    SELECT BILLING_ACCOUNT
         , AGE
         , AGE_GROUP
    FROM LD.T_DAILY_DATA_EXTRACT AS LD
    WHERE LD.AGE > 30
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
         , COUNT(CASE WHEN created_date = D.DT THEN 1 END) AS COVERAGE
    FROM T1
       , RPT.T_DATES AS D
    WHERE SUPERVISOR_NAME_1 = 'Brittany Percival'
      AND D.DT BETWEEN '2019-04-01' AND '2019-05-01'
      AND AGE > 30
    GROUP BY D.DT
           , FULL_NAME
    ORDER BY D.DT DESC
)

   , CASE_WIP AS (
    SELECT *
    FROM WORK_TABLE
       , RPT.T_DATES AS D
    WHERE D.DT BETWEEN DATEADD('Y', -1, CURRENT_DATE) AND CURRENT_DATE
)

SELECT *
FROM AGENT_COVERAGE