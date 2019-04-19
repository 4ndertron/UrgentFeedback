WITH ENTIRE_HISTORY AS (
    SELECT HR.EMPLOYEE_ID
         , HR.SUPERVISORY_ORG
         , ANY_VALUE(COST_CENTER)                                                  AS COST_CENTER
         , ANY_VALUE(HR.FULL_NAME)                                                 AS FULL_NAME
         , ANY_VALUE(HR.SUPERVISOR_NAME_1)                                         AS SUPERVISOR_NAME_1
         , DATEDIFF('MM', ANY_VALUE(HR.HIRE_DATE),
                    NVL(ANY_VALUE(HR.TERMINATION_DATE), CURRENT_DATE()))           AS MONTH_TENURE1
         , ANY_VALUE(HR.HIRE_DATE)                                                 AS HIRE_DATE1
         , ANY_VALUE(HR.TERMINATED)                                                AS TERMINATED
         , ANY_VALUE(HR.TERMINATION_DATE)                                          AS TERMINATION_DATE
         , ANY_VALUE(HR.TERMINATION_CATEGORY)                                      AS TERMINATION_CATEGORY
         , ANY_VALUE(HR.TERMINATION_REASON)                                        AS TERMINATION_REASON
         , MIN(HR.CREATED_DATE)                                                    AS TEAM_START_DATE
         -- Begin custom fields IN the TABLE
         , CASE
               WHEN MAX(HR.EXPIRY_DATE) >= CURRENT_DATE() AND ANY_VALUE(HR.TERMINATION_DATE) IS NOT NULL
                   THEN ANY_VALUE(HR.TERMINATION_DATE)
               WHEN MAX(HR.EXPIRY_DATE) >= ANY_VALUE(HR.TERMINATION_DATE) THEN ANY_VALUE(HR.TERMINATION_DATE)
               ELSE MAX(HR.EXPIRY_DATE) END                                        AS TEAM_END_DATE1
         , ROW_NUMBER() OVER(PARTITION BY HR.EMPLOYEE_ID ORDER BY TEAM_START_DATE) AS RN
         , CASE
               WHEN ANY_VALUE(HR.COST_CENTER_ID) IN ('3400', '3700', '4967-60') THEN TRUE
               ELSE FALSE END                                                      AS DIRECTOR_ORG
         , LEAD(DIRECTOR_ORG, 1, DIRECTOR_ORG) OVER(PARTITION BY HR.EMPLOYEE_ID
                ORDER BY TEAM_START_DATE)                                          AS NEXT_DIRECTOR
         , CASE
               WHEN ANY_VALUE(TERMINATION_DATE) >= TEAM_END_DATE1 AND NOT NEXT_DIRECTOR THEN TRUE
               WHEN DIRECTOR_ORG != NEXT_DIRECTOR THEN TRUE
               ELSE FALSE END                                                      AS TRANSFER
    FROM HR.T_EMPLOYEE_ALL AS HR
    GROUP BY HR.EMPLOYEE_ID
           , HR.SUPERVISORY_ORG
    ORDER BY HR.EMPLOYEE_ID
           , TEAM_START_DATE DESC
)

   , CORE_TABLE AS (
    SELECT EMPLOYEE_ID
         , ANY_VALUE(COST_CENTER)                   AS COST_CENTER
         , ANY_VALUE(FULL_NAME)                     AS FULL_NAME
         , ANY_VALUE(SUPERVISOR_NAME_1)             AS SUPERVISOR_NAME_1
         , ANY_VALUE(HIRE_DATE1)                    AS HIRE_DATE
         , ANY_VALUE(TERMINATED)                    AS TERMINATED
         , ANY_VALUE(TERMINATION_DATE)              AS TERMINATION_DATE
         , ANY_VALUE(TERMINATION_CATEGORY)          AS TERMINATION_CATEGORY
         , ANY_VALUE(TERMINATION_REASON)            AS TERMINATION_REASON
         , MIN(TEAM_START_DATE)                     AS TEAM_START_DATE
         , MAX(TEAM_END_DATE1)                      AS TEAM_END_DATE
         , DATEDIFF('MM', HIRE_DATE, TEAM_END_DATE) AS MONTH_TENURE
         , MAX(RN)                                  AS RN
         , DIRECTOR_ORG
         , MAX(TRANSFER)                            AS TRANSFER
    FROM ENTIRE_HISTORY
    WHERE DIRECTOR_ORG
      AND TEAM_END_DATE1 >= TEAM_START_DATE
    GROUP BY EMPLOYEE_ID, DIRECTOR_ORG
)
   , ION_TABLE AS (
    SELECT LAST_DAY(D.DT)                                                                  AS MONTH_1
         , COUNT(CASE WHEN TO_DATE(CT.TEAM_START_DATE) = D.DT THEN 1 END)                  AS E_INFLOW
         , COUNT(CASE WHEN TO_DATE(CT.TEAM_END_DATE) = D.DT THEN 1 END)                    AS E_OUTFLOW
         , E_INFLOW - E_OUTFLOW                                                            AS ATTRITION
         , ROUND(AVG(CASE WHEN TO_DATE(CT.TEAM_END_DATE) = D.DT THEN MONTH_TENURE END), 2) AS AVG_OUTFLOW_TENURE
         , COUNT(CASE WHEN TO_DATE(CT.TEAM_END_DATE) = D.DT AND TRANSFER THEN 1 END)       AS TRANSFER_COUNT
    FROM RPT.T_dates AS D
       , CORE_TABLE AS CT
    WHERE D.DT BETWEEN DATEADD('Y', -2, DATE_TRUNC('MM', CURRENT_DATE())) AND CURRENT_DATE()
    GROUP BY MONTH_1
    ORDER BY MONTH_1
)

   , WIP_TABLE AS (
    SELECT D.DT
         , COUNT(CASE
                     WHEN CT.TEAM_START_DATE <= D.DT AND
                          (CT.TEAM_END_DATE > D.DT)
                         THEN 1 END)                                                  AS WIP
         , ROUND(AVG(WIP) OVER(PARTITION BY DATE_TRUNC('MM', D.DT) ORDER BY D.DT), 2) AS MONTH_AVG
    FROM RPT.T_dates AS D
       , CORE_TABLE AS CT
    WHERE D.DT BETWEEN DATEADD('Y', -2, DATE_TRUNC('MM', CURRENT_DATE())) AND CURRENT_DATE()
    GROUP BY D.DT
    ORDER BY D.DT
)

   , MONTH_MERGE AS (
    SELECT WT.DT
         , YEAR(WT.DT)                                       AS YEAR
         , MONTH(WT.DT)                                      AS MONTH
         , E_INFLOW                                          AS ORG_INFLOW
         , E_OUTFLOW                                         AS ORG_OUTFLOW
         , TRANSFER_COUNT
         , AVG_OUTFLOW_TENURE
         , ATTRITION
         , MONTH_AVG                                         AS MONTH_AVG_HEADCOUNT
         , ROUND((ATTRITION / MONTH_AVG_HEADCOUNT) * 100, 2) AS ATTRITION_RATE
         , WIP                                               AS ACTIVE_HEADCOUNT
    FROM WIP_TABLE AS WT
             INNER JOIN
         ION_TABLE AS IT
         ON IT.MONTH_1 = WT.DT
    WHERE WT.DT = LAST_DAY(WT.DT)
       OR WT.DT = CURRENT_DATE()
)

SELECT *
FROM MONTH_MERGE
ORDER BY DT