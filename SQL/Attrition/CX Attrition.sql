WITH ENTIRE_HISTORY AS (
    SELECT HR.EMPLOYEE_ID
         , HR.SUPERVISORY_ORG
         , ANY_VALUE(COST_CENTER)                                                  AS COST_CENTER
         , ANY_VALUE(HR.FULL_NAME)                                                 AS FULL_NAME
         , ANY_VALUE(HR.SUPERVISOR_NAME_1)                                         AS SUPERVISOR_NAME_1
         , MIN(HR.CREATED_DATE)                                                    AS TEAM_START_DATE
         , MAX(HR.EXPIRY_DATE)                                                     AS TEAM_EXPIRY_DATE
         , ANY_VALUE(HR.HIRE_DATE)                                                 AS HIRE_DATE
         , ANY_VALUE(HR.TERMINATION_DATE)                                          AS TERMINATION_DATE
         , DATEDIFF('MM', ANY_VALUE(HR.HIRE_DATE),
                    NVL(ANY_VALUE(HR.TERMINATION_DATE), CURRENT_DATE()))           AS MONTH_TENURE
         , ANY_VALUE(HR.TERMINATION_CATEGORY)                                      AS TERMINATION_CATEGORY
         , ANY_VALUE(HR.TERMINATION_REASON)                                        AS TERMINATION_REASON
         -- Begin custom fields in the table
         , ROW_NUMBER() OVER(PARTITION BY HR.EMPLOYEE_ID ORDER BY TEAM_START_DATE) AS RN
         , CASE
               WHEN ANY_VALUE(HR.SUPERVISORY_ORG) IN
                    ('Business Analytics', 'Call & Workflow Quality Control', 'Central Scheduling', 'Click Support',
                     'Customer Experience', 'Customer Relations', 'Customer Service', 'Customer Solutions',
                     'Customer Success', 'Customer Success 1', 'Customer Success I', 'Customer Success II',
                     'Customer Support', 'Default Managers', 'Executive Resolutions', 'Inbound', 'PIO Ops', 'PMO/BA',
                     'Project Specialists', 'Scheduling', 'Solutions', 'Training', 'Transfers',
                     'WFM', 'Workforce Management', 'E-mail Administration') THEN TRUE
               WHEN ANY_VALUE(HR.COST_CENTER) IN ('Solar Customer Experience', 'Solar Performance Corp') THEN TRUE
               ELSE FALSE END                                                      AS DIRECTOR_ORG
         , LEAD(DIRECTOR_ORG, 1, DIRECTOR_ORG) OVER(PARTITION BY HR.EMPLOYEE_ID
                ORDER BY TEAM_START_DATE)                                          AS NEXT_DIRECTOR
         , CASE
               WHEN DIRECTOR_ORG = NEXT_DIRECTOR THEN FALSE
               WHEN DIRECTOR_ORG AND TERMINATION_DATE IS NOT NULL THEN TRUE
               ELSE TRUE END                                                       AS TRANSFER_OUT
--     , NVL(ANY_VALUE(HR.TERMINATION_DATE),TEAM_EXPIRY_DATE) AS TRANSFER_OUT_DATE
    FROM HR.T_EMPLOYEE_ALL AS HR
    GROUP BY HR.EMPLOYEE_ID
           , HR.SUPERVISORY_ORG
    ORDER BY HR.EMPLOYEE_ID
           , TEAM_START_DATE
)

   , TRANSFER_HISTORY AS (
    SELECT *
    FROM ENTIRE_HISTORY
    WHERE TRANSFER_OUT
)

   , CORE_TABLE AS (
    SELECT EMPLOYEE_ID
         , FULL_NAME
         , SUPERVISOR_NAME_1
         , SUPERVISORY_ORG
         , TEAM_START_DATE
         , NVL(TERMINATION_DATE, TEAM_EXPIRY_DATE) AS TEAM_END_DATE
         , MONTH_TENURE
         , TERMINATION_CATEGORY
    FROM TRANSFER_HISTORY AS T
)

-- objectives table
/*
-- X TODO: Build an agent "ION" graph.
-- X TODO: Build an active volume line graph based on number of employees.
-- X TODO: Figure out the formula for Attrition (how it's calculated)
        Average Number of Employees = (Active Volume on Start Date + Active Volume on End Date) / 2
        Attrition Rate = (Number of Attrition / Average Number of Employees) * 100
-- X TODO: Figure out what YOY means (Year over Year)
        Compare Attrition from Aug '17 - Mar '18 against Aug '18 - Mar '19
-- TODO: Get the number of terms that an agent had after they left.
-- TODO: Identify the difference between Transfer and termination.
-- X TODO: Distinguish between voluntary and involuntary termination.
-- TODO: Get the average tenure of agents terminated in a given month
 */

   , ION_TABLE AS (
    SELECT LAST_DAY(D.DT)                                                 AS MONTH_1
         , COUNT(CASE WHEN TO_DATE(CT.TEAM_START_DATE) = D.DT THEN 1 END) AS E_INFLOW
         , COUNT(CASE WHEN TO_DATE(CT.TEAM_END_DATE) = D.DT THEN 1 END)   AS E_OUTFLOW
         , E_INFLOW - E_OUTFLOW                                           AS NET
    FROM RPT.T_dates AS D
       , CORE_TABLE AS CT
    WHERE D.DT BETWEEN '2017-08-01' AND CURRENT_DATE()
      AND CT.SUPERVISORY_ORG IS NOT NULL
    GROUP BY MONTH_1
    ORDER BY MONTH_1
)

   , WIP_TABLE AS (
    SELECT D.DT
         , COUNT(CASE
                     WHEN CT.TEAM_START_DATE <= D.DT AND
                          (CT.TEAM_END_DATE >= D.DT OR CT.TEAM_END_DATE >= CURRENT_DATE())
                         THEN 1 END)                                                  AS WIP
         , ROUND(AVG(WIP) OVER(PARTITION BY DATE_TRUNC('MM', D.DT) ORDER BY D.DT), 2) AS MONTH_AVG
    FROM RPT.T_dates AS D
       , CORE_TABLE AS CT
    WHERE D.DT BETWEEN '2017-08-01' AND CURRENT_DATE()
      AND CT.SUPERVISORY_ORG IS NOT NULL
    GROUP BY D.DT
    ORDER BY D.DT
)

   , MONTH_MERGE AS (
    SELECT WT.DT
         , E_INFLOW                                          AS HIRED_INFLOW
         , E_OUTFLOW                                         AS HIRED_TERMINATIONS
         , E_OUTFLOW - E_INFLOW                              AS ATTRITION
         , WIP                                               AS ACTIVE_HEADCOUNT
         , MONTH_AVG                                         AS MONTH_AVG_HEADCOUNT
         , ROUND((ATTRITION / MONTH_AVG_HEADCOUNT) * 100, 2) AS ATTRITION_RATE
    FROM WIP_TABLE AS WT
             INNER JOIN
         ION_TABLE AS IT
         ON IT.MONTH_1 = WT.DT
    WHERE WT.DT = LAST_DAY(WT.DT)
)

SELECT *
FROM ENTIRE_HISTORY
;