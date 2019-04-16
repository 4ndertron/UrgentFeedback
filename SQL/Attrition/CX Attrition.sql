-- Research
-- ========
-- Where TERMINATED is true, TERMINATION_DATE, TERMINATION_CATEGORY, and TERMINATION_REASON are always filled
-- TERMINATION_COMPLETE_DATE can be filled when TERMINATED is false and null when TERMINATED is true
--     Only 3.7% of the time. Generally, TERMINATION_COMPLETE_DATE matches nullness of TERMINATION_DATE
-- Dates can be different though...
--     Looks like logic on TERMINATION_COMPLETED_DATE transcends the first row it applied to (it can be retroactively applied)
--         That is, it each EMPLOYEE_ID on HR.T_EMPLOYEE_ALL has only one distinct TERMINATION_COMPLETE_DATE
--     **Probably best to trust TERMINATION_DATE only**
--         Reached out to Jed's team via email to get distinction

-- Get termination dates and reasons for our org, YTD
WITH ALL_EMPLOYEES AS (
    SELECT EMPLOYEE_ID
         , ANY_VALUE(FULL_NAME)                                              AS FULL_NAME
         , ANY_VALUE(SUPERVISOR_NAME_1)                                      AS SUPERVISOR_NAME_1
         , ANY_VALUE(SUPERVISORY_ORG)                                        AS SUPERVISORY_ORG
         , MIN(HR.CREATED_DATE)                                              AS TEAM_START_DATE
         , MAX(HR.EXPIRY_DATE)                                               AS TEAM_END_DATE
         , ANY_VALUE(HIRE_DATE)                                              AS HIRE_DATE
         , ANY_VALUE(TERMINATION_DATE)                                       AS TERMINATION_DATE
         , DATEDIFF('MM', ANY_VALUE(HIRE_DATE), ANY_VALUE(TERMINATION_DATE)) AS DAY_TENURE
         , ANY_VALUE(TERMINATION_CATEGORY)                                   AS TERMINATION_CATEGORY
         , ANY_VALUE(TERMINATION_REASON)                                     AS TERMINATION_REASON
    FROM HR.T_EMPLOYEE_ALL AS HR
    WHERE MGR_ID_3 = 209122
    GROUP BY HR.EMPLOYEE_ID
    ORDER BY TEAM_START_DATE DESC
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
         , COUNT(CASE WHEN TO_DATE(E.HIRE_DATE) = D.DT THEN 1 END)        AS E_INFLOW
         , COUNT(CASE WHEN TO_DATE(E.TERMINATION_DATE) = D.DT THEN 1 END) AS E_OUTFLOW
         , E_INFLOW - E_OUTFLOW                                           AS NET
    FROM RPT.T_dates AS D
       , ALL_EMPLOYEES AS E
    WHERE D.DT BETWEEN '2017-08-01' AND CURRENT_DATE()
      AND E.SUPERVISORY_ORG IS NOT NULL
    GROUP BY MONTH_1
    ORDER BY MONTH_1
)

   , WIP_TABLE AS (
    SELECT D.DT
         , COUNT(CASE
                     WHEN E.HIRE_DATE <= D.DT AND
                          (E.TERMINATION_DATE >= D.DT OR E.TERMINATION_DATE IS NULL)
                         THEN 1 END)                                                  AS WIP
         , ROUND(AVG(WIP) OVER(PARTITION BY DATE_TRUNC('MM', D.DT) ORDER BY D.DT), 2) AS MONTH_AVG
    FROM RPT.T_dates AS D
       , ALL_EMPLOYEES AS E
    WHERE D.DT BETWEEN '2017-08-01' AND CURRENT_DATE()
      AND E.SUPERVISORY_ORG IS NOT NULL
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
FROM MONTH_MERGE
ORDER BY DT
;