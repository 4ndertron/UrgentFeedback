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
WITH ATTRITIONED AS (
    SELECT EMPLOYEE_ID
         , FULL_NAME
         , SUPERVISOR_NAME_1
         , SUPERVISORY_ORG
         , TERMINATION_DATE
         , TERMINATION_CATEGORY
         , TERMINATION_REASON
    FROM HR.T_EMPLOYEE
    WHERE MGR_ID_3 = 209122 -- Chuck Browne
      AND TERMINATED
      AND TERMINATION_DATE >= '2017-08-01'
    ORDER BY TERMINATION_DATE ASC
)

   , ALL_EMPLOYEES AS (
    SELECT EMPLOYEE_ID
         , ANY_VALUE(FULL_NAME)            AS FULL_NAME
         , ANY_VALUE(SUPERVISOR_NAME_1)    AS SUPERVISOR_NAME_1
         , ANY_VALUE(SUPERVISORY_ORG)      AS SUPERVISORY_ORG
         , MIN(HR.CREATED_DATE)            AS TEAM_START_DATE
         , MAX(HR.EXPIRY_DATE)             AS TEAM_END_DATE
         , ANY_VALUE(HIRE_DATE)            AS HIRE_DATE
         , ANY_VALUE(TERMINATION_DATE)     AS TERMINATION_DATE
         , ANY_VALUE(TERMINATION_CATEGORY) AS TERMINATION_CATEGORY
         , ANY_VALUE(TERMINATION_REASON)   AS TERMINATION_REASON
    FROM HR.T_EMPLOYEE_ALL AS HR
    WHERE MGR_ID_3 = 209122 -- Chuck Browne
    GROUP BY HR.EMPLOYEE_ID
    ORDER BY TEAM_START_DATE DESC
)

-- objectives table
/*
-- TODO: Build an agent "ION" graph.
-- TODO: Build an active volume line graph based on number of employees.
-- TODO: Figure out the formula for Attrition (how it's calculated)
    Average Number of Employees = (Active Volume on Start Date + Active Volume on End Date) / 2
    Attrition Rate = (Number of Attrition / Average Number of Employees) * 100
-- TODO: Figure out what YOY means
    Compare Attrition from Aug '17 - Mar '18 against Aug '18 - Mar '19
-- TODO: Get the number of terms that an agent had after they left.
-- TODO: Identify the difference between Transfer and termination.
-- TODO: Distinguish between voluntary and involuntary termination.
-- TODO: Get the average tenure of agents terminated in a given month
 */

   , ION_TABLE AS (
    SELECT DATE_TRUNC('MM', D.DT)                                         AS MONTH_1
         , E.SUPERVISORY_ORG
         , COUNT(CASE WHEN TO_DATE(E.HIRE_DATE) = D.DT THEN 1 END)        AS PRIORITY_CREATED
         , COUNT(CASE WHEN TO_DATE(E.TERMINATION_DATE) = D.DT THEN 1 END) AS PRIORITY_CLOSED
         , PRIORITY_CREATED - PRIORITY_CLOSED                             AS NET
    FROM RPT.T_dates AS D
       , ALL_EMPLOYEES AS E
    WHERE D.DT BETWEEN DATEADD('y', -1, DATE_TRUNC('MM', CURRENT_DATE())) AND CURRENT_DATE()
      AND E.SUPERVISORY_ORG IS NOT NULL
    GROUP BY MONTH_1
           , E.SUPERVISORY_ORG
    ORDER BY E.SUPERVISORY_ORG
           , MONTH_1
)

SELECT *
FROM ION_TABLE