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
      AND TERMINATION_DATE >= '2019-01-01'
    ORDER BY TERMINATION_DATE ASC
)
/*
-- TODO: Figure out what YOY means
    Compare Attrition from Aug '17 - Mar '18 against Aug '18 - Mar '19
-- TODO: Figure out the formula for Attrition (how it's calculated)
    Average Number of Employees = (Active Volume on Start Date + Active Volume on End Date) / 2
    Attrition Rate = (Number of Attrition / Average Number of Employees) * 100
-- TODO: Build an agent "inflow" graph based on hire date.
-- TODO: Build an active volume line graph based on number of employees.
-- TODO: Get the number of terms that an agent had after they left.
-- TODO: Identify the difference between Transfer and termination.
-- TODO: Distinguish between voluntary and involuntary termination.
-- TODO: Get the average tenure of agents terminated in a given month
 */

SELECT *
FROM ATTRITIONED
