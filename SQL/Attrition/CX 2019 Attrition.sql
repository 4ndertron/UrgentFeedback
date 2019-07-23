-- Research
-- ========
-- Where TERMINATED is true, TERMINATION_DATE, TERMINATION_CATEGORY, and TERMINATION_REASON are always filled
-- TERMINATION_COMPLETE_DATE can be filled when TERMINATED is false and null when TERMINATED is true
-- Only 3.7% of the time. Generally, TERMINATION_COMPLETE_DATE matches nullness of TERMINATION_DATE
-- Dates can be different though...
-- Looks like logic on TERMINATION_COMPLETED_DATE transcends the first row it applied to (it can be retroactively applied)
-- That is, it each EMPLOYEE_ID on HR.T_EMPLOYEE_ALL has only one distinct TERMINATION_COMPLETE_DATE
-- **Probably best to trust TERMINATION_DATE only**
-- Reached out to Jed's team via email to get distinction

-- Get termination dates and reasons for our org, YTD
SELECT employee_id
     , full_name
     , supervisor_name_1
     , supervisory_org
    //
     , terminated
     , termination_date
    //
     , termination_complete_date
     , termination_category
     , termination_reason
    //
     , created_date
    //
     , expiry_date
FROM hr.t_employee
WHERE MGR_ID_4 = 209122 -- Chuck Browne
  AND terminated
  AND termination_date >= '2019-01-01'
ORDER BY termination_date ASC
;