/*
 The purpose of this file is to help identify when an employee is moved in or out of the director's org to help
 calculate attrition rate.
 */
WITH CX_ORGS AS (
    SELECT DISTINCT SUPERVISORY_ORG
    FROM HR.T_EMPLOYEE_ALL
    WHERE MGR_ID_3 = 209122
)

   , ENTIRE_TABLE AS (
    SELECT HR.EMPLOYEE_ID
         , HR.SUPERVISORY_ORG
         , ANY_VALUE(HR.FULL_NAME)                                                 AS FULL_NAME
         , ANY_VALUE(HR.SUPERVISOR_NAME_1)                                         AS SUPERVISOR_NAME_1
         , CASE
               WHEN ANY_VALUE(HR.SUPERVISORY_ORG) IN
                    ('Incentive Processing', 'New England, Grit & Movement MA, CT, NH, RI, VT, NY, NJ, PA',
                     'Customer Success II', 'Business Analytics', 'Meter Readers (East)', 'Workforce Management',
                     'PMO/BA', 'Operations Support', 'PIO Ops', 'Permit Specialists (SOL-CA-26 Palo Alto)', 'Inbound',
                     'E-mail Administration', 'Billing', 'Customer Relations', 'Incentives Processing',
                     'Sales Concierge', 'Transfers', 'Central Scheduling', 'Training', 'RECs & Rebates', 'Solutions',
                     'Customer Solutions', 'Call & Workflow Quality Control', 'Executive Resolutions',
                     'Customer Success', 'Click Support', 'Customer Experience', 'Scheduling', 'Customer Service',
                     'Customer Success I', 'Customer Support', 'Project Specialists',
                     'Operations Support Project Management', 'SREC Incentives', 'Default Managers',
                     'Customer Success 1', 'WFM') THEN 1
               ELSE 0 END                                                          AS DIRECTOR_KPI
         , MIN(HR.CREATED_DATE)                                                    AS TEAM_START_DATE
         , MAX(HR.EXPIRY_DATE)                                                     AS TEAM_END_DATE
         , ANY_VALUE(HR.HIRE_DATE)                                                 AS HIRE_DATE
         , ANY_VALUE(HR.TERMINATION_DATE)                                          AS TERMINATION_DATE
         , DATEDIFF('MM', ANY_VALUE(HR.HIRE_DATE), ANY_VALUE(HR.TERMINATION_DATE)) AS DAY_TENURE
         , ANY_VALUE(HR.TERMINATION_CATEGORY)                                      AS TERMINATION_CATEGORY
         , ANY_VALUE(HR.TERMINATION_REASON)                                        AS TERMINATION_REASON
    FROM HR.T_EMPLOYEE_ALL AS HR
--              INNER JOIN CX_ORGS AS ORG
--                         ON HR.SUPERVISORY_ORG = ORG.SUPERVISORY_ORG
--     WHERE HR.SUPERVISORY_ORG IS NOT NULL
    GROUP BY HR.EMPLOYEE_ID
           , HR.SUPERVISORY_ORG
    ORDER BY TEAM_START_DATE DESC
)


SELECT COUNT(*), SUM(DIRECTOR_KPI)
FROM ENTIRE_TABLE

-- SELECT *
-- FROM CX_ORGS