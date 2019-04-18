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

SELECT *
FROM ENTIRE_HISTORY