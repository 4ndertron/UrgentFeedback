WITH ENTIRE_HISTORY AS (
    SELECT HR.EMPLOYEE_ID
         , HR.SUPERVISORY_ORG
         , ANY_VALUE(COST_CENTER)                                                  AS COST_CENTER
         , ANY_VALUE(HR.FULL_NAME)                                                 AS FULL_NAME
         , ANY_VALUE(HR.SUPERVISOR_NAME_1)                                         AS SUPERVISOR_NAME_1
         , DATEDIFF('MM', ANY_VALUE(HR.HIRE_DATE),
                    NVL(ANY_VALUE(HR.TERMINATION_DATE), CURRENT_DATE()))           AS MONTH_TENURE
         , ANY_VALUE(HR.HIRE_DATE)                                                 AS HIRE_DATE
         , ANY_VALUE(HR.TERMINATION_DATE)                                          AS TERMINATION_DATE
         , ANY_VALUE(HR.TERMINATION_CATEGORY)                                      AS TERMINATION_CATEGORY
         , ANY_VALUE(HR.TERMINATION_REASON)                                        AS TERMINATION_REASON
         , MIN(HR.CREATED_DATE)                                                    AS TEAM_START_DATE
         -- Begin custom fields in the table
         , CASE
               WHEN MAX(HR.EXPIRY_DATE) >= CURRENT_DATE() AND ANY_VALUE(HR.TERMINATION_DATE) IS NOT NULL
                   THEN ANY_VALUE(HR.TERMINATION_DATE)
               ELSE MAX(HR.EXPIRY_DATE) END                                        AS TEAM_EXPIRY_DATE
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
--          , CASE
--                WHEN DIRECTOR_ORG = NEXT_DIRECTOR THEN FALSE
-- --                WHEN DIRECTOR_ORG AND TERMINATION_DATE IS NOT NULL THEN TRUE
--                ELSE TRUE END                                                       AS TRANSFER_OUT
         , CASE
               WHEN ANY_VALUE(TERMINATION_DATE) >= TEAM_EXPIRY_DATE AND NOT NEXT_DIRECTOR THEN TRUE
               WHEN DIRECTOR_ORG != NEXT_DIRECTOR THEN TRUE
               ELSE FALSE END                                                      AS TRANSFER
    FROM HR.T_EMPLOYEE_ALL AS HR
    WHERE HR.HIRE_DATE >= '2018-03-01'
    GROUP BY HR.EMPLOYEE_ID
           , HR.SUPERVISORY_ORG
    ORDER BY HR.EMPLOYEE_ID
           , TEAM_START_DATE DESC
)

   , CORE_TABLE AS (
    SELECT *
    FROM ENTIRE_HISTORY
    WHERE DIRECTOR_ORG
)

SELECT *
FROM CORE_TABLE
ORDER BY EMPLOYEE_ID
       , TEAM_START_DATE