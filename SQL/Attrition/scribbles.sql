/*
 The purpose of this file is to help identify when an employee is moved in or out of the director's org to help
 calculate attrition rate.
 */

WITH ENTIRE_TABLE AS (
    SELECT EMPLOYEE_ID
         , ANY_VALUE(FULL_NAME)                                              AS FULL_NAME
         , ANY_VALUE(SUPERVISOR_NAME_1)                                      AS SUPERVISOR_NAME_1
         , ANY_VALUE(SUPERVISORY_ORG)                                        AS SUPERVISORY_ORG
         , CASE WHEN ANY_VALUE(MGR_ID_3) = 209122 THEN 1 ELSE 0 END          AS DIRECTOR_KPI
         , MIN(HR.CREATED_DATE)                                              AS TEAM_START_DATE
         , MAX(HR.EXPIRY_DATE)                                               AS TEAM_END_DATE
         , ANY_VALUE(HIRE_DATE)                                              AS HIRE_DATE
         , ANY_VALUE(TERMINATION_DATE)                                       AS TERMINATION_DATE
         , DATEDIFF('MM', ANY_VALUE(HIRE_DATE), ANY_VALUE(TERMINATION_DATE)) AS DAY_TENURE
         , ANY_VALUE(TERMINATION_CATEGORY)                                   AS TERMINATION_CATEGORY
         , ANY_VALUE(TERMINATION_REASON)                                     AS TERMINATION_REASON
    FROM HR.T_EMPLOYEE_ALL AS HR
    GROUP BY HR.EMPLOYEE_ID
    ORDER BY TEAM_START_DATE DESC
)

SELECT *
FROM ENTIRE_TABLE