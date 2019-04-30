/*
TODO: required fields
    Active Cases
    Avg Open Case Age
    Last 30 Day Coverage
    Last 30 Day Closed Case
    Last 30 Day Avg Daily Comments
*/

WITH ALL_DEFAULT AS (
    SELECT HR.FULL_NAME
         , ANY_VALUE(HR.FIRST_NAME)            AS FIRST_NAME
         , ANY_VALUE(HR.LAST_NAME)             AS LAST_NAME
         , ANY_VALUE(HR.SUPERVISOR_BADGE_ID_1) AS SUPERVISOR
         , MIN(HR.CREATED_DATE)                AS TEAM_START_DATE
         , MAX(HR.EXPIRY_DATE)                 AS TEAM_END_DATE
         , ANY_VALUE(HR.TERMINATED)            AS TERMINATED
         , ANY_VALUE(HR.SF_REP_ID)             AS SF_ID
    FROM HR.T_EMPLOYEE_ALL AS HR
    WHERE HR.SUPERVISORY_ORG = 'Executive Resolutions'
    GROUP BY HR.FULL_NAME
    ORDER BY TEAM_START_DATE DESC
)

SELECT *
FROM ALL_DEFAULT