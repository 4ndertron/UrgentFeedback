SELECT EMPLOYEE_ID
     , ANY_VALUE(COST_CENTER)                   AS COST_CENTER
     , ANY_VALUE(FULL_NAME)                     AS FULL_NAME
     , ANY_VALUE(POSITION_TITLE)                AS POSITION_TITLE
     , ANY_VALUE(SUPERVISOR_NAME_1)             AS DIRECT_MANAGER
     , ANY_VALUE(SUPERVISORY_ORG)               AS SUPERVISORY_ORG
     , ANY_VALUE(HIRE_DATE1)                    AS HIRE_DATE
     , ANY_VALUE(TERMINATED)                    AS TERMINATED
     , ANY_VALUE(TERMINATION_DATE)              AS TERMINATION_DATE
     , ANY_VALUE(TERMINATION_CATEGORY)          AS TERMINATION_CATEGORY
     , ANY_VALUE(TERMINATION_REASON)            AS TERMINATION_REASON
     , MIN(TEAM_START_DATE)                     AS TEAM_START_DATE
     , MAX(TEAM_END_DATE1)                      AS TEAM_END_DATE
     , DATEDIFF('MM', HIRE_DATE, TEAM_END_DATE) AS MONTH_TENURE
     , MAX(RN)                                  AS RN
     , DIRECTOR_ORG
     , '<img style="max-height:300px;max-width:300px;height:auto;width:auto;" src="https://46nsgon4l7.execute-api.us-west-2.amazonaws.com/prod/tms/workday-photo/' ||
       EMPLOYEE_ID || '">'                      as EMPLOYEE_PHOTO
     , MAX(TRANSFER)                            AS TRANSFER
FROM (
         SELECT HR.EMPLOYEE_ID
              , HR.SUPERVISORY_ORG
              , ANY_VALUE(COST_CENTER)                                                   AS COST_CENTER
              , ANY_VALUE(HR.FULL_NAME)                                                  AS FULL_NAME
              , ANY_VALUE(HR.POSITION_TITLE)                                             AS POSITION_TITLE
              , ANY_VALUE(HR.SUPERVISOR_NAME_1)                                          AS SUPERVISOR_NAME_1
              , DATEDIFF('MM', ANY_VALUE(HR.HIRE_DATE),
                         NVL(ANY_VALUE(HR.TERMINATION_DATE), CURRENT_DATE()))            AS MONTH_TENURE1
              , ANY_VALUE(HR.HIRE_DATE)                                                  AS HIRE_DATE1
              , ANY_VALUE(HR.TERMINATED)                                                 AS TERMINATED
              , ANY_VALUE(HR.TERMINATION_DATE)                                           AS TERMINATION_DATE
              , ANY_VALUE(HR.TERMINATION_CATEGORY)                                       AS TERMINATION_CATEGORY
              , ANY_VALUE(HR.TERMINATION_REASON)                                         AS TERMINATION_REASON
              , MIN(HR.CREATED_DATE)                                                     AS TEAM_START_DATE
              -- Begin custom fields IN the TABLE
              , CASE
                    WHEN MAX(HR.EXPIRY_DATE) >= CURRENT_DATE() AND ANY_VALUE(HR.TERMINATION_DATE) IS NOT NULL
                        THEN ANY_VALUE(HR.TERMINATION_DATE)
                    WHEN MAX(HR.EXPIRY_DATE) >= ANY_VALUE(HR.TERMINATION_DATE)
                        THEN ANY_VALUE(HR.TERMINATION_DATE)
                    ELSE MAX(HR.EXPIRY_DATE) END                                         AS TEAM_END_DATE1
              , ROW_NUMBER() OVER (PARTITION BY HR.EMPLOYEE_ID ORDER BY TEAM_START_DATE) AS RN
              , IFF(ANY_VALUE(HR.COST_CENTER_ID) IN
                    ('3400', '3700', '4967-60'), TRUE, FALSE)                            AS DIRECTOR_ORG
              , LEAD(DIRECTOR_ORG, 1, DIRECTOR_ORG) OVER (PARTITION BY HR.EMPLOYEE_ID
             ORDER BY TEAM_START_DATE)                                                   AS NEXT_DIRECTOR
              , CASE
                    WHEN ANY_VALUE(HR.TERMINATION_DATE) >= TEAM_END_DATE1 AND NOT NEXT_DIRECTOR THEN TRUE
                    WHEN DIRECTOR_ORG != NEXT_DIRECTOR THEN TRUE
                    ELSE FALSE END                                                       AS TRANSFER
         FROM HR.T_EMPLOYEE_ALL AS HR
         GROUP BY HR.EMPLOYEE_ID
                , HR.SUPERVISORY_ORG
         ORDER BY HR.EMPLOYEE_ID
                , TEAM_START_DATE DESC
     ) AS ENTIRE_HISTORY
WHERE DIRECTOR_ORG
  AND TEAM_END_DATE1 >= TEAM_START_DATE
  AND ENTIRE_HISTORY.SUPERVISOR_NAME_1 ILIKE '%PERC%'
  AND TEAM_END_DATE1 >= CURRENT_DATE
GROUP BY EMPLOYEE_ID, DIRECTOR_ORG