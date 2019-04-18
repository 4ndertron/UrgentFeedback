WITH ENTIRE_HISTORY AS (
    SELECT WH.BADGE_ID
         , ANY_VALUE(WH.FIRST_NAME) || ' ' || ANY_VALUE(WH.LAST_NAME)      AS FULL_NAME
         , MIN(WH.WEEK_START)                                              AS START_WEEK
         , MAX(WH.WEEK_END)                                                AS END_WEEK
         , ANY_VALUE(WH.TEAM)                                              AS TEAM
         , WH.ORG
         , ANY_VALUE(WH.COST_CENTER)                                       AS COST_CODE
         , ROW_NUMBER() OVER(PARTITION BY WH.BADGE_ID ORDER BY START_WEEK) AS RN
         , CASE
               WHEN ANY_VALUE(WH.TEAM) IN
                    ('Business Analytics', 'Call & Workflow Quality Control', 'Central Scheduling', 'Click Support',
                     'Customer Experience', 'Customer Relations', 'Customer Service', 'Customer Solutions',
                     'Customer Success', 'Customer Success 1', 'Customer Success I', 'Customer Success II',
                     'Customer Support', 'Default Managers', 'Executive Resolutions', 'Inbound', 'PIO Ops', 'PMO/BA',
                     'Project Specialists', 'Sales Concierge', 'Scheduling', 'Solutions', 'Training', 'Transfers',
                     'WFM', 'Workforce Management', 'E-mail Administration')
                   AND COST_CODE LIKE ('%Customer Experience%', '%Solar Performance Corp%') THEN TRUE
               ELSE FALSE END                                              AS DIRECTOR_ORG
         , LEAD(DIRECTOR_ORG, 1, DIRECTOR_ORG) OVER(PARTITION BY WH.BADGE_ID
                ORDER BY START_WEEK)                                       AS NEXT_DIRECTOR
         , CASE
               WHEN RN = 1 AND DIRECTOR_ORG THEN TRUE
               WHEN DIRECTOR_ORG = NEXT_DIRECTOR THEN FALSE
               ELSE TRUE END                                               AS TRANSFER_FLAG
    FROM D_POST_INSTALL.T_WORKDAY_HISTORY AS WH
    GROUP BY WH.BADGE_ID
           , WH.ORG
    ORDER BY WH.BADGE_ID
           , START_WEEK
)
SELECT *
FROM ENTIRE_HISTORY
