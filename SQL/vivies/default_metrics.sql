WITH AGENT_TABLE AS (
    SELECT e.EMPLOYEE_ID
         , e.FULL_NAME
         , e.BUSINESS_TITLE
         , e.SUPERVISOR_NAME_1 || ' (' || e.SUPERVISOR_BADGE_ID_1 || ')' AS direct_manager
         , e.SUPERVISORY_ORG
         , TO_DATE(NVL(ea.max_dt, e.HIRE_DATE))                          AS team_start_date
    FROM hr.T_EMPLOYEE e
             LEFT OUTER JOIN -- Determine last time each employee WASN'T on target manager's team
        (SELECT EMPLOYEE_ID
              , MAX(EXPIRY_DATE) AS max_dt
         FROM hr.T_EMPLOYEE_ALL
         WHERE NOT TERMINATED
           AND MGR_ID_6 <> '67600'
           -- Placeholder Manager (Tyler Anderson)
         GROUP BY EMPLOYEE_ID) ea
                             ON e.employee_id = ea.employee_id
    WHERE NOT e.TERMINATED
      AND e.PAY_RATE_TYPE = 'Hourly'
      AND e.MGR_ID_6 = '67600'
      AND E.BUSINESS_TITLE ILIKE '%DEFAULT MANAGER%'
)

   , RANKING_TABLE AS (
    SELECT DE.FULL_NAME
         , DE.EMPLOYEE_ID
         , DE.EFFECTIVENESS
         , DE.EFFICIENCY
         , DE.QUALITY
         , POW(PERCENT_RANK() OVER (ORDER BY NVL(DE.EFFECTIVENESS, 0) ASC), 1) AS EFFECTIVENESS_RANK
         , POW(PERCENT_RANK() OVER (ORDER BY DE.EFFICIENCY DESC), 1)           AS EFFICIENCY_RANK
         , POW(PERCENT_RANK() OVER (ORDER BY NVL(DE.QUALITY, 0) ASC), 1)       AS QUALITY_RANK
         , (EFFECTIVENESS_RANK + EFFICIENCY_RANK + QUALITY_RANK) / 3           AS WEIGHTED_RANK
    FROM D_POST_INSTALL.V_CX_KPIS_DEFAULT AS DE
             LEFT JOIN AGENT_TABLE AS AT
                       ON AT.EMPLOYEE_ID = DE.EMPLOYEE_ID
    WHERE AT.EMPLOYEE_ID IS NOT NULL
)

   , MAIN AS (
    SELECT *
    FROM RANKING_TABLE
    ORDER BY WEIGHTED_RANK DESC
    LIMIT 5
)

   , TEST_CTE AS (
    SELECT *
    FROM AGENT_TABLE
)

SELECT *
FROM MAIN
