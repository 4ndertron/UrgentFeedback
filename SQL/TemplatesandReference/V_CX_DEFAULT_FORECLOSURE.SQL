WITH employees AS -- Team Specific Members
    (SELECT e.EMPLOYEE_ID
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
            AND MGR_ID_5 <> '67600'
            -- Placeholder Manager (Tyler Anderson)
          GROUP BY EMPLOYEE_ID) ea
                              ON e.employee_id = ea.employee_id
     WHERE NOT e.TERMINATED
       AND e.PAY_RATE_TYPE = 'Hourly'
       AND e.MGR_ID_5 = '67600'
        -- Placeholder Manager (Tyler Anderson)
    )
   , cases AS -- Active Escalation Cases | Closed > Today - 90
    (SELECT c.project_id
          , c.OWNER_EMPLOYEE_ID
          , c.OWNER
          , c.STATUS
          , c.CREATED_DATE
          , c.CLOSED_DATE
     FROM rpt.T_CASE c
              LEFT JOIN rpt.T_PROJECT p
                        ON p.PROJECT_ID = c.PROJECT_ID
     WHERE c.RECORD_TYPE = 'Solar - Customer Default'
       AND C.SUBJECT NOT ILIKE '%D3%'
       AND C.PRIMARY_REASON = 'Foreclosure'
       AND (c.CLOSED_DATE >= current_date - 90 OR c.CLOSED_DATE IS NULL))
   , qa AS -- Calabrio QA Scores
    (SELECT DISTINCT p.EMPLOYEE_ID             AS agent_badge
                   , rc.AGENT_DISPLAY_ID       AS agent_evaluated
                   , rc.TEAM_NAME
                   , rc.EVALUATION_EVALUATED   AS evaluation_date
                   , rc.RECORDING_CONTACT_ID   AS contact_id
                   , rc.EVALUATION_TOTAL_SCORE AS qa_score
                   , rc.EVALUATOR_DISPLAY_ID   AS evaluator
                   , rc.EVALUATOR_USER_NAME    AS evaluator_email
     FROM CALABRIO.T_RECORDING_CONTACT rc
              LEFT JOIN CALABRIO.T_PERSONS p
                        ON p.ACD_ID = rc.AGENT_ACD_ID
     WHERE rc.EVALUATION_EVALUATED >= current_date - 90)
   , main AS -- Joins | Aggregations
    (SELECT row_number() over (partition by ca.OWNER order by ca.OWNER)   as rn
          , ca.OWNER
          , ca.OWNER_EMPLOYEE_ID
          , e.BUSINESS_TITLE
          , e.direct_manager
          , e.SUPERVISORY_ORG
          , sum(CASE WHEN ca.closed_date >= CURRENT_DATE - 90 THEN 1 END) AS closed
          , sum(CASE WHEN ca.CLOSED_DATE IS NULL THEN 1 END)              AS wip_count
          , round(avg(CASE
                          WHEN ca.CLOSED_DATE IS NULL
                              THEN datediff('m', ca.CREATED_DATE, current_timestamp) / 1440
            END), 2)                                                      AS avg_wip_cycle
          , round(median(CASE
                             WHEN ca.CLOSED_DATE IS NULL
                                 THEN datediff('m', ca.CREATED_DATE, current_timestamp) /
                                      1440
            END), 2)                                                      AS med_wip_cycle
          , round(avg(qa.qa_score), 2)                                    AS avg_qa_score
          , round(median(qa.qa_score), 2)                                 AS med_qa_score
     FROM employees e
              INNER JOIN cases ca
                         ON ca.OWNER_EMPLOYEE_ID = e.EMPLOYEE_ID
                             AND NVL(ca.CLOSED_DATE, current_timestamp) >= e.team_start_date
              LEFT JOIN qa
                        ON qa.agent_badge = e.EMPLOYEE_ID
                            AND qa.evaluation_date >= e.team_start_date
     GROUP BY ca.OWNER, ca.OWNER_EMPLOYEE_ID, e.BUSINESS_TITLE, e.direct_manager, e.SUPERVISORY_ORG)
SELECT *
FROM main m
WHERE (m.SUPERVISORY_ORG IS NULL AND m.wip_count IS NOT NULL)
   OR m.SUPERVISORY_ORG IS NOT NULL