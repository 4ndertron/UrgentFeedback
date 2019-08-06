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
            AND MGR_ID_6 <> '67600'
            -- Placeholder Manager (Tyler Anderson)
          GROUP BY EMPLOYEE_ID) ea
                              ON e.employee_id = ea.employee_id
     WHERE NOT e.TERMINATED
       AND e.PAY_RATE_TYPE = 'Hourly'
       AND e.MGR_ID_6 = '67600'
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
       AND c.CLOSED_DATE BETWEEN
         DATE_TRUNC('MM', DATEADD('MM', -1, CURRENT_DATE)) AND
         LAST_DAY(DATEADD('MM', -1, CURRENT_DATE)))
   , CASES_METRICS AS (
    SELECT row_number() OVER (PARTITION BY ca.OWNER ORDER BY ca.OWNER) AS rn
         , ca.OWNER
         , ca.OWNER_EMPLOYEE_ID
         , e.BUSINESS_TITLE
         , e.direct_manager
         , e.SUPERVISORY_ORG
         , sum(CASE
                   WHEN ca.closed_date BETWEEN
                            DATE_TRUNC('MM', DATEADD('MM', -1, CURRENT_DATE)) AND
                            LAST_DAY(DATEADD('MM', -1, CURRENT_DATE)) AND
                        CA.CLOSED_DATE IS NOT NULL
                       THEN 1 END)                                     AS closed
    FROM employees e
             INNER JOIN cases ca
                        ON ca.OWNER_EMPLOYEE_ID = e.EMPLOYEE_ID
                            AND NVL(ca.CLOSED_DATE, current_timestamp) >= e.team_start_date
    GROUP BY ca.OWNER, ca.OWNER_EMPLOYEE_ID, e.BUSINESS_TITLE, e.direct_manager, e.SUPERVISORY_ORG
)
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
     WHERE rc.EVALUATION_EVALUATED BETWEEN
               DATE_TRUNC('MM', DATEADD('MM', -1, CURRENT_DATE)) AND
               LAST_DAY(DATEADD('MM', -1, CURRENT_DATE)))
   , QA_METRICS AS (
    SELECT row_number() OVER (PARTITION BY E.FULL_NAME ORDER BY E.FULL_NAME) AS rn
         , e.FULL_NAME
         , e.EMPLOYEE_ID
         , e.BUSINESS_TITLE
         , e.DIRECT_MANAGER
         , e.SUPERVISORY_ORG
         , round(avg(qa.QA_SCORE), 2)                                        AS avg_qa_score
         , round(median(qa.QA_SCORE), 2)                                     AS med_qa_score
    FROM employees e
             LEFT JOIN qa
                       ON qa.agent_badge = e.EMPLOYEE_ID
                           AND qa.evaluation_date >= e.team_start_date
    GROUP BY e.full_name, e.employee_id, e.BUSINESS_TITLE, e.direct_manager, e.SUPERVISORY_ORG
)
   , main AS -- Joins | Aggregations
    (SELECT E.EMPLOYEE_ID
          , E.FULL_NAME                                       AS EMPLOYEE_NAME
          , E.TEAM_START_DATE
          , ca.closed                                         AS CLOSED_CASES
          , qa.avg_qa_score                                   AS QA
          , DATE_TRUNC('MM', DATEADD('MM', -1, CURRENT_DATE)) AS METRIC_MONTH
     FROM employees e
              INNER JOIN CASES_METRICS ca
                         ON ca.OWNER_EMPLOYEE_ID = e.EMPLOYEE_ID
              LEFT JOIN QA_METRICS as qa
                        ON qa.EMPLOYEE_ID = e.EMPLOYEE_ID
    )
SELECT *
FROM main
ORDER BY EMPLOYEE_NAME