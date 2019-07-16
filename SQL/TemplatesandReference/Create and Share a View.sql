CREATE OR REPLACE VIEW D_POST_INSTALL.V_CX_DEFAULT AS ( -- Create the view
    WITH cases AS -- Active Escalation Cases | Closed > Today - 90
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
--        AND c.STATUS != 'In Dispute'
--        AND c.EXECUTIVE_RESOLUTIONS_ACCEPTED IS NOT NULL
           AND (c.CLOSED_DATE >= current_date - 90 OR c.CLOSED_DATE IS NULL))
       , employees AS -- Team Specific Members
        (SELECT e.BADGE_ID
              , e.FULL_NAME
              , e.BUSINESS_TITLE
              , e.SUPERVISOR_NAME_1 || ' (' || e.SUPERVISOR_BADGE_ID_1 || ')' AS direct_manager
              , e.SUPERVISORY_ORG
         FROM hr.T_EMPLOYEE e
         WHERE e.TERMINATED = FALSE
           AND e.PAY_RATE_TYPE = 'Hourly'
           AND e.MGR_ID_5 = '67600')
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
        (SELECT ca.OWNER
              , ca.OWNER_EMPLOYEE_ID
              , e.BUSINESS_TITLE
              , e.direct_manager
              , e.SUPERVISORY_ORG
              , sum(CASE WHEN ca.closed_date >= current_date - 90 THEN 1 END) AS closed
              , sum(CASE WHEN ca.CLOSED_DATE IS NULL THEN 1 END)              AS wip_count
              , round(avg(CASE
                              WHEN ca.CLOSED_DATE IS NULL
                                  THEN datediff('m', ca.CREATED_DATE, current_date) / 1440
                END), 2)                                                      AS avg_wip_cycle
              , round(median(CASE
                                 WHEN ca.CLOSED_DATE IS NULL
                                     THEN datediff('m', ca.CREATED_DATE, current_date) / 1440
                END), 2)                                                      AS med_wip_cycle
              , round(avg(qa.qa_score), 2)                                    AS avg_qa_score
              , round(median(qa.qa_score), 2)                                 AS med_qa_score
         FROM cases ca
                  LEFT JOIN employees e
                            ON e.BADGE_ID = ca.OWNER_EMPLOYEE_ID
                  LEFT JOIN qa
                            ON qa.agent_badge = ca.OWNER_EMPLOYEE_ID
         GROUP BY ca.OWNER, ca.OWNER_EMPLOYEE_ID, e.BUSINESS_TITLE, e.direct_manager, e.SUPERVISORY_ORG)
    SELECT *
    FROM main m
    WHERE (m.SUPERVISORY_ORG IS NULL AND m.wip_count IS NOT NULL)
       OR m.SUPERVISORY_ORG IS NOT NULL
);

GRANT SELECT ON VIEW D_POST_INSTALL.V_CX_DEFAULT TO GENERAL_REPORTING_R -- Share the view