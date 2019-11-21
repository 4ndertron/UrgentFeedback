-- CREATE OR REPLACE VIEW D_POST_INSTALL.v_cx_kpis_ace_t1
--
-- COPY GRANTS COMMENT = 'Performance metrics (rolling 90) and metadata for ACE (Tier 1, Tier 2, and Email Agents)'
--
-- AS
WITH my_dates AS
    (
        SELECT dt AS report_date
        FROM RPT.V_DATES
        WHERE dt >= DATEADD('day', -30, current_date)
          AND dt < current_date
    )

   , employees AS -- Team Specific Members
    (
        SELECT e.EMPLOYEE_ID
             , e.FULL_NAME
             , e.BUSINESS_TITLE
             , e.SUPERVISOR_NAME_1 || ' (' || e.SUPERVISOR_BADGE_ID_1 || ')' AS direct_manager
             , e.SUPERVISORY_ORG
             , TO_DATE(NVL(ea.max_dt, e.HIRE_DATE))                          AS team_start_date
             , to_date(e.HIRE_DATE)                                          AS hire_date
        FROM hr.T_EMPLOYEE e
                 LEFT OUTER JOIN -- Determine last time each employee WASN'T on target manager's team
            (
                SELECT EMPLOYEE_ID
                     , MAX(EXPIRY_DATE) AS max_dt
                FROM hr.T_EMPLOYEE_ALL
                WHERE NOT TERMINATED
                  AND (
                        MGR_ID_5 IS DISTINCT FROM '200023'
                        AND MGR_ID_6 IS DISTINCT FROM '200023'
                    )
                  -- Placeholder Manager (Kristen Rawlings)
                GROUP BY EMPLOYEE_ID
            ) ea
                                 ON
                                     e.employee_id = ea.employee_id
        WHERE NOT e.TERMINATED
          AND e.PAY_RATE_TYPE = 'Hourly'
          AND
          -- Placeholder Manager (Rawlings)
            (
                    e.MGR_ID_5 = '200023'
                    OR e.MGR_ID_6 = '200023'
                )
    )

   , activities AS -- Activities | Times
    (
        SELECT t.project_id
             , t.SUBJECT
             , a.MINUTES
             , to_date(t.CREATED_DATE) AS created
             , t.CREATED_BY_EMPLOYEE_ID
        FROM rpt.T_TASK t
                 INNER JOIN
             D_POST_INSTALL.T_CX_ACTIVITY_TIMES a
             ON
                 upper(a.CRITERION_1) = left(upper(t.SUBJECT), 9)
    )

   , time_clock AS -- Hours/Days Worked
    (
        SELECT t.EMPLOYEE_ID
             , to_date(t.REPORTED_DATE) AS work_day
             , SUM(t.TOTAL_HOURS)       AS hours_worked
        FROM hr.V_TIME t
        WHERE t.CALCULATION_TAGS != 'Paid Holiday'
          AND work_day >= date_trunc('w', current_date) - 90
        GROUP BY t.EMPLOYEE_ID
               , work_day
    )

   , qa AS -- Calabrio QA Scores
    (
        SELECT DISTINCT p.EMPLOYEE_ID                         AS agent_badge
                      , rc.AGENT_DISPLAY_ID                   AS agent_evaluated
                      , rc.TEAM_NAME
                      , TRUNC(rc.EVALUATION_EVALUATED, 'day') AS evaluation_date
                      , SUM(rc.EVALUATION_TOTAL_SCORE)        AS qa_score
                      , COUNT(rc.EVALUATION_TOTAL_SCORE)      AS qa_tally
        FROM CALABRIO.T_RECORDING_CONTACT rc
                 LEFT JOIN
             CALABRIO.T_PERSONS p
             ON
                 p.ACD_ID = rc.AGENT_ACD_ID
--                 where EMPLOYEE_ID = 210143
        GROUP BY agent_badge
               , agent_evaluated
               , rc.TEAM_NAME
               , evaluation_date
    )

   , main AS -- Joins | Productivity Logic
    (
        SELECT tc.work_day
             , tc.EMPLOYEE_ID
             , e.FULL_NAME
             , NVL(tc.hours_worked * 60, 0)                                   AS work_min
             , NVL(tc.hours_worked, 0)                                        AS hours_worked
             , CASE WHEN COUNT(a.PROJECT_ID) > 0 THEN COUNT(a.project_id) END AS total_activities
             , NVL(SUM(a.MINUTES), 0)                                         AS activity_min
             , NVL(60 / COUNT(tc.work_day) OVER (PARTITION BY date_trunc('w', tc.work_day), tc.EMPLOYEE_ID),
                   0)                                                         AS ancillary_min
             , NVL(floor(tc.hours_worked / 4, 0) * 15, 0)                     AS break_min
             , avg(qa.qa_score)                                               AS daily_qa_score
             , qa.qa_tally
             , qa.qa_score                                                    AS qa_sum
        FROM employees e
                 INNER JOIN
             time_clock tc
             ON
                     tc.EMPLOYEE_ID = e.EMPLOYEE_ID
                     AND tc.work_day >= e.team_start_date
                 LEFT JOIN
             activities a
             ON
                     a.CREATED = tc.work_day
                     AND a.CREATED_BY_EMPLOYEE_ID = tc.EMPLOYEE_ID
                 LEFT JOIN
             qa
             ON
                     qa.agent_badge = e.EMPLOYEE_ID
                     AND qa.evaluation_date = tc.work_day
        WHERE A.SUBJECT != 'ACE Audit'
        GROUP BY tc.work_day
               , tc.EMPLOYEE_ID
               , e.FULL_NAME
               , work_min
               , hours_worked
               , break_min
               , qa.qa_tally
               , qa.qa_score
    )

   , main_aggregate_data AS
    (
        SELECT EMPLOYEE_ID
             , FULL_NAME
             , work_day              AS report_date
             , COUNT(work_day)       AS total_work_days
             , SUM(WORK_MIN)         AS WORK_MIN
             , SUM(HOURS_WORKED)     AS HOURS_WORKED
             , SUM(TOTAL_ACTIVITIES) AS TOTAL_ACTIVITIES
             , SUM(ACTIVITY_MIN)     AS ACTIVITY_MIN
             , SUM(ANCILLARY_MIN)    AS ANCILLARY_MIN
             , SUM(BREAK_MIN)        AS BREAK_MIN
             , SUM(DAILY_QA_SCORE)   AS DAILY_QA_SCORE
             , SUM(QA_TALLY)         AS QA_TALLY
             , SUM(QA_SUM)           AS QA_SUM
        FROM main
        GROUP BY EMPLOYEE_ID
               , FULL_NAME
               , report_date
    )

   , raw_effectiveness_data AS
    (
        SELECT ta.CREATED_BY_EMPLOYEE_ID
             , TRUNC(ta.CLOSE_DATE_TIME, 'day') AS report_date
             , CASE
                   WHEN ta.DUE_DATE IS NOT NULL AND ta.DUE_DATE <= TRUNC(ta.CLOSE_DATE_TIME, 'day')
                       THEN 'In SLA'
                   WHEN ta.DUE_DATE IS NULL AND
                        DATEDIFF('hour', ta.CREATED_DATE, NVL(ta.CLOSE_DATE_TIME, current_date)) <= 48
                       THEN 'In SLA'
                   ELSE 'Out SLA'
            END                                 AS in_sla
        FROM RPT.T_TASK ta
                 INNER JOIN
             employees em
             ON
                 ta.CREATED_BY_EMPLOYEE_ID = em.EMPLOYEE_ID
        WHERE (
                ta.SUBJECT ILIKE '%QEM%'
                OR ta.SUBJECT ILIKE '%QCS%'
            )
          AND NVL(ta.DUE_DATE, current_date) <= current_date
          AND ta.CREATED_DATE <= DATEADD('day', -2, current_date)
          AND ta.CREATED_DATE >= DATEADD('day', -30, current_date)
    )

   , effectiveness_data AS
    (
        SELECT re.CREATED_BY_EMPLOYEE_ID
             , re.report_date
             , COUNT(CASE WHEN re.in_sla = 'In SLA' THEN 1 END)                                    AS in_sla_tally
             , COUNT(re.CREATED_BY_EMPLOYEE_ID)                                                    AS total_tally
             , COUNT(CASE WHEN re.in_sla = 'In SLA' THEN 1 END) / COUNT(re.CREATED_BY_EMPLOYEE_ID) AS effectiveness
        FROM raw_effectiveness_data re
        GROUP BY re.CREATED_BY_EMPLOYEE_ID
               , re.report_date
    )

   , COMPLETE AS (
    SELECT m.EMPLOYEE_ID
         , m.FULL_NAME
         , md.report_date
         , ed.effectiveness
         , ed.in_sla_tally                                               AS effectiveness_num
         , ed.total_tally                                                AS effectiveness_den
         , M.ACTIVITY_MIN
         , M.ANCILLARY_MIN
         , M.BREAK_MIN
         , (m.activity_min + m.ancillary_min + m.break_min) / m.work_min AS efficiency
         , (m.activity_min + m.ancillary_min + m.break_min)              AS efficiency_num
         , m.work_min                                                    AS efficiency_den
         , m.qa_sum / m.QA_TALLY                                         AS quality
         , m.qa_sum                                                      AS qa_num
         , m.qa_tally                                                    AS qa_den
    FROM my_dates md
             LEFT JOIN
         main_aggregate_data m
         ON
             m.report_date = md.report_date
             LEFT JOIN
         effectiveness_data ed
         ON
                 m.EMPLOYEE_ID = ed.CREATED_BY_EMPLOYEE_ID
                 AND m.report_date = ed.report_date
)

   , TEST_RESULTS AS (
    SELECT CREATED_BY_EMPLOYEE_ID
         , SUBJECT
         , COUNT(*)
         , SUM(MINUTES)

    FROM activities
    WHERE CREATED_BY_EMPLOYEE_ID IN ('210143', '211498', '211714')
      AND created >= DATEADD('MM', -2, CURRENT_DATE)
    GROUP BY SUBJECT
           , CREATED_BY_EMPLOYEE_ID
)

SELECT *
FROM COMPLETE
;

