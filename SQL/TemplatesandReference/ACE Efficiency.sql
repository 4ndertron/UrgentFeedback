WITH activities AS -- Activities | Times
    (SELECT t.project_id, t.SUBJECT, a.MINUTES, to_date(t.CREATED_DATE) AS created, t.CREATED_BY_EMPLOYEE_ID
     FROM rpt.T_TASK t
              INNER JOIN D_POST_INSTALL.T_CX_ACTIVITY_TIMES a
                         ON upper(a.TYPE_1) = left(upper(t.SUBJECT), 9))
   , employees AS -- Team Specific Members
    (SELECT e.BADGE_ID
          , e.FULL_NAME
          , e.BUSINESS_TITLE
          , e.SUPERVISOR_NAME_1 || ' (' || e.SUPERVISOR_BADGE_ID_1 || ')' AS direct_manager
          , e.SUPERVISORY_ORG
     FROM hr.T_EMPLOYEE e
     WHERE e.TERMINATED = FALSE
       AND e.PAY_RATE_TYPE = 'Hourly'
       AND e.MGR_ID_6 = '200023')
   , time_clock AS -- Hours/Days Worked
    (SELECT t.EMPLOYEE_ID, to_date(t.REPORTED_DATE) AS work_day, SUM(t.TOTAL_HOURS) AS hours_worked
     FROM hr.V_TIME t
     WHERE t.CALCULATION_TAGS != 'Paid Holiday'
       AND work_day >= date_trunc('w', current_date) - 90
     GROUP BY t.EMPLOYEE_ID, work_day)
   , main AS -- Joins | Productivity Logic
    (SELECT tc.work_day
          , tc.EMPLOYEE_ID
          , e.FULL_NAME
          , e.BUSINESS_TITLE
          , e.direct_manager
          , e.SUPERVISORY_ORG
          , NVL(tc.hours_worked * 60, 0)                                   AS work_min
          , NVL(tc.hours_worked, 0)                                        AS hours_worked
          , CASE WHEN COUNT(a.PROJECT_ID) > 0 THEN COUNT(a.project_id) END AS total_activities
          , NVL(SUM(a.MINUTES), 0)
                                                                           AS activity_min
          , NVL(60 / COUNT(tc.work_day)
                           OVER (PARTITION BY date_trunc('w', tc.work_day), tc.EMPLOYEE_ID),
                0)                                                         AS ancillary_min
          , NVL(floor(tc.hours_worked / 4, 0) * 15, 0)                     AS break_min
          , CASE WHEN tc.hours_worked >= 6 THEN 30 ELSE 0 END              AS lunch_min
     FROM time_clock tc
              INNER JOIN employees e
                         ON e.BADGE_ID = tc.EMPLOYEE_ID
              LEFT JOIN activities a
                        ON a.CREATED = tc.work_day
                            AND a.CREATED_BY_EMPLOYEE_ID = tc.EMPLOYEE_ID
     GROUP BY tc.work_day, tc.EMPLOYEE_ID, e.FULL_NAME, e.BUSINESS_TITLE, e.direct_manager
            , e.SUPERVISORY_ORG
            , tc.hours_worked)
SELECT m.EMPLOYEE_ID
     , m.FULL_NAME
     , m.direct_manager
     , m.SUPERVISORY_ORG
     , NVL(ROUND(SUM(m.hours_worked) / COUNT(m.work_day), 2), 0) AS avg_daily_hours
     , SUM(m.total_activities)                                   AS activities_90
     , (SUM(m.activity_min) + SUM(m.ancillary_min) + SUM(m.break_min) + SUM(m.lunch_min)) /
       SUM(m.work_min)                                           AS productivity_90
     , rank() OVER (ORDER BY productivity_90 DESC)               AS prod_rank
FROM main m
GROUP BY m.EMPLOYEE_ID, m.FULL_NAME, m.direct_manager, m.SUPERVISORY_ORG;