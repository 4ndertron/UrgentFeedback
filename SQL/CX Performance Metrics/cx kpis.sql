CREATE OR REPLACE VIEW d_post_install.v_cx_kpis
            COPY GRANTS
            COMMENT = 'Performance metrics for CX agents over rolling 90 days.'
AS
WITH
   -- Target dates
    dates AS
        (
            SELECT dt AS report_date
            FROM rpt.v_dates
            WHERE
              -- !!!!!!!!!!!!!!
              -- FUTURE CHANGES
              -- Change to 90. Temporarily 7 to make testing/development faster.
              -- !!!!!!!!!!!!!!
                dt >= DATEADD(d, -30, CURRENT_DATE)
              AND dt < CURRENT_DATE
        )

   -- Target employees
   -- !!!!!!!!!!!!!!
   -- FUTURE CHANGES
   -- Incorporate D_POST_INSTALL.V_CX_EMPLOYEES to look at active dates
   -- !!!!!!!!!!!!!!
   , employees AS
    (
        SELECT em.employee_id
             -- TEMPORARY final version might get name from another table (not in this query at all)
             , em.full_name
             -- TEMPORARY solution for team_start_date
             , em.hire_date AS team_start_date
             , ma.cjp_id
             , ma.calabrio_id
             , ma.salesforce_id
        FROM hr.t_employee AS em
                 LEFT OUTER JOIN
             -- Temporary fix to address duplication on T_EMPLOYEE_MASTER
                     (SELECT DISTINCT * FROM d_post_install.t_employee_master) AS ma
             ON
                 em.employee_id = ma.employee_id
        WHERE NOT em.terminated
          AND (
                em.pay_rate_type = 'Hourly'
                OR em.emp_compensation_type = 'Teleperformance'
            )
          AND (
                em.mgr_id_4 = '209122' -- Chuck Browne
                OR em.mgr_id_7 IN ('9501230', '9501356') -- Sebastian Moreno, Melissa Rodriguez
                OR em.mgr_id_8 = '9501068' -- Daniel Galindo Correa
                OR
                em.employee_id IN ('9501230', '9501356', '9501068') -- Sebastian Moreno, Melissa Rodriguez, Daniel Galindo Correa
            )
    )

   -- ======
   -- SCORES
   -- ======

   -- Total QA Evaluation Score
   -- Total QA Evaluation Tally
   , qa AS
    (
        SELECT em.employee_id
             -- QA currently holds times in UTC. A ticket has been submitted to BI Dev to get this corrected.
             , TO_DATE(CONVERT_TIMEZONE('UTC', 'America/Denver', qa.start_time)) AS report_date
             , SUM(qa.evaluation_total_score) / 100                              AS qa_score_total
             , COUNT(qa.evaluation_total_score)                                  AS qa_tally
        FROM employees AS em
                 INNER JOIN
             calabrio.t_persons AS cp
             ON
                 em.employee_id = cp.employee_id
                 INNER JOIN
             calabrio.t_recording_contact AS qa
             ON
                 cp.acd_id = qa.agent_acd_id
        WHERE TO_DATE(CONVERT_TIMEZONE('UTC', 'America/Denver', qa.start_time)) BETWEEN (SELECT MIN(report_date) FROM dates) AND (SELECT MAX(report_date) FROM dates)
        GROUP BY em.employee_id
               , TO_DATE(CONVERT_TIMEZONE('UTC', 'America/Denver', qa.start_time))
    )

   -- Total Worked Seconds (Workday)
   , worked_time AS
    (
        SELECT em.employee_id
             , t.reported_date                             AS report_date
             -- Okay to round because the times use minute granularity and all results get extremely close to whole numbers
             , ROUND(SUM(t.total_hours) * 3600, 0)         AS worked_seconds
             , NVL(FLOOR(worked_seconds / 240, 0) * 15, 0) AS break_seconds
             -- Free 15 minutes per day to account for meetings, 1-on-1, training, etc.
             , 15 * 60                                     AS ancillary_seconds
        FROM employees AS em
                 INNER JOIN
             hr.v_time AS t
             ON
                 em.employee_id = t.employee_id
        WHERE t.reported_date BETWEEN (SELECT MIN(report_date) FROM dates) AND (SELECT MAX(report_date) FROM dates)
          AND t.calculation_tags <> 'Paid Holiday'
        GROUP BY em.employee_id
               , t.reported_date
    )

   -- Call Back Tally
   -- Call Back Eligible Tally
   , call_back_prep1_eligible AS
    (
        SELECT em.employee_id
             , cjp.session_id
             , DATEADD(s, HOUR(cjp.call_start) * 3600 + MINUTE(cjp.call_start) * 60 + SECOND(cjp.call_start),
                       cjp.date)                                                  AS call_start
             -- Get customer number
             -- If Outbound then DNIS
             -- If Inbound then ANI
             , IFF(cjp.queue_1 LIKE ANY ('%Outdial%', '%CB%'), cjp.dnis, cjp.ani) AS customer_number
             , SUBSTR(customer_number, IFF(customer_number LIKE '1%', 2, 1),
                      LENGTH(customer_number))                                    AS customer_number_clean
        FROM employees AS em
                 INNER JOIN
             d_post_install.t_cjp_cdr_temp AS cjp
             ON
                 em.cjp_id = cjp.agent_1_acd_id
        WHERE
          -- Go back an extra 7 days so the window of [Next 7 Days] doesn't extend into the future
            cjp.date BETWEEN (SELECT DATEADD(d, -7, MIN(report_date)) FROM dates) AND (SELECT DATEADD(d, -7, MAX(report_date)) FROM dates)
          AND cjp.contact_type <> 'Master'
          -- !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
          -- FUTURE: Double check this value
          -- !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
          AND cjp.connected >= 30
          AND cjp.queue_1 IS NOT NULL
          -- Keep only phone numbers that contain 2+ numerals and only numerals
          AND customer_number_clean REGEXP '^\\d\\d+$'
    )

   -- Calls that could count as call backs (inbound only)
   , call_back_prep2_potential_call_backs AS
    (
        SELECT cjp.session_id
             , DATEADD(s, HOUR(cjp.call_start) * 3600 + MINUTE(cjp.call_start) * 60 + SECOND(cjp.call_start),
                       cjp.date)               AS call_start
             -- Get customer number
             -- If Inbound then ANI
             , cjp.ani                         AS customer_number
             , SUBSTR(customer_number, IFF(customer_number LIKE '1%', 2, 1),
                      LENGTH(customer_number)) AS customer_number_clean
             , cjp.connected
             , cjp.queue_1
             , cjp.dnis
        FROM d_post_install.t_cjp_cdr_temp AS cjp
        WHERE cjp.date BETWEEN (SELECT DATEADD(d, -7, MIN(report_date)) FROM dates) AND (SELECT MAX(report_date) FROM dates)
          AND cjp.contact_type <> 'Master'
          -- !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
          -- FUTURE: Double check this value
          -- !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
          AND cjp.connected >= 30
          AND NOT cjp.queue_1 LIKE ANY ('%Outdial%', '%CB%')
          -- Keep only phone numbers that contain 2+ numerals and only numerals
          AND customer_number_clean REGEXP '^\\d\\d+$'
    )

   -- For each call eligible for call backs, determine whether or not they received any (within criteria, e.g., following 7 days)
   -- !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
   -- FUTURE: Account for phone numbers that aren't customers?
   -- Look at phone numbers with lots of different accounts over the time period or a high number of daily calls?
   -- !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
   , call_back_prep3_call_backs AS
    (
        SELECT cb1.employee_id
             , cb1.session_id
             , MIN(cb1.call_start)       AS call_start
             , COUNT(cb2.session_id) > 0 AS has_call_back
        FROM call_back_prep1_eligible cb1
                 LEFT OUTER JOIN
             call_back_prep2_potential_call_backs AS cb2
             ON
                     cb1.customer_number_clean = cb2.customer_number_clean
                     AND cb1.session_id <> cb2.session_id
                     -- Call back must be after eligible call and within the following 7 days
                     AND cb2.call_start > cb1.call_start
                     AND cb2.call_start < TO_DATE(DATEADD(d, 8, cb1.call_start))
        GROUP BY cb1.employee_id
               , cb1.session_id
    )

   -- Call Back % (Final)
   -- Calls With Call Backs Tally
   -- Calls Tally
   , call_back AS
    (
        SELECT cb.employee_id
             , TO_DATE(cb.call_start)        AS report_date
             , SUM(IFF(has_call_back, 1, 0)) AS handled_calls_with_call_back
             , COUNT(has_call_back)          AS handled_calls
        FROM call_back_prep3_call_backs AS cb
        GROUP BY cb.employee_id
               , TO_DATE(cb.call_start)
    )

   -- Total Scheduled Talk Seconds
   , scheduled_talk_time AS
    (
        SELECT em.employee_id
             , TO_DATE(sh.scheduled_activity_start_time) AS report_date
             -- Always whole minutes
             , ROUND(SUM(DATEDIFF(s, sh.scheduled_activity_start_time, sh.scheduled_activity_end_time)),
                     0)                                  AS scheduled_talk_seconds
        FROM employees AS em
                 INNER JOIN
             calabrio.t_shifts_by_agent AS sh
             ON
                 em.calabrio_id = sh.agent_id
        WHERE TO_DATE(sh.scheduled_activity_start_time) BETWEEN (SELECT MIN(report_date) FROM dates) AND (SELECT MAX(report_date) FROM dates)
          -- NOTE: 'Available' schedule indicates flexibility, not time scheduled to talk to customers
          AND IFF(sh.scheduled_activity_type_label = 'Exception', sh.scheduled_activity_detail_name,
                  sh.scheduled_activity_type_label) IN ('Flex-Time (Add)', 'Overtime', 'In Service')
        GROUP BY em.employee_id
               , TO_DATE(sh.scheduled_activity_start_time)
    )

   -- Total Available To Customer Seconds
   -- Requires multiple CTEs: must merge timestamps to avoid duplicate credit for overlapping times
   , atc_prep1_periods AS
    (
        SELECT em.employee_id
             , CONVERT_TIMEZONE('UTC', 'America/Denver', aa.start_timestamp) AS start_timestamp
             , CONVERT_TIMEZONE('UTC', 'America/Denver', aa.end_timestamp)   AS end_timestamp
        FROM employees AS em
                 INNER JOIN
             cjp.v_agent_activity AS aa
             ON
                 em.cjp_id = aa.agent_acd_id
        WHERE TO_DATE(
                CONVERT_TIMEZONE('UTC', 'America/Denver', aa.start_timestamp)) BETWEEN (SELECT MIN(report_date) FROM dates) AND (SELECT MAX(report_date) FROM dates)
          AND IFF(aa.state = 'IDLE', aa.idle_code_name, aa.state) IN ('AVAILABLE', 'CONNECTED')
          AND aa.start_timestamp < aa.end_timestamp
    )

   -- Keep only times that are "heads": the start of a new contiguous time period
   , atc_prep2_heads AS
    (
        SELECT p1.employee_id
             , p1.start_timestamp
             , LEAD(p1.start_timestamp)
                    OVER (PARTITION BY p1.employee_id ORDER BY p1.start_timestamp) AS next_start_timestamp
        FROM atc_prep1_periods AS p1
                 LEFT OUTER JOIN
             atc_prep1_periods AS p2
             ON
                     p1.employee_id = p2.employee_id
                     AND p1.start_timestamp > p2.start_timestamp
                     AND p1.start_timestamp <= p2.end_timestamp
        WHERE p2.employee_id IS NULL
    )

   -- Join times to heads based on which heads they fit between
   -- Keep only the latest end time per head
   , atc_prep3_merged AS
    (
        SELECT h.employee_id
             , h.start_timestamp
             , DATEDIFF(s, h.start_timestamp, MAX(p.end_timestamp)) AS atc_seconds
        FROM atc_prep2_heads AS h
                 INNER JOIN
             atc_prep1_periods AS p
             ON
                     h.employee_id = p.employee_id
                     AND p.start_timestamp >= h.start_timestamp
                     AND p.start_timestamp < NVL(h.next_start_timestamp, '9999-12-31')
        GROUP BY h.employee_id
               , h.start_timestamp
    )

   -- Total Available To Customer Seconds (Final)
   , atc_time AS
    (
        SELECT m.employee_id
             , TO_DATE(m.start_timestamp) AS report_date
             , SUM(m.atc_seconds)         AS atc_seconds
        FROM atc_prep3_merged AS m
        GROUP BY m.employee_id
               , TO_DATE(m.start_timestamp)
    )

   -- Total Available To Customer (Email) Seconds
   -- Variant that treats email states as ATC time
   -- Requires multiple CTEs: must merge timestamps to avoid duplicate credit for overlapping times
   , atc_email_prep1_periods AS
    (
        SELECT em.employee_id
             , CONVERT_TIMEZONE('UTC', 'America/Denver', aa.start_timestamp) AS start_timestamp
             , CONVERT_TIMEZONE('UTC', 'America/Denver', aa.end_timestamp)   AS end_timestamp
        FROM employees AS em
                 INNER JOIN
             cjp.v_agent_activity AS aa
             ON
                 em.cjp_id = aa.agent_acd_id
        WHERE TO_DATE(
                CONVERT_TIMEZONE('UTC', 'America/Denver', aa.start_timestamp)) BETWEEN (SELECT MIN(report_date) FROM dates) AND (SELECT MAX(report_date) FROM dates)
          AND IFF(aa.state = 'IDLE', aa.idle_code_name, aa.state) IN ('AVAILABLE', 'CONNECTED', 'CS - Email', 'Email')
          AND aa.start_timestamp < aa.end_timestamp
    )

   -- Keep only times that are "heads": the start of a new contiguous time period
   , atc_email_prep2_heads AS
    (
        SELECT p1.employee_id
             , p1.start_timestamp
             , LEAD(p1.start_timestamp)
                    OVER (PARTITION BY p1.employee_id ORDER BY p1.start_timestamp) AS next_start_timestamp
        FROM atc_email_prep1_periods AS p1
                 LEFT OUTER JOIN
             atc_email_prep1_periods AS p2
             ON
                     p1.employee_id = p2.employee_id
                     AND p1.start_timestamp > p2.start_timestamp
                     AND p1.start_timestamp <= p2.end_timestamp
        WHERE p2.employee_id IS NULL
    )

   -- Join times to heads based on which heads they fit between
   -- Keep only the latest end time per head
   , atc_email_prep3_merged AS
    (
        SELECT h.employee_id
             , h.start_timestamp
             , DATEDIFF(s, h.start_timestamp, MAX(p.end_timestamp)) AS atc_seconds
        FROM atc_email_prep2_heads AS h
                 INNER JOIN
             atc_email_prep1_periods AS p
             ON
                     h.employee_id = p.employee_id
                     AND p.start_timestamp >= h.start_timestamp
                     AND p.start_timestamp < NVL(h.next_start_timestamp, '9999-12-31')
        GROUP BY h.employee_id
               , h.start_timestamp
    )

   -- Total Available To Customer Seconds (Final)
   , atc_email_time AS
    (
        SELECT m.employee_id
             , TO_DATE(m.start_timestamp) AS report_date
             , SUM(m.atc_seconds)         AS atc_seconds
        FROM atc_email_prep3_merged AS m
        GROUP BY m.employee_id
               , TO_DATE(m.start_timestamp)
    )

-- Final query: combine metrics
-- ============================
SELECT em.employee_id
     , em.full_name
     , d.report_date
     , NVL(qa.qa_score_total, 0)                                           AS qa_score
     , NVL(qa.qa_tally, 0)                                                 AS qa_evaluation_tally
     , NVL(wt.worked_seconds - wt.break_seconds - wt.ancillary_seconds, 0) AS worked_seconds
     , NVL(cb.handled_calls_with_call_back, 0)                             AS handled_calls_with_call_back
     , NVL(cb.handled_calls, 0)                                            AS handled_calls
     , NVL(stt.scheduled_talk_seconds, 0)                                  AS scheduled_talk_seconds
     , NVL(atct.atc_seconds, 0)                                            AS atc_seconds
     , NVL(atcet.atc_seconds, 0)                                           AS atc_email_seconds
FROM employees AS em
         LEFT OUTER JOIN
     dates AS d
     ON
         d.report_date >= em.team_start_date
         LEFT OUTER JOIN
     qa
     ON
             em.employee_id = qa.employee_id
             AND d.report_date = qa.report_date
         LEFT OUTER JOIN
     worked_time AS wt
     ON
             em.employee_id = wt.employee_id
             AND d.report_date = wt.report_date
         LEFT OUTER JOIN
     scheduled_talk_time AS stt
     ON
             em.employee_id = stt.employee_id
             AND d.report_date = stt.report_date
         LEFT OUTER JOIN
     call_back AS cb
     ON
             em.employee_id = cb.employee_id
             AND d.report_date = cb.report_date
         LEFT OUTER JOIN
     atc_time AS atct
     ON
             em.employee_id = atct.employee_id
             AND d.report_date = atct.report_date
         LEFT OUTER JOIN
     atc_email_time AS atcet
     ON
             em.employee_id = atcet.employee_id
             AND d.report_date = atcet.report_date
ORDER BY em.full_name
       , d.report_date
;

