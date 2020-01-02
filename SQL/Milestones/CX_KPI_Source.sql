CREATE OR REPLACE VIEW d_post_install.v_cx_kpis
            COPY GRANTS
            COMMENT = 'Performance metrics for CX agents over 120 days (including rolling 90 days over the past 30 days).'
AS
WITH
   -- =========
   -- BASE INFO
   -- =========
   -- TARGET DATES
    dates AS
        (
            SELECT dt
            FROM rpt.v_dates
            WHERE
              -- Should be -120 when building the view, keep it smaller for quicker testing
              -- Perhaps the final results can be reduced back to 30, but initially need 120 to calculate 90-day scores over the past 30 days
                dt >= DATEADD(d, -120, CURRENT_DATE)
              AND dt < CURRENT_DATE
        )

   -- TARGET EMPLOYEES
   -- List everybody with their overall first and last date
   , employees_prep1_basic AS
    (
        SELECT employee_id
             , MIN(start_date) AS team_start_date
             , MAX(end_date)   AS team_end_date
        FROM d_post_install.v_cx_employees
        GROUP BY employee_id
    )

   -- Attach additional info (IDs for other platforms)
   , employees AS
    (
        SELECT em.employee_id
             -- Adjust start and end date to consider target date range
             , em.team_start_date
             , em.team_end_date
             , ma.cjp_id
             , ma.calabrio_id
             , ma.salesforce_id
        FROM employees_prep1_basic AS em
                 LEFT OUTER JOIN
             d_post_install.t_employee_master AS ma
             ON
                 em.employee_id = ma.employee_id
        WHERE em.team_end_date >= (SELECT MIN(dt) FROM dates)
    )

   -- ======
   -- SCORES
   -- ======

   -- QA SCORE
   -- QA EVALUATIONS
   , qa AS
    (
        SELECT em.employee_id
             -- QA currently holds times in UTC. A ticket has been submitted to BI Dev to get this corrected.
             , TO_DATE(CONVERT_TIMEZONE('UTC', 'America/Denver', qa.start_time)) AS dt
             , SUM(qa.evaluation_total_score) / 100                              AS qa_score
             , COUNT(qa.evaluation_total_score)                                  AS qa_evaluations
        FROM employees AS em
                 INNER JOIN
             calabrio.t_persons AS cp
             ON
                 em.employee_id = cp.employee_id
                 INNER JOIN
             calabrio.t_recording_contact AS qa
             ON
                 cp.acd_id = qa.agent_acd_id
        WHERE TO_DATE(CONVERT_TIMEZONE('UTC', 'America/Denver', qa.start_time)) BETWEEN (SELECT MIN(dt) FROM dates) AND (SELECT MAX(dt) FROM dates)
        GROUP BY em.employee_id
               , TO_DATE(CONVERT_TIMEZONE('UTC', 'America/Denver', qa.start_time))
    )

   -- WORKED SECONDS
   -- Additional components: break_seconds, ancillary_seconds
   -- From Workday    
   , worked_time AS
    (
        SELECT em.employee_id
             , t.reported_date                             AS dt
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
        WHERE t.reported_date BETWEEN (SELECT MIN(dt) FROM dates) AND (SELECT MAX(dt) FROM dates)
          AND t.calculation_tags <> 'Paid Holiday'
        GROUP BY em.employee_id
               , t.reported_date
    )

   -- Call Back Rate:
   -- CALL BACK CALLS
   -- HANDLED CALLS (eligible for call back)
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
            cjp.date BETWEEN (SELECT DATEADD(d, -7, MIN(dt)) FROM dates) AND (SELECT DATEADD(d, -7, MAX(dt)) FROM dates)
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
        WHERE cjp.date BETWEEN (SELECT DATEADD(d, -7, MIN(dt)) FROM dates) AND (SELECT MAX(dt) FROM dates)
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

   -- Call Back Rate: Final aggregation by employee and date
   , call_back AS
    (
        SELECT cb.employee_id
             , TO_DATE(cb.call_start)        AS dt
             , SUM(IFF(has_call_back, 1, 0)) AS call_back_calls
             , COUNT(has_call_back)          AS handled_calls
        FROM call_back_prep3_call_backs AS cb
        GROUP BY cb.employee_id
               , TO_DATE(cb.call_start)
    )

   -- AVAILABLE TO CUSTOMER SECONDS
   -- Requires multiple CTEs: must merge timestamps to avoid duplicate credit for overlapping times
   , atc_prep1_periods AS
    (
        SELECT em.employee_id
             , aa.start_timestamp
             , aa.end_timestamp
        FROM employees AS em
                 INNER JOIN
             cjp.v_agent_activity AS aa
             ON
                 em.cjp_id = aa.agent_acd_id
        WHERE TO_DATE(aa.start_timestamp) BETWEEN (SELECT MIN(dt) FROM dates) AND (SELECT MAX(dt) FROM dates)
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

   -- Available To Customer: Final aggregation by employee and date
   , atc_time AS
    (
        SELECT m.employee_id
             , TO_DATE(m.start_timestamp) AS dt
             , SUM(m.atc_seconds)         AS atc_seconds
        FROM atc_prep3_merged AS m
        GROUP BY m.employee_id
               , TO_DATE(m.start_timestamp)
    )

   -- AVAILABLE TO CUSTOMER (EMAIL) SECONDS
   -- Variant that treats email states as ATC time
   -- Requires multiple CTEs: must merge timestamps to avoid duplicate credit for overlapping times
   , atc_email_prep1_periods AS
    (
        SELECT em.employee_id
             , aa.start_timestamp
             , aa.end_timestamp
        FROM employees AS em
                 INNER JOIN
             cjp.v_agent_activity AS aa
             ON
                 em.cjp_id = aa.agent_acd_id
        WHERE TO_DATE(aa.start_timestamp) BETWEEN (SELECT MIN(dt) FROM dates) AND (SELECT MAX(dt) FROM dates)
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

   -- Available To Customer (Email): Final aggregation by employee and date
   , atc_email_time AS
    (
        SELECT m.employee_id
             , TO_DATE(m.start_timestamp) AS dt
             , SUM(m.atc_seconds)         AS atc_seconds
        FROM atc_email_prep3_merged AS m
        GROUP BY m.employee_id
               , TO_DATE(m.start_timestamp)
    )

   -- SCHEDULED TALK SECONDS
   , scheduled_talk_time AS
    (
        SELECT em.employee_id
             , TO_DATE(sh.scheduled_activity_start_time) AS dt
             -- Always whole minutes
             , ROUND(SUM(DATEDIFF(s, sh.scheduled_activity_start_time, sh.scheduled_activity_end_time)),
                     0)                                  AS scheduled_talk_seconds
        FROM employees AS em
                 INNER JOIN
             calabrio.t_shifts_by_agent AS sh
             ON
                 em.calabrio_id = sh.agent_id
        WHERE TO_DATE(sh.scheduled_activity_start_time) BETWEEN (SELECT MIN(dt) FROM dates) AND (SELECT MAX(dt) FROM dates)
          -- NOTE: 'Available' schedule indicates flexibility, not time scheduled to talk to customers
          AND IFF(sh.scheduled_activity_type_label = 'Exception', sh.scheduled_activity_detail_name,
                  sh.scheduled_activity_type_label) IN ('Flex-Time (Add)', 'Overtime', 'In Service')
        GROUP BY em.employee_id
               , TO_DATE(sh.scheduled_activity_start_time)
    )

   -- OCCUPIED SECONDS
   -- AVAILABLE SECONDS
   -- PHONE SECONDS
   -- Requires several steps to dedupe overlapping times.
   -- (This is why availability and occupancy are solved separately, combined for overall phone time, and then occupancy is joined back in)
   -- Get occupied times
   , occupancy_prep1_occupied_blocks AS
    (
        SELECT em.employee_id
             , MIN(aa.start_timestamp) AS start_timestamp
             , MAX(aa.end_timestamp)   AS end_timestamp
        FROM employees AS em
                 INNER JOIN
             cjp.v_agent_activity AS aa
             ON
                 em.cjp_id = aa.agent_acd_id
        WHERE
          -- !!!!!!
          -- FUTURE
          -- Currently includes 'WRAPUP' state. Should it?
          -- Is occupancy about handling calls or about talking to customers?
          -- Is wrapup "productive"
          -- Question for Nick/WFM
          -- !!!!!!
            aa.state NOT IN ('AVAILABLE', 'IDLE')
          AND aa.call_session_id IS NOT NULL
          AND TO_DATE(aa.start_timestamp) BETWEEN (SELECT MIN(dt) FROM dates) AND (SELECT MAX(dt) FROM dates)
        GROUP BY em.employee_id
               , aa.call_session_id
    )

   -- Get available times
   , occupancy_prep2_available_periods AS
    (
        SELECT em.employee_id
             , aa.start_timestamp
             , aa.end_timestamp
        FROM employees AS em
                 INNER JOIN
             cjp.v_agent_activity AS aa
             ON
                 em.cjp_id = aa.agent_acd_id
        WHERE aa.state = 'AVAILABLE'
          AND TO_DATE(aa.start_timestamp) BETWEEN (SELECT MIN(dt) FROM dates) AND (SELECT MAX(dt) FROM dates)
          AND aa.start_timestamp < aa.end_timestamp
    )

   -- Prepare merge of contiguous/overlapping actuals
   -- Identify headers (times that begin a block of contiguous time)
   , occupancy_prep3_available_headers AS
    (
        SELECT ap1.employee_id
             , ap1.start_timestamp
             , LEAD(ap1.start_timestamp)
                    OVER (PARTITION BY ap1.employee_id ORDER BY ap1.start_timestamp) AS next_start_timestamp
        FROM occupancy_prep2_available_periods AS ap1
                 LEFT OUTER JOIN
             occupancy_prep2_available_periods AS ap2
             ON
                     ap1.employee_id = ap2.employee_id
                     AND ap1.start_timestamp > ap2.start_timestamp
                     AND ap1.start_timestamp <= ap2.end_timestamp
        WHERE ap2.employee_id IS NULL
    )

   -- Connect all periods to the headers they fit under
   -- Keep earliest start and latest end times
   , occupancy_prep4_available_blocks AS
    (
        SELECT ah.employee_id
             , ah.start_timestamp
             , MAX(ap.end_timestamp) AS end_timestamp
        FROM occupancy_prep3_available_headers AS ah
                 INNER JOIN
             occupancy_prep2_available_periods AS ap
             ON
                     ah.employee_id = ap.employee_id
                     AND ap.start_timestamp >= ah.start_timestamp
                     AND ap.start_timestamp < NVL(ah.next_start_timestamp, '9999-12-31')
        GROUP BY ah.employee_id
               , ah.start_timestamp
    )

   -- Get on-phone periods
   -- Combine occupied blocks and available blocks
   , occupancy_prep5_phone_periods AS
    (
        SELECT *
             , TRUE AS is_occupied
        FROM occupancy_prep1_occupied_blocks
        UNION ALL
        SELECT *
             , FALSE AS is_occupied
        FROM occupancy_prep4_available_blocks
    )

   -- Prepare merge of contiguous/overlapping actuals
   -- Identify headers (times that begin a block of contiguous time)
   , occupancy_prep6_phone_headers AS
    (
        SELECT pp1.employee_id
             , pp1.start_timestamp
             , LEAD(pp1.start_timestamp)
                    OVER (PARTITION BY pp1.employee_id ORDER BY pp1.start_timestamp) AS next_start_timestamp
        FROM occupancy_prep5_phone_periods AS pp1
                 LEFT OUTER JOIN
             occupancy_prep5_phone_periods AS pp2
             ON
                     pp1.employee_id = pp2.employee_id
                     AND pp1.start_timestamp > pp2.start_timestamp
                     AND pp1.start_timestamp <= pp2.end_timestamp
        WHERE pp2.employee_id IS NULL
    )

   -- Connect all periods to the headers they fit under
   -- Keep earliest start and latest end times
   , occupancy_prep7_phone_blocks AS
    (
        SELECT ph.employee_id
             , ph.start_timestamp
             , MAX(pp.end_timestamp) AS end_timestamp
        FROM occupancy_prep6_phone_headers AS ph
                 INNER JOIN
             occupancy_prep5_phone_periods AS pp
             ON
                     ph.employee_id = pp.employee_id
                     AND pp.start_timestamp >= ph.start_timestamp
                     AND pp.start_timestamp < NVL(ph.next_start_timestamp, '9999-12-31')
        GROUP BY ph.employee_id
               , ph.start_timestamp
    )

   -- Combine occupied and phone blocks
   -- Sum up occupied seconds per block of working seconds
   , occupancy_prep8_combined_blocks AS
    (
        SELECT pb.employee_id
             , TO_DATE(pb.start_timestamp)                                            AS dt
             -- Don't round yet (round after sum)
             , DATEDIFF(ms, pb.start_timestamp, ANY_VALUE(pb.end_timestamp)) / 1000   AS phone_seconds
             , NVL(SUM(DATEDIFF(ms, ob.start_timestamp, ob.end_timestamp)), 0) / 1000 AS occupied_seconds
        FROM occupancy_prep7_phone_blocks AS pb
                 LEFT OUTER JOIN
             occupancy_prep1_occupied_blocks AS ob
             ON
                     pb.employee_id = ob.employee_id
                     AND ob.start_timestamp >= pb.start_timestamp
                     AND ob.start_timestamp <= pb.end_timestamp
        GROUP BY pb.employee_id
               , pb.start_timestamp
    )

   -- Occupancy: Final aggregation by employee and date
   , occupancy AS
    (
        SELECT employee_id
             , dt
             , ROUND(SUM(NVL(occupied_seconds, 0)), 0)                                        AS occupied_seconds
             , ROUND(SUM(NVL(phone_seconds, 0)), 0) - ROUND(SUM(NVL(occupied_seconds, 0)), 0) AS available_seconds
             , ROUND(SUM(NVL(phone_seconds, 0)), 0)                                           AS phone_seconds
        FROM occupancy_prep8_combined_blocks
        GROUP BY employee_id
               , dt
    )

   -- IN ADHERENCE SECONDS
   -- ADHERENCE SCHEDULED SECONDS
   -- Get schedule details for activities that are eligible for Adherence
   , adherence_prep1_schedules AS
    (
        SELECT em.employee_id
             , sh.scheduled_activity_type_label  AS schedule_activity
             , sh.scheduled_activity_detail_name AS schedule_detail
             , sh.scheduled_activity_start_time  AS start_timestamp
             , sh.scheduled_activity_end_time    AS end_timestamp
        FROM employees AS em
                 INNER JOIN
             calabrio.t_shifts_by_agent AS sh
             ON
                 em.calabrio_id = sh.agent_id
        WHERE TO_DATE(sh.scheduled_activity_start_time) BETWEEN (SELECT MIN(dt) FROM dates) AND (SELECT MAX(dt) FROM dates)
          AND IFF(sh.scheduled_activity_type_label = 'Exception', sh.scheduled_activity_detail_name,
                  sh.scheduled_activity_type_label) NOT IN
              ('Not Available', 'Bereavement', 'PTO', 'UTO', 'VTO', 'Flex-Time (Remove)', 'LOA')
    )

   -- Get actual details for activities that are eligible for Adherence
   , adherence_prep2_actuals AS
    (
        SELECT em.employee_id
             , ca.schedule_activity
             , ca.schedule_detail
             , aa.start_timestamp
             , aa.end_timestamp
        FROM employees AS em
                 INNER JOIN
             cjp.v_agent_activity AS aa
             ON
                 em.cjp_id = aa.agent_acd_id
                 INNER JOIN
             d_post_install.t_calabrio_adherence_mapping AS ca
             ON
                     IFF(ca.actual_state = 'IDLE', ca.actual_idle_code, ca.actual_state) =
                     IFF(aa.state = 'IDLE', aa.idle_code_name, aa.state)
        WHERE TO_DATE(aa.start_timestamp) BETWEEN (SELECT MIN(dt) FROM dates) AND (SELECT MAX(dt) FROM dates)
          AND aa.start_timestamp < aa.end_timestamp
    )

   -- Connect actuals to schedules (where the activity types align to qualify for adherence AND the times overlap)
   , adherence_prep3_adherence_periods AS
    (
        SELECT sc.employee_id
             , sc.start_timestamp                               AS schedule_start_timestamp
             , sc.end_timestamp                                 AS schedule_end_timestamp
             -- GREATEST and LEAST return NULL if either value is NULL, but we know schedules will never be NULL since they're the backbone of this query
             , GREATEST(ac.start_timestamp, sc.start_timestamp) AS actual_start_timestamp
             , LEAST(ac.end_timestamp, sc.end_timestamp)        AS actual_end_timestamp
        FROM adherence_prep1_schedules AS sc
                 LEFT OUTER JOIN
             adherence_prep2_actuals AS ac
             ON
                     sc.employee_id = ac.employee_id
                     AND sc.schedule_activity = ac.schedule_activity
                     AND IFF(sc.schedule_activity IN ('In Service', 'Overtime') OR sc.schedule_detail = 'Overtime', '*',
                             sc.schedule_detail) = ac.schedule_detail
                     AND ac.end_timestamp >= sc.start_timestamp
                     AND ac.start_timestamp <= sc.end_timestamp
        WHERE sc.start_timestamp < sc.end_timestamp
    )

   -- Prepare merge of contiguous/overlapping actuals
   -- Identify headers (times that begin a block of contiguous time)
   , adherence_prep4_adherence_headers AS
    (
        SELECT ap1.employee_id
             , ap1.schedule_start_timestamp
             , ap1.schedule_end_timestamp
             , ap1.actual_start_timestamp
             , LEAD(ap1.actual_start_timestamp)
                    OVER (PARTITION BY ap1.employee_id ORDER BY ap1.actual_start_timestamp) AS next_start_timestamp
        FROM adherence_prep3_adherence_periods AS ap1
                 LEFT OUTER JOIN
             adherence_prep3_adherence_periods AS ap2
             ON
                     ap1.employee_id = ap2.employee_id
                     AND ap1.actual_start_timestamp > ap2.actual_start_timestamp
                     AND ap1.actual_start_timestamp <= ap2.actual_end_timestamp
        WHERE ap2.employee_id IS NULL
    )

   -- Connect all periods to the headers they fit under
   -- Keep earliest actual start and latest actual end times
   , adherence_prep5_adherence_merged AS
    (
        SELECT ah.employee_id
             , ah.schedule_start_timestamp
             , ANY_VALUE(ah.schedule_end_timestamp)                                      AS schedule_end_timestamp
             -- GREATEST and LEAST return NULL if either value is NULL, but we know schedules will never be NULL since they're the backbone of this query
             , GREATEST(ah.schedule_start_timestamp, ah.actual_start_timestamp)          AS actual_start_timestamp
             , LEAST(ANY_VALUE(ah.schedule_end_timestamp), MAX(ap.actual_end_timestamp)) AS actual_end_timestamp
        FROM adherence_prep4_adherence_headers AS ah
                 LEFT OUTER JOIN
             adherence_prep3_adherence_periods AS ap
             ON
                     ah.employee_id = ap.employee_id
                     AND ap.actual_start_timestamp >= ah.actual_start_timestamp
                     AND ap.actual_start_timestamp < NVL(ah.next_start_timestamp, '9999-12-31')
        GROUP BY ah.employee_id
               , ah.schedule_start_timestamp
               , ah.actual_start_timestamp
    )

   -- Sum up in-adherence seconds per block of scheduled time (per employee)
   , adherence_prep6_adherence_blocks AS
    (
        SELECT employee_id
             , TO_DATE(schedule_start_timestamp)                                              AS dt
             , DATEDIFF(s, schedule_start_timestamp, ANY_VALUE(schedule_end_timestamp))       AS scheduled_seconds
             -- Don't round yet (round after sum)
             , NVL(SUM(DATEDIFF(ms, actual_start_timestamp, actual_end_timestamp)), 0) / 1000 AS adherence_seconds
        FROM adherence_prep5_adherence_merged
        GROUP BY employee_id
               , schedule_start_timestamp
    )

   -- Adherence: Final aggregation by employee and date
   , adherence AS
    (
        SELECT employee_id
             , dt
             , ROUND(SUM(adherence_seconds), 0) AS in_adherence_seconds
             , SUM(scheduled_seconds)           AS scheduled_seconds
        FROM adherence_prep6_adherence_blocks
        GROUP BY employee_id
               , dt
    )

   -- Call Metrics:
   -- INBOUND RESOLUTION RATE
   -- INBOUND CALLS
   -- OUTBOUND CALLS
   -- HANDLE SECONDS
   -- CONNECTED SECONDS
   -- WRAPUP SECONDS
   -- ON HOLD SECONDS
   -- !!!!!!!!!!!!!!
   -- FUTURE CHANGES
   -- Replace T_CJP_CDR_TEMP with updated view that has better logic (currently WIP by Tanner)
   -- !!!!!!!!!!!!!!
   -- Possible problem with Inbound Call Resolution
   -- We can tell who joined the call last but not necessarily who stayed on the call the latest 
   -- For now, we are going with who joined the call last. This may also be the final intention anyway.
   -- CALL_END possibly unreliable. Does it or doesn't it include WRAPUP? Sometimes WRAPUP is longer than CALL_END - CALL_START
   -- Seems it does include WRAPUP, but sometimes there can be two overlapping WRAPUP sessions.
   -- This can eventually be addressed with new logic against AAR/CAR.
   , call_metrics_prep1_calls AS
    (
        SELECT em.employee_id
             , cdr.date                                                                 AS dt
             , cdr.queue_1 <> 'Q_Outdial'                                               AS is_inbound
             , ROW_NUMBER() OVER (PARTITION BY cdr.session_id ORDER BY call_start DESC) AS leg_number
             , cdr.connected
             , cdr.wrapup
             , cdr.on_hold
        FROM employees AS em
                 INNER JOIN
             d_post_install.t_cjp_cdr_temp AS cdr
             ON
                     em.cjp_id = cdr.agent_1_acd_id
                     AND contact_type <> 'Master'
        WHERE cdr.date BETWEEN (SELECT MIN(dt) FROM dates) AND (SELECT MAX(dt) FROM dates)
          AND cdr.connected > 0
    )

   -- Call Metrics: Final aggregation by employee and date
   , call_metrics AS
    (
        SELECT employee_id
             , dt
             , SUM(IFF(is_inbound AND leg_number = 1, 1, 0)) AS inbound_calls_resolved
             , SUM(IFF(is_inbound, 1, 0))                    AS inbound_calls
             , SUM(IFF(NOT is_inbound, 1, 0))                AS outbound_calls
             , SUM(connected + wrapup + on_hold)             AS handle_seconds
             , SUM(connected)                                AS talk_seconds
             , SUM(wrapup)                                   AS wrapup_seconds
             , SUM(on_hold)                                  AS hold_seconds
        FROM call_metrics_prep1_calls
        GROUP BY employee_id
               , dt
    )

   , case_metrics AS (
    SELECT D.DT
         , C.OWNER_EMPLOYEE_ID                                                   as employee_id
         , COUNT(CASE WHEN DATE_TRUNC(dd, C.CASE_CLOSED_DATE) = D.DT THEN 1 END) AS CLOSED_CASES
         , AVG(DISTINCT CASE_AGE_SECONDS)                                        AS AVG_SECONDS
    FROM RPT.T_DATES AS D
             LEFT JOIN D_POST_INSTALL.T_CX_CASE_CUBE AS C
                       ON DATE_TRUNC(dd, C.CASE_CLOSED_DATE) = D.DT
    WHERE D.DT BETWEEN DATEADD(MM, -4, DATE_TRUNC(MM, CURRENT_DATE)) AND CURRENT_DATE
    GROUP BY D.DT, C.OWNER_EMPLOYEE_ID
    ORDER BY C.OWNER_EMPLOYEE_ID, D.DT
)

   -- Combine CTEs and get daily scores per agent
   , combined_scores AS
    (
        SELECT em.employee_id
             , d.dt
             -- Scores
             , NVL(qa.qa_score, 0)                                                 AS qa_score
             , NVL(qa.qa_evaluations, 0)                                           AS qa_evaluations
             , NVL(wt.worked_seconds - wt.break_seconds - wt.ancillary_seconds, 0) AS pro_worked_seconds
             , NVL(cb.call_back_calls, 0)                                          AS cb_call_back_calls
             , NVL(cb.handled_calls, 0)                                            AS cb_handled_calls
             , NVL(atct.atc_seconds, 0)                                            AS atc_atc_seconds
             , NVL(atcet.atc_seconds, 0)                                           AS atce_atc_seconds
             , NVL(stt.scheduled_talk_seconds, 0)                                  AS atc_scheduled_talk_seconds
             , NVL(adh.in_adherence_seconds, 0)                                    AS adh_in_adherence_seconds
             , NVL(adh.scheduled_seconds, 0)                                       AS adh_scheduled_seconds
             , NVL(occ.occupied_seconds, 0)                                        AS occ_occupied_seconds
             , NVL(occ.available_seconds, 0)                                       AS occ_available_seconds
             , NVL(occ.phone_seconds, 0)                                           AS occ_phone_seconds
             , NVL(cm.inbound_calls_resolved, 0)                                   AS ib_resolved_calls
             , NVL(cm.inbound_calls, 0)                                            AS ib_calls
             , NVL(cm.outbound_calls, 0)                                           AS ob_calls
             , NVL(cm.inbound_calls, 0) + NVL(cm.outbound_calls, 0)                AS handled_calls
             , NVL(cm.handle_seconds, 0)                                           AS aht_handle_seconds
             , NVL(cm.talk_seconds, 0)                                             AS att_talk_seconds
             , NVL(cm.wrapup_seconds, 0)                                           AS awt_wrapup_seconds
             , NVL(cm.hold_seconds, 0)                                             AS hld_hold_seconds
             , NVL(ca.AVG_SECONDS, 0)                                              AS case_age_seconds
             , NVL(ca.CLOSED_CASES, 0)                                             AS closed_cases
        FROM employees AS em
                 LEFT OUTER JOIN
             dates AS d
             ON
                 d.dt BETWEEN em.team_start_date AND em.team_end_date
                 LEFT OUTER JOIN
             qa
             ON
                     em.employee_id = qa.employee_id
                     AND d.dt = qa.dt
                 LEFT OUTER JOIN
             worked_time AS wt
             ON
                     em.employee_id = wt.employee_id
                     AND d.dt = wt.dt
                 LEFT OUTER JOIN
             call_back AS cb
             ON
                     em.employee_id = cb.employee_id
                     AND d.dt = cb.dt
                 LEFT OUTER JOIN
             atc_time AS atct
             ON
                     em.employee_id = atct.employee_id
                     AND d.dt = atct.dt
                 LEFT OUTER JOIN
             atc_email_time AS atcet
             ON
                     em.employee_id = atcet.employee_id
                     AND d.dt = atcet.dt
                 LEFT OUTER JOIN
             scheduled_talk_time AS stt
             ON
                     em.employee_id = stt.employee_id
                     AND d.dt = stt.dt
                 LEFT OUTER JOIN
             adherence AS adh
             ON
                     em.employee_id = adh.employee_id
                     AND d.dt = adh.dt
                 LEFT OUTER JOIN
             occupancy AS occ
             ON
                     em.employee_id = occ.employee_id
                     AND d.dt = occ.dt
                 LEFT OUTER JOIN
             case_metrics AS ca
             ON
                     em.employee_id::string = ca.employee_id
                     AND d.dt = ca.dt
                 LEFT OUTER JOIN
             call_metrics AS cm
             ON
                     em.employee_id = cm.employee_id
                     AND d.dt = cm.dt
    )

-- Final query: add scores over rolling 90 days 
-- ============================================
SELECT employee_id
     , dt
     -- Scores
     , qa_score
     , qa_evaluations
     , pro_worked_seconds
     , cb_call_back_calls
     , cb_handled_calls
     , atc_scheduled_talk_seconds
     , atc_atc_seconds
     , atce_atc_seconds
     , adh_in_adherence_seconds
     , adh_scheduled_seconds
     , occ_occupied_seconds
     , occ_available_seconds
     , occ_phone_seconds
     , ib_resolved_calls
     , ib_calls
     , ob_calls
     , handled_calls
     , aht_handle_seconds
     , att_talk_seconds
     , awt_wrapup_seconds
     , hld_hold_seconds
     , case_age_seconds
     , closed_cases
     -- Scores over rolling 30 days
     , SUM(qa_score) OVER (PARTITION BY employee_id ORDER BY dt ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
    / NULLIF(SUM(qa_evaluations) OVER (PARTITION BY employee_id ORDER BY dt ROWS BETWEEN 29 PRECEDING AND CURRENT ROW),
             0)                                                                                            AS qa_30r
     , SUM(cb_call_back_calls) OVER (PARTITION BY employee_id ORDER BY dt ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
    / NULLIF(SUM(cb_handled_calls)
                 OVER (PARTITION BY employee_id ORDER BY dt ROWS BETWEEN 29 PRECEDING AND CURRENT ROW), 0) AS cb_30r
     , SUM(atc_atc_seconds) OVER (PARTITION BY employee_id ORDER BY dt ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
    / NULLIF(SUM(atc_scheduled_talk_seconds)
                 OVER (PARTITION BY employee_id ORDER BY dt ROWS BETWEEN 29 PRECEDING AND CURRENT ROW), 0) AS atc_30r
     , SUM(atce_atc_seconds) OVER (PARTITION BY employee_id ORDER BY dt ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
    / NULLIF(SUM(atc_scheduled_talk_seconds)
                 OVER (PARTITION BY employee_id ORDER BY dt ROWS BETWEEN 29 PRECEDING AND CURRENT ROW), 0) AS atce_30r
     , SUM(adh_in_adherence_seconds)
           OVER (PARTITION BY employee_id ORDER BY dt ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
    / NULLIF(SUM(adh_scheduled_seconds)
                 OVER (PARTITION BY employee_id ORDER BY dt ROWS BETWEEN 29 PRECEDING AND CURRENT ROW), 0) AS adh_30r
     , SUM(occ_occupied_seconds) OVER (PARTITION BY employee_id ORDER BY dt ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
    / NULLIF(SUM(occ_phone_seconds)
                 OVER (PARTITION BY employee_id ORDER BY dt ROWS BETWEEN 29 PRECEDING AND CURRENT ROW), 0) AS occ_30r
     , SUM(ib_resolved_calls) OVER (PARTITION BY employee_id ORDER BY dt ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
    / NULLIF(SUM(ib_calls) OVER (PARTITION BY employee_id ORDER BY dt ROWS BETWEEN 29 PRECEDING AND CURRENT ROW),
             0)                                                                                            AS ib_res_30r
     , SUM(aht_handle_seconds) OVER (PARTITION BY employee_id ORDER BY dt ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
    / NULLIF(SUM(handled_calls) OVER (PARTITION BY employee_id ORDER BY dt ROWS BETWEEN 29 PRECEDING AND CURRENT ROW),
             0)                                                                                            AS aht_30r
     , SUM(att_talk_seconds) OVER (PARTITION BY employee_id ORDER BY dt ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
    / NULLIF(SUM(handled_calls) OVER (PARTITION BY employee_id ORDER BY dt ROWS BETWEEN 29 PRECEDING AND CURRENT ROW),
             0)                                                                                            AS att_30r
     , SUM(awt_wrapup_seconds) OVER (PARTITION BY employee_id ORDER BY dt ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
    / NULLIF(SUM(handled_calls) OVER (PARTITION BY employee_id ORDER BY dt ROWS BETWEEN 29 PRECEDING AND CURRENT ROW),
             0)                                                                                            AS awt_30r
     , SUM(hld_hold_seconds) OVER (PARTITION BY employee_id ORDER BY dt ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
    / NULLIF(SUM(handled_calls) OVER (PARTITION BY employee_id ORDER BY dt ROWS BETWEEN 29 PRECEDING AND CURRENT ROW),
             0)                                                                                            AS hld_30r
     , avg(case_age_seconds) OVER
    (PARTITION BY employee_id ORDER BY dt ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)                       AS aca_30r
     , sum(closed_cases) OVER
    (PARTITION BY employee_id ORDER BY dt ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)                       AS cc_30r
     -- Scores over rolling 90 days
     , SUM(qa_score) OVER (PARTITION BY employee_id ORDER BY dt ROWS BETWEEN 89 PRECEDING AND CURRENT ROW)
    / NULLIF(SUM(qa_evaluations) OVER (PARTITION BY employee_id ORDER BY dt ROWS BETWEEN 89 PRECEDING AND CURRENT ROW),
             0)                                                                                            AS qa_90r
     , SUM(cb_call_back_calls) OVER (PARTITION BY employee_id ORDER BY dt ROWS BETWEEN 89 PRECEDING AND CURRENT ROW)
    / NULLIF(SUM(cb_handled_calls)
                 OVER (PARTITION BY employee_id ORDER BY dt ROWS BETWEEN 89 PRECEDING AND CURRENT ROW), 0) AS cb_90r
     , SUM(atc_atc_seconds) OVER (PARTITION BY employee_id ORDER BY dt ROWS BETWEEN 89 PRECEDING AND CURRENT ROW)
    / NULLIF(SUM(atc_scheduled_talk_seconds)
                 OVER (PARTITION BY employee_id ORDER BY dt ROWS BETWEEN 89 PRECEDING AND CURRENT ROW), 0) AS atc_90r
     , SUM(atce_atc_seconds) OVER (PARTITION BY employee_id ORDER BY dt ROWS BETWEEN 89 PRECEDING AND CURRENT ROW)
    / NULLIF(SUM(atc_scheduled_talk_seconds)
                 OVER (PARTITION BY employee_id ORDER BY dt ROWS BETWEEN 89 PRECEDING AND CURRENT ROW), 0) AS atce_90r
     , SUM(adh_in_adherence_seconds)
           OVER (PARTITION BY employee_id ORDER BY dt ROWS BETWEEN 89 PRECEDING AND CURRENT ROW)
    / NULLIF(SUM(adh_scheduled_seconds)
                 OVER (PARTITION BY employee_id ORDER BY dt ROWS BETWEEN 89 PRECEDING AND CURRENT ROW), 0) AS adh_90r
     , SUM(occ_occupied_seconds) OVER (PARTITION BY employee_id ORDER BY dt ROWS BETWEEN 89 PRECEDING AND CURRENT ROW)
    / NULLIF(SUM(occ_phone_seconds)
                 OVER (PARTITION BY employee_id ORDER BY dt ROWS BETWEEN 89 PRECEDING AND CURRENT ROW), 0) AS occ_90r
     , SUM(ib_resolved_calls) OVER (PARTITION BY employee_id ORDER BY dt ROWS BETWEEN 89 PRECEDING AND CURRENT ROW)
    / NULLIF(SUM(ib_calls) OVER (PARTITION BY employee_id ORDER BY dt ROWS BETWEEN 89 PRECEDING AND CURRENT ROW),
             0)                                                                                            AS ib_res_90r
     , SUM(aht_handle_seconds) OVER (PARTITION BY employee_id ORDER BY dt ROWS BETWEEN 89 PRECEDING AND CURRENT ROW)
    / NULLIF(SUM(handled_calls) OVER (PARTITION BY employee_id ORDER BY dt ROWS BETWEEN 89 PRECEDING AND CURRENT ROW),
             0)                                                                                            AS aht_90r
     , SUM(att_talk_seconds) OVER (PARTITION BY employee_id ORDER BY dt ROWS BETWEEN 89 PRECEDING AND CURRENT ROW)
    / NULLIF(SUM(handled_calls) OVER (PARTITION BY employee_id ORDER BY dt ROWS BETWEEN 89 PRECEDING AND CURRENT ROW),
             0)                                                                                            AS att_90r
     , SUM(awt_wrapup_seconds) OVER (PARTITION BY employee_id ORDER BY dt ROWS BETWEEN 89 PRECEDING AND CURRENT ROW)
    / NULLIF(SUM(handled_calls) OVER (PARTITION BY employee_id ORDER BY dt ROWS BETWEEN 89 PRECEDING AND CURRENT ROW),
             0)                                                                                            AS awt_90r
     , SUM(hld_hold_seconds) OVER (PARTITION BY employee_id ORDER BY dt ROWS BETWEEN 89 PRECEDING AND CURRENT ROW)
    / NULLIF(SUM(handled_calls) OVER (PARTITION BY employee_id ORDER BY dt ROWS BETWEEN 89 PRECEDING AND CURRENT ROW),
             0)                                                                                            AS hld_90r
     , avg(case_age_seconds) OVER
    (PARTITION BY employee_id ORDER BY dt ROWS BETWEEN 89 PRECEDING AND CURRENT ROW)                       AS aca_90r
     , sum(closed_cases) OVER
    (PARTITION BY employee_id ORDER BY dt ROWS BETWEEN 89 PRECEDING AND CURRENT ROW)                       AS cc_90r
FROM combined_scores
ORDER BY employee_id
       , dt
;


