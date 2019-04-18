WITH w1 AS -- Employee Info
    (SELECT wd.FULL_NAME            AS name
          , TO_CHAR(wd.EMPLOYEE_ID) AS badge
          , wd.SUPERVISOR_NAME_1    AS supervisor
          , wd.WORK_EMAIL_ADDRESS   AS agent_email
          , wd.BUSINESS_TITLE
     FROM hr.T_EMPLOYEE wd
     WHERE wd.MGR_ID_4 = '101769'
       AND wd.TERMINATED = 0
       AND wd.BUSINESS_TITLE NOT LIKE '%Supervisor%'
       AND wd.BUSINESS_TITLE NOT LIKE '%Project%')
   , w2 AS -- Time Clock Data
    (SELECT DATE_TRUNC('d', wdt.REPORTED_DATE) :: timestamp_ntz AS reported_date
          , wdt.EMPLOYEE_ID                                     AS badge
          , w1.NAME
          , w1.AGENT_EMAIL
          , w1.SUPERVISOR
          , wdt.TOTAL_HOURS * 60                                AS total_minutes
          , TRUNC(ROUND((wdt.TOTAL_HOURS) / 4) * 15)            AS break_time
     FROM hr.V_TIME wdt
              INNER JOIN w1
                         ON w1.BADGE = wdt.EMPLOYEE_ID
     WHERE DATE_TRUNC('d', wdt.REPORTED_DATE) >=
           DATE_TRUNC('d', DATEADD('mm', -4, CURRENT_TIMESTAMP :: datetime))
       AND wdt.CALCULATION_TAGS = 'Hourly')
   , W AS -- Finalized Time
    (SELECT w2.REPORTED_DATE                AS reported_date
          , w2.BADGE
          , w2.NAME
          , w2.AGENT_EMAIL
          , w2.SUPERVISOR
          , ROUND(SUM(w2.total_minutes), 2) AS min_worked
          , ROUND(SUM(w2.BREAK_TIME), 2)    AS break_time
     FROM w2
     GROUP BY w2.REPORTED_DATE
            , w2.BADGE
            , w2.NAME
            , w2.AGENT_EMAIL
            , w2.SUPERVISOR)
   , i1 AS -- Handle Time
    (SELECT io.REPORT_DATE, io.BADGE_ID, io.TOTAL_TIME / 60 AS min_total, io.HANDLE_TIME / 60 AS min_handle
     FROM D_POST_INSTALL.V_IC_OCCUPANCY io
              INNER JOIN w1
                         ON w1.badge = io.badge_id)
   , cc1 AS -- Comments left in 24 hours of ERA
    (SELECT ca.CASE_ID, count(cc.id) > 0 AS comment_in_24hr
     FROM rpt.T_CASE ca
              LEFT JOIN rpt.V_SF_CASECOMMENT cc
                        ON cc.PARENTID = ca.CASE_ID
                            AND
                           cc.CREATEDATE BETWEEN ca.EXECUTIVE_RESOLUTIONS_ACCEPTED AND dateadd('d', 1, ca.EXECUTIVE_RESOLUTIONS_ACCEPTED)
     GROUP BY ca.CASE_ID)
   , cc2 AS -- All Comments (for use of cases w/comment within 7 days)
    (SELECT DISTINCT ca.CASE_ID, date_trunc('d', cc.CREATEDATE) AS created_date
     FROM rpt.T_CASE ca
              INNER JOIN rpt.V_SF_CASECOMMENT cc
                         ON cc.PARENTID = ca.CASE_ID)
   , ch1 AS -- SQL template for looking at the status history of Cases
    (SELECT ca.case_id
          , ca.OWNER_EMPLOYEE_ID
          , CASE
                WHEN ch.caseid IS NOT NULL
                    THEN COALESCE(ch.newvalue,
                                  LEAD(ch.oldvalue) OVER(PARTITION BY ca.case_id ORDER BY ch.createddate),
                                  ca.status)
            END                                                AS case_hist_status
          , ch.createddate                                     AS case_hist_status_start
          , CASE
                WHEN ch.caseid IS NOT NULL
                    THEN NVL(LEAD(ch.createddate) OVER(PARTITION BY ca.case_id ORDER BY ch.createddate),
                             dateadd('d', 1, CURRENT_TIMESTAMP :: datetime))
            END                                                AS case_hist_status_end
          , DATE_TRUNC('d', CA.EXECUTIVE_RESOLUTIONS_ACCEPTED) AS ERA
          , (
                CA.ORIGIN = 'CEO Promise' OR
                CA.ORIGIN = 'Executive' OR
                CA.PRIORITY = '1' OR
                CA.SUBJECT ILIKE '[CEO]%' OR
                CA.SUBJECT LIKE '%1%'
            )                                                  AS IS_P1
          , cc1.comment_in_24hr
     FROM rpt.t_case ca
              INNER JOIN rpt.v_sf_casehistory ch
                         ON ca.case_id = ch.caseid
                             AND ch.field IN ('created', 'Status')
                             AND RECORD_TYPE = 'Solar - Customer Escalation'
                             AND EXECUTIVE_RESOLUTIONS_ACCEPTED IS NOT NULL
                             AND SUBJECT NOT ILIKE '[NPS]%'
                             AND SUBJECT NOT ILIKE '%VIP%'
                             AND ORIGIN != 'NPS'
              LEFT JOIN cc1
                        ON cc1.CASE_ID = ca.CASE_ID)
   , ch2 AS -- PREPARE DE-DUPE: Mark each time a new date range is started
    (SELECT case_id
          , case_hist_status_start
          , case_hist_status_end
          , IFF(DATEDIFF(D,
                         LAG(case_hist_status_end) OVER(PARTITION BY case_id ORDER BY case_hist_status_start),
                         case_hist_status_start) <= 1, 0,
                1) AS new_date_range
          , ERA
          , OWNER_EMPLOYEE_ID
          , IS_P1
          , comment_in_24hr
     FROM ch1
     WHERE case_hist_status NOT IN ('In Dispute', 'Closed', 'Closed - No Contact'))
   , ch3 AS -- PREPARE DE-DUPE: Apply unique number each time a new date range is started
    (SELECT case_id
          , case_hist_status_start
          , case_hist_status_end
          , SUM(new_date_range) OVER(PARTITION BY case_id ORDER BY case_hist_status_start) AS date_range_count
          , IS_P1
          , ERA
          , OWNER_EMPLOYEE_ID
          , comment_in_24hr
     FROM ch2)
   , ch4 AS -- EXECUTE DE-DUPE: Consolidate consecutive rows where there is a good status without skipping any days
    (SELECT case_id
          , date_trunc('d', MIN(case_hist_status_start)) AS created
          , date_trunc('d', MAX(case_hist_status_end))   AS closed
          , CASE
                WHEN date_trunc('d', MIN(case_hist_status_start)) IS NOT NULL
                    THEN 1
                ELSE 0
            END                                          AS TOTAL_CASES
          , any_value(IS_P1)                             AS is_p1
          , any_value(ERA)                               AS era
          , any_value(OWNER_EMPLOYEE_ID)                 AS owner_id
          , any_value(comment_in_24hr)                   AS comment_in_24hr
     FROM ch3
     GROUP BY case_id
            , date_range_count)
   , ch5 AS -- Historical WIP Cases.
    (SELECT D.DT
          , ch4.owner_id
          , d.week_day_num
          , sum(CASE WHEN ch4.era = d.dt THEN 1 END)                     AS era_inflow
          , sum(CASE WHEN ch4.era = d.dt AND comment_in_24hr THEN 1 END) AS inflow_comment_within_24hr
          , SUM(CASE
                    WHEN ch4.era <= d.dt AND
                         (ch4.closed > d.dt OR ch4.closed IS NULL)
                        THEN 1
            END)                                                         AS wip
          , SUM(CASE
                    WHEN ch4.closed = d.dt AND
                         ch4.closed IS NOT NULL
                        THEN 1
            END)                                                         AS closed
          , ROUND(AVG(CASE
                          WHEN ch4.closed = d.dt AND
                               ch4.closed IS NOT NULL
                              THEN datediff('d', ch4.era, ch4.closed)
            END), 2)                                                     AS closed_avg_age
          , SUM(CASE
                    WHEN ch4.closed = d.dt AND
                         datediff('d', date_trunc('d', ch4.era), date_trunc('d', ch4.closed))
                             <= 30
                        THEN total_cases
            END)                                                         AS closed_in_30
          , ROUND(AVG(CASE
                          WHEN ch4.created <= d.dt AND
                               (ch4.closed > d.dt OR ch4.closed IS NULL)
                              THEN datediff('d', ch4.era, d.dt)
            END),
                  2)                                                     AS wip_avg_age
          , ROUND(MEDIAN(CASE
                             WHEN ch4.created <= d.dt AND
                                  (ch4.closed > d.dt OR ch4.closed IS NULL)
                                 THEN datediff('d', ch4.era, d.dt)
            END),
                  2)                                                     AS wip_median_age
          , MAX(CASE
                    WHEN ch4.created <= d.dt AND
                         (ch4.closed > d.dt OR ch4.closed IS NULL)
                        THEN datediff('d', ch4.era, d.dt)
            END)                                                         AS max_age
          , MIN(CASE
                    WHEN ch4.created <= d.dt AND
                         (ch4.closed > d.dt OR ch4.closed IS NULL)
                        THEN datediff('d', ch4.era, d.dt)
            END)                                                         AS min_age
          , count(CASE
                      WHEN exists(SELECT NULL
                                  FROM cc2
                                  WHERE cc2.CASE_ID = ch4.CASE_ID
                                    AND cc2.created_date >= dateadd('d', -7, d.DT)
                                    AND cc2.created_date < d.dt
                                    AND cc2.created_date < ch4.closed)
                          THEN 1
            END)                                                         AS comment_in_past_7
     FROM rpt.V_DATES d
              INNER JOIN ch4
                         ON d.dt <= date_trunc('d', current_timestamp :: datetime)
     WHERE (ch4.CLOSED >= DATEADD('y', -1, DATE_TRUNC('d', CURRENT_TIMESTAMP ::
         datetime))
         OR ch4.CLOSED IS NULL)
     GROUP BY d.DT, ch4.owner_id, d.WEEK_DAY_NUM)
   , s1 AS -- All aNPS Surveys
    (SELECT date_trunc('d', nps.survey_ended_at) AS survey_date,
            ma.workday_name                      AS full_name,
            ma.badge_id                          AS badge_id,
            CASE
                WHEN nps.anps_score >= 9
                    THEN 100
                WHEN nps.anps_score >= 7
                    THEN 0
                WHEN nps.anps_score >= 0
                    THEN -100
                END                              AS anps_value
     FROM d_post_install.t_nps_survey_response nps
              INNER JOIN d_post_install.t_master_agents ma
                         ON nps.employee_user_id = ma.salesforce_user_id
     WHERE nps.survey_type = '530 - Executive Resolutions')
   , s2 AS (SELECT s1.survey_date     AS survey_date
                 , s1.badge_id        AS badge_id
                 , count(*)           AS survey_count
                 , avg(s1.anps_value) AS avg_anps_score
            FROM s1
            GROUP BY s1.survey_date, s1.badge_id)
SELECT w.reported_date                AS date_workday
     , w.badge                        AS employee_badge
     , w.name                         AS employee
     , w.min_worked                   AS minutes_worked
     , i1.min_total                   AS phone_logged
     , i1.min_handle                  AS handle_time
     , ch5.era_inflow                 AS inflow
     , ch5.inflow_comment_within_24hr AS inflow_comment_within_24hr
     , ch5.closed                     AS outflow
     , ch5.closed_avg_age             AS outflow_avg_age
     , ch5.wip                        AS wip
     , ch5.comment_in_past_7          AS comment_in_past_7
     , ch5.wip_avg_age                AS wip_avg_age
     , ch5.wip_median_age             AS wip_median_age
     , ch5.max_age                    AS wip_max_age
     , ch5.min_age                    AS wip_min_age
     , s2.survey_count                AS email_survey_count
     , s2.avg_anps_score              AS email_survey_avg_anps
FROM w
         LEFT JOIN i1
                   ON i1.REPORT_DATE = w.reported_date
                       AND i1.BADGE_ID = w.badge
         LEFT JOIN ch5
                   ON ch5.dt = w.reported_date
                       AND ch5.owner_id = w.badge
         LEFT JOIN s2
                   ON s2.survey_date = w.reported_date
                       AND s2.badge_id = w.badge
ORDER BY w.badge, w.reported_date DESC