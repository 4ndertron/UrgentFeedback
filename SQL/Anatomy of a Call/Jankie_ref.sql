WITH qa        AS -- Calabrio QA Scores
    (SELECT DISTINCT
            rc.ASSOC_CALL_ID
          , p.EMPLOYEE_ID                        AS agent_badge
          , rc.AGENT_DISPLAY_ID                  AS agent_evaluated
          , rc.TEAM_NAME
          , to_date(rc.EVALUATION_EVALUATED)     AS evaluation_date
          , rc.RECORDING_CONTACT_ID              AS contact_id
          , to_number(rc.EVALUATION_TOTAL_SCORE) AS qa_score
          , rc.EVALUATOR_DISPLAY_ID              AS evaluator
          , rc.EVALUATOR_USER_NAME               AS evaluator_email
     FROM CALABRIO.T_RECORDING_CONTACT rc
              LEFT JOIN CALABRIO.T_PERSONS p
                        ON p.ACD_ID = rc.AGENT_ACD_ID)
   , calls     AS (SELECT cj.session_id
                        , ea.EMPLOYEE_ID
                        , ea.FULL_NAME
                        , IFF(ea.SUPERVISORY_ORG ILIKE '%Teleperformance%', 'TP', 'VSLR') AS location
                        , CASE WHEN ea.BUSINESS_SITE_NAME ILIKE '%Work from Home%'
                                   THEN 'Home'
                               WHEN location = 'TP'
                                   THEN 'Bogota'
                                   ELSE 'Lehi'
                          END                                                             AS site_location
                        , NVL(NULLIF(TRIM(replace(ea.SUPERVISORY_ORG, 'Teleperformance', '')), ''),
                              'Undefined')                                                AS supervisory_org
                        , TRIM(REPLACE(ea.COST_CENTER, 'Solar', ''))                      AS cost_center
                        , e.SUPERVISOR_NAME_1                                             AS direct_supervisor
                        , e.SUPERVISOR_NAME_2                                             AS ACCM
                        , cj.date                                                         AS call_date
                        , CASE WHEN cj.QUEUE_1 ILIKE '%outdial%'
                                   THEN 'Outbound'
                               WHEN cj.QUEUE_1 IS NULL
                                   THEN 'Unknown'
                                   ELSE 'Inbound'
                          END                                                             AS in_out
                        , NVL(REPLACE(cj.queue_1, '_', ' '), 'Unknown')                   AS queue_name
                        , ROUND((IFF(cj.CONNECTED > 0, cj.ON_HOLD + cj.WRAPUP + cj.CONNECTED, NULL)) / 60,
                                2)                                                        AS handle_time
                        , ROUND((IFF(cj.CONNECTED > 0, cj.parked + cj.ringing, NULL)) / 60,
                                2)                                                        AS answer_speed_time
                        , ROUND(cj.on_hold / 60, 2)                                       AS hold_time
                        , ROUND(cj.WRAPUP / 60, 2)                                        AS after_call_work
                        , qa.qa_score
                        , datediff('d', ea.HIRE_DATE, cj.DATE)                            AS agent_tenure_as_of_call
                        , CASE WHEN agent_tenure_as_of_call BETWEEN 0 AND 30
                                   THEN '1. 0 - 30'
                               WHEN agent_tenure_as_of_call BETWEEN 30 AND 60
                                   THEN '2. 30 - 60'
                               WHEN agent_tenure_as_of_call BETWEEN 60 AND 90
                                   THEN '3. 60 - 90'
                               WHEN agent_tenure_as_of_call BETWEEN 90 AND 180
                                   THEN '4. 90 - 180'
                               WHEN agent_tenure_as_of_call >= 180
                                   THEN '5. 180 +'
                          END                                                             AS agent_tenure_bucket
                        , CURRENT_DATE                                                    AS last_refreshed
                   FROM d_post_install.t_cjp_cdr_temp cj
                            INNER JOIN rpt.t_dates dt
                                       ON cj.date = dt.dt
                            LEFT JOIN qa
                                      ON qa.ASSOC_CALL_ID = cj.SESSION_ID
                            LEFT JOIN CALABRIO.T_PERSONS p
                                      ON p.ACD_ID = cj.AGENT_1_ACD_ID
                            LEFT JOIN hr.T_EMPLOYEE_ALL ea
                                      ON ea.EMPLOYEE_ID = p.EMPLOYEE_ID AND
                                         cj.DATE BETWEEN ea.CREATED_DATE AND ea.EXPIRY_DATE
                            LEFT JOIN hr.T_EMPLOYEE e
                                      ON e.EMPLOYEE_ID = p.EMPLOYEE_ID
                   WHERE cj.DATE >= current_date - 90
                     AND cj.session_id IS NOT NULL
                     AND cj.contact_type != 'Master'
                     AND cj.connected > 0)
   , tp_queues AS (SELECT DISTINCT c.queue_name FROM calls c WHERE c.location = 'TP')
   , main      AS (SELECT c.*
                   FROM calls c
                            INNER JOIN tp_queues tq
                                       ON tq.queue_name = c.queue_name)
   , union_all AS (SELECT *, '0. Yesterday' AS date_slicer
                   FROM main m
                   WHERE m.call_date >= current_date - 1
                   UNION ALL
                   (SELECT *, '1. Last 7 Days' AS date_slicer
                    FROM main m
                    WHERE m.call_date >= current_date - 7)
                   UNION ALL
                   (SELECT *, '2. Last 30 Days' AS date_slicer
                    FROM main m
                    WHERE m.call_date >= current_date - 30)
                   UNION ALL
                   (SELECT *, '3. Last 90 Days' AS date_slicer
                    FROM main m
                    WHERE m.call_date >= current_date - 90))
SELECT *
FROM calls