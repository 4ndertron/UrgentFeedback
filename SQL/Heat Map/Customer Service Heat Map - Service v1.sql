WITH projects_raw AS
    (
        SELECT project_id
             , NVL(roc_name, '[blank]') AS roc_name
        FROM sfrpt.t_dm_project
        WHERE installation_complete IS NOT NULL
          AND cancellation_date IS NULL
    )

   , installs_by_roc AS
    (
        SELECT roc_name
             , COUNT(project_id)                                                        AS install_tally
             , TO_CHAR(100 * RATIO_TO_REPORT(COUNT(project_id)) OVER(), '90.00') || '%' AS install_ratio
        FROM projects_raw
        GROUP BY roc_name
    )

   , cases_service AS
    (
        SELECT pr.roc_name
             , pr.project_id
             , (CASE WHEN ca.solar_queue = 'Outbound' AND pa1.case_id IS NOT NULL THEN 1 END) AS is_compensation
             , (CASE WHEN ca.solar_queue = 'Outbound' AND pa2.case_id IS NOT NULL THEN 1 END) AS is_compensation_review
             , (CASE WHEN ca.solar_queue = 'Outbound' AND pa3.case_id IS NOT NULL THEN 1 END) AS is_system_damage
             , (CASE WHEN ca.solar_queue = 'Tier II' AND pa4.case_id IS NOT NULL THEN 1 END)  AS is_service_billing
             , (CASE WHEN ca.solar_queue = 'Tier II' AND pa5.case_id IS NOT NULL THEN 1 END)  AS is_sales_promise
             , (CASE WHEN ca.solar_queue = 'Tier II' AND pa6.case_id IS NULL THEN 1 END)      AS is_performance_analysis
             , (CASE WHEN ca.solar_queue = 'Outbound' AND pa6.case_id IS NULL THEN 1 END)     AS is_general
        FROM sfrpt.t_dm_case ca
                 INNER JOIN
             projects_raw pr
             ON
                 ca.project_id = pr.project_id
                 LEFT OUTER JOIN
             (SELECT DISTINCT case_id
              FROM sfrpt.t_dm_payment
              WHERE case_id IS NOT NULL
                AND record_type = 'Customer Compensation'
                AND status = 'Denied') pa1
             ON
                 ca.case_id = pa1.case_id
                 LEFT OUTER JOIN
             (SELECT DISTINCT case_id
              FROM sfrpt.t_dm_payment
              WHERE case_id IS NOT NULL
                AND record_type = 'Customer Compensation'
                AND NVL(status, 0) <> 'Denied') pa2
             ON
                 ca.case_id = pa2.case_id
                 LEFT OUTER JOIN
             (SELECT DISTINCT case_id
              FROM sfrpt.t_dm_payment
              WHERE case_id IS NOT NULL AND record_type = 'Customer Payments') pa3
             ON
                 ca.case_id = pa3.case_id
                 LEFT OUTER JOIN
             (SELECT DISTINCT case_id
              FROM sfrpt.t_dm_payment
              WHERE case_id IS NOT NULL AND record_type = 'Customer Payments') pa4
             ON
                 ca.case_id = pa4.case_id
                 LEFT OUTER JOIN
             (SELECT DISTINCT case_id
              FROM sfrpt.t_dm_payment
              WHERE case_id IS NOT NULL AND record_type = 'Customer Compensation') pa5
             ON
                 ca.case_id = pa5.case_id
                 LEFT OUTER JOIN
             (SELECT DISTINCT case_id FROM sfrpt.t_dm_payment WHERE case_id IS NOT NULL) pa6
             ON
                 ca.case_id = pa6.case_id
        WHERE ca.record_type = 'Solar - Service'
          AND UPPER(ca.subject) LIKE '%NF%'
          AND ca.solar_queue IN ('Outbound', 'Tier II')
          AND ca.closed_date IS NULL
    )

   , g_cases_compensation AS
    (
        SELECT roc_name
             , COUNT(roc_name)            AS case_tally
             , COUNT(DISTINCT project_id) AS project_tally
        FROM cases_service
        WHERE is_compensation = 1
        GROUP BY roc_name
    )

   , g_cases_compensation_review AS
    (
        SELECT roc_name
             , COUNT(roc_name)            AS case_tally
             , COUNT(DISTINCT project_id) AS project_tally
        FROM cases_service
        WHERE is_compensation_review = 1
        GROUP BY roc_name
    )

   , g_cases_system_damage AS
    (
        SELECT roc_name
             , COUNT(roc_name)            AS case_tally
             , COUNT(DISTINCT project_id) AS project_tally
        FROM cases_service
        WHERE is_system_damage = 1
        GROUP BY roc_name
    )

   , g_cases_service_billing AS
    (
        SELECT roc_name
             , COUNT(roc_name)            AS case_tally
             , COUNT(DISTINCT project_id) AS project_tally
        FROM cases_service
        WHERE is_service_billing = 1
        GROUP BY roc_name
    )

   , g_cases_sales_promise AS
    (
        SELECT roc_name
             , COUNT(roc_name)            AS case_tally
             , COUNT(DISTINCT project_id) AS project_tally
        FROM cases_service
        WHERE is_sales_promise = 1
        GROUP BY roc_name
    )

   , g_cases_performance_analysis AS
    (
        SELECT roc_name
             , COUNT(roc_name)            AS case_tally
             , COUNT(DISTINCT project_id) AS project_tally
        FROM cases_service
        WHERE is_performance_analysis = 1
        GROUP BY roc_name
    )

   , g_cases_general AS
    (
        SELECT roc_name
             , COUNT(roc_name)            AS case_tally
             , COUNT(DISTINCT project_id) AS project_tally
        FROM cases_service
        WHERE is_general = 1
        GROUP BY roc_name
    )

   , cases_removal_reinstall AS
    (
        SELECT pr.roc_name
             , pr.project_id
        FROM sfrpt.t_dm_case ca
                 INNER JOIN
             projects_raw pr
             ON
                 ca.project_id = pr.project_id
        WHERE ca.record_type = 'Solar - Panel Removal'
          AND ca.closed_date IS NULL
    )

   , g_cases_removal_reinstall AS
    (
        SELECT roc_name
             , COUNT(roc_name)            AS case_tally
             , COUNT(DISTINCT project_id) AS project_tally
        FROM cases_removal_reinstall
        GROUP BY roc_name
    )

   , g_cases_overall AS
    (
        SELECT roc_name
             , COUNT(roc_name)            AS case_tally
             , COUNT(DISTINCT project_id) AS project_tally
        FROM (
                 SELECT roc_name
                      , project_id
                 FROM cases_service
                 WHERE is_compensation = 1
                    OR is_compensation_review = 1
                    OR is_system_damage = 1
                    OR is_service_billing = 1
                    OR is_sales_promise = 1
                    OR is_performance_analysis = 1
                    OR is_general = 1
                 UNION ALL
                 SELECT roc_name
                      , project_id
                 FROM cases_removal_reinstall
             )
        GROUP BY roc_name
    )

SELECT ibr.roc_name
     , NVL(cc.case_tally, 0)                                                         AS compensation_case_tally
     , NVL(cc.project_tally, 0)                                                      AS compensation_project_tally
     , TO_CHAR(100 * NVL(cc.project_tally, 0) / ibr.install_tally, '990.00') || '%'  AS compensation_project_ratio
     , NVL(ccr.case_tally, 0)                                                        AS comp_review_case_tally
     , NVL(ccr.project_tally, 0)                                                     AS comp_review_project_tally
     , TO_CHAR(100 * NVL(ccr.project_tally, 0) / ibr.install_tally, '990.00') || '%' AS comp_review_project_ratio
     , NVL(csd.case_tally, 0)                                                        AS system_damage_case_tally
     , NVL(csd.project_tally, 0)                                                     AS system_damage_project_tally
     , TO_CHAR(100 * NVL(csd.project_tally, 0) / ibr.install_tally, '990.00') || '%' AS system_damage_project_ratio
     , NVL(csb.case_tally, 0)                                                        AS service_billing_case_tally
     , NVL(csb.project_tally, 0)                                                     AS service_billing_project_tally
     , TO_CHAR(100 * NVL(csb.project_tally, 0) / ibr.install_tally, '990.00') || '%' AS service_billing_project_ratio
     , NVL(csp.case_tally, 0)                                                        AS sales_promise_case_tally
     , NVL(csp.project_tally, 0)                                                     AS sales_promise_project_tally
     , TO_CHAR(100 * NVL(csp.project_tally, 0) / ibr.install_tally, '990.00') || '%' AS sales_promise_project_ratio
     , NVL(cpa.case_tally, 0)                                                        AS perf_analysis_case_tally
     , NVL(cpa.project_tally, 0)                                                     AS perf_analysis_project_tally
     , TO_CHAR(100 * NVL(cpa.project_tally, 0) / ibr.install_tally, '990.00') || '%' AS perf_analysis_project_ratio
     , NVL(cg.case_tally, 0)                                                         AS general_case_tally
     , NVL(cg.project_tally, 0)                                                      AS general_project_tally
     , TO_CHAR(100 * NVL(cg.project_tally, 0) / ibr.install_tally, '990.00') || '%'  AS general_project_ratio
     , NVL(crr.case_tally, 0)                                                        AS removal_case_tally
     , NVL(crr.project_tally, 0)                                                     AS removal_project_tally
     , TO_CHAR(100 * NVL(crr.project_tally, 0) / ibr.install_tally, '990.00') || '%' AS removal_project_ratio
     , NVL(co.case_tally, 0)                                                         AS overall_case_tally
     , NVL(co.project_tally, 0)                                                      AS overall_project_tally
     , TO_CHAR(100 * NVL(co.project_tally, 0) / ibr.install_tally, '990.00') || '%'  AS overall_project_ratio
FROM installs_by_roc ibr
         LEFT OUTER JOIN
     g_cases_compensation cc
     ON
         ibr.roc_name = cc.roc_name
         LEFT OUTER JOIN
     g_cases_compensation_review ccr
     ON
         ibr.roc_name = ccr.roc_name
         LEFT OUTER JOIN
     g_cases_system_damage csd
     ON
         ibr.roc_name = csd.roc_name
         LEFT OUTER JOIN
     g_cases_service_billing csb
     ON
         ibr.roc_name = csb.roc_name
         LEFT OUTER JOIN
     g_cases_sales_promise csp
     ON
         ibr.roc_name = csp.roc_name
         LEFT OUTER JOIN
     g_cases_performance_analysis cpa
     ON
         ibr.roc_name = cpa.roc_name
         LEFT OUTER JOIN
     g_cases_general cg
     ON
         ibr.roc_name = cg.roc_name
         LEFT OUTER JOIN
     g_cases_removal_reinstall crr
     ON
         ibr.roc_name = crr.roc_name
         LEFT OUTER JOIN
     g_cases_overall co
     ON
         ibr.roc_name = co.roc_name
ORDER BY overall_case_tally DESC;