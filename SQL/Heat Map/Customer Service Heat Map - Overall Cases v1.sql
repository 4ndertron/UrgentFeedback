-- v1: Branched from Customer Service Heat Map - Dashboard v2.sql
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
        FROM sfrpt.t_dm_case ca
                 INNER JOIN
             projects_raw pr
             ON
                 ca.project_id = pr.project_id
        WHERE ca.record_type = 'Solar - Service'
          AND UPPER(ca.subject) LIKE '%NF%'
          AND ca.solar_queue IN ('Outbound', 'Tier II')
          AND ca.closed_date IS NULL
    )

   , g_cases_service AS
    (
        SELECT roc_name
             , COUNT(roc_name)            AS case_tally
             , COUNT(DISTINCT project_id) AS project_tally
        FROM cases_service
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

   , cases_troubleshooting AS
    (
        SELECT pr.roc_name
             , pr.project_id
        FROM sfrpt.t_dm_case ca
                 INNER JOIN
             projects_raw pr
             ON
                 ca.project_id = pr.project_id
        WHERE ca.record_type = 'Solar - Troubleshooting'
          AND UPPER(ca.subject) LIKE '%NF%'
          AND ca.closed_date IS NULL
    )

   , g_cases_troubleshooting AS
    (
        SELECT roc_name
             , COUNT(roc_name)            AS case_tally
             , COUNT(DISTINCT project_id) AS project_tally
        FROM cases_troubleshooting
        GROUP BY roc_name
    )

   , cases_damage AS
    (
        SELECT pr.roc_name
             , pr.project_id
        FROM sfrpt.t_dm_case ca
                 INNER JOIN
             projects_raw pr
             ON
                 ca.project_id = pr.project_id
        WHERE ca.record_type IN ('Solar Damage Resolutions', 'Home Damage')
          AND ca.closed_date IS NULL
    )

   , g_cases_damage AS
    (
        SELECT roc_name
             , COUNT(roc_name)            AS case_tally
             , COUNT(DISTINCT project_id) AS project_tally
        FROM cases_damage
        GROUP BY roc_name
    )

   , cases_escalation AS
    (
        SELECT pr.roc_name
             , pr.project_id
        FROM sfrpt.t_dm_case ca
                 INNER JOIN
             projects_raw pr
             ON
                 ca.project_id = pr.project_id
        WHERE ca.record_type = 'Solar - Customer Escalation'
          AND ca.closed_date IS NULL
    )

   , g_cases_escalation AS
    (
        SELECT roc_name
             , COUNT(roc_name)            AS case_tally
             , COUNT(DISTINCT project_id) AS project_tally
        FROM cases_escalation
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
                 UNION ALL
                 SELECT roc_name
                      , project_id
                 FROM cases_removal_reinstall
                 UNION ALL
                 SELECT roc_name
                      , project_id
                 FROM cases_troubleshooting
                 UNION ALL
                 SELECT roc_name
                      , project_id
                 FROM cases_damage
                 UNION ALL
                 SELECT roc_name
                      , project_id
                 FROM cases_escalation
             )
        GROUP BY roc_name
    )

SELECT ibr.roc_name
     , TO_CHAR(ibr.install_tally, '999,990') AS install_tally
     , NVL(cs.case_tally, 0)                 AS service_case_tally
     , NVL(cs.project_tally, 0)              AS service_project_tally
     , NVL(cr.case_tally, 0)                 AS removal_case_tally
     , NVL(cr.project_tally, 0)              AS removal_project_tally
     , NVL(ct.case_tally, 0)                 AS troubleshooting_case_tally
     , NVL(ct.project_tally, 0)              AS troubleshooting_project_tally
     , NVL(cd.case_tally, 0)                 AS damage_case_tally
     , NVL(cd.project_tally, 0)              AS damage_project_tally
     , NVL(ce.case_tally, 0)                 AS escalation_case_tally
     , NVL(ce.project_tally, 0)              AS escalation_project_tally
     , NVL(co.case_tally, 0)                 AS overall_case_tally
     , NVL(co.project_tally, 0)              AS overall_project_tally
FROM installs_by_roc ibr
         LEFT OUTER JOIN
     g_cases_service cs
     ON
         ibr.roc_name = cs.roc_name
         LEFT OUTER JOIN
     g_cases_removal_reinstall cr
     ON
         ibr.roc_name = cr.roc_name
         LEFT OUTER JOIN
     g_cases_troubleshooting ct
     ON
         ibr.roc_name = ct.roc_name
         LEFT OUTER JOIN
     g_cases_damage cd
     ON
         ibr.roc_name = cd.roc_name
         LEFT OUTER JOIN
     g_cases_escalation ce
     ON
         ibr.roc_name = ce.roc_name
         LEFT OUTER JOIN
     g_cases_overall co
     ON
         ibr.roc_name = co.roc_name
ORDER BY overall_case_tally DESC
;