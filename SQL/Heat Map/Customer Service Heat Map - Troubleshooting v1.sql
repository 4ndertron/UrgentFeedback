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

   , cases_troubleshooting AS
    (
        SELECT pr.roc_name
             , pr.project_id
             , (CASE WHEN ca.primary_reason = 'Generation' THEN 1 END)          AS is_generation
             , (CASE WHEN NVL(ca.primary_reason, 0) <> 'Generation' THEN 1 END) AS is_communication
        FROM sfrpt.t_dm_case ca
                 INNER JOIN
             projects_raw pr
             ON
                 ca.project_id = pr.project_id
        WHERE ca.record_type = 'Solar - Troubleshooting'
          AND UPPER(ca.subject) LIKE '%NF%'
          AND ca.closed_date IS NULL
    )

   , g_cases_generation AS
    (
        SELECT roc_name
             , COUNT(roc_name)            AS case_tally
             , COUNT(DISTINCT project_id) AS project_tally
        FROM cases_troubleshooting
        WHERE is_generation = 1
        GROUP BY roc_name
    )

   , g_cases_communication AS
    (
        SELECT roc_name
             , COUNT(roc_name)            AS case_tally
             , COUNT(DISTINCT project_id) AS project_tally
        FROM cases_troubleshooting
        WHERE is_communication = 1
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
                 FROM cases_troubleshooting
             )
        GROUP BY roc_name
    )

SELECT ibr.roc_name
     , NVL(cg.case_tally, 0)                                                        AS generation_case_tally
     , NVL(cg.project_tally, 0)                                                     AS generation_project_tally
     , TO_CHAR(100 * NVL(cg.project_tally, 0) / ibr.install_tally, '990.00') || '%' AS generation_project_ratio
     , NVL(cc.case_tally, 0)                                                        AS communication_case_tally
     , NVL(cc.project_tally, 0)                                                     AS communication_project_tally
     , TO_CHAR(100 * NVL(cc.project_tally, 0) / ibr.install_tally, '990.00') || '%' AS communication_project_ratio
     , NVL(co.case_tally, 0)                                                        AS overall_case_tally
     , NVL(co.project_tally, 0)                                                     AS overall_project_tally
     , TO_CHAR(100 * NVL(co.project_tally, 0) / ibr.install_tally, '990.00') || '%' AS overall_project_ratio
FROM installs_by_roc ibr
         LEFT OUTER JOIN
     g_cases_generation cg
     ON
         ibr.roc_name = cg.roc_name
         LEFT OUTER JOIN
     g_cases_communication cc
     ON
         ibr.roc_name = cc.roc_name
         LEFT OUTER JOIN
     g_cases_overall co
     ON
         ibr.roc_name = co.roc_name
ORDER BY overall_case_tally DESC;