--Case Inventory
WITH projects_raw AS
    (
        SELECT project_id
             , NVL(roc_name, '[blank]') AS roc_name
        FROM sfrpt.t_dm_project
        WHERE installation_complete IS NOT NULL
          AND cancellation_date IS NULL
    )

   , cases_service AS
    (
        SELECT pr.roc_name
             , COUNT(pr.roc_name) AS case_tally
        FROM sfrpt.t_dm_case ca
                 INNER JOIN
             projects_raw pr
             ON
                 ca.project_id = pr.project_id
        WHERE ca.record_type = 'Solar - Service'
          AND UPPER(ca.subject) LIKE '%NF%'
          AND ca.closed_date IS NULL
        GROUP BY pr.roc_name
    )

   , cases_troubleshooting AS
    (
        SELECT pr.roc_name
             , COUNT(pr.roc_name) AS case_tally
        FROM sfrpt.t_dm_case ca
                 INNER JOIN
             projects_raw pr
             ON
                 ca.project_id = pr.project_id
        WHERE ca.record_type = 'Solar - Troubleshooting'
          AND UPPER(ca.subject) LIKE '%NF%'
          AND ca.closed_date IS NULL
        GROUP BY pr.roc_name
    )

   , cases_damage AS
    (
        SELECT pr.roc_name
             , COUNT(pr.roc_name) AS case_tally
        FROM sfrpt.t_dm_case ca
                 INNER JOIN
             projects_raw pr
             ON
                 ca.project_id = pr.project_id
        WHERE ca.record_type IN ('Solar Damage Resolutions', 'Home Damage')
          AND ca.closed_date IS NULL
        GROUP BY pr.roc_name
    )

   , cases_escalation AS
    (
        SELECT pr.roc_name
             , COUNT(pr.roc_name) AS case_tally
        FROM sfrpt.t_dm_case ca
                 INNER JOIN
             projects_raw pr
             ON
                 ca.project_id = pr.project_id
        WHERE ca.record_type = 'Solar - Customer Escalation'
          AND ca.closed_date IS NULL
        GROUP BY pr.roc_name
    )

   , installs_by_roc AS
    (
        SELECT roc_name
             , COUNT(project_id)                                                        AS install_tally
             , TO_CHAR(100 * RATIO_TO_REPORT(COUNT(project_id)) OVER(), '90.00') || '%' AS install_ratio
        FROM projects_raw
        GROUP BY roc_name
    )

SELECT ibr.roc_name
     , TO_CHAR(ibr.install_tally, '999,990')                                    AS install_tally
     , ibr.install_ratio
     , NVL(cs.case_tally, 0)                                                    AS service_case_tally
     , TO_CHAR(100 * NVL(cs.case_tally, 0) / ibr.install_tally, '90.00') || '%' AS service_case_ratio
     , NVL(ct.case_tally, 0)                                                    AS troubleshooting_case_tally
     , TO_CHAR(100 * NVL(ct.case_tally, 0) / ibr.install_tally, '90.00') || '%' AS troubleshooting_case_ratio
     , NVL(cd.case_tally, 0)                                                    AS damage_case_tally
     , TO_CHAR(100 * NVL(cd.case_tally, 0) / ibr.install_tally, '90.00') || '%' AS damage_case_ratio
     , NVL(ce.case_tally, 0)                                                    AS escalation_case_tally
     , TO_CHAR(100 * NVL(ce.case_tally, 0) / ibr.install_tally, '90.00') || '%' AS escalation_case_ratio
     , NVL(cs.case_tally + ct.case_tally + cd.case_tally + ce.case_tally, 0)    AS overall_case_tally
     , TO_CHAR(100 * (NVL(cs.case_tally, 0) + NVL(ct.case_tally, 0) + NVL(cd.case_tally, 0) + NVL(ce.case_tally, 0)) /
               ibr.install_tally, '90.00') || '%'                               AS overall_case_ratio
FROM installs_by_roc ibr
         LEFT OUTER JOIN
     cases_service cs
     ON
         ibr.roc_name = cs.roc_name
         LEFT OUTER JOIN
     cases_troubleshooting ct
     ON
         ibr.roc_name = ct.roc_name
         LEFT OUTER JOIN
     cases_damage cd
     ON
         ibr.roc_name = cd.roc_name
         LEFT OUTER JOIN
     cases_escalation ce
     ON
         ibr.roc_name = ce.roc_name
ORDER BY DECODE(ibr.roc_name, '[blank]', 'ZZ-99', ibr.roc_name);