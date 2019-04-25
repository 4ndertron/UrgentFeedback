WITH projects_raw AS
    (
        SELECT service_name
             , project_id
             , project_name
             , NVL(roc_name, '[blank]') AS roc_name
        FROM sfrpt.t_dm_project
        WHERE installation_complete IS NOT NULL
          AND cancellation_date IS NULL
    )

   , cases_service AS
    (
        SELECT pr.roc_name
             , ca.service_id
             , pr.service_name
             , pr.project_name
             , ca.case_number
             , ca.record_type
             , RTRIM(
                    (CASE WHEN ca.solar_queue = 'Outbound' AND pa1.case_id IS NOT NULL THEN 'Compensation, ' END) ||
                    (CASE
                         WHEN ca.solar_queue = 'Outbound' AND pa2.case_id IS NOT NULL
                             THEN 'Compensation Review, ' END) ||
                    (CASE WHEN ca.solar_queue = 'Outbound' AND pa3.case_id IS NOT NULL THEN 'System Damage, ' END) ||
                    (CASE WHEN ca.solar_queue = 'Tier II' AND pa4.case_id IS NOT NULL THEN 'Service Billing, ' END) ||
                    (CASE WHEN ca.solar_queue = 'Tier II' AND pa5.case_id IS NOT NULL THEN 'Sales Promise, ' END) ||
                    (CASE WHEN ca.solar_queue = 'Tier II' AND pa6.case_id IS NULL THEN 'Performance Analysis, ' END) ||
                    (CASE WHEN ca.solar_queue = 'Outbound' AND pa6.case_id IS NULL THEN 'General, ' END)
            , ', ') AS bucket
--          , (CASE WHEN ca.solar_queue = 'Outbound' AND pa1.case_id IS NOT NULL THEN 1 END) AS is_compensation
--          , (CASE WHEN ca.solar_queue = 'Outbound' AND pa2.case_id IS NOT NULL THEN 1 END) AS is_compensation_review
--          , (CASE WHEN ca.solar_queue = 'Outbound' AND pa3.case_id IS NOT NULL THEN 1 END) AS is_system_damage
--          , (CASE WHEN ca.solar_queue = 'Tier II' AND pa4.case_id IS NOT NULL THEN 1 END) AS is_service_billing
--          , (CASE WHEN ca.solar_queue = 'Tier II' AND pa5.case_id IS NOT NULL THEN 1 END) AS is_sales_promise
--          , (CASE WHEN ca.solar_queue = 'Tier II' AND pa6.case_id IS NULL THEN 1 END) AS is_performance_analysis
--          , (CASE WHEN ca.solar_queue = 'Outbound' AND pa6.case_id IS NULL THEN 1 END) AS is_general
             , ca.status
             , ca.created_date
             , ca.last_modified_date
             , ca.last_comment_date
             , ca.solar_queue
             , ca.primary_reason
             , ca.damage_type
             , ca.origin
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

   , cases_removal_reinstall AS
    (
        SELECT pr.roc_name
             , ca.service_id
             , pr.project_name
             , pr.service_name
             , ca.case_number
             , ca.record_type
             , 'Removal/Reinstall' AS bucket
             , ca.status
             , ca.created_date
             , ca.last_modified_date
             , ca.last_comment_date
             , ca.solar_queue
             , ca.primary_reason
             , ca.damage_type
             , ca.origin
        FROM sfrpt.t_dm_case ca
                 INNER JOIN
             projects_raw pr
             ON
                 ca.project_id = pr.project_id
        WHERE ca.record_type = 'Solar - Panel Removal'
          AND ca.closed_date IS NULL
    )

   , cases_troubleshooting AS
    (
        SELECT pr.roc_name
             , ca.service_id
             , pr.project_name
             , pr.service_name
             , ca.case_number
             , ca.record_type
             , CASE
                   WHEN ca.primary_reason = 'Generation'
                       THEN 'Generation'
                   ELSE 'Communication'
            END AS bucket
             , ca.status
             , ca.created_date
             , ca.last_modified_date
             , ca.last_comment_date
             , ca.solar_queue
             , ca.primary_reason
             , ca.damage_type
             , ca.origin
        FROM sfrpt.t_dm_case ca
                 INNER JOIN
             projects_raw pr
             ON
                 ca.project_id = pr.project_id
        WHERE ca.record_type = 'Solar - Troubleshooting'
          AND UPPER(ca.subject) LIKE '%NF%'
          AND ca.closed_date IS NULL
    )

   , cases_damage AS
    (
        SELECT pr.roc_name
             , ca.service_id
             , pr.project_name
             , pr.service_name
             , ca.case_number
             , ca.record_type
             , CASE
                   WHEN ca.damage_type IN
                        ('Electrical', 'Gutter', 'Miscellaneous', 'Plumbing', 'Home Exterior', 'Home Interior',
                         'Roof Leak', 'Roofing Material', 'Solar Guard', 'Tool Drop')
                       THEN TO_CHAR(ca.damage_type)
                   WHEN ca.damage_type = 'Roofing'
                       THEN 'Roofing Material'
                   ELSE 'Uncategorized'
            END AS bucket
             , ca.status
             , ca.created_date
             , ca.last_modified_date
             , ca.last_comment_date
             , ca.solar_queue
             , ca.primary_reason
             , ca.damage_type
             , ca.origin
        FROM sfrpt.t_dm_case ca
                 INNER JOIN
             projects_raw pr
             ON
                 ca.project_id = pr.project_id
        WHERE ca.record_type IN ('Solar Damage Resolutions', 'Home Damage')
          AND ca.closed_date IS NULL
    )

   , cases_escalation AS
    (
        SELECT pr.roc_name
             , ca.service_id
             , pr.project_name
             , pr.service_name
             , ca.case_number
             , ca.record_type
             , CASE
                   WHEN ca.origin IN
                        ('BBB', 'Executive', 'External', 'Internal', 'Legal', 'Mosaic', 'News Media', 'NPS',
                         'Online Review', 'Other', 'Social Media', 'Special Projects')
                       THEN TO_CHAR(ca.origin)
                   WHEN ca.origin = 'CEO Promise'
                       THEN 'Executive'
                   WHEN ca.origin IN ('Email', 'Phone')
                       THEN 'External'
                   WHEN ca.origin = 'Consumer Review'
                       THEN 'Online Review'
                   ELSE 'Uncategorized'
            END AS bucket
             , ca.status
             , ca.created_date
             , ca.last_modified_date
             , ca.last_comment_date
             , ca.solar_queue
             , ca.primary_reason
             , ca.damage_type
             , ca.origin
        FROM sfrpt.t_dm_case ca
                 INNER JOIN
             projects_raw pr
             ON
                 ca.project_id = pr.project_id
        WHERE ca.record_type = 'Solar - Customer Escalation'
          AND ca.closed_date IS NULL
    )

SELECT ca.roc_name
     , COALESCE(se.service_name, ca.service_name)               AS service_name
     , ca.project_name
     , ca.case_number
     , ca.record_type
     , ca.bucket
     , ca.status
     , TO_CHAR(ca.created_date, 'mm/dd/yyyy hh:mi:ss am')       AS created_date
     , TO_CHAR(ca.last_modified_date, 'mm/dd/yyyy hh:mi:ss am') AS last_modified_date
     , TO_CHAR(ca.last_comment_date, 'mm/dd/yyyy hh:mi:ss am')  AS last_comment_date
     , ca.solar_queue
     , ca.primary_reason
     , ca.damage_type
     , ca.origin
FROM (
         SELECT *
         FROM cases_service
         UNION ALL
         SELECT *
         FROM cases_removal_reinstall
         UNION ALL
         SELECT *
         FROM cases_troubleshooting
         UNION ALL
         SELECT *
         FROM cases_damage
         UNION ALL
         SELECT *
         FROM cases_escalation
     ) ca
         LEFT OUTER JOIN
     sfrpt.t_dm_service se
     ON
         ca.service_id = se.service_id
ORDER BY record_type
       , bucket
       , created_date
;