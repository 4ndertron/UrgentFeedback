WITH
   -- ==========================================
   -- Target projects (installed, not cancelled)
   -- ==========================================
    projects_raw AS
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
        FROM projects_raw
        GROUP BY roc_name
    )

   -- ========================
   -- Raw Cases by record type
   -- ========================
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
          AND ca.created_date >= TRUNC(SYSDATE - 90)
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
          AND ca.created_date >= TRUNC(SYSDATE - 90)
    )

   , cases_troubleshooting AS
    (
        SELECT pr.roc_name
             , pr.project_id
             , CASE
                   WHEN ca.primary_reason = 'Generation'
                       THEN 'Generation'
                   ELSE 'Communication'
            END AS bucket
        FROM sfrpt.t_dm_case ca
                 INNER JOIN
             projects_raw pr
             ON
                 ca.project_id = pr.project_id
        WHERE ca.record_type = 'Solar - Troubleshooting'
          AND UPPER(ca.subject) LIKE '%NF%'
          AND ca.closed_date IS NULL
          AND ca.created_date >= TRUNC(SYSDATE - 90)
    )

   , cases_damage AS
    (
        SELECT pr.roc_name
             , pr.project_id
             , CASE
                   WHEN ca.damage_type IN
                        ('Electrical', 'Gutter', 'Miscellaneous', 'Plumbing', 'Home Exterior', 'Home Interior',
                         'Roof Leak', 'Roofing Material', 'Solar Guard', 'Tool Drop')
                       THEN TO_CHAR(ca.damage_type)
                   WHEN ca.damage_type = 'Roofing'
                       THEN 'Roofing Material'
                   ELSE 'Other'
            END AS bucket
        FROM sfrpt.t_dm_case ca
                 INNER JOIN
             projects_raw pr
             ON
                 ca.project_id = pr.project_id
        WHERE ca.record_type IN ('Solar Damage Resolutions', 'Home Damage')
          AND ca.closed_date IS NULL
          AND ca.created_date >= TRUNC(SYSDATE - 90)
    )

   , cases_escalation AS
    (
        SELECT pr.roc_name
             , pr.project_id
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
        FROM sfrpt.t_dm_case ca
                 INNER JOIN
             projects_raw pr
             ON
                 ca.project_id = pr.project_id
        WHERE ca.record_type = 'Solar - Customer Escalation'
          AND ca.closed_date IS NULL
          AND ca.created_date >= TRUNC(SYSDATE - 90)
    )

   -- ==================
   -- Case counts by ROC
   -- ==================
   , g_cases_service AS
    (
        SELECT roc_name
             , COUNT(roc_name) AS case_tally
        FROM cases_service
        GROUP BY roc_name
    )

   , g_cases_removal_reinstall AS
    (
        SELECT roc_name
             , COUNT(roc_name) AS case_tally
        FROM cases_removal_reinstall
        GROUP BY roc_name
    )

   , g_cases_troubleshooting AS
    (
        SELECT roc_name
             , COUNT(roc_name) AS case_tally
        FROM cases_troubleshooting
        GROUP BY roc_name
    )

   , g_cases_damage AS
    (
        SELECT roc_name
             , COUNT(roc_name) AS case_tally
        FROM cases_damage
        GROUP BY roc_name
    )

   , g_cases_escalation AS
    (
        SELECT roc_name
             , COUNT(roc_name) AS case_tally
        FROM cases_escalation
        GROUP BY roc_name
    )

   , g_cases_s_compensation AS
    (
        SELECT roc_name
             , COUNT(roc_name) AS case_tally
        FROM cases_service
        WHERE is_compensation = 1
        GROUP BY roc_name
    )

   , g_cases_s_compensation_review AS
    (
        SELECT roc_name
             , COUNT(roc_name) AS case_tally
        FROM cases_service
        WHERE is_compensation_review = 1
        GROUP BY roc_name
    )

   , g_cases_s_system_damage AS
    (
        SELECT roc_name
             , COUNT(roc_name) AS case_tally
        FROM cases_service
        WHERE is_system_damage = 1
        GROUP BY roc_name
    )

   , g_cases_s_service_billing AS
    (
        SELECT roc_name
             , COUNT(roc_name) AS case_tally
        FROM cases_service
        WHERE is_service_billing = 1
        GROUP BY roc_name
    )

   , g_cases_s_sales_promise AS
    (
        SELECT roc_name
             , COUNT(roc_name) AS case_tally
        FROM cases_service
        WHERE is_sales_promise = 1
        GROUP BY roc_name
    )

   , g_cases_s_performance_analysis AS
    (
        SELECT roc_name
             , COUNT(roc_name) AS case_tally
        FROM cases_service
        WHERE is_performance_analysis = 1
        GROUP BY roc_name
    )

   , g_cases_s_general AS
    (
        SELECT roc_name
             , COUNT(roc_name) AS case_tally
        FROM cases_service
        WHERE is_general = 1
        GROUP BY roc_name
    )

   , g_cases_t_generation AS
    (
        SELECT roc_name
             , COUNT(roc_name) AS case_tally
        FROM cases_troubleshooting
        WHERE bucket = 'Generation'
        GROUP BY roc_name
    )

   , g_cases_t_communication AS
    (
        SELECT roc_name
             , COUNT(roc_name) AS case_tally
        FROM cases_troubleshooting
        WHERE bucket = 'Communication'
        GROUP BY roc_name
    )

   , g_cases_d_electrical AS
    (
        SELECT roc_name
             , COUNT(roc_name) AS case_tally
        FROM cases_damage
        WHERE bucket = 'Electrical'
        GROUP BY roc_name
    )

   , g_cases_d_gutter AS
    (
        SELECT roc_name
             , COUNT(roc_name) AS case_tally
        FROM cases_damage
        WHERE bucket = 'Gutter'
        GROUP BY roc_name
    )

   , g_cases_d_miscellaneous AS
    (
        SELECT roc_name
             , COUNT(roc_name) AS case_tally
        FROM cases_damage
        WHERE bucket = 'Miscellaneous'
        GROUP BY roc_name
    )

   , g_cases_d_plumbing AS
    (
        SELECT roc_name
             , COUNT(roc_name) AS case_tally
        FROM cases_damage
        WHERE bucket = 'Plumbing'
        GROUP BY roc_name
    )

   , g_cases_d_home_exterior AS
    (
        SELECT roc_name
             , COUNT(roc_name) AS case_tally
        FROM cases_damage
        WHERE bucket = 'Home Exterior'
        GROUP BY roc_name
    )

   , g_cases_d_home_interior AS
    (
        SELECT roc_name
             , COUNT(roc_name) AS case_tally
        FROM cases_damage
        WHERE bucket = 'Home Interior'
        GROUP BY roc_name
    )

   , g_cases_d_roof_leak AS
    (
        SELECT roc_name
             , COUNT(roc_name) AS case_tally
        FROM cases_damage
        WHERE bucket = 'Roof Leak'
        GROUP BY roc_name
    )

   , g_cases_d_roofing_material AS
    (
        SELECT roc_name
             , COUNT(roc_name) AS case_tally
        FROM cases_damage
        WHERE bucket = 'Roofing Material'
        GROUP BY roc_name
    )

   , g_cases_d_solar_guard AS
    (
        SELECT roc_name
             , COUNT(roc_name) AS case_tally
        FROM cases_damage
        WHERE bucket = 'Solar Guard'
        GROUP BY roc_name
    )

   , g_cases_d_tool_drop AS
    (
        SELECT roc_name
             , COUNT(roc_name) AS case_tally
        FROM cases_damage
        WHERE bucket = 'Tool Drop'
        GROUP BY roc_name
    )

   , g_cases_d_other AS
    (
        SELECT roc_name
             , COUNT(roc_name) AS case_tally
        FROM cases_damage
        WHERE bucket = 'Other'
        GROUP BY roc_name
    )

   , g_cases_e_bbb AS
    (
        SELECT roc_name
             , COUNT(roc_name) AS case_tally
        FROM cases_escalation
        WHERE bucket = 'BBB'
        GROUP BY roc_name
    )

   , g_cases_e_executive AS
    (
        SELECT roc_name
             , COUNT(roc_name) AS case_tally
        FROM cases_escalation
        WHERE bucket = 'Executive'
        GROUP BY roc_name
    )

   , g_cases_e_external AS
    (
        SELECT roc_name
             , COUNT(roc_name) AS case_tally
        FROM cases_escalation
        WHERE bucket = 'External'
        GROUP BY roc_name
    )

   , g_cases_e_internal AS
    (
        SELECT roc_name
             , COUNT(roc_name) AS case_tally
        FROM cases_escalation
        WHERE bucket = 'Internal'
        GROUP BY roc_name
    )

   , g_cases_e_legal AS
    (
        SELECT roc_name
             , COUNT(roc_name) AS case_tally
        FROM cases_escalation
        WHERE bucket = 'Legal'
        GROUP BY roc_name
    )

   , g_cases_e_mosaic AS
    (
        SELECT roc_name
             , COUNT(roc_name) AS case_tally
        FROM cases_escalation
        WHERE bucket = 'Mosaic'
        GROUP BY roc_name
    )

   , g_cases_e_news_media AS
    (
        SELECT roc_name
             , COUNT(roc_name) AS case_tally
        FROM cases_escalation
        WHERE bucket = 'News Media'
        GROUP BY roc_name
    )

   , g_cases_e_nps AS
    (
        SELECT roc_name
             , COUNT(roc_name) AS case_tally
        FROM cases_escalation
        WHERE bucket = 'NPS'
        GROUP BY roc_name
    )

   , g_cases_e_online_review AS
    (
        SELECT roc_name
             , COUNT(roc_name) AS case_tally
        FROM cases_escalation
        WHERE bucket = 'Online Review'
        GROUP BY roc_name
    )

   , g_cases_e_other AS
    (
        SELECT roc_name
             , COUNT(roc_name) AS case_tally
        FROM cases_escalation
        WHERE bucket = 'Other'
        GROUP BY roc_name
    )

   , g_cases_e_social_media AS
    (
        SELECT roc_name
             , COUNT(roc_name) AS case_tally
        FROM cases_escalation
        WHERE bucket = 'Social Media'
        GROUP BY roc_name
    )

   , g_cases_e_special_projects AS
    (
        SELECT roc_name
             , COUNT(roc_name) AS case_tally
        FROM cases_escalation
        WHERE bucket = 'Special Projects'
        GROUP BY roc_name
    )

SELECT ibr.roc_name
     , NVL(cs.case_tally, 0)   AS dash_service
     , NVL(cr.case_tally, 0)   AS dash_removal_reinstall
     , NVL(ct.case_tally, 0)   AS dash_troubleshooting
     , NVL(cd.case_tally, 0)   AS dash_damage
     , NVL(ce.case_tally, 0)   AS dash_escalation
     , NVL(cs.case_tally, 0) + NVL(cr.case_tally, 0) + NVL(ct.case_tally, 0) + NVL(cd.case_tally, 0) +
       NVL(ce.case_tally, 0)   AS dash_overall
     , NVL(csc.case_tally, 0)  AS service_compensation
     , NVL(cscr.case_tally, 0) AS service_compensation_review
     , NVL(cssd.case_tally, 0) AS service_system_damage
     , NVL(cssb.case_tally, 0) AS service_service_billing
     , NVL(cssp.case_tally, 0) AS service_sales_promise
     , NVL(cspa.case_tally, 0) AS service_performance_analysis
     , NVL(csg.case_tally, 0)  AS service_general
     , NVL(cr.case_tally, 0)   AS service_removal_reinstall
     , NVL(cs.case_tally, 0)   AS service_overall
     , NVL(ctg.case_tally, 0)  AS troubleshooting_generation
     , NVL(ctc.case_tally, 0)  AS troubleshooting_communication
     , NVL(ct.case_tally, 0)   AS troubleshooting_overall
     , NVL(cde.case_tally, 0)  AS damage_electrical
     , NVL(cdg.case_tally, 0)  AS damage_gutter
     , NVL(cdm.case_tally, 0)  AS damage_miscellaneous
     , NVL(cdp.case_tally, 0)  AS damage_plumbing
     , NVL(cdhe.case_tally, 0) AS damage_home_exterior
     , NVL(cdhi.case_tally, 0) AS damage_home_interior
     , NVL(cdrl.case_tally, 0) AS damage_roof_leak
     , NVL(cdrm.case_tally, 0) AS damage_roofing_material
     , NVL(cdsg.case_tally, 0) AS damage_solar_guard
     , NVL(cdtd.case_tally, 0) AS damage_tool_drop
     , NVL(cdo.case_tally, 0)  AS damage_other
     , NVL(cd.case_tally, 0)   AS damage_overall
     , NVL(ceb.case_tally, 0)  AS escalation_bbb
     , NVL(cee.case_tally, 0)  AS escalation_executive
     , NVL(cee2.case_tally, 0) AS escalation_external
     , NVL(cei.case_tally, 0)  AS escalation_internal
     , NVL(cel.case_tally, 0)  AS escalation_legal
     , NVL(cem.case_tally, 0)  AS escalation_mosaic
     , NVL(cenm.case_tally, 0) AS escalation_news_media
     , NVL(cen.case_tally, 0)  AS escalation_nps
     , NVL(ceor.case_tally, 0) AS escalation_online_review
     , NVL(ceo.case_tally, 0)  AS escalation_other
     , NVL(cesm.case_tally, 0) AS escalation_social_media
     , NVL(cesp.case_tally, 0) AS escalation_special_projects
     , NVL(ce.case_tally, 0)   AS escalation_overall
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
     g_cases_s_compensation csc
     ON
         ibr.roc_name = csc.roc_name
         LEFT OUTER JOIN
     g_cases_s_compensation_review cscr
     ON
         ibr.roc_name = cscr.roc_name
         LEFT OUTER JOIN
     g_cases_s_system_damage cssd
     ON
         ibr.roc_name = cssd.roc_name
         LEFT OUTER JOIN
     g_cases_s_service_billing cssb
     ON
         ibr.roc_name = cssb.roc_name
         LEFT OUTER JOIN
     g_cases_s_sales_promise cssp
     ON
         ibr.roc_name = cssp.roc_name
         LEFT OUTER JOIN
     g_cases_s_performance_analysis cspa
     ON
         ibr.roc_name = cspa.roc_name
         LEFT OUTER JOIN
     g_cases_s_general csg
     ON
         ibr.roc_name = csg.roc_name
         LEFT OUTER JOIN
     g_cases_t_generation ctg
     ON
         ibr.roc_name = ctg.roc_name
         LEFT OUTER JOIN
     g_cases_t_communication ctc
     ON
         ibr.roc_name = ctc.roc_name
         LEFT OUTER JOIN
     g_cases_d_electrical cde
     ON
         ibr.roc_name = cde.roc_name
         LEFT OUTER JOIN
     g_cases_d_gutter cdg
     ON
         ibr.roc_name = cdg.roc_name
         LEFT OUTER JOIN
     g_cases_d_miscellaneous cdm
     ON
         ibr.roc_name = cdm.roc_name
         LEFT OUTER JOIN
     g_cases_d_plumbing cdp
     ON
         ibr.roc_name = cdp.roc_name
         LEFT OUTER JOIN
     g_cases_d_home_exterior cdhe
     ON
         ibr.roc_name = cdhe.roc_name
         LEFT OUTER JOIN
     g_cases_d_home_interior cdhi
     ON
         ibr.roc_name = cdhi.roc_name
         LEFT OUTER JOIN
     g_cases_d_roof_leak cdrl
     ON
         ibr.roc_name = cdrl.roc_name
         LEFT OUTER JOIN
     g_cases_d_roofing_material cdrm
     ON
         ibr.roc_name = cdrm.roc_name
         LEFT OUTER JOIN
     g_cases_d_solar_guard cdsg
     ON
         ibr.roc_name = cdsg.roc_name
         LEFT OUTER JOIN
     g_cases_d_tool_drop cdtd
     ON
         ibr.roc_name = cdtd.roc_name
         LEFT OUTER JOIN
     g_cases_d_other cdo
     ON
         ibr.roc_name = cdo.roc_name
         LEFT OUTER JOIN
     g_cases_e_bbb ceb
     ON
         ibr.roc_name = ceb.roc_name
         LEFT OUTER JOIN
     g_cases_e_executive cee
     ON
         ibr.roc_name = cee.roc_name
         LEFT OUTER JOIN
     g_cases_e_external cee2
     ON
         ibr.roc_name = cee2.roc_name
         LEFT OUTER JOIN
     g_cases_e_internal cei
     ON
         ibr.roc_name = cei.roc_name
         LEFT OUTER JOIN
     g_cases_e_legal cel
     ON
         ibr.roc_name = cel.roc_name
         LEFT OUTER JOIN
     g_cases_e_mosaic cem
     ON
         ibr.roc_name = cem.roc_name
         LEFT OUTER JOIN
     g_cases_e_news_media cenm
     ON
         ibr.roc_name = cenm.roc_name
         LEFT OUTER JOIN
     g_cases_e_nps cen
     ON
         ibr.roc_name = cen.roc_name
         LEFT OUTER JOIN
     g_cases_e_online_review ceor
     ON
         ibr.roc_name = ceor.roc_name
         LEFT OUTER JOIN
     g_cases_e_other ceo
     ON
         ibr.roc_name = ceo.roc_name
         LEFT OUTER JOIN
     g_cases_e_social_media cesm
     ON
         ibr.roc_name = cesm.roc_name
         LEFT OUTER JOIN
     g_cases_e_special_projects cesp
     ON
         ibr.roc_name = cesp.roc_name
ORDER BY dash_overall DESC
;