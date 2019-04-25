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

   , cases_damage AS
    (
        SELECT pr.roc_name
             , pr.project_id
             , ca.damage_type
             , (CASE WHEN ca.damage_type = 'Electrical' THEN 1 END)                     AS is_electrical
             , (CASE WHEN ca.damage_type = 'Gutter' THEN 1 END)                         AS is_gutter
             , (CASE WHEN ca.damage_type = 'Miscellaneous' THEN 1 END)                  AS is_miscellaneous
             , (CASE WHEN ca.damage_type = 'Plumbing' THEN 1 END)                       AS is_plumbing
             , (CASE WHEN ca.damage_type = 'Home Exterior' THEN 1 END)                  AS is_home_exterior
             , (CASE WHEN ca.damage_type = 'Home Interior' THEN 1 END)                  AS is_home_interior
             , (CASE WHEN ca.damage_type = 'Roof Leak' THEN 1 END)                      AS is_roof_leak
             , (CASE WHEN ca.damage_type IN ('Roofing', 'Roofing Material') THEN 1 END) AS is_roofing_material
             , (CASE WHEN ca.damage_type = 'Solar Guard' THEN 1 END)                    AS is_solar_guard
             , (CASE WHEN ca.damage_type = 'Tool Drop' THEN 1 END)                      AS is_tool_drop
        FROM sfrpt.t_dm_case ca
                 INNER JOIN
             projects_raw pr
             ON
                 ca.project_id = pr.project_id
        WHERE ca.record_type IN ('Solar Damage Resolutions', 'Home Damage')
          AND ca.closed_date IS NULL
    )

   , g_cases_electrical AS
    (
        SELECT roc_name
             , COUNT(roc_name)            AS case_tally
             , COUNT(DISTINCT project_id) AS project_tally
        FROM cases_damage
        WHERE is_electrical = 1
        GROUP BY roc_name
    )

   , g_cases_gutter AS
    (
        SELECT roc_name
             , COUNT(roc_name)            AS case_tally
             , COUNT(DISTINCT project_id) AS project_tally
        FROM cases_damage
        WHERE is_gutter = 1
        GROUP BY roc_name
    )

   , g_cases_miscellaneous AS
    (
        SELECT roc_name
             , COUNT(roc_name)            AS case_tally
             , COUNT(DISTINCT project_id) AS project_tally
        FROM cases_damage
        WHERE is_miscellaneous = 1
        GROUP BY roc_name
    )

   , g_cases_plumbing AS
    (
        SELECT roc_name
             , COUNT(roc_name)            AS case_tally
             , COUNT(DISTINCT project_id) AS project_tally
        FROM cases_damage
        WHERE is_plumbing = 1
        GROUP BY roc_name
    )

   , g_cases_home_exterior AS
    (
        SELECT roc_name
             , COUNT(roc_name)            AS case_tally
             , COUNT(DISTINCT project_id) AS project_tally
        FROM cases_damage
        WHERE is_home_exterior = 1
        GROUP BY roc_name
    )

   , g_cases_home_interior AS
    (
        SELECT roc_name
             , COUNT(roc_name)            AS case_tally
             , COUNT(DISTINCT project_id) AS project_tally
        FROM cases_damage
        WHERE is_home_interior = 1
        GROUP BY roc_name
    )

   , g_cases_roof_leak AS
    (
        SELECT roc_name
             , COUNT(roc_name)            AS case_tally
             , COUNT(DISTINCT project_id) AS project_tally
        FROM cases_damage
        WHERE is_roof_leak = 1
        GROUP BY roc_name
    )

   , g_cases_roofing_material AS
    (
        SELECT roc_name
             , COUNT(roc_name)            AS case_tally
             , COUNT(DISTINCT project_id) AS project_tally
        FROM cases_damage
        WHERE is_roofing_material = 1
        GROUP BY roc_name
    )

   , g_cases_solar_guard AS
    (
        SELECT roc_name
             , COUNT(roc_name)            AS case_tally
             , COUNT(DISTINCT project_id) AS project_tally
        FROM cases_damage
        WHERE is_solar_guard = 1
        GROUP BY roc_name
    )

   , g_cases_tool_drop AS
    (
        SELECT roc_name
             , COUNT(roc_name)            AS case_tally
             , COUNT(DISTINCT project_id) AS project_tally
        FROM cases_damage
        WHERE is_tool_drop = 1
        GROUP BY roc_name
    )

   , g_cases_other AS
    (
        SELECT roc_name
             , COUNT(roc_name)            AS case_tally
             , COUNT(DISTINCT project_id) AS project_tally
        FROM cases_damage
        WHERE is_electrical IS NULL
          AND is_gutter IS NULL
          AND is_miscellaneous IS NULL
          AND is_plumbing IS NULL
          AND is_home_exterior IS NULL
          AND is_home_interior IS NULL
          AND is_roof_leak IS NULL
          AND is_roofing_material IS NULL
          AND is_solar_guard IS NULL
          AND is_tool_drop IS NULL
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
                 FROM cases_damage
             )
        GROUP BY roc_name
    )

SELECT ibr.roc_name
     , NVL(ce.case_tally, 0)                                                         AS electrical_case_tally
     , NVL(ce.project_tally, 0)                                                      AS electrical_project_tally
     , TO_CHAR(100 * NVL(ce.project_tally, 0) / ibr.install_tally, '990.00') || '%'  AS electrical_project_ratio
     , NVL(cg.case_tally, 0)                                                         AS gutter_case_tally
     , NVL(cg.project_tally, 0)                                                      AS gutter_project_tally
     , TO_CHAR(100 * NVL(cg.project_tally, 0) / ibr.install_tally, '990.00') || '%'  AS gutter_project_ratio
     , NVL(cm.case_tally, 0)                                                         AS miscellaneous_case_tally
     , NVL(cm.project_tally, 0)                                                      AS miscellaneous_project_tally
     , TO_CHAR(100 * NVL(cm.project_tally, 0) / ibr.install_tally, '990.00') || '%'  AS miscellaneous_project_ratio
     , NVL(cp.case_tally, 0)                                                         AS plumbing_case_tally
     , NVL(cp.project_tally, 0)                                                      AS plumbing_project_tally
     , TO_CHAR(100 * NVL(cp.project_tally, 0) / ibr.install_tally, '990.00') || '%'  AS plumbing_project_ratio
     , NVL(che.case_tally, 0)                                                        AS home_exterior_case_tally
     , NVL(che.project_tally, 0)                                                     AS home_exterior_project_tally
     , TO_CHAR(100 * NVL(che.project_tally, 0) / ibr.install_tally, '990.00') || '%' AS home_exterior_project_ratio
     , NVL(chi.case_tally, 0)                                                        AS home_interior_case_tally
     , NVL(chi.project_tally, 0)                                                     AS home_interior_project_tally
     , TO_CHAR(100 * NVL(chi.project_tally, 0) / ibr.install_tally, '990.00') || '%' AS home_interior_project_ratio
     , NVL(crl.case_tally, 0)                                                        AS roof_leak_case_tally
     , NVL(crl.project_tally, 0)                                                     AS roof_leak_project_tally
     , TO_CHAR(100 * NVL(crl.project_tally, 0) / ibr.install_tally, '990.00') || '%' AS roof_leak_project_ratio
     , NVL(crm.case_tally, 0)                                                        AS roofing_material_case_tally
     , NVL(crm.project_tally, 0)                                                     AS roofing_material_project_tally
     , TO_CHAR(100 * NVL(crm.project_tally, 0) / ibr.install_tally, '990.00') || '%' AS roofing_material_project_ratio
     , NVL(csg.case_tally, 0)                                                        AS solar_guard_case_tally
     , NVL(csg.project_tally, 0)                                                     AS solar_guard_project_tally
     , TO_CHAR(100 * NVL(csg.project_tally, 0) / ibr.install_tally, '990.00') || '%' AS solar_guard_project_ratio
     , NVL(ctd.case_tally, 0)                                                        AS tool_drop_case_tally
     , NVL(ctd.project_tally, 0)                                                     AS tool_drop_project_tally
     , TO_CHAR(100 * NVL(ctd.project_tally, 0) / ibr.install_tally, '990.00') || '%' AS tool_drop_project_ratio
     , NVL(cx.case_tally, 0)                                                         AS other_case_tally
     , NVL(cx.project_tally, 0)                                                      AS other_project_tally
     , TO_CHAR(100 * NVL(cx.project_tally, 0) / ibr.install_tally, '990.00') || '%'  AS other_project_ratio
     , NVL(co.case_tally, 0)                                                         AS overall_case_tally
     , NVL(co.project_tally, 0)                                                      AS overall_project_tally
     , TO_CHAR(100 * NVL(co.project_tally, 0) / ibr.install_tally, '990.00') || '%'  AS overall_project_ratio
FROM installs_by_roc ibr
         LEFT OUTER JOIN
     g_cases_electrical ce
     ON
         ibr.roc_name = ce.roc_name
         LEFT OUTER JOIN
     g_cases_gutter cg
     ON
         ibr.roc_name = cg.roc_name
         LEFT OUTER JOIN
     g_cases_miscellaneous cm
     ON
         ibr.roc_name = cm.roc_name
         LEFT OUTER JOIN
     g_cases_plumbing cp
     ON
         ibr.roc_name = cp.roc_name
         LEFT OUTER JOIN
     g_cases_home_exterior che
     ON
         ibr.roc_name = che.roc_name
         LEFT OUTER JOIN
     g_cases_home_interior chi
     ON
         ibr.roc_name = chi.roc_name
         LEFT OUTER JOIN
     g_cases_roof_leak crl
     ON
         ibr.roc_name = crl.roc_name
         LEFT OUTER JOIN
     g_cases_roofing_material crm
     ON
         ibr.roc_name = crm.roc_name
         LEFT OUTER JOIN
     g_cases_solar_guard csg
     ON
         ibr.roc_name = csg.roc_name
         LEFT OUTER JOIN
     g_cases_tool_drop ctd
     ON
         ibr.roc_name = ctd.roc_name
         LEFT OUTER JOIN
     g_cases_other cx
     ON
         ibr.roc_name = cx.roc_name
         LEFT OUTER JOIN
     g_cases_overall co
     ON
         ibr.roc_name = co.roc_name
ORDER BY overall_case_tally DESC;