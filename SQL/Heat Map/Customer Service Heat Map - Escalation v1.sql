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

   , cases_escalation AS
    (
        SELECT pr.roc_name
             , pr.project_id
             , (CASE WHEN ca.origin = 'BBB' THEN 1 END)                                 AS is_bbb
             , (CASE WHEN ca.origin IN ('CEO Promise', 'Executive') THEN 1 END)         AS is_executive
             , (CASE WHEN ca.origin IN ('Email', 'External', 'Phone') THEN 1 END)       AS is_external
             , (CASE WHEN ca.origin = 'Internal' THEN 1 END)                            AS is_internal
             , (CASE WHEN ca.origin = 'Legal' THEN 1 END)                               AS is_legal
             , (CASE WHEN ca.origin = 'Mosaic' THEN 1 END)                              AS is_mosaic
             , (CASE WHEN ca.origin = 'News Media' THEN 1 END)                          AS is_news_media
             , (CASE WHEN ca.origin = 'NPS' THEN 1 END)                                 AS is_nps
             , (CASE WHEN ca.origin IN ('Consumer Review', 'Online Review') THEN 1 END) AS is_online_review
             , (CASE WHEN ca.origin = 'Other' THEN 1 END)                               AS is_other
             , (CASE WHEN ca.origin = 'Social Media' THEN 1 END)                        AS is_social_media
             , (CASE WHEN ca.origin = 'Special Projects' THEN 1 END)                    AS is_special_projects
        FROM sfrpt.t_dm_case ca
                 INNER JOIN
             projects_raw pr
             ON
                 ca.project_id = pr.project_id
        WHERE ca.record_type = 'Solar - Customer Escalation'
          AND ca.closed_date IS NULL
    )

   , g_cases_bbb AS
    (
        SELECT roc_name
             , COUNT(roc_name)            AS case_tally
             , COUNT(DISTINCT project_id) AS project_tally
        FROM cases_escalation
        WHERE is_bbb = 1
        GROUP BY roc_name
    )

   , g_cases_executive AS
    (
        SELECT roc_name
             , COUNT(roc_name)            AS case_tally
             , COUNT(DISTINCT project_id) AS project_tally
        FROM cases_escalation
        WHERE is_executive = 1
        GROUP BY roc_name
    )

   , g_cases_external AS
    (
        SELECT roc_name
             , COUNT(roc_name)            AS case_tally
             , COUNT(DISTINCT project_id) AS project_tally
        FROM cases_escalation
        WHERE is_external = 1
        GROUP BY roc_name
    )

   , g_cases_internal AS
    (
        SELECT roc_name
             , COUNT(roc_name)            AS case_tally
             , COUNT(DISTINCT project_id) AS project_tally
        FROM cases_escalation
        WHERE is_internal = 1
        GROUP BY roc_name
    )

   , g_cases_legal AS
    (
        SELECT roc_name
             , COUNT(roc_name)            AS case_tally
             , COUNT(DISTINCT project_id) AS project_tally
        FROM cases_escalation
        WHERE is_legal = 1
        GROUP BY roc_name
    )

   , g_cases_mosaic AS
    (
        SELECT roc_name
             , COUNT(roc_name)            AS case_tally
             , COUNT(DISTINCT project_id) AS project_tally
        FROM cases_escalation
        WHERE is_mosaic = 1
        GROUP BY roc_name
    )

   , g_cases_news_media AS
    (
        SELECT roc_name
             , COUNT(roc_name)            AS case_tally
             , COUNT(DISTINCT project_id) AS project_tally
        FROM cases_escalation
        WHERE is_news_media = 1
        GROUP BY roc_name
    )

   , g_cases_nps AS
    (
        SELECT roc_name
             , COUNT(roc_name)            AS case_tally
             , COUNT(DISTINCT project_id) AS project_tally
        FROM cases_escalation
        WHERE is_nps = 1
        GROUP BY roc_name
    )

   , g_cases_online_review AS
    (
        SELECT roc_name
             , COUNT(roc_name)            AS case_tally
             , COUNT(DISTINCT project_id) AS project_tally
        FROM cases_escalation
        WHERE is_online_review = 1
        GROUP BY roc_name
    )

   , g_cases_other AS
    (
        SELECT roc_name
             , COUNT(roc_name)            AS case_tally
             , COUNT(DISTINCT project_id) AS project_tally
        FROM cases_escalation
        WHERE is_other = 1
        GROUP BY roc_name
    )

   , g_cases_social_media AS
    (
        SELECT roc_name
             , COUNT(roc_name)            AS case_tally
             , COUNT(DISTINCT project_id) AS project_tally
        FROM cases_escalation
        WHERE is_social_media = 1
        GROUP BY roc_name
    )

   , g_cases_special_projects AS
    (
        SELECT roc_name
             , COUNT(roc_name)            AS case_tally
             , COUNT(DISTINCT project_id) AS project_tally
        FROM cases_escalation
        WHERE is_special_projects = 1
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
                 FROM cases_escalation
             )
        GROUP BY roc_name
    )

SELECT ibr.roc_name
     , NVL(cb.case_tally, 0)                                                          AS bbb_case_tally
     , NVL(cb.project_tally, 0)                                                       AS bbb_project_tally
     , TO_CHAR(100 * NVL(cb.project_tally, 0) / ibr.install_tally, '990.00') || '%'   AS bbb_project_ratio
     , NVL(ce.case_tally, 0)                                                          AS executive_case_tally
     , NVL(ce.project_tally, 0)                                                       AS executive_project_tally
     , TO_CHAR(100 * NVL(ce.project_tally, 0) / ibr.install_tally, '990.00') || '%'   AS executive_project_ratio
     , NVL(cext.case_tally, 0)                                                        AS external_case_tally
     , NVL(cext.project_tally, 0)                                                     AS external_project_tally
     , TO_CHAR(100 * NVL(cext.project_tally, 0) / ibr.install_tally, '990.00') || '%' AS external_project_ratio
     , NVL(cint.case_tally, 0)                                                        AS internal_case_tally
     , NVL(cint.project_tally, 0)                                                     AS internal_project_tally
     , TO_CHAR(100 * NVL(cint.project_tally, 0) / ibr.install_tally, '990.00') || '%' AS internal_project_ratio
     , NVL(cl.case_tally, 0)                                                          AS legal_case_tally
     , NVL(cl.project_tally, 0)                                                       AS legal_project_tally
     , TO_CHAR(100 * NVL(cl.project_tally, 0) / ibr.install_tally, '990.00') || '%'   AS legal_project_ratio
     , NVL(cm.case_tally, 0)                                                          AS mosaic_case_tally
     , NVL(cm.project_tally, 0)                                                       AS mosaic_project_tally
     , TO_CHAR(100 * NVL(cm.project_tally, 0) / ibr.install_tally, '990.00') || '%'   AS mosaic_project_ratio
     , NVL(cnm.case_tally, 0)                                                         AS news_media_case_tally
     , NVL(cnm.project_tally, 0)                                                      AS news_media_project_tally
     , TO_CHAR(100 * NVL(cnm.project_tally, 0) / ibr.install_tally, '990.00') || '%'  AS news_media_project_ratio
     , NVL(cn.case_tally, 0)                                                          AS nps_case_tally
     , NVL(cn.project_tally, 0)                                                       AS nps_project_tally
     , TO_CHAR(100 * NVL(cn.project_tally, 0) / ibr.install_tally, '990.00') || '%'   AS nps_project_ratio
     , NVL(cor.case_tally, 0)                                                         AS online_review_case_tally
     , NVL(cor.project_tally, 0)                                                      AS online_review_project_tally
     , TO_CHAR(100 * NVL(cor.project_tally, 0) / ibr.install_tally, '990.00') || '%'  AS online_review_project_ratio
     , NVL(cx.case_tally, 0)                                                          AS other_case_tally
     , NVL(cx.project_tally, 0)                                                       AS other_project_tally
     , TO_CHAR(100 * NVL(cx.project_tally, 0) / ibr.install_tally, '990.00') || '%'   AS other_project_ratio
     , NVL(csm.case_tally, 0)                                                         AS social_media_case_tally
     , NVL(csm.project_tally, 0)                                                      AS social_media_project_tally
     , TO_CHAR(100 * NVL(csm.project_tally, 0) / ibr.install_tally, '990.00') || '%'  AS social_media_project_ratio
     , NVL(csp.case_tally, 0)                                                         AS special_projects_case_tally
     , NVL(csp.project_tally, 0)                                                      AS special_projects_project_tally
     , TO_CHAR(100 * NVL(csp.project_tally, 0) / ibr.install_tally, '990.00') || '%'  AS special_projects_project_ratio
     , NVL(co.case_tally, 0)                                                          AS overall_case_tally
     , NVL(co.project_tally, 0)                                                       AS overall_project_tally
     , TO_CHAR(100 * NVL(co.project_tally, 0) / ibr.install_tally, '990.00') || '%'   AS overall_project_ratio
FROM installs_by_roc ibr
         LEFT OUTER JOIN
     g_cases_bbb cb
     ON
         ibr.roc_name = cb.roc_name
         LEFT OUTER JOIN
     g_cases_executive ce
     ON
         ibr.roc_name = ce.roc_name
         LEFT OUTER JOIN
     g_cases_external cext
     ON
         ibr.roc_name = cext.roc_name
         LEFT OUTER JOIN
     g_cases_internal cint
     ON
         ibr.roc_name = cint.roc_name
         LEFT OUTER JOIN
     g_cases_legal cl
     ON
         ibr.roc_name = cl.roc_name
         LEFT OUTER JOIN
     g_cases_mosaic cm
     ON
         ibr.roc_name = cm.roc_name
         LEFT OUTER JOIN
     g_cases_news_media cnm
     ON
         ibr.roc_name = cnm.roc_name
         LEFT OUTER JOIN
     g_cases_nps cn
     ON
         ibr.roc_name = cn.roc_name
         LEFT OUTER JOIN
     g_cases_online_review cor
     ON
         ibr.roc_name = cor.roc_name
         LEFT OUTER JOIN
     g_cases_other cx
     ON
         ibr.roc_name = cx.roc_name
         LEFT OUTER JOIN
     g_cases_social_media csm
     ON
         ibr.roc_name = csm.roc_name
         LEFT OUTER JOIN
     g_cases_special_projects csp
     ON
         ibr.roc_name = csp.roc_name
         LEFT OUTER JOIN
     g_cases_overall co
     ON
         ibr.roc_name = co.roc_name
ORDER BY overall_case_tally DESC;