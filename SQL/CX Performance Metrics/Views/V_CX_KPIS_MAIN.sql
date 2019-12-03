CREATE OR REPLACE VIEW D_POST_INSTALL.v_cx_kpis_main
    COPY GRANTS
    COMMENT = 'All CX Facing Effectiveness, Efficiency, and Quality Metrics Under Chuck Browne. Last Updated 7/30/2019'
AS
    (
        WITH central_scheduling AS -- Central Scheduling | Outbound/AUX
            (SELECT ss.FULL_NAME         AS employee_name
                  , ss.EMPLOYEE_ID       AS employee_id
                  , ss.TEAM_START_DATE   AS team_start_date
                  , ss.EFFECTIVENESS     AS effectiveness
                  , ss.EFFICIENCY        AS efficiency
                  , ss.QUALITY           AS quality
                  , 'Central Scheduling' AS team
                  , 'Outbound/AUX'       AS subteam
             FROM D_POST_INSTALL.V_CX_KPIS_SS ss)

           , ace_t1_t2 AS -- ACE (Tier 1 | Tier 2 | Email Admin)
            (SELECT ae.FULL_NAME       AS employee_name
                  , ae.EMPLOYEE_ID     AS employee_id
                  , ae.TEAM_START_DATE AS team_start_date
                  , ae.EFFECTIVENESS   AS effectiveness
                  , ae.EFFICIENCY      AS efficiency
                  , ae.QUALITY         AS quality
                  , 'ACE'              AS team
                  , CASE
                        WHEN ae.EMPLOYEE_ID = '200731'
                            THEN 'ACE (Tier 1)'
                        WHEN ae.EMPLOYEE_ID = '210144'
                            THEN 'Transfers'
                        ELSE
                            decode(ae.SUPERVISORY_ORG
                                , 'Customer Success II', 'ACE (Tier 1)'
                                , 'Customer Relations', 'ACE (Tier 1)'
                                , 'Project Specialists', 'Project Specialists (Tier 2)'
                                , 'Transfers', 'ACE - Transfers'
                                , 'Email Admin', 'Email')
                    END                AS subteam
             FROM D_POST_INSTALL.v_cx_kpis_ace_t1_t2_email ae
             WHERE subteam != 'ACE - Transfers')

           , ace_transfers_case AS -- ACE | Transfers - Case Managers
            (SELECT cm.FULL_NAME                AS employee_name
                  , cm.EMPLOYEE_ID              AS employee_id
                  , cm.TEAM_START_DATE          AS team_start_date
                  , cm.EFFECTIVENESS            AS effectiveness
                  , cm.EFFICIENCY               AS efficiency
                  , cm.QUALITY                  AS quality
                  , 'ACE'                       AS team
                  , 'Transfers - Case Managers' AS subteam
             FROM D_POST_INSTALL.V_CX_KPIS_TRANSFERS_CASE_MANAGERS cm)

           , ace_transfers_ib AS -- ACE | Transfers - IB
            (SELECT ib.FULL_NAME                                                  AS employee_name
                  , ib.EMPLOYEE_ID                                                AS employee_id
                  , ib.TEAM_START_DATE                                            AS team_start_date
                  , CASE WHEN ib.EFFECTIVENESS != 'TBD' THEN ib.EFFECTIVENESS END AS effectiveness
                  , CASE WHEN ib.EFFICIENCY != 'TBD' THEN ib.EFFICIENCY END       AS efficiency
                  , ib.QUALITY                                                    AS quality
                  , 'ACE'                                                         AS team
                  , 'Transfers - IB'                                              AS subteam
             FROM D_POST_INSTALL.V_CX_KPIS_TRANSFERS_IB ib)

           , customer_care_ib AS -- Customer Care | Inbound
            (SELECT cib.FULL_NAME       AS employee_name
                  , cib.EMPLOYEE_ID     AS employee_id
                  , cib.TEAM_START_DATE AS team_start_date
                  , cib.EFFECTIVENESS   AS effectiveness
                  , cib.EFFICIENCY      AS efficiency
                  , cib.QUALITY         AS quality
                  , 'Customer Care'     AS team
                  , 'Inbound'           AS subteam
             FROM D_POST_INSTALL.V_CX_KPIS_CC_IB cib)

           , customer_care_ob AS -- Customer Care | Outbound
            (SELECT ob.FULL_NAME         AS employee_name
                  , ob.EMPLOYEE_ID       AS employee_id
                  , ob.TEAM_START_DATE   AS team_start_date
                  , ob.EFFECTIVENESS_PCT AS effectiveness
                  , ob.EFFICIENCY_PCT    AS efficiency
                  , ob.QUALITY_PCT       AS quality
                  , 'Customer Care'      AS team
                  , 'Outbound'           AS subteam
             FROM D_POST_INSTALL.V_CX_KPIS_CC_OB ob)

           , special_ops AS -- Special Ops
            (SELECT so.FULL_NAME       AS employee_name
                  , so.EMPLOYEE_ID     AS employee_id
                  , so.TEAM_START_DATE AS team_start_date
                  , so.EFFECTIVENESS   AS effectiveness
                  , so.EFFICIENCY      AS efficiency
                  , so.QUALITY         AS quality
                  , 'Special Ops'      AS team
                  , 'Special Ops'      AS subteam
             FROM D_POST_INSTALL.V_CX_KPIS_SPECIAL_OPS so)

           , er_t1 AS -- Executive Resolutions - Tier 1
            (SELECT e.FULL_NAME            AS employee_name
                  , e.EMPLOYEE_ID          AS employee_id
                  , e.TEAM_START_DATE      AS team_start_date
                  , e.EFFECTIVENESS        AS effectiveness
                  , e.EFFICIENCY           AS efficiency
                  , e.QUALITY              AS quality
                  , 'Executive Resolution' AS team
                  , 'Tier I'               AS subteam
             FROM D_POST_INSTALL.V_CX_KPIS_ER_T1 e)

           , er_t2 AS -- Executive Resolutions - Tier 2
            (SELECT e2.FULL_NAME           AS employee_name
                  , e2.EMPLOYEE_ID         AS employee_id
                  , e2.TEAM_START_DATE     AS team_start_date
                  , e2.EFFECTIVENESS       AS effectiveness
                  , e2.EFFICIENCY          AS efficiency
                  , e2.QUALITY             AS quality
                  , 'Executive Resolution' AS team
                  , 'Tier II'              AS subteam
             FROM D_POST_INSTALL.V_CX_KPIS_ER_T2 e2)

           , default AS -- Default Management | Default
            (SELECT d.FULL_NAME          AS employee_name
                  , d.EMPLOYEE_ID        AS employee_id
                  , d.team_start_date    AS team_start_date
                  , d.EFFECTIVENESS      AS effectiveness
                  , d.EFFICIENCY         AS efficiency
                  , d.QUALITY            AS quality
                  , 'Default Management' AS team
                  , 'Default'            AS subteam
             FROM D_POST_INSTALL.V_CX_KPIS_DEFAULT d)

           , default_risk_mitigation AS -- Default Management | Deceased
            (SELECT RM.FULL_NAME         AS employee_name
                  , RM.EMPLOYEE_ID       AS employee_id
                  , RM.team_start_date   AS team_start_date
                  , RM.EFFECTIVENESS     AS effectiveness
                  , RM.EFFICIENCY        AS efficiency
                  , RM.QUALITY           AS quality
                  , 'Default Management' AS team
                  , 'Deceased'           AS subteam
             FROM D_POST_INSTALL.V_CX_KPIS_DEFAULT_RISK_MITIGATION RM)

           , master_union AS -- Union All CTEs
            (SELECT *
             FROM ace_t1_t2
             UNION ALL
             (SELECT * FROM ace_transfers_case)
             UNION ALL
             (SELECT * FROM ace_transfers_ib)
             UNION ALL
             (SELECT * FROM central_scheduling)
             UNION ALL
             (SELECT * FROM customer_care_ib)
             UNION ALL
             (SELECT * FROM customer_care_ob)
             UNION ALL
             (SELECT * FROM default)
             UNION ALL
             (SELECT * FROM default_risk_mitigation)
             UNION ALL
             (SELECT * FROM er_t1)
             UNION ALL
             (SELECT * FROM er_t2)
             UNION ALL
             (SELECT * FROM special_ops)
            )

        SELECT e.EMPLOYEE_ID
             , e.FULL_NAME
             , e.BUSINESS_TITLE
             , CASE WHEN e.IS_PEOPLE_MANAGER = 1 THEN 'Yes' ELSE 'No' END      AS is_people_manager
             , CASE
                   WHEN e.BUSINESS_TITLE ILIKE '%Manager%' AND e.IS_PEOPLE_MANAGER = 1
                       THEN '2. Manager'
                   WHEN e.BUSINESS_TITLE ILIKE '%Supervisor%' AND e.IS_PEOPLE_MANAGER = 1
                       THEN '1. Supervisor'
                   WHEN (e.PAY_RATE_TYPE = 'Salary' OR e.JOB_PROFILE ILIKE '%Data Specialist%')
                       THEN '3. Operation Support'
                   ELSE '0. Agent'
            END                                                                AS business_title_type
             , e.MGR_NAME_5 || ' (' || e.MGR_ID_5 || ')'                       AS manager
             , e.SUPERVISOR_NAME_1 || ' (' || e.SUPERVISOR_BADGE_ID_1 || ')'   AS direct_supervisor
             , mu.effectiveness
             , mu.efficiency
             , mu.quality
             , NVL(mu.team, 'No Team')                                         AS team
             , NVL(mu.subteam, 'No Subteam')                                   AS subteam
             , e.HIRE_DATE
             , ROUND(datediff('d', mu.team_start_date, CURRENT_DATE) / 365, 2) AS team_tenure
             , CASE
                   WHEN datediff('d', mu.team_start_date, CURRENT_DATE) >= 365
                       THEN '4. 365+ days'
                   WHEN datediff('d', mu.team_start_date, CURRENT_DATE) >= 180
                       THEN '3. 180+ days'
                   WHEN datediff('d', mu.team_start_date, CURRENT_DATE) >= 90
                       THEN '2. 90+ days'
                   WHEN datediff('d', mu.team_start_date, CURRENT_DATE) >= 30
                       THEN '1. 30+ days'
                   ELSE '0. < 30 days'
            END                                                                AS team_tenure_bucket
             , mu.team_start_date
             , ROUND(datediff('d', e.HIRE_DATE, current_date) / 365, 2)        AS tenure
             , CASE
                   WHEN mu.effectiveness IS NOT NULL
                       THEN percent_rank() OVER (PARTITION BY team, subteam ORDER BY mu.effectiveness)
            END                                                                AS rank_effectiveness
             , CASE
                   WHEN mu.efficiency IS NOT NULL
                       THEN percent_rank() OVER (PARTITION BY team, subteam ORDER BY mu.efficiency)
            END                                                                AS rank_efficiency
             , CASE
                   WHEN mu.quality IS NOT NULL
                       THEN percent_rank() OVER (PARTITION BY team, subteam ORDER BY mu.quality)
            END                                                                AS rank_quality
             , NVL((NVL(rank_effectiveness, 0) + NVL(rank_efficiency, 0) + NVL(rank_quality, 0)) /
                   NULLIF((nvl2(rank_effectiveness, 1, 0) + nvl2(rank_efficiency, 1, 0) + nvl2(rank_quality, 1, 0)), 0),
                   0)                                                          AS rank_overall
        FROM hr.T_EMPLOYEE e
                 LEFT JOIN master_union mu
                           ON mu.employee_id = e.EMPLOYEE_ID
        WHERE e.MGR_ID_4 = '209122'
          AND e.TERMINATED = FALSE
          AND e.IS_PEOPLE_MANAGER = 0
        -- Placeholder: Chuck Browne
        --      (all employees under this individual)
    );


