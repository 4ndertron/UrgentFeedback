With Emp as (SELECT cx.employee_id
                  , em.full_name
                  , em.PREFERRED_NAME
                  , cx.agent_type
                  , em.WORK_EMAIL_ADDRESS
                  , em.WORK_ADDRESS_STATE
                  , cx.team_name
                  , cx.location

                  , cx.start_date
                  , cx.end_date
                  , '<img style="max-height:300px;max-width:300px;height:auto;width:auto;" src="https://46nsgon4l7.execute-api.us-west-2.amazonaws.com/prod/tms/workday-photo/' ||
                    cx.employee_id || '">'            as employee_photo
                  , em.mgr_name_4                     AS mgr_director
                  , em.mgr_name_5                     AS mgr_senior_manager
                  , em.mgr_name_6                     AS mgr_manager
                  -- Nobody has mgr_name_9 or mgr_name_10 (yet)
                  , NVL(em.mgr_name_8, em.mgr_name_7) AS mgr_supervisor
                  , em.BUSINESS_TITLE
                  , em.HIRE_DATE
                  , em.TERMINATION_DATE
                  , em.TERMINATION_REASON
                  , em.IS_PEOPLE_MANAGER
                  , em.NUMBER_OF_DIRECT_REPORTS
                  , em.MANAGEMENT_LEVEL
                  , em.SUPERVISORY_ORG
                  , em.BUSINESS_SITE_NAME
                  , em.SALESFORCE_OFFICE_ID


             FROM d_post_install.v_cx_employees AS cx
                      INNER JOIN
                  hr.t_employee AS em
                  ON
                      cx.employee_id = em.employee_id)

Select emp.*, d.dt, kpi.*
From emp
         INNER JOIN
     rpt.v_dates AS d
     ON
             d.dt BETWEEN emp.start_date AND emp.end_date
             and d.dt <= current_date

         left join
     d_post_install.v_cx_kpis as kpi
     on d.dt = kpi.REPORT_DATE and emp.employee_id = kpi.employee_id