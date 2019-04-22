WITH hold_report as (
    SELECT h.type,
           h.hold_name,
           p.project_name,
           h.status                                                                            as hold_status,
           h.created_date,
           h.hold_resolved_date,
           UPPER(p.service_state)                                                              as service_state,
           UPPER(p.service_city)                                                               as service_city,
           UPPER(p.service_county)                                                             as service_county,
           p.sales_rep_emp_id,
           DATEDIFF(s, h.created_date, NVL(h.hold_resolved_date, CURRENT_TIMESTAMP())) / 86400 as TAT_Days
    FROM rpt.t_hold h
             INNER JOIN
         rpt.t_project p
         ON
             h.project_id = p.project_id
    WHERE
--        p.service_state LIKE '%NY%'
--    AND
h.created_date >= '2018-01-01'
),

     employee_crossing as (
         SELECT e.badge_id,
                e.full_name,
                h.*
         FROM hold_report h
                  INNER JOIN
              hr.t_employee e
              ON
                  h.sales_rep_emp_id = e.badge_id
         WHERE e.supervisor_name_1 LIKE '%%'
--    AND
--       e.terminated = false
--    OR
--        e.full_name LIKE '%%'
     )

SELECT
--    service_county as county,
type                                                      as Hold_Type,
COUNT(CASE WHEN hold_status = 'Active' THEN 1 END)        as Active_count,
COUNT(CASE WHEN hold_status = 'Resolved' THEN 1 END)      as Resolved_count,
COUNT(hold_status)                                        as Total_count,
AVG(CASE WHEN hold_status = 'Active' THEN TAT_Days END)   as Active_average,
AVG(CASE WHEN hold_status = 'Resolved' THEN TAT_Days END) as Reolved_average,
AVG(TAT_Days)                                             as Total_average
FROM employee_crossing hr
WHERE hr.service_state LIKE '%NY%'
GROUP BY
--    county,
hold_type
ORDER BY
--    county,
-total_count;