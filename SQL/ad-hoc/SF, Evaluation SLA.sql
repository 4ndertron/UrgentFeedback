WITH cases_raw as (
    SELECT pr.service_name
         , pr.project_name
         , ca.owner
         , ca.subject
         , ca.origin
         , TO_DATE(ca.created_date)                                                        as created_date
         , iff(ca.executive_resolutions_accepted = NULL, to_date(current_timestamp()),
               to_date(ca.executive_resolutions_accepted))                                 as executive_resolutions_accepted
         , DATEDIFF(s, ca.created_date, NVL(ca.executive_resolutions_accepted, current_timestamp())) / (24 * 60 * 60)
        - (DATEDIFF(wk, ca.created_date, ca.executive_resolutions_accepted) * 2)
        - (IFF(DAYNAME(ca.created_date) = 'Sun', 1, 0))
        - (IFF(DAYNAME(ca.executive_resolutions_accepted) = 'Sat', 1, 0))
                                                                                           as SLA -- integrate V_Dates to account for hollidays
         , CASE WHEN ca.executive_resolutions_accepted <= current_timestamp() THEN SLA END as TAT
    FROM rpt.T_Case ca
             INNER JOIN
         rpt.T_Project pr
         ON ca.project_id = pr.project_id
    WHERE ca.CREATED_DATE >= '2019-01-01'
      AND ca.record_type = 'Solar - Customer Escalation'
      AND ca.assigned_department IN ('Advocate Response', 'Executive Resolutions')
      AND ca.created_date < ca.executive_resolutions_accepted
),

     dates as (
         SELECT created_date as day
         FROM cases_raw
         UNION
         SELECT executive_resolutions_accepted
         FROM cases_raw
         WHERE executive_resolutions_accepted IS NOT NULL
     )

SELECT TO_CHAR(d.day, 'mm/dd/yyyy')                                                           as day
     , COUNT(CASE WHEN d.day = cr.created_date THEN 1 END)                                    as inflow
     , COUNT(CASE WHEN d.day = cr.executive_resolutions_accepted THEN 1 END)                  as outflow
     , COUNT(CASE WHEN d.day = cr.created_date AND cr.SLA <= (2 / 6) THEN 1 END)              as In_SLA
     , iff(inflow = 0, NULL, In_SLA / inflow)                                                 as Ratio
     , (SUM(CASE WHEN d.day = cr.executive_resolutions_accepted THEN TAT END) / outflow) * 24 AS TAT
from dates d
--temp fix, remove cross join later
         CROSS JOIN
     cases_raw cr
GROUP BY d.day
ORDER BY d.day