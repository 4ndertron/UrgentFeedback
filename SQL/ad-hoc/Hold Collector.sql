WITH holds_list as (
    SELECT h.hold_name,
           h.project_id
    FROM rpt.t_hold as h
    WHERE h.type = 'Add-on System: Fleet Performance Review Required'
      AND h.status = 'Active'
),

     address_list as (
         SELECT h.*,
                p.service_address
         FROM holds_list as h
                  INNER JOIN
              rpt.t_project as p
              ON
                  h.project_id = p.project_id
     ),

     original_service as (
         SELECT a.*,
                s.service_name,
                s.service_status
         FROM address_list AS a
                  INNER JOIN
              rpt.t_service as s
              ON
                  a.service_address = s.service_address
--    WHERE
--        s.service_status IN ('Installed: Pending PTO','Active','Takeover','Not Active','Transfer','Solar - Transfer')
     )

SELECT *
FROM original_service
;

SELECT DISTINCT s.service_status
FROM rpt.t_service as s;