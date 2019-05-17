select distinct u.name                        names,
                c.project_id,
                date_trunc(day, s.createdate) created_date
from rpt.v_sf_casecomment s
         inner join rpt.v_sf_user u
                    on u.id = s.createdbyid
         inner join rpt.t_case c
                    on s.parentid = c.case_id
where (c.subject ilike 'D3%'
    or c.subject ilike 'corp%')
  and date_trunc(day, s.createdate) between
    to_date(dateadd(day, -90, current_date)) and dateadd(day, -1, current_date)

union

select t.created_by                    names,
       t.project_id,
       date_trunc(day, t.created_date) created_date
from rpt.t_task t
where date_trunc(day, t.created_date) between
    to_date(dateadd(day, -90, current_date)) and dateadd(day, -1, current_date)
  and (upper(t.subject) like 'CR1%'
    or upper(t.subject) like 'CRA%')
order by created_date desc
;