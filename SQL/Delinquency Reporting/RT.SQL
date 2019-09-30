--regular accounts inflow/outflow dataset
with soln as (select distinct p.solar_billing_account_number
              from rpt.t_service p
              inner join rpt.t_case c
                on p.service_id = c.service_id
              where c.record_type = 'Solar - Customer Default'
                and c.is_closed = 'false'
                and upper(c.subject) like 'CORP%'),
                
     coln as (select distinct p.solar_billing_account_number
              from rpt.t_service p
              inner join rpt.t_case c
                on p.service_id = c.service_id
              where c.record_type = 'Solar - Customer Default'
                and c.is_closed = 'false'
                and upper(c.subject) like 'D3%'),
                
     soln_yest as (select distinct solar_billing_account_number
                  from d_post_install.t_default_snapshot
                  where subject ilike 'corp%'),
                  
     coln_yest as (select distinct solar_billing_account_number
                  from d_post_install.t_default_snapshot
                  where subject ilike 'd3%'),
     
     ld_yest as (select nvl(a.collection_code, 'BLANK') filled_in,
                    case
                        when filled_in in ('BK07','BK13') then 'BK07/BK13'
                        when filled_in in ('LDSY','FCRP','HOST','REMV','WOFF','PREP','REPO','COFF','DNCC','SERV','CXLD','ESCL') then 'ADMIN'
                        else filled_in
                    end collection_code_v2,
                    a.*
                from ld.t_daily_data_extract_hist a
                where date_trunc(day, a.load_date) = (select max(date_trunc(day, t.load_date))
                                  from ld.t_daily_data_extract_hist t
                                  where date_trunc(day, t.load_date) < date_trunc(day, current_timestamp))),
     
     ld_today as (select nvl(a.collection_code, 'BLANK') filled_in,
                    case
                        when filled_in in ('BK07','BK13') then 'BK07/BK13'
                        when filled_in in ('LDSY','FCRP','HOST','REMV','WOFF','PREP','REPO','COFF','DNCC','SERV','CXLD','ESCL') then 'ADMIN'
                        else filled_in
                    end collection_code_v2,
                    a.*
                from ld.t_daily_data_extract a),
                
-- //                where (s.opty_contract_type not in ('CASH','LOAN') or s.opty_contract_type is null)
-- //    and (s.service_status != 'Cancelled' or s.service_status is null)
-- //    and (p.sales_office != 'Restore Utah' or p.sales_office is null)
                
table1 as (select 
    case
        when a.collection_code_v2 = 'ADMIN' and t.collection_code_v2 != 'ADMIN' then 1
        else 0
    end soln_admin_outflow_1,
    case
        when a.collection_code_v2 != 'FORE' and t.collection_code_v2 = 'FORE' then 1
        else 0
    end fore_inflow_1,
    case
        when a.collection_code_v2 != 'NONE' and t.collection_code_v2 = 'NONE' then 1
        else 0
    end NONE_inflow_1,
    case
        when a.collection_code_v2 != 'WOUT' and t.collection_code_v2= 'WOUT' then 1
        else 0
    end WOUT_inflow_1,
    case
        when a.collection_code_v2 != 'SKIP' and t.collection_code_v2 = 'SKIP' then 1
        else 0
    end SKIP_inflow_1,
    case
        when a.collection_code_v2 != 'PAYP' and t.collection_code_v2 = 'PAYP' then 1
        else 0
    end PAYP_inflow_1,
    case
        when a.collection_code_v2 != 'TRAN' and t.collection_code_v2 = 'TRAN' then 1
        else 0
    end TRAN_inflow_1,
    case
        when a.collection_code_v2 != 'TRLD' and t.collection_code_v2 = 'TRLD' then 1
        else 0
    end TRLD_inflow_1,
    case
        when a.collection_code_v2 != 'TRRC' and t.collection_code_v2 = 'TRRC' then 1
        else 0
    end TRRC_inflow_1,
    case
        when a.collection_code_v2 != 'LDBL' and t.collection_code_v2 = 'LDBL' then 1
        else 0
    end LDBL_inflow_1,
    case
        when a.collection_code_v2 != 'CRRC' and t.collection_code_v2 = 'CRRC' then 1
        else 0
    end CRRC_inflow_1,
    case
        when a.collection_code_v2 != 'PEND' and t.collection_code_v2 = 'PEND' then 1
        else 0
    end PEND_inflow_1,
    case
        when a.collection_code_v2 != 'SLOW' and t.collection_code_v2 = 'SLOW' then 1
        else 0
    end SLOW_inflow_1,
    case
        when a.collection_code_v2 != 'DKNK' and t.collection_code_v2 = 'DKNK' then 1
        else 0
    end DKNK_inflow_1,
    case
        when a.collection_code_v2 != 'LWST' and t.collection_code_v2 = 'LWST' then 1
        else 0
    end lwst_inflow_1,
    case
        when a.collection_code_v2 != 'BK07/BK13' and t.collection_code_v2 = 'BK07/BK13' then 1
        else 0
    end bankruptcy_inflow_1,
    case
        when a.collection_code_v2 != 'REVW' and t.collection_code_v2 = 'REVW' then 1
        else 0
    end revw_inflow_1,
    case
        when a.collection_code_v2 != 'STOP' and t.collection_code_v2 = 'STOP' then 1
        else 0
    end stop_inflow_1,
    case
        when a.collection_code_v2 != 'DECT' and t.collection_code_v2 = 'DECT' then 1
        else 0
    end dect_inflow_1,
    case
        when a.collection_code_v2 != 'VSPP' and t.collection_code_v2 = 'VSPP' then 1
        else 0
    end vspp_inflow_1,
    case
        when (a.collection_code_v2 != 'CORP') and (t.collection_code_v2 = 'CORP' and soln.solar_billing_account_number is not null) then 1
        when (soln_yest.solar_billing_account_number is null) and (t.collection_code_v2 = 'CORP' and soln.solar_billing_account_number is not null) then 1
-- //        when (a.collection_code_v2 != 'CORP' and soln_yest.solar_billing_account_number is not null) and (t.collection_code_v2 = 'CORP' and soln_solar_billing_account_number is not null) then 1
        else 0
    end default_corp_inflow_1,
    case
        when (a.collection_code_v2 != 'CORP') and (t.collection_code_v2 = 'CORP' and soln.solar_billing_account_number is null) then 1
-- //        when (a.collection_code_v2 = 'CORP' and soln_yest.solar_billing_account_number is null) and (t.collection_code_v2 = 'CORP' and soln.solar_billing_account_number is null) then 1
        when (soln_yest.solar_billing_account_number is not null) and (t.collection_code_v2 = 'CORP' and soln.solar_billing_account_number is null) then 1
        else 0
    end owned_corp_inflow_1,
    case
        when a.collection_code_v2 != 'COLL' and (t.collection_code_v2 = 'COLL' and soln.solar_billing_account_number is not null) then 1
        when soln_yest.solar_billing_account_number is null and (t.collection_code_v2 = 'COLL' and soln.solar_billing_account_number is not null) then 1
        else 0
    end soln_mbw_inflow_1,
    case
        when a.collection_code_v2 != 'ATTY' and (t.collection_code_v2 = 'ATTY' and soln.solar_billing_account_number is not null) then 1
        when soln_yest.solar_billing_account_number is null and (t.collection_code_v2 = 'ATTY' and soln.solar_billing_account_number is not null) then 1
        else 0
    end soln_dra_inflow_1,
    case
        when a.collection_code_v2 != 'COLL' and (t.collection_code_v2 = 'COLL' and coln.solar_billing_account_number is not null) then 1
        when coln_yest.solar_billing_account_number is null and (t.collection_code_v2 = 'COLL' and coln.solar_billing_account_number is not null) then 1
        else 0
    end coln_mbw_inflow_1,
    case
        when a.collection_code_v2 != 'ATTY' and (t.collection_code_v2 = 'ATTY' and coln.solar_billing_account_number is not null) then 1
        when coln_yest.solar_billing_account_number is null and (t.collection_code_v2 = 'ATTY' and coln.solar_billing_account_number is not null) then 1
        else 0
    end coln_dra_inflow_1,
    case
        when a.collection_code_v2 != 'COLL' and t.collection_code_v2 = 'COLL' and soln.solar_billing_account_number is null and coln.solar_billing_account_number is null then 1
        else 0
    end unowned_mbw_inflow_1,
    case
        when a.collection_code_v2 != 'ATTY' and t.collection_code_v2 = 'ATTY' and soln.solar_billing_account_number is null and coln.solar_billing_account_number is null then 1
        else 0
    end unowned_dra_inflow_1,
    case
        when a.collection_code_v2 != 'ADMIN' and t.collection_code_v2 = 'ADMIN' then 1
        else 0
    end soln_admin_inflow_1,
    case
        when a.collection_code_v2 = 'FORE' and t.collection_code_v2 != 'FORE' then 1
        else 0
    end fore_outflow_1,
    case
        when a.collection_code_v2 = 'NONE' and t.collection_code_v2 != 'NONE' then 1
        else 0
    end NONE_outflow_1,
    case
        when a.collection_code_v2 = 'WOUT' and t.collection_code_v2 != 'WOUT' then 1
        else 0
    end WOUT_outflow_1,
    case
        when a.collection_code_v2 = 'SKIP' and t.collection_code_v2 != 'SKIP' then 1
        else 0
    end SKIP_outflow_1,
    case
        when a.collection_code_v2 = 'PAYP' and t.collection_code_v2 != 'PAYP' then 1
        else 0
    end PAYP_outflow_1,
    case
        when a.collection_code_v2 = 'TRAN' and t.collection_code_v2 != 'TRAN' then 1
        else 0
    end TRAN_outflow_1,
    case
        when a.collection_code_v2 = 'TRLD' and t.collection_code_v2 != 'TRLD' then 1
        else 0
    end TRLD_outflow_1,
    case
        when a.collection_code_v2 = 'TRRC' and t.collection_code_v2 != 'TRRC' then 1
        else 0
    end TRRC_outflow_1,
    case
        when a.collection_code_v2 = 'LDBL' and t.collection_code_v2 != 'LDBL' then 1
        else 0
    end LDBL_outflow_1,
    case
        when a.collection_code_v2 = 'CRRC' and t.collection_code_v2 != 'CRRC' then 1
        else 0
    end CRRC_outflow_1,
    case
        when a.collection_code_v2 = 'PEND' and t.collection_code_v2 != 'PEND' then 1
        else 0
    end PEND_outflow_1,
    case
        when a.collection_code_v2 = 'SLOW' and t.collection_code_v2 != 'SLOW' then 1
        else 0
    end SLOW_outflow_1,
    case
        when a.collection_code_v2 = 'DKNK' and t.collection_code_v2 != 'DKNK' then 1
        else 0
    end DKNK_outflow_1,
    case
        when a.collection_code_v2 = 'LWST' and t.collection_code_v2 != 'LWST' then 1
        else 0
    end lwst_outflow_1,
    case
        when a.collection_code_v2 = 'BK07/BK13' and t.collection_code_V2 != 'BK07/BK13' then 1
        else 0
    end bankruptcy_outflow_1,
    case
        when a.collection_code_v2 = 'REVW' and t.collection_code_v2 != 'REVW' then 1
        else 0
    end revw_outflow_1,
    case
        when a.collection_code_v2 = 'STOP' and t.collection_code_v2 != 'STOP' then 1
        else 0
    end stop_outflow_1,
    case
        when a.collection_code_v2 = 'DECT' and t.collection_code_v2 != 'DECT' then 1
        else 0
    end dect_outflow_1,
    case
        when a.collection_code_v2 = 'VSPP' and t.collection_code_v2 != 'VSPP' then 1
        else 0
    end vspp_outflow_1,
    case
        when (a.collection_code_v2 = 'CORP' and soln_yest.solar_billing_account_number is not null) and (t.collection_code_v2 != 'CORP') then 1
        when (a.collection_code_v2 = 'CORP' and soln_yest.solar_billing_account_number is not null) and (soln.solar_billing_account_number is null) then 1
-- //        when (a.collection_code_v2 = 'CORP' and soln_yest.solar_billing_account_number is not null) and (t.collection_code_v2 != 'CORP' and soln.solar_billing_account_number is null) then 1
        else 0
    end default_corp_outflow_1,
    case
        when (a.collection_code = 'CORP' and soln_yest.solar_billing_account_number is null) and (t.collection_code_v2 != 'CORP') then 1
        when (a.collection_code = 'CORP' and soln_yest.solar_billing_account_number is null) and (soln.solar_billing_account_number is not null) then 1
        else 0
    end owned_corp_outflow_1,
    case
        when (a.collection_code = 'COLL' and soln_yest.solar_billing_account_number is not null) and t.collection_code_v2 != 'COLL' then 1
        when (a.collection_code = 'COLL' and soln_yest.solar_billing_account_number is not null) and soln.solar_billing_account_number is null then 1
        else 0
    end soln_mbw_outflow_1,
    case
        when (a.collection_code = 'ATTY' and soln_yest.solar_billing_account_number is not null) and (t.collection_code_v2 != 'ATTY') then 1
        when (a.collection_code = 'ATTY' and soln_yest.solar_billing_account_number is not null) and (soln.solar_billing_account_number is null) then 1
        else 0
    end soln_dra_outflow_1,
    case
        when (a.collection_code = 'COLL' and coln_yest.solar_billing_account_number is not null) and (t.collection_code_v2 != 'COLL') then 1
        when (a.collection_code = 'COLL' and coln_yest.solar_billing_account_number is not null) and (coln.solar_billing_account_number is null) then 1
        else 0
    end coln_mbw_outflow_1,
    case
        when (a.collection_code = 'ATTY' and coln_yest.solar_billing_account_number is not null) and (t.collection_code_v2 != 'ATTY') then 1
        when (a.collection_code = 'ATTY' and coln_yest.solar_billing_account_number is not null) and (coln.solar_billing_account_number is null) then 1
        else 0
    end coln_dra_outflow_1,
    case
        when (a.collection_code = 'COLL' and coln_yest.solar_billing_account_number is null and soln_yest.solar_billing_account_number is null) and (t.collection_code_v2 != 'COLL') then 1
        when (a.collection_code = 'COLL' and coln_yest.solar_billing_account_number is null and soln_yest.solar_billing_account_number is null) and (soln.solar_billing_account_number is not null) then 1
        when (a.collection_code = 'COLL' and coln_yest.solar_billing_account_number is null and soln_yest.solar_billing_account_number is null) and (coln.solar_billing_account_number) is not null then 1
        else 0
    end unowned_mbw_outflow_1,
    case
        when (a.collection_code = 'ATTY' and coln_yest.solar_billing_account_number is null and soln_yest.solar_billing_account_number is null) and (t.collection_code_v2 != 'ATTY') then 1
        when (a.collection_code = 'ATTY' and coln_yest.solar_billing_account_number is null and soln_yest.solar_billing_account_number is null) and (soln.solar_billing_account_number is not null) then 1
        when (a.collection_code = 'ATTY' and coln_yest.solar_billing_account_number is null and soln_yest.solar_billing_account_number is null) and (coln.solar_billing_account_number is not null) then 1
        else 0
    end unowned_dra_outflow_1
    
from ld_today t
inner join ld_yest a
    on t.billing_account = a.billing_account
left join rpt.t_project p
    on t.billing_account = p.solar_billing_account_number
left join soln
    on t.billing_account = soln.solar_billing_account_number
left join coln
    on t.billing_account = coln.solar_billing_account_number
left join soln_yest
    on t.billing_account = soln_yest.solar_billing_account_number
left join coln_yest
    on t.billing_account = coln_yest.solar_billing_account_number
left join rpt.t_service s
    on t.billing_account = s.solar_billing_account_number
where (s.opty_contract_type not in ('CASH','LOAN') or s.opty_contract_type is null)
    and (s.service_status != 'Cancelled' or s.service_status is null)
    and (p.sales_office != 'Restore Utah' or p.sales_office is null)),
    
table2 as (select 
    case
        when a.collection_code_v2 = 'ADMIN' and t.collection_code_v2 != 'ADMIN' then 1
        else 0
    end soln_admin_outflow_1,
    case
        when a.collection_code_v2 != 'FORE' and t.collection_code_v2 = 'FORE' then 1
        else 0
    end fore_inflow_1,
    case
        when a.collection_code_v2 != 'NONE' and t.collection_code_v2 = 'NONE' then 1
        else 0
    end NONE_inflow_1,
    case
        when a.collection_code_v2 != 'WOUT' and t.collection_code_v2= 'WOUT' then 1
        else 0
    end WOUT_inflow_1,
    case
        when a.collection_code_v2 != 'SKIP' and t.collection_code_v2 = 'SKIP' then 1
        else 0
    end SKIP_inflow_1,
    case
        when a.collection_code_v2 != 'PAYP' and t.collection_code_v2 = 'PAYP' then 1
        else 0
    end PAYP_inflow_1,
    case
        when a.collection_code_v2 != 'TRAN' and t.collection_code_v2 = 'TRAN' then 1
        else 0
    end TRAN_inflow_1,
    case
        when a.collection_code_v2 != 'TRLD' and t.collection_code_v2 = 'TRLD' then 1
        else 0
    end TRLD_inflow_1,
    case
        when a.collection_code_v2 != 'TRRC' and t.collection_code_v2 = 'TRRC' then 1
        else 0
    end TRRC_inflow_1,
    case
        when a.collection_code_v2 != 'LDBL' and t.collection_code_v2 = 'LDBL' then 1
        else 0
    end LDBL_inflow_1,
    case
        when a.collection_code_v2 != 'CRRC' and t.collection_code_v2 = 'CRRC' then 1
        else 0
    end CRRC_inflow_1,
    case
        when a.collection_code_v2 != 'PEND' and t.collection_code_v2 = 'PEND' then 1
        else 0
    end PEND_inflow_1,
    case
        when a.collection_code_v2 != 'SLOW' and t.collection_code_v2 = 'SLOW' then 1
        else 0
    end SLOW_inflow_1,
    case
        when a.collection_code_v2 != 'DKNK' and t.collection_code_v2 = 'DKNK' then 1
        else 0
    end DKNK_inflow_1,
    case
        when a.collection_code_v2 != 'LWST' and t.collection_code_v2 = 'LWST' then 1
        else 0
    end lwst_inflow_1,
    case
        when a.collection_code_v2 != 'BK07/BK13' and t.collection_code_v2 = 'BK07/BK13' then 1
        else 0
    end bankruptcy_inflow_1,
    case
        when a.collection_code_v2 != 'REVW' and t.collection_code_v2 = 'REVW' then 1
        else 0
    end revw_inflow_1,
    case
        when a.collection_code_v2 != 'STOP' and t.collection_code_v2 = 'STOP' then 1
        else 0
    end stop_inflow_1,
    case
        when a.collection_code_v2 != 'DECT' and t.collection_code_v2 = 'DECT' then 1
        else 0
    end dect_inflow_1,
    case
        when a.collection_code_v2 != 'VSPP' and t.collection_code_v2 = 'VSPP' then 1
        else 0
    end vspp_inflow_1,
    case
        when (a.collection_code_v2 != 'CORP') and (t.collection_code_v2 = 'CORP' and soln.solar_billing_account_number is not null) then 1
        when (soln_yest.solar_billing_account_number is null) and (t.collection_code_v2 = 'CORP' and soln.solar_billing_account_number is not null) then 1
//        when (a.collection_code_v2 != 'CORP' and soln_yest.solar_billing_account_number is not null) and (t.collection_code_v2 = 'CORP' and soln_solar_billing_account_number is not null) then 1
        else 0
    end default_corp_inflow_1,
    case
        when (a.collection_code_v2 != 'CORP') and (t.collection_code_v2 = 'CORP' and soln.solar_billing_account_number is null) then 1
-- //        when (a.collection_code_v2 = 'CORP' and soln_yest.solar_billing_account_number is null) and (t.collection_code_v2 = 'CORP' and soln.solar_billing_account_number is null) then 1
        when (soln_yest.solar_billing_account_number is not null) and (t.collection_code_v2 = 'CORP' and soln.solar_billing_account_number is null) then 1
        else 0
    end owned_corp_inflow_1,
    case
        when a.collection_code_v2 != 'COLL' and (t.collection_code_v2 = 'COLL' and soln.solar_billing_account_number is not null) then 1
        when soln_yest.solar_billing_account_number is null and (t.collection_code_v2 = 'COLL' and soln.solar_billing_account_number is not null) then 1
        else 0
    end soln_mbw_inflow_1,
    case
        when a.collection_code_v2 != 'ATTY' and (t.collection_code_v2 = 'ATTY' and soln.solar_billing_account_number is not null) then 1
        when soln_yest.solar_billing_account_number is null and (t.collection_code_v2 = 'ATTY' and soln.solar_billing_account_number is not null) then 1
        else 0
    end soln_dra_inflow_1,
    case
        when a.collection_code_v2 != 'COLL' and (t.collection_code_v2 = 'COLL' and coln.solar_billing_account_number is not null) then 1
        when coln_yest.solar_billing_account_number is null and (t.collection_code_v2 = 'COLL' and coln.solar_billing_account_number is not null) then 1
        else 0
    end coln_mbw_inflow_1,
    case
        when a.collection_code_v2 != 'ATTY' and (t.collection_code_v2 = 'ATTY' and coln.solar_billing_account_number is not null) then 1
        when coln_yest.solar_billing_account_number is null and (t.collection_code_v2 = 'ATTY' and coln.solar_billing_account_number is not null) then 1
        else 0
    end coln_dra_inflow_1,
    case
        when a.collection_code_v2 != 'COLL' and t.collection_code_v2 = 'COLL' and soln.solar_billing_account_number is null and coln.solar_billing_account_number is null then 1
        else 0
    end unowned_mbw_inflow_1,
    case
        when a.collection_code_v2 != 'ATTY' and t.collection_code_v2 = 'ATTY' and soln.solar_billing_account_number is null and coln.solar_billing_account_number is null then 1
        else 0
    end unowned_dra_inflow_1,
    case
        when a.collection_code_v2 != 'ADMIN' and t.collection_code_v2 = 'ADMIN' then 1
        else 0
    end soln_admin_inflow_1,
    case
        when a.collection_code_v2 = 'FORE' and t.collection_code_v2 != 'FORE' then 1
        else 0
    end fore_outflow_1,
    case
        when a.collection_code_v2 = 'NONE' and t.collection_code_v2 != 'NONE' then 1
        else 0
    end NONE_outflow_1,
    case
        when a.collection_code_v2 = 'WOUT' and t.collection_code_v2 != 'WOUT' then 1
        else 0
    end WOUT_outflow_1,
    case
        when a.collection_code_v2 = 'SKIP' and t.collection_code_v2 != 'SKIP' then 1
        else 0
    end SKIP_outflow_1,
    case
        when a.collection_code_v2 = 'PAYP' and t.collection_code_v2 != 'PAYP' then 1
        else 0
    end PAYP_outflow_1,
    case
        when a.collection_code_v2 = 'TRAN' and t.collection_code_v2 != 'TRAN' then 1
        else 0
    end TRAN_outflow_1,
    case
        when a.collection_code_v2 = 'TRLD' and t.collection_code_v2 != 'TRLD' then 1
        else 0
    end TRLD_outflow_1,
    case
        when a.collection_code_v2 = 'TRRC' and t.collection_code_v2 != 'TRRC' then 1
        else 0
    end TRRC_outflow_1,
    case
        when a.collection_code_v2 = 'LDBL' and t.collection_code_v2 != 'LDBL' then 1
        else 0
    end LDBL_outflow_1,
    case
        when a.collection_code_v2 = 'CRRC' and t.collection_code_v2 != 'CRRC' then 1
        else 0
    end CRRC_outflow_1,
    case
        when a.collection_code_v2 = 'PEND' and t.collection_code_v2 != 'PEND' then 1
        else 0
    end PEND_outflow_1,
    case
        when a.collection_code_v2 = 'SLOW' and t.collection_code_v2 != 'SLOW' then 1
        else 0
    end SLOW_outflow_1,
    case
        when a.collection_code_v2 = 'DKNK' and t.collection_code_v2 != 'DKNK' then 1
        else 0
    end DKNK_outflow_1,
    case
        when a.collection_code_v2 = 'LWST' and t.collection_code_v2 != 'LWST' then 1
        else 0
    end lwst_outflow_1,
    case
        when a.collection_code_v2 = 'BK07/BK13' and t.collection_code_V2 != 'BK07/BK13' then 1
        else 0
    end bankruptcy_outflow_1,
    case
        when a.collection_code_v2 = 'REVW' and t.collection_code_v2 != 'REVW' then 1
        else 0
    end revw_outflow_1,
    case
        when a.collection_code_v2 = 'STOP' and t.collection_code_v2 != 'STOP' then 1
        else 0
    end stop_outflow_1,
    case
        when a.collection_code_v2 = 'DECT' and t.collection_code_v2 != 'DECT' then 1
        else 0
    end dect_outflow_1,
    case
        when a.collection_code_v2 = 'VSPP' and t.collection_code_v2 != 'VSPP' then 1
        else 0
    end vspp_outflow_1,
    case
        when (a.collection_code_v2 = 'CORP' and soln_yest.solar_billing_account_number is not null) and (t.collection_code_v2 != 'CORP') then 1
        when (a.collection_code_v2 = 'CORP' and soln_yest.solar_billing_account_number is not null) and (soln.solar_billing_account_number is null) then 1
-- //        when (a.collection_code_v2 = 'CORP' and soln_yest.solar_billing_account_number is not null) and (t.collection_code_v2 != 'CORP' and soln.solar_billing_account_number is null) then 1
        else 0
    end default_corp_outflow_1,
    case
        when (a.collection_code = 'CORP' and soln_yest.solar_billing_account_number is null) and (t.collection_code_v2 != 'CORP') then 1
        when (a.collection_code = 'CORP' and soln_yest.solar_billing_account_number is null) and (soln.solar_billing_account_number is not null) then 1
        else 0
    end owned_corp_outflow_1,
    case
        when (a.collection_code = 'COLL' and soln_yest.solar_billing_account_number is not null) and t.collection_code_v2 != 'COLL' then 1
        when (a.collection_code = 'COLL' and soln_yest.solar_billing_account_number is not null) and soln.solar_billing_account_number is null then 1
        else 0
    end soln_mbw_outflow_1,
    case
        when (a.collection_code = 'ATTY' and soln_yest.solar_billing_account_number is not null) and (t.collection_code_v2 != 'ATTY') then 1
        when (a.collection_code = 'ATTY' and soln_yest.solar_billing_account_number is not null) and (soln.solar_billing_account_number is null) then 1
        else 0
    end soln_dra_outflow_1,
    case
        when (a.collection_code = 'COLL' and coln_yest.solar_billing_account_number is not null) and (t.collection_code_v2 != 'COLL') then 1
        when (a.collection_code = 'COLL' and coln_yest.solar_billing_account_number is not null) and (coln.solar_billing_account_number is null) then 1
        else 0
    end coln_mbw_outflow_1,
    case
        when (a.collection_code = 'ATTY' and coln_yest.solar_billing_account_number is not null) and (t.collection_code_v2 != 'ATTY') then 1
        when (a.collection_code = 'ATTY' and coln_yest.solar_billing_account_number is not null) and (coln.solar_billing_account_number is null) then 1
        else 0
    end coln_dra_outflow_1,
    case
        when (a.collection_code = 'COLL' and coln_yest.solar_billing_account_number is null and soln_yest.solar_billing_account_number is null) and (t.collection_code_v2 != 'COLL') then 1
        when (a.collection_code = 'COLL' and coln_yest.solar_billing_account_number is null and soln_yest.solar_billing_account_number is null) and (soln.solar_billing_account_number is not null) then 1
        when (a.collection_code = 'COLL' and coln_yest.solar_billing_account_number is null and soln_yest.solar_billing_account_number is null) and (coln.solar_billing_account_number) is not null then 1
        else 0
    end unowned_mbw_outflow_1,
    case
        when (a.collection_code = 'ATTY' and coln_yest.solar_billing_account_number is null and soln_yest.solar_billing_account_number is null) and (t.collection_code_v2 != 'ATTY') then 1
        when (a.collection_code = 'ATTY' and coln_yest.solar_billing_account_number is null and soln_yest.solar_billing_account_number is null) and (soln.solar_billing_account_number is not null) then 1
        when (a.collection_code = 'ATTY' and coln_yest.solar_billing_account_number is null and soln_yest.solar_billing_account_number is null) and (coln.solar_billing_account_number is not null) then 1
        else 0
    end unowned_dra_outflow_1
    
from ld_today t
inner join ld_yest a
    on t.billing_account = a.billing_account
left join rpt.t_project p
    on t.billing_account = p.solar_billing_account_number
left join soln
    on t.billing_account = soln.solar_billing_account_number
left join coln
    on t.billing_account = coln.solar_billing_account_number
left join soln_yest
    on t.billing_account = soln_yest.solar_billing_account_number
left join coln_yest
    on t.billing_account = coln_yest.solar_billing_account_number
left join rpt.t_service s
    on t.billing_account = s.solar_billing_account_number
where (s.opty_contract_type in ('CASH','LOAN')
    or s.service_status = 'Cancelled'
    or p.sales_office = 'Restore Utah')),

a_thru_e as (select 1 as id, 'Collections Team' as ownership,
    'REVW' as ld_code_or_other_criteria,
    'Account being reviewed by Collections Team' as definitions,
    sum(revw_inflow_1) as inflow,
    sum(revw_outflow_1) as outflow
from table1
union
select 2, 'Collections Team',
    'COLL w/D3 Default Case',
    'Collections Team MB&W',
    sum(coln_mbw_inflow_1),
    sum(coln_mbw_outflow_1)
from table1
union 
select 3, 'Collections Team',
    'ATTY w/D3 Default Case',
    'Collections Team DRA',
    sum(coln_dra_inflow_1),
    sum(coln_dra_outflow_1)
from table1
union 
select 4, 'Collections Team',
    'STOP',
    'Stop request by Customer',
    sum(stop_inflow_1),
    sum(stop_outflow_1)
from table1
union 
select 5, 'Collections Team',
    'DECT',
    'System Deactivated',
    sum(dect_inflow_1),
    sum(dect_outflow_1)
from table1
union 
select 6, 'Collections Team',
    'VSPP',
    'Accounts with Payment Plans',
    sum(vspp_inflow_1),
    sum(vspp_outflow_1)
from table1
union 
select 7, 'Solutions Team',
    'CORP w/Default Case',
    'Accounts in Default and worked by Solutions Team',
    sum(default_corp_inflow_1),
    sum(default_corp_outflow_1)
from table1
union 
select 8, 'Solutions Team',
    'CORP w/out Default Case',
    'Accounts worked by Solutions Team',
    sum(owned_corp_inflow_1),
    sum(owned_corp_outflow_1)
from table1
union 
select 9, 'Solutions Team',
    'COLL w/CORP Default Case',
    'Solutions Team MB&W',
    sum(soln_mbw_inflow_1),
    sum(soln_mbw_outflow_1)
from table1
union 
select 10, 'Solutions Team',
    'ATTY w/CORP Default Case',
    'Solutions Team DRA',
    sum(soln_dra_inflow_1),
    sum(soln_dra_outflow_1)
from table1
union 
select 11, 'Solutions Team',
    'FORE',
    'Foreclosures',
    sum(fore_inflow_1),
    sum(fore_outflow_1)
from table1
union 
select 12, 'Solutions Team',
    'LWST',
    'Accounts with Legal Team',
    sum(lwst_inflow_1),
    sum(lwst_outflow_1)
from table1
union 
select 13, 'Solutions Team',
    'BK07/BK13',
    'Bankruptcy',
    sum(bankruptcy_inflow_1),
    sum(bankruptcy_outflow_1)
from table1
union 
select 14, 'Solutions Team',
    'Admin Codes',
    'Please see \'Solutions\' Worksheet',
    sum(soln_admin_inflow_1),
    sum(soln_admin_outflow_1)
from table1
union 

select 15, 'CASH/LOAN, Cancelled, Restore Utah',
    'REVW',
    'Account being reviewed by Collections Team',
    sum(revw_inflow_1),
    sum(revw_outflow_1)
from table2
union
select 16, 'CASH/LOAN, Cancelled, Restore Utah',
    'COLL w/D3 Default Case',
    'Collections Team MB&W',
    sum(coln_mbw_inflow_1),
    sum(coln_mbw_outflow_1)
from table2
union 
select 17, 'CASH/LOAN, Cancelled, Restore Utah',
    'ATTY w/D3 Default Case',
    'Collections Team DRA',
    sum(coln_dra_inflow_1),
    sum(coln_dra_outflow_1)
from table2
union 
select 18, 'CASH/LOAN, Cancelled, Restore Utah',
    'STOP',
    'Stop request by Customer',
    sum(stop_inflow_1),
    sum(stop_outflow_1)
from table2
union 
select 19, 'CASH/LOAN, Cancelled, Restore Utah',
    'DECT',
    'System Deactivated',
    sum(dect_inflow_1),
    sum(dect_outflow_1)
from table2
union 
select 20, 'CASH/LOAN, Cancelled, Restore Utah',
    'VSPP',
    'Accounts with Payment Plans',
    sum(vspp_inflow_1),
    sum(vspp_outflow_1)
from table2
union 
select 21, 'CASH/LOAN, Cancelled, Restore Utah',
    'CORP w/Default Case',
    'Accounts in Default and worked by Solutions Team',
    sum(default_corp_inflow_1),
    sum(default_corp_outflow_1)
from table2
union 
select 22, 'CASH/LOAN, Cancelled, Restore Utah',
    'CORP w/out Default Case',
    'Accounts worked by Solutions Team',
    sum(owned_corp_inflow_1),
    sum(owned_corp_outflow_1)
from table2
union 
select 23, 'CASH/LOAN, Cancelled, Restore Utah',
    'COLL w/CORP Default Case',
    'Solutions Team MB&W',
    sum(soln_mbw_inflow_1),
    sum(soln_mbw_outflow_1)
from table2
union 
select 24, 'CASH/LOAN, Cancelled, Restore Utah',
    'ATTY w/CORP Default Case',
    'Solutions Team DRA',
    sum(soln_dra_inflow_1),
    sum(soln_dra_outflow_1)
from table2
union 
select 25, 'CASH/LOAN, Cancelled, Restore Utah',
    'FORE',
    'Foreclosures',
    sum(fore_inflow_1),
    sum(fore_outflow_1)
from table2
union 
select 26, 'CASH/LOAN, Cancelled, Restore Utah',
    'LWST',
    'Accounts with Legal Team',
    sum(lwst_inflow_1),
    sum(lwst_outflow_1)
from table2
union 
select 27, 'CASH/LOAN, Cancelled, Restore Utah',
    'BK07/BK13',
    'Bankruptcy',
    sum(bankruptcy_inflow_1),
    sum(bankruptcy_outflow_1)
from table2
union 
select 28, 'CASH/LOAN, Cancelled, Restore Utah',
    'Admin Codes',
    'Please see \'Solutions\' Worksheet',
    sum(soln_admin_inflow_1),
    sum(soln_admin_outflow_1)
from table2),

f_top as (select 
    case
        when t.collection_code in ('BK07','BK13') then 'BK07/BK13'
        when t.collection_code in ('LDSY','FCRP','HOST','REMV','WOFF','PREP','REPO','COFF','DNCC','SERV','CXLD','ESCL') then 'ADMIN'
        else t.collection_code
    end collection_codes,
    case
        when t.collection_code = 'CORP' and soln.solar_billing_account_number is not null then 'Default'
        when t.collection_code = 'CORP' and soln.solar_billing_account_number is null then 'Non-Default'
        when t.collection_code = 'COLL' and soln.solar_billing_account_number is not null then 'Solutions'
        when t.collection_code = 'ATTY' and soln.solar_billing_account_number is not null then 'Solutions'
        when t.collection_code = 'COLL' and coln.solar_billing_account_number is not null then 'Collections'
        when t.collection_code = 'ATTY' and coln.solar_billing_account_number is not null then 'Collections'
    end default_case,
    case
        when collection_codes = 'REVW' then 1
        when collection_codes = 'COLL' and default_case = 'Collections' then 2
        when collection_codes = 'ATTY' and default_case = 'Collections' then 3
        when collection_codes = 'STOP' then 4
        when collection_codes = 'DECT' then 5
        when collection_codes = 'VSPP' then 6
        when collection_codes = 'CORP' and default_case = 'Default' then 7
        when collection_codes = 'CORP' and default_case = 'Non-Default' then 8
        when collection_codes = 'COLL' and default_case = 'Solutions' then 9
        when collection_codes = 'ATTY' and default_case = 'Solutions' then 10
        when collection_codes = 'FORE' then 11
        when collection_codes = 'LWST' then 12
        when collection_codes = 'BK07/BK13' then 13
        when collection_codes = 'ADMIN' then 14
    end rankorder,
    count(*) counts
from ld.t_daily_data_extract t
left join rpt.t_project p
    on p.solar_billing_account_number = t.billing_account
left join soln
    on p.solar_billing_account_number = soln.solar_billing_account_number
left join coln
    on p.solar_billing_account_number = coln.solar_billing_account_number
left join rpt.t_service s
    on s.solar_billing_account_number = t.billing_account
where date_trunc(day, t.load_date) = (select max(date_trunc(day, t.load_date))
                            from ld.t_daily_data_extract t
                            where date_trunc(day, t.load_date) <= date_trunc(day, current_timestamp))
    and t.collection_code is not null
    and (s.opty_contract_type not in ('CASH','LOAN') or s.opty_contract_type is null)
    and (s.service_status != 'Cancelled' or s.service_status is null)
    and (p.sales_office != 'Restore Utah' or p.sales_office is null)
group by 
    collection_codes, default_case, rankorder
order by collection_codes),
                
f_bottom as (select 
    case
        when t.collection_code in ('BK07','BK13') then 'BK07/BK13'
        when t.collection_code in ('LDSY','FCRP','HOST','REMV','WOFF','PREP','REPO','COFF','DNCC','SERV','CXLD','ESCL') then 'ADMIN'
        else t.collection_code
    end collection_codes,
    case
        when t.collection_code = 'CORP' and soln.solar_billing_account_number is not null then 'Default'
        when t.collection_code = 'CORP' and soln.solar_billing_account_number is null then 'Non-Default'
        when t.collection_code = 'COLL' and soln.solar_billing_account_number is not null then 'Solutions'
        when t.collection_code = 'ATTY' and soln.solar_billing_account_number is not null then 'Solutions'
        when t.collection_code = 'COLL' and coln.solar_billing_account_number is not null then 'Collections'
        when t.collection_code = 'ATTY' and coln.solar_billing_account_number is not null then 'Collections'
    end default_case,
    case
        when collection_codes = 'REVW' then 15
        when collection_codes = 'COLL' and default_case = 'Collections' then 16
        when collection_codes = 'ATTY' and default_case = 'Collections' then 17
        when collection_codes = 'STOP' then 18
        when collection_codes = 'DECT' then 19
        when collection_codes = 'VSPP' then 20
        when collection_codes = 'CORP' and default_case = 'Default' then 21
        when collection_codes = 'CORP' and default_case = 'Non-Default' then 22
        when collection_codes = 'COLL' and default_case = 'Solutions' then 23
        when collection_codes = 'ATTY' and default_case = 'Solutions' then 24
        when collection_codes = 'FORE' then 25
        when collection_codes = 'LWST' then 26
        when collection_codes = 'BK07/BK13' then 27
        when collection_codes = 'ADMIN' then 28
    end rankorder,
    count(*) counts
from ld.t_daily_data_extract_hist t
left join rpt.t_project p
    on p.solar_billing_account_number = t.billing_account
left join soln
    on p.solar_billing_account_number = soln.solar_billing_account_number
left join coln
    on p.solar_billing_account_number = coln.solar_billing_account_number
left join rpt.t_service s
    on s.solar_billing_account_number = t.billing_account
where date_trunc(day, t.load_date) = (select max(date_trunc(day, t.load_date))
                            from ld.t_daily_data_extract_hist t
                            where date_trunc(day, t.load_date) <= date_trunc(day, current_timestamp))
    and t.collection_code is not null
    and (s.opty_contract_type in ('CASH','LOAN')
    or s.service_status = 'Cancelled'
    or p.sales_office = 'Restore Utah')
group by 
    collection_codes, default_case, rankorder
order by collection_codes),

f as (select * from f_top
        union all
      select * from f_bottom),
               
gh_top as (select
    case
        when t.collection_code in ('BK07','BK13') then 'BK07/BK13'
        when t.collection_code in ('LDSY','FCRP','HOST','REMV','WOFF','PREP','REPO','COFF','DNCC','SERV','CXLD','ESCL') then 'ADMIN'
        else t.collection_code
    end collection_codes,
    case
        when t.collection_code = 'CORP' and soln.solar_billing_account_number is not null then 'Default'
        when t.collection_code = 'CORP' and soln.solar_billing_account_number is null then 'Non-Default'
        when t.collection_code = 'COLL' and soln.solar_billing_account_number is not null then 'Solutions'
        when t.collection_code = 'ATTY' and soln.solar_billing_account_number is not null then 'Solutions'
        when t.collection_code = 'COLL' and coln.solar_billing_account_number is not null then 'Collections'
        when t.collection_code = 'ATTY' and coln.solar_billing_account_number is not null then 'Collections'
    end default_case,
    case
        when collection_codes = 'REVW' then 1
        when collection_codes = 'COLL' and default_case = 'Collections' then 2
        when collection_codes = 'ATTY' and default_case = 'Collections' then 3
        when collection_codes = 'STOP' then 4
        when collection_codes = 'DECT' then 5
        when collection_codes = 'VSPP' then 6
        when collection_codes = 'CORP' and default_case = 'Default' then 7
        when collection_codes = 'CORP' and default_case = 'Non-Default' then 8
        when collection_codes = 'COLL' and default_case = 'Solutions' then 9
        when collection_codes = 'ATTY' and default_case = 'Solutions' then 10
        when collection_codes = 'FORE' then 11
        when collection_codes = 'LWST' then 12
        when collection_codes = 'BK07/BK13' then 13
        when collection_codes = 'ADMIN' then 14
    end rankorder,
    round(avg(datediff(day,date_trunc(day, t.collection_date),date_trunc(day, current_timestamp))),0) avg_age,
    round(avg(t.age),0) aging
from ld.t_daily_data_extract t
left join soln
    on soln.solar_billing_account_number = t.billing_account
left join coln
    on coln.solar_billing_account_number = t.billing_account
left join rpt.t_service s
    on s.solar_billing_account_number = t.billing_account
left join rpt.t_project p
    on p.project_id = s.project_id and p.service_id = s.service_id
where collection_code is not null
    and (s.opty_contract_type not in ('CASH','LOAN') or s.opty_contract_type is null)
    and (s.service_status != 'Cancelled' or s.service_status is null)
    and (p.sales_office != 'Restore Utah' or p.sales_office is null)
group by
    collection_codes, default_case, rankorder
order by collection_codes),
               
gh_bottom as (select
    case
        when t.collection_code in ('BK07','BK13') then 'BK07/BK13'
        when t.collection_code in ('LDSY','FCRP','HOST','REMV','WOFF','PREP','REPO','COFF','DNCC','SERV','CXLD','ESCL') then 'ADMIN'
        else t.collection_code
    end collection_codes,
    case
        when t.collection_code = 'CORP' and soln.solar_billing_account_number is not null then 'Default'
        when t.collection_code = 'CORP' and soln.solar_billing_account_number is null then 'Non-Default'
        when t.collection_code = 'COLL' and soln.solar_billing_account_number is not null then 'Solutions'
        when t.collection_code = 'ATTY' and soln.solar_billing_account_number is not null then 'Solutions'
        when t.collection_code = 'COLL' and coln.solar_billing_account_number is not null then 'Collections'
        when t.collection_code = 'ATTY' and coln.solar_billing_account_number is not null then 'Collections'
    end default_case,
    case
        when collection_codes = 'REVW' then 15
        when collection_codes = 'COLL' and default_case = 'Collections' then 16
        when collection_codes = 'ATTY' and default_case = 'Collections' then 17
        when collection_codes = 'STOP' then 18
        when collection_codes = 'DECT' then 19
        when collection_codes = 'VSPP' then 20
        when collection_codes = 'CORP' and default_case = 'Default' then 21
        when collection_codes = 'CORP' and default_case = 'Non-Default' then 22
        when collection_codes = 'COLL' and default_case = 'Solutions' then 23
        when collection_codes = 'ATTY' and default_case = 'Solutions' then 24
        when collection_codes = 'FORE' then 25
        when collection_codes = 'LWST' then 26
        when collection_codes = 'BK07/BK13' then 27
        when collection_codes = 'ADMIN' then 28
    end rankorder,
    round(avg(datediff(day, date_trunc(day, t.collection_date), date_trunc(day, current_timestamp))),0) avg_age,
    round(avg(t.age),0) aging
from ld.t_daily_data_extract t
left join soln
    on soln.solar_billing_account_number = t.billing_account
left join coln
    on coln.solar_billing_account_number = t.billing_account
left join rpt.t_service s
    on s.solar_billing_account_number = t.billing_account
left join rpt.t_project p
    on p.project_id = s.project_id and p.service_id = s.service_id
where collection_code is not null
    and (s.opty_contract_type in ('CASH','LOAN') 
    or s.service_status = 'Cancelled' 
    or p.sales_office = 'Restore Utah')
group by
    collection_codes, default_case, rankorder
order by collection_codes),

g_thru_h as (select * from gh_top
        union all
      select * from gh_bottom)
      
select a.ownership,
    a.ld_code_or_other_criteria,
    a.definitions,
    a.inflow,
    a.outflow,
    nvl(f.counts, 0),
    nvl(g.aging, 0),
    nvl(g.avg_age, 0)
from a_thru_e a 
left join f on a.id = f.rankorder
left join g_thru_h g on a.id = g.rankorder
order by a.id
;
