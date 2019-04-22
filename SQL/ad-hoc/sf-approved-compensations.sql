-- =================================================================================================================================================================================
-- Changelog
-- =================================================================================================================================================================================
-- 2018-07-16 | Landon | Adjusted compensations queries so that pre-PTO accounts are moved from the non-SPA list to the SPA list.
-- 2018-07-16 | Landon | Changed JOIN between payments and projects to INNER instead of LEFT.
-- 2018-07-13 | Landon | Added criterion that requires payment_amount to be non-null (requested by Tyler Anderson).
-- 2018-08-22 | Mack   | Added criterion that requires fee_type to be not one of 'Sales Incentive', 'Sales Promise', or 'Sales Referral'.
-- 2018-08-22 | Mack   | Re-ordered join criteria to go back through Case then Service to get the correct Solar Billing Account Number
-- 2018-09-22 | Mack   | Updated to use Snowflake
-- 2018-12-14 | Mack   | Updated for automation through Automator
-- 2019-02-28 | Robert | Added the Service Number, Full Address, and Customer Full Name fields to the report to fill out the check request formstack easier (requested by Tyler and Alfonso).
-- =================================================================================================================================================================================

SELECT DISTINCT s.solar_billing_account_number
              , f.portfolio
              , pm.loss_of_savings
              , pm.reimbursement
              , pm.goodwill
              , pm.payment_amount
              , pm.approval_date
              , S.SERVICE_NUMBER
              , S.SERVICE_ADDRESS
              , S.SERVICE_CITY
              , S.SERVICE_STATE
              , S.SERVICE_ZIP_CODE
              , CT.FULL_NAME
              , case
                    when pm.reimbursement is not null then 'Estimated Production'
                    else 'Goodwill'
    end type

FROM rpt.t_payment pm
         inner join rpt.t_case AS c
                    on pm.case_id = c.case_id

         inner join rpt.t_service AS s
                    on c.service_id = s.service_id

         inner join rpt.t_project AS pr
                    on pr.project_id = s.project_id

         left join rpt.t_funding_portfolio AS f
                   on pr.project_id = f.project_id

         LEFT JOIN RPT.T_CONTACT AS CT
                   ON CT.CONTACT_ID = S.CONTRACT_SIGNER

WHERE pm.record_type = 'Customer Compensation'
  AND pm.status = 'Approved'
  AND date_TRUNC(day, pm.approval_date) >= dateadd(day, -7, CURRENT_DATE() - 1)--dateadd(day, -8, date_TRUNC(day, current_timestamp))--
  AND date_TRUNC(day, pm.approval_date) < date_trunc(day, CURRENT_DATE() - 2)--dateadd(day, -1, date_TRUNC(day, current_timestamp))--
  AND upper(s.opty_contract_type) NOT IN ('LOAN', 'CASH')
  AND (
        pm.payment_date IS NULL
        OR date_TRUNC(day, pm.approval_date) != date_TRUNC(day, pm.payment_date)
    )
  AND pm.payment_amount IS NOT NULL
  AND pr.in_service_date IS NOT NULL
  AND pm.fee_type NOT IN ('Sales Incentive', 'Sales Promise', 'Sales Referral')
  AND nvl(pm.payment_method, 'null') != 'Check'
;