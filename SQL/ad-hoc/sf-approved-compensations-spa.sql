-- =================================================================================================================================================================================
-- Changelog
-- =================================================================================================================================================================================
-- 2018-07-16 | Landon | Adjusted compensations queries so that pre-PTO accounts are moved from the non-SPA list to the SPA list.
-- 2018-07-16 | Landon | Changed JOIN between payments and projects to INNER instead of LEFT.
-- 2018-08-22 | Mack   | Added criterion that includes payments where the fee_type is one of 'Sales Incentive', 'Sales Promise', or 'Sales Referral'.
-- 2018-08-22 | Mack   | Re-ordered join criteria to go back through Case then Service to get the correct Solar Report Card - Billing Summary.sql Account Number
-- 2018-09-22 | Mack   | Updated to work with Snowflake
-- 2019-02-28 | Robert | Added the Service Number, Full Address, and Customer Full Name fields to the report to fill out the check request formstack easier (requested by Tyler and Alfonso).
-- =================================================================================================================================================================================


SELECT s.solar_billing_account_number
     , f.portfolio
     , pm.loss_of_savings
     , pm.reimbursement
     , pm.goodwill
     , pm.payment_amount
     , pm.approval_date
     , s.opty_contract_type
     , S.SERVICE_NUMBER
     , S.SERVICE_ADDRESS
     , S.SERVICE_CITY
     , S.SERVICE_STATE
     , S.SERVICE_ZIP_CODE
     , CT.FULL_NAME
     , CASE
           WHEN pr.in_service_date IS NOT NULL
               THEN 'Yes'
           ELSE 'No'
    END AS pto

FROM rpt.t_payment pm
         inner join rpt.t_case c
                    on pm.case_id = c.case_id

         inner join rpt.t_service s
                    on c.service_id = s.service_id

         inner join rpt.t_project pr
                    on pr.project_id = s.project_id

         left join rpt.t_funding_portfolio f
                   on pr.project_id = f.project_id

         LEFT JOIN RPT.T_CONTACT AS CT
                   ON CT.CONTACT_ID = S.CONTRACT_SIGNER

WHERE pm.record_type = 'Customer Compensation'
  AND pm.status = 'Approved'
  AND DATE_TRUNC(day, pm.approval_date) >= dateadd(day, -7, CURRENT_DATE() - 1)--dateadd(day, -8, date_TRUNC(day, current_timestamp))--
  AND date_TRUNC(day, pm.approval_date) < CURRENT_DATE() - 2--dateadd(day, -1, date_TRUNC(day, current_timestamp))--
  AND (
        upper(s.opty_contract_type) IN ('LOAN', 'CASH')
        OR (
                upper(s.opty_contract_type) NOT IN ('LOAN', 'CASH')
                AND (
                        pr.in_service_date IS NULL
                        OR (
                                pm.payment_date IS NOT NULL
                                AND date_TRUNC(day, pm.payment_date) = date_TRUNC(day, pm.approval_date)
                            )
                        OR pm.fee_type IN ('Sales Incentive', 'Sales Promise', 'Sales Referral')
                        OR nvl(pm.payment_method, 'null') = 'Check'
                    )
            )
    )
;