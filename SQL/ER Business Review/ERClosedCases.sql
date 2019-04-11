SELECT C.CASE_NUMBER
     , P.SOLAR_BILLING_ACCOUNT_NUMBER
     , C.CLOSED_DATE
     , C.OWNER
     , C.OWNER_EMPLOYEE_ID
     , C.RECORD_TYPE
     , C.STATUS
     , C.SUBJECT
     , H.SUPERVISOR_NAME_1
FROM RPT.T_CASE AS C
         LEFT JOIN
     HR.T_EMPLOYEE AS H
     ON
         H.EMPLOYEE_ID = C.OWNER_EMPLOYEE_ID

         LEFT OUTER JOIN
     RPT.T_PROJECT AS P
     ON P.PROJECT_ID = C.PROJECT_ID
WHERE C.STATUS IN ('Closed', 'Closed - Saved')
  AND C.RECORD_TYPE IN ('Solar - Customer Default', 'Solar - Customer Escalation')
  AND H.SUPERVISOR_NAME_1 = 'Jacob Azevedo'
  AND H.TERMINATED = 'False'
  AND C.CLOSED_DATE >= DATEADD('d', -30, CURRENT_DATE())