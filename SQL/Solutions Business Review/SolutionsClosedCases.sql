SELECT C.CASE_NUMBER
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
WHERE C.STATUS IN ('Closed', 'Closed - Saved')
  AND C.RECORD_TYPE IN ('Solar - Customer Default')
  AND H.SUPERVISOR_NAME_1 = 'Brittany Percival'
  AND H.TERMINATED = 'False'
  AND C.CLOSED_DATE >= DATEADD('D', -30, CURRENT_DATE())
