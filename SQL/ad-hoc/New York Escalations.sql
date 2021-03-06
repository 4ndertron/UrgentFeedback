SELECT C.CASE_NUMBER
     , P.ROC_NAME
     , P.SERVICE_NAME
     , CT.FULL_NAME AS CONTACT_NAME
     , C.SUBJECT
     , C.STATUS
     , C.ORIGIN
     , C.OWNER
     , C.CREATED_DATE
     , C.CLOSED_DATE
FROM RPT.T_CASE AS C
         LEFT JOIN
     RPT.T_PROJECT AS P
     ON P.PROJECT_ID = C.PROJECT_ID

         LEFT JOIN
     RPT.T_CONTACT AS CT
     ON CT.ACCOUNT_ID = C.ACCOUNT_ID
WHERE C.SUBJECT ILIKE '%[LEGAL]%'
  AND C.RECORD_TYPE = 'Solar - Customer Escalation'
  AND C.CLOSED_DATE IS NULL
  AND P.SERVICE_STATE = 'NY'