WITH T1 AS (
    SELECT P.PROJECT_NUMBER
         , S.SERVICE_NAME
         , S.SOLAR_BILLING_ACCOUNT_NUMBER
         , CT.FULL_NAME  AS SIGNER_FULL_NAME
         , CTO.FULL_NAME AS C0_SIGNER_FULL_NAME
         , S.SERVICE_ADDRESS
         , S.SERVICE_CITY
         , S.SERVICE_COUNTY
         , S.SERVICE_STATE
         , S.SERVICE_ZIP_CODE
    FROM RPT.T_CASE AS C
             LEFT JOIN
         RPT.T_PROJECT AS P
         ON P.PROJECT_ID = C.PROJECT_ID
             LEFT JOIN
         RPT.T_SERVICE AS S
         ON S.PROJECT_ID = P.PROJECT_ID
             LEFT JOIN
         RPT.T_CONTACT AS CT
         ON CT.CONTACT_ID = S.CONTRACT_SIGNER
             LEFT JOIN
         RPT.T_CONTACT AS CTO
         ON CTO.CONTACT_ID = S.CONTRACT_CO_SIGNER
    WHERE C.RECORD_TYPE = 'Solar - Transfer'
      AND C.CREATED_DATE >= '2018-09-01'
      AND C.STATUS = 'Closed - Processed'
      AND S.SERVICE_STATUS != 'Solar - Transfer'
    ORDER BY C.CASE_NUMBER
)

SELECT COUNT(*)
FROM T1