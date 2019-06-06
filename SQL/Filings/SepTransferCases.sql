WITH T1 AS (
    SELECT P.PROJECT_NUMBER
         , S.SERVICE_NAME
         , S.SOLAR_BILLING_ACCOUNT_NUMBER
         , CT.FULL_NAME                                         AS SIGNER_FULL_NAME
         , CTO.FULL_NAME                                        AS C0_SIGNER_FULL_NAME
         , S.SERVICE_ADDRESS
         , S.SERVICE_CITY
         , S.SERVICE_COUNTY
         , S.SERVICE_STATE
         , S.SERVICE_ZIP_CODE
         , CASE
               WHEN C.RECORD_TYPE = 'Solar - Customer Default' AND
                    C.CLOSED_DATE IS NOT NULL
                   THEN TRUE END                                AS DEFAULT_BOOL
         , CASE
               WHEN C.RECORD_TYPE = 'Solar - Customer Escalation' AND
                    C.CLOSED_DATE IS NOT NULL
                   THEN TRUE END                                AS ER_BOOL
         , CASE
               WHEN C.RECORD_TYPE = 'Solar - Troubleshooting' AND
                    C.CLOSED_DATE IS NOT NULL
                   THEN TRUE END                                AS TS_BOOL
         , TO_DATE(C.HOME_CLOSING_DATE)                         AS TRANSACTION_DATE
         , DATEADD('MM', 246, TO_DATE(P.INSTALLATION_COMPLETE)) AS TERMINATION_DATE
         , LDD.TOTAL_CURRENT_AMOUNT_DUE
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
             LEFT JOIN
         LD.T_DAILY_DATA_EXTRACT AS LDD
         ON LDD.BILLING_ACCOUNT = P.SOLAR_BILLING_ACCOUNT_NUMBER
    WHERE C.RECORD_TYPE = 'Solar - Transfer'
      AND C.CREATED_DATE >= '2018-09-01'
      AND C.CLOSED_DATE < '2019-05-15'
--       AND C.CLOSED_DATE BETWEEN DATEADD('D', -7, CURRENT_DATE) AND CURRENT_DATE -- 291
--       AND C.CLOSED_DATE BETWEEN '2019-05-15' AND '2019-05-21' -- 289
--       AND C.CLOSED_DATE BETWEEN '2019-05-22' AND '2019-05-28' -- 290
      AND C.STATUS = 'Closed - Processed'
      AND S.SERVICE_STATUS != 'Solar - Transfer'
    ORDER BY TOTAL_CURRENT_AMOUNT_DUE DESC
)

SELECT *
FROM T1