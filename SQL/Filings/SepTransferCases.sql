WITH T1 AS (
    SELECT P.PROJECT_NUMBER
         , S.SERVICE_NAME
         , S.SOLAR_BILLING_ACCOUNT_NUMBER
         , CT.FIRST_NAME                                                                       AS CUSTOMER_1_First
         , ''                                                                                  AS Customer_1_Middle
         ----------------------------------------------------------
         , REVERSE(REGEXP_SUBSTR(REVERSE(CT.LAST_NAME), '^(\.?rJ|\.?rS|III|VI)', 1, 1, 'ie'))  AS CUSTOMER_1_SUFFIX
         , TRIM(REPLACE(CT.LAST_NAME, NVL(CUSTOMER_1_SUFFIX, '')))                             AS CUSTOMER_1_LAST
         ----------------------------------------------------------
         , CTO.FIRST_NAME                                                                      AS CUSTOMER_2_First
         , ''                                                                                  AS Customer_2_Middle
         ----------------------------------------------------------
         , REVERSE(REGEXP_SUBSTR(REVERSE(CTO.LAST_NAME), '^(\.?rJ|\.?rS|III|VI)', 1, 1, 'ie')) AS CUSTOMER_2_SUFFIX_NAME
         , TRIM(REPLACE(CTO.LAST_NAME, NVL(CUSTOMER_2_SUFFIX_NAME, '')))                       AS CUSTOMER_2_LAST_NAME
         ----------------------------------------------------------
         , S.SERVICE_ADDRESS
         , S.SERVICE_CITY
         , S.SERVICE_COUNTY
         , S.SERVICE_STATE
         , S.SERVICE_ZIP_CODE
         , CASE
               WHEN C.RECORD_TYPE = 'Solar - Customer Default' AND
                    C.CLOSED_DATE IS NOT NULL
                   THEN TRUE END                                                               AS DEFAULT_BOOL
         , CASE
               WHEN C.RECORD_TYPE = 'Solar - Customer Escalation' AND
                    C.CLOSED_DATE IS NOT NULL
                   THEN TRUE END                                                               AS ESCALATION_BOOL
         , CASE
               WHEN C.RECORD_TYPE = 'Solar - Troubleshooting' AND
                    C.CLOSED_DATE IS NOT NULL
                   THEN TRUE END                                                               AS TS_BOOL
         , TO_DATE(CN.TRANSACTION_DATE)                                                        AS TRANSACTION_DATE
         , DATEADD('MM', 246, TO_DATE(P.INSTALLATION_COMPLETE))                                AS TERMINATION_DATE
         , TO_DATE(P.INSTALLATION_COMPLETE)                                                    AS INSTALLATION_DATE
         , CN.RECORD_TYPE                                                                      AS CONTRACT_TYPE
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
             LEFT JOIN
         RPT.T_CONTRACT AS CN
         ON CN.CONTRACT_ID = P.PRIMARY_CONTRACT_ID
    WHERE C.RECORD_TYPE = 'Solar - Transfer'
      AND C.CREATED_DATE >= '2018-09-01'
--       AND C.CLOSED_DATE < '2019-05-15' -- < 289
--       AND C.CLOSED_DATE BETWEEN '2019-05-15' AND '2019-05-21' -- 289
--       AND C.CLOSED_DATE BETWEEN '2019-05-22' AND '2019-05-28' -- 290
--       AND C.CLOSED_DATE BETWEEN '2019-05-29' AND '2019-06-05' -- 291
--       AND C.CLOSED_DATE BETWEEN '2019-06-26' AND '2019-07-02' -- 295
      AND C.CLOSED_DATE BETWEEN DATEADD('D', -7, CURRENT_DATE) AND CURRENT_DATE -- CURRENT
      AND C.STATUS = 'Closed - Processed'
      AND S.SERVICE_STATUS != 'Solar - Transfer'
    ORDER BY TOTAL_CURRENT_AMOUNT_DUE DESC
           , DEFAULT_BOOL
           , ESCALATION_BOOL
)

SELECT SERVICE_NAME
     , PROJECT_NUMBER
     , CUSTOMER_1_First
     , Customer_1_Middle
     , CUSTOMER_1_LAST
     , CUSTOMER_1_SUFFIX
     , CUSTOMER_2_First
     , Customer_2_Middle
     , CUSTOMER_2_LAST_NAME
     , CUSTOMER_2_SUFFIX_NAME
     , SERVICE_ADDRESS
     , SERVICE_CITY
     , SERVICE_COUNTY
     , SERVICE_STATE
     , SERVICE_ZIP_CODE
     , TRANSACTION_DATE
     , TERMINATION_DATE
     , INSTALLATION_DATE
     , CONTRACT_TYPE
FROM T1