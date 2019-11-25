WITH HOMEBUILDER_ACCOUNTS AS (
    SELECT P.SERVICE_NAME
         , P.PROJECT_NAME
         , CT.FIRST_NAME                                                                      AS CUSTOMER_1_First
         , ''                                                                                 AS Customer_1_Middle
         ----------------------------------------------------------
         , REVERSE(REGEXP_SUBSTR(REVERSE(CT.LAST_NAME), '^(\.?rJ|\.?rS|III|VI)', 1, 1, 'ie')) AS CUSTOMER_1_SUFFIX
         , TRIM(REPLACE(CT.LAST_NAME, NVL(CUSTOMER_1_SUFFIX, '')))                            AS CUSTOMER_1_LAST
         ----------------------------------------------------------
         , CNT.FIRST_NAME                                                                     AS CUSTOMER_2_First
         , ''                                                                                 AS Customer_2_Middle
         ----------------------------------------------------------
         , REVERSE(REGEXP_SUBSTR(REVERSE(CNT.LAST_NAME), '^(\.?rJ|\.?rS|III|VI)', 1, 1,
                                 'ie'))                                                       AS CUSTOMER_2_SUFFIX_NAME
         , TRIM(REPLACE(CNT.LAST_NAME, NVL(CUSTOMER_2_SUFFIX_NAME, '')))                      AS CUSTOMER_2_LAST_NAME
         ----------------------------------------------------------
         , P.SERVICE_ADDRESS
         , P.SERVICE_CITY
         , P.SERVICE_COUNTY
         , P.SERVICE_STATE
         , P.SERVICE_ZIP_CODE
         , TO_DATE(P.INSTALLATION_COMPLETE)                                                   AS INSTALL_DATE
         , TO_DATE(CON.TRANSACTION_DATE)                                                      AS TRANSACTION_DATE
         , DATEADD('MM', 246, TO_DATE(INSTALL_DATE))                                          AS TERMINATION_DATE
         , CON.RECORD_TYPE                                                                    AS CONTRACT_TYPE
         , ROUND(DATEDIFF('D', '2013-11-02', TO_DATE(P.ESCROW)) / 7, 0)                       AS WEEK_BATCH
         , 'New Account'                                                                      AS BATCH_TYPE
    FROM RPT.T_PROJECT AS P
             LEFT JOIN
         RPT.T_CONTRACT AS CON
         ON P.PRIMARY_CONTRACT_ID = CON.CONTRACT_ID
             LEFT JOIN
         RPT.T_CONTACT AS CT
         ON CON.SIGNER_CONTACT_ID = CT.CONTACT_ID
             LEFT JOIN
         RPT.T_CONTACT AS CNT
         ON CON.COSIGNER_CONTACT_ID = CNT.CONTACT_ID
    WHERE P.INSTALLATION_COMPLETE IS NOT NULL
      AND P.INSTALLATION_COMPLETE >= '2013-11-02'
      AND CON.RECORD_TYPE NOT IN ('Solar Loan', 'Solar Cash')
      AND (P.SALES_OFFICE != 'Homebuilder Corporate' OR
           (P.SALES_OFFICE = 'Homebuilder Corporate' AND P.ESCROW IS NOT NULL))
)

   , TRANSFER_ACCOUNTS AS (
    SELECT S.SERVICE_NAME
         , P.PROJECT_NAME
         , CT.FIRST_NAME                                                                      AS CUSTOMER_1_First
         , ''                                                                                 AS Customer_1_Middle
         ----------------------------------------------------------
         , REVERSE(REGEXP_SUBSTR(REVERSE(CT.LAST_NAME), '^(\.?rJ|\.?rS|III|VI)', 1, 1, 'ie')) AS CUSTOMER_1_SUFFIX
         , TRIM(REPLACE(CT.LAST_NAME, NVL(CUSTOMER_1_SUFFIX, '')))                            AS CUSTOMER_1_LAST
         ----------------------------------------------------------
         , CTO.FIRST_NAME                                                                     AS CUSTOMER_2_First
         , ''                                                                                 AS Customer_2_Middle
         ----------------------------------------------------------
         , REVERSE(REGEXP_SUBSTR(REVERSE(CTO.LAST_NAME), '^(\.?rJ|\.?rS|III|VI)', 1, 1,
                                 'ie'))                                                       AS CUSTOMER_2_SUFFIX_NAME
         , TRIM(REPLACE(CTO.LAST_NAME, NVL(CUSTOMER_2_SUFFIX_NAME, '')))                      AS CUSTOMER_2_LAST_NAME
         ----------------------------------------------------------
         , S.SERVICE_ADDRESS
         , S.SERVICE_CITY
         , S.SERVICE_COUNTY
         , S.SERVICE_STATE
         , S.SERVICE_ZIP_CODE
         , TO_DATE(P.INSTALLATION_COMPLETE)                                                   AS INSTALL_DATE
         , TO_DATE(CN.TRANSACTION_DATE)                                                       AS TRANSACTION_DATE
         , DATEADD('MM', 246, TO_DATE(P.INSTALLATION_COMPLETE))                               AS TERMINATION_DATE
         , CN.RECORD_TYPE                                                                     AS CONTRACT_TYPE
         , ROUND(DATEDIFF('D', '2013-11-02', TO_DATE(C.CLOSED_DATE)) / 7, 0)                  AS WEEK_BATCH
         , 'Transfer Account'                                                                 AS BATCH_TYPE
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
         RPT.T_CONTRACT AS CN
         ON CN.CONTRACT_ID = P.PRIMARY_CONTRACT_ID
    WHERE C.RECORD_TYPE = 'Solar - Transfer'
      AND C.CREATED_DATE >= '2018-09-01'
      AND C.STATUS = 'Closed - Processed'
      AND S.SERVICE_STATUS != 'Solar - Transfer'
)

   , REFINANCE_ACCOUNTS AS (
    /*
     This table will need to be built with the case history table.
     Each record will contain previous, this, next (nvl now) datetime stamps on the "new value" type.
     The determining factor will be the date difference between this and next when the "new value" is
     a status of the value "Ready to Process". If it's difference is >= 60.
     */
    SELECT S.SERVICE_NAME
         , P.PROJECT_NAME
         , CT.FIRST_NAME                                                 AS CUSTOMER_1_First
         , ''                                                            AS Customer_1_Middle
         ----------------------------------------------------------
         , REVERSE(REGEXP_SUBSTR(REVERSE(CT.LAST_NAME), '^(\.?rJ|\.?rS|III|VI)', 1, 1,
                                 'ie'))                                  AS CUSTOMER_1_SUFFIX
         , TRIM(REPLACE(CT.LAST_NAME, NVL(CUSTOMER_1_SUFFIX, '')))       AS CUSTOMER_1_LAST
         ----------------------------------------------------------
         , CTO.FIRST_NAME                                                AS CUSTOMER_2_First
         , ''                                                            AS Customer_2_Middle
         ----------------------------------------------------------
         , REVERSE(REGEXP_SUBSTR(REVERSE(CTO.LAST_NAME), '^(\.?rJ|\.?rS|III|VI)', 1, 1,
                                 'ie'))                                  AS CUSTOMER_2_SUFFIX_NAME
         , TRIM(REPLACE(CTO.LAST_NAME, NVL(CUSTOMER_2_SUFFIX_NAME, ''))) AS CUSTOMER_2_LAST_NAME
         ----------------------------------------------------------
         , S.SERVICE_ADDRESS
         , S.SERVICE_CITY
         , S.SERVICE_COUNTY
         , S.SERVICE_STATE
         , S.SERVICE_ZIP_CODE
         , TO_DATE(P.INSTALLATION_COMPLETE)                              AS INSTALL_DATE
         , TO_DATE(CN.TRANSACTION_DATE)                                  AS TRANSACTION_DATE
         , DATEADD('MM', 246, TO_DATE(P.INSTALLATION_COMPLETE))          AS TERMINATION_DATE
         , CN.RECORD_TYPE                                                AS CONTRACT_TYPE
         , ROUND(DATEDIFF('D', DATEADD(dd, 60, '2013-11-02'),
                          TO_DATE(C.QUEUE_REMOVAL_DATE)) / 7, 0)         AS WEEK_BATCH
         , 'Refinance Account'                                           AS BATCH_TYPE
    FROM D_POST_INSTALL.V_FILINGS_REFINANCE_CASE_LIST AS C
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
         RPT.T_CONTRACT AS CN
         ON CN.CONTRACT_ID = P.PRIMARY_CONTRACT_ID
    WHERE C.GAP >= 60
)

   , MERGE AS (
    SELECT *
    FROM (
                 (SELECT * FROM HOMEBUILDER_ACCOUNTS)
                 UNION
                 (SELECT * FROM TRANSFER_ACCOUNTS)
                 UNION
                 (SELECT * FROM REFINANCE_ACCOUNTS)
         )
)

   , MAIN AS (
    SELECT *
    FROM MERGE
    WHERE WEEK_BATCH >= 252
)

SELECT *
FROM MAIN