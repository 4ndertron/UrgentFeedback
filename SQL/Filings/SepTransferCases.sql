WITH T1 AS (
    SELECT P.PROJECT_NAME
         , S.SERVICE_NAME
         , S.SOLAR_BILLING_ACCOUNT_NUMBER
         , ROUND(DATEDIFF('D', '2013-11-02', TO_DATE(C.CLOSED_DATE)) / 7, 0)                   AS WEEK_BATCH
         , 'Transfer Accounts'                                                                 AS BATCH_TYPE
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
         , NVL(S.SERVICE_COUNTY, SD.AVALARA_SHIPPING_COUNTY)                                   AS SERVICE_COUNTY -- 157
--          , S.SERVICE_COUNTY -- 158
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
         , TO_DATE(P.INSTALLATION_COMPLETE)                                                    AS INSTALL_DATE
         , CN.RECORD_TYPE                                                                      AS CONTRACT_TYPE
         , LDD.TOTAL_CURRENT_AMOUNT_DUE
         , C.CLOSED_DATE
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
             LEFT JOIN
         RPT.V_SF_ORDER AS SD
         ON SD.CONTRACT_ID = P.PRIMARY_CONTRACT_ID
    WHERE C.RECORD_TYPE = 'Solar - Transfer'
      AND C.CREATED_DATE >= '2018-09-01'
--       AND C.CLOSED_DATE < '2019-05-15' -- < 289
--       AND C.CLOSED_DATE BETWEEN '2019-05-15' AND '2019-05-22' -- 289
--       AND C.CLOSED_DATE BETWEEN '2019-05-22' AND '2019-05-29' -- 290
--       AND C.CLOSED_DATE BETWEEN '2019-05-29' AND '2019-06-06' -- 291
--       AND C.CLOSED_DATE BETWEEN '2019-06-26' AND '2019-07-03' -- 295
--       AND C.CLOSED_DATE BETWEEN '2019-08-07' AND '2019-08-14' -- 301
--       AND C.CLOSED_DATE BETWEEN '2019-09-11' AND '2019-09-18' -- 306
--       AND C.CLOSED_DATE BETWEEN '2019-09-18' AND '2019-09-25' -- 307
--       AND C.CLOSED_DATE BETWEEN '2019-09-24' AND '2019-10-02' -- 308
--       AND C.CLOSED_DATE BETWEEN DATEADD('D', -7, CURRENT_DATE) AND CURRENT_DATE -- CURRENT
      AND C.STATUS = 'Closed - Processed'
      AND S.SERVICE_STATUS != 'Solar - Transfer'
    ORDER BY TOTAL_CURRENT_AMOUNT_DUE DESC
           , DEFAULT_BOOL
           , ESCALATION_BOOL
)

   , ION AS (
    SELECT DATE_TRUNC('MM', D.DT)                                  AS MONTH1
         , COUNT(CASE WHEN TO_DATE(CLOSED_DATE) = D.DT THEN 1 END) AS ACCOUNT_TOTAL
    FROM RPT.T_DATES AS D,
         T1
    WHERE D.DT BETWEEN
              DATE_TRUNC('Y', DATEADD('Y', -1, CURRENT_DATE)) AND
              LAST_DAY(DATEADD('MM', -1, CURRENT_DATE))
    GROUP BY MONTH1
    ORDER BY MONTH1
)

   , WB_LIST AS (
    SELECT SERVICE_NAME
         , PROJECT_NAME
         , CUSTOMER_1_First
         , Customer_1_Middle
         , CUSTOMER_1_SUFFIX
         , CUSTOMER_1_LAST
         , CUSTOMER_2_First
         , Customer_2_Middle
         , CUSTOMER_2_SUFFIX_NAME
         , CUSTOMER_2_LAST_NAME
         , SERVICE_ADDRESS
         , SERVICE_CITY
         , SERVICE_COUNTY
         , SERVICE_STATE
         , SERVICE_ZIP_CODE
         , INSTALL_DATE
         , TRANSACTION_DATE
         , TERMINATION_DATE
         , CONTRACT_TYPE
         , WEEK_BATCH
         , BATCH_TYPE
    FROM T1
    WHERE SERVICE_COUNTY IS NULL
)

SELECT *
FROM WB_LIST