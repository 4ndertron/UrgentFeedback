WITH Filings AS--
    (SELECT a.PROJECT_ID--
          , Max(a.CREATED_DATE) AS Created_Date
--           , ANY_VALUE(DOWNLOAD) AS DOWNLOAD_LINK
     FROM RPT.T_ATTACHMENT a
     WHERE a.DOCUMENT_TYPE LIKE '%County%'
     GROUP BY a.PROJECT_ID)
   --
   --
   , BAD_CASES AS --
    (SELECT C.PROJECT_ID--
          , Max(C.CREATED_DATE) AS Created_Date
     FROM RPT.T_CASE c
     WHERE C.RECORD_TYPE = 'Solar - Transfer'
     GROUP BY C.PROJECT_ID)

   , CASE_BOOL_RAW AS (
    SELECT C.PROJECT_ID
         , C.RECORD_TYPE
         , C.CREATED_DATE
         , ROW_NUMBER() OVER (PARTITION BY C.PROJECT_ID, C.RECORD_TYPE ORDER BY C.CREATED_DATE DESC) AS RN
         , CASE
               WHEN C.RECORD_TYPE = 'Solar - Customer Escalation' AND
                    C.CLOSED_DATE IS NOT NULL AND
                    (C.ORIGIN NOT IN ('NPS', 'Marketing') OR UPPER(C.SUBJECT) NOT LIKE '%NPS%')
                   THEN TRUE END                                                                     AS ESCALATION_BOOL
         , CASE
               WHEN C.RECORD_TYPE = 'Solar - Customer Default' AND
                    C.CLOSED_DATE IS NOT NULL AND
                    C.SUBJECT NOT ILIKE '%D3%'
                   THEN TRUE END                                                                     AS DEFAULT_BOOL
    FROM RPT.T_CASE AS C
    WHERE C.RECORD_TYPE IN
          ('Solar - Customer Escalation', 'Solar - Customer Default')
)

   , CASE_BOOL AS (
    SELECT *
    FROM CASE_BOOL_RAW
    WHERE RN = 1
)
   --
   --
   , CONTACT AS--
    (SELECT C.FULL_NAME--
          , C.FIRST_NAME--
          , C.LAST_NAME--
          , C.CONTACT_ID --
     FROM RPT.T_CONTACT c --
     GROUP BY C.CONTACT_ID, C.FULL_NAME, C.FIRST_NAME, C.LAST_NAME)
   --
   --
   , CONTRACT AS--
    (SELECT S.OPTY_CONTRACT_TYPE--
          , S.PRIMARY_CONTRACT_ID
     FROM RPT.T_SERVICE S
     WHERE S.OPTY_CONTRACT_TYPE NOT IN ('Cash', 'Loan')
     GROUP BY S.PRIMARY_CONTRACT_ID, S.OPTY_CONTRACT_TYPE)
   --
   --
   , TRANSACTION_DATE AS --
    (SELECT MAX(T.TRANSACTION_DATE) AS TRANSACTION_DATE--
          , T.PROJECT_ID
     FROM RPT.T_CONTRACT t
     GROUP BY T.PROJECT_ID)
   --
   --
   , M1 AS--
    (SELECT P.PROJECT_ID
--           , T1.DOWNLOAD_LINK
          , T1.CREATED_DATE                                AS FILING_CREATED_DATE
          , P.SERVICE_NUMBER                               AS SERVICE
          , regexp_replace(P.PROJECT_NUMBER, '[^0-9]', '') AS PROJECT
          , P.SERVICE_ID
          , P.CONTRACT_SIGNER                              AS CUSTOMER_1
          , P.CONTRACT_COSIGNER                            AS CUSTOMER_2
          , P.SERVICE_ADDRESS                              AS ADDRESS
          , P.SERVICE_CITY                                 AS CITY
          , P.SERVICE_COUNTY                               AS COUNTY
          , P.SERVICE_STATE                                AS STATE
          , P.SERVICE_ZIP_CODE                             AS ZIP_CODE
          , P.INSTALLATION_COMPLETE                        AS INSTALL_COMPLETE
          , P.ELECTRICAL_FINISH_DATE
          , P.PRIMARY_CONTRACT_ID
          , ROUND(NVL(LD.TOTAL_CURRENT_AMOUNT_DUE, 0), 2)  AS PAST_DUE
          , CB.DEFAULT_BOOL
          , CB.ESCALATION_BOOL
          , CASE
                WHEN T1.Created_Date < '2015-09-01'
                    THEN 1
                WHEN T1.PROJECT_ID IS NULL
                    THEN 1
            END                                            AS Expired_Filing
     FROM RPT.T_PROJECT p
              LEFT JOIN RPT.V_TRANSFER_SUMMARY T
                        ON T.PROJECT_ID = P.PROJECT_ID
              LEFT JOIN Filings T1
                        ON T1.PROJECT_ID = p.PROJECT_ID
              LEFT JOIN
          LD.T_DAILY_DATA_EXTRACT AS LD
          ON LD.BILLING_ACCOUNT = P.SOLAR_BILLING_ACCOUNT_NUMBER
              LEFT JOIN
          CASE_BOOL AS CB
          ON CB.PROJECT_ID = P.PROJECT_ID
     WHERE p.PROJECT_STATUS != 'Cancelled'
       AND P.INSTALLATION_COMPLETE IS NOT NULL
       AND P.INSTALLATION_COMPLETE < '2015-10-01'
       AND T.PROJECT_ID IS NULL)
   --
   --
   , M2 AS
    (SELECT M1.Service
          , M1.Project
          , M1.PAST_DUE
          , M1.DEFAULT_BOOL
          , M1.ESCALATION_BOOL
          , C.FULL_NAME                             AS CONTRACT_SIGNER
          , CO.FULL_NAME                            AS CONTRACT_COSIGNER
          , M1.ADDRESS
          , M1.CITY
          , M1.COUNTY
          , M1.STATE
          , M1.ZIP_CODE
          , DATE_TRUNC('d', M1.INSTALL_COMPLETE)    AS INSTALL_COMPLETE
          , DATE_TRUNC('d', T.TRANSACTION_DATE)     AS TRANSACTION_DATE
          , S.OPTY_CONTRACT_TYPE
          , DATE_TRUNC('d', M1.FILING_CREATED_DATE) AS Filing_Date
          , CASE
                WHEN M1.INSTALL_COMPLETE > '2015-09-31' AND
                     TRANSACTION_DATE IS NULL
                    THEN 1
            END                                     AS EXCLUDE_INSTALLS
          , CURRENT_DATE                            AS LAST_RUN_DATE
     FROM M1
              LEFT JOIN CONTACT c
                        ON C.CONTACT_ID = M1.CUSTOMER_1
              LEFT JOIN CONTACT CO
                        ON CO.CONTACT_ID = M1.CUSTOMER_2
              LEFT JOIN CONTRACT s
                        ON s.PRIMARY_CONTRACT_ID = M1.PRIMARY_CONTRACT_ID
              LEFT JOIN TRANSACTION_DATE t
                        ON t.PROJECT_ID = M1.PROJECT_ID
              LEFT JOIN BAD_CASES R
                        ON R.PROJECT_ID = M1.PROJECT_ID
     WHERE R.PROJECT_ID IS NULL
       AND M1.Expired_Filing IS NOT NULL
       AND (TRANSACTION_DATE < '2015-10-01' OR
            TRANSACTION_DATE IS NULL)
       AND EXCLUDE_INSTALLS IS NULL)

   , M3 AS (
    SELECT M.SERVICE
         , M.PROJECT
         , M.PAST_DUE
         , M.ESCALATION_BOOL
         , M.DEFAULT_BOOL
         , M.CONTRACT_SIGNER
         , M.CONTRACT_COSIGNER
         , M.ADDRESS
         , M.CITY
         , M.COUNTY
         , M.STATE
         , M.ZIP_CODE
         , TO_DATE(M.TRANSACTION_DATE)                     AS TRANSACTION_DATE
         , DATEADD('mm', 246, TO_DATE(M.INSTALL_COMPLETE)) AS TERMINATION_DATE
         , TO_DATE(M.INSTALL_COMPLETE)                     AS INSTALL_DATE
         , M.OPTY_CONTRACT_TYPE
         , TO_DATE(M.Filing_Date)                          AS FILING_DATE
         , M.LAST_RUN_DATE
    FROM M2 M
    WHERE M.STATE != 'CA'
    ORDER BY M.PAST_DUE DESC
           , DEFAULT_BOOL
           , ESCALATION_BOOL
)

   , FINAL AS (
    SELECT SERVICE
         , PROJECT
         , CONTRACT_SIGNER
         , CONTRACT_COSIGNER
         , ADDRESS
         , CITY
         , COUNTY
         , STATE
         , ZIP_CODE
         , TRANSACTION_DATE
         , TERMINATION_DATE
         , INSTALL_DATE
         , OPTY_CONTRACT_TYPE AS CONTRACT_TYPE
    FROM M3
)

SELECT *
FROM FINAL