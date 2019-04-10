WITH Filings AS--
    (SELECT a.PROJECT_ID--
          , Max(a.CREATED_DATE) AS Created_Date
          , ANY_VALUE(DOWNLOAD) AS DOWNLOAD_LINK
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
          , T1.DOWNLOAD_LINK
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
     WHERE p.PROJECT_STATUS != 'Cancelled'
       AND P.INSTALLATION_COMPLETE IS NOT NULL
       AND P.INSTALLATION_COMPLETE < '2015-10-01'
       AND T.PROJECT_ID IS NULL)
   --
   --
   , M2 AS
    (SELECT M1.Service
          , M1.Project
          , M1.DOWNLOAD_LINK
          , C.FIRST_NAME                                                                       AS CUSTOMER_1_First
          , ''                                                                                 AS Customer_1_Middle
          ----------------------------------------------------------
          , REVERSE(REGEXP_SUBSTR(REVERSE(C.LAST_NAME), '^(\.?rJ|\.?rS|III|VI)', 1, 1, 'ie'))  AS CUSTOMER_1_SUFFIX
          , TRIM(REPLACE(C.LAST_NAME, NVL(CUSTOMER_1_SUFFIX, '')))                             AS CUSTOMER_1_LAST
          ----------------------------------------------------------
          , CO.FIRST_NAME                                                                      AS CUSTOMER_2_First
          , ''                                                                                 AS Customer_2_Middle
          ----------------------------------------------------------
          , REVERSE(REGEXP_SUBSTR(REVERSE(CO.LAST_NAME), '^(\.?rJ|\.?rS|III|VI)', 1, 1, 'ie')) AS CUSTOMER_2_SUFFIX_NAME
          , TRIM(REPLACE(CO.LAST_NAME, NVL(CUSTOMER_2_SUFFIX_NAME, '')))                       AS CUSTOMER_2_LAST_NAME
          ----------------------------------------------------------
          , M1.ADDRESS
          , M1.CITY
          , M1.COUNTY
          , M1.STATE
          , M1.ZIP_CODE
          , DATE_TRUNC('d', M1.INSTALL_COMPLETE)                                               AS INSTALL_COMPLETE
          , DATE_TRUNC('d', T.TRANSACTION_DATE)                                                AS TRANSACTION_DATE
          , S.OPTY_CONTRACT_TYPE
          , DATE_TRUNC('d', M1.FILING_CREATED_DATE)                                            AS Filing_Date
          , CASE
                WHEN M1.INSTALL_COMPLETE > '2015-09-31' AND
                     TRANSACTION_DATE IS NULL
                    THEN 1
            END                                                                                AS EXCLUDE_INSTALLS
          , CURRENT_DATE                                                                       AS LAST_RUN_DATE
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

SELECT M.SERVICE
     , M.PROJECT
     , M.DOWNLOAD_LINK
     , M.CUSTOMER_1_FIRST
     , M.Customer_1_MIDDLE
     , M.CUSTOMER_1_LAST
     , M.CUSTOMER_1_SUFFIX
     , M.CUSTOMER_2_FIRST
     , M.Customer_2_MIDDLE
     , M.CUSTOMER_2_LAST_NAME                 AS CUSTOMER_2_LAST
     , M.CUSTOMER_2_SUFFIX_NAME               AS CUSTOMER_2_SUFFIX
     , M.ADDRESS
     , M.CITY
     , M.COUNTY
     , M.STATE
     , M.ZIP_CODE
     , M.TRANSACTION_DATE
     , DATEADD('mm', 246, M.INSTALL_COMPLETE) AS TERMINATION_DATE
     , M.INSTALL_COMPLETE                     AS INSTALL_DATE
     , M.OPTY_CONTRACT_TYPE
     , M.Filing_Date
     , M.LAST_RUN_DATE
FROM M2 M
WHERE M.STATE != 'CA'
ORDER BY M.Filing_Date
;
--
--
--Automate on a weekly basis on Monday's