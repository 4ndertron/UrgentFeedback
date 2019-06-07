WITH ALL_INSTALLS AS (
    SELECT P.PROJECT_NAME
         , P.SOLAR_BILLING_ACCOUNT_NUMBER
         , CT.FULL_NAME                                            AS SIGNER_NAME
         , CNT.FULL_NAME                                           AS COSIGNER_NAME
         , P.SERVICE_ADDRESS
         , P.SERVICE_CITY
         , P.SERVICE_COUNTY
         , P.SERVICE_STATE
         , P.SERVICE_ZIP_CODE
         , TO_DATE(P.INSTALLATION_COMPLETE)                        AS INSTALL_DATE
         , TO_DATE(CON.TRANSACTION_DATE)                           AS TRANSACTION_DATE
         , DATEADD('MM', 246, TO_DATE(INSTALL_DATE))               AS TERMINATION_DATE
         , CON.RECORD_TYPE                                         AS CONTRACT_TYPE
         , ROUND(DATEDIFF('D', '2013-11-02', INSTALL_DATE) / 7, 0) AS WEEK_BATCH
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
)

   , TIMEFRAME_CALCULATION AS (
    SELECT D.DT
         , D.WEEK_DAY_NUM
         , COUNT(CASE
                     WHEN TO_DATE(ALL_INSTALLS.INSTALL_DATE) = D.DT
                         THEN 1 END)               AS DAILY_INSTALLS
         , COUNT(CASE
                     WHEN TO_DATE(ALL_INSTALLS.INSTALL_DATE) = D.DT
                         AND ALL_INSTALLS.SERVICE_STATE = 'CA'
                         THEN 1 END)               AS PUC_INSTALLS
         , COUNT(CASE
                     WHEN TO_DATE(ALL_INSTALLS.INSTALL_DATE) = D.DT
                         AND ALL_INSTALLS.SERVICE_STATE != 'CA'
                         THEN 1 END)               AS UCC_INSTALLS
         , DATEADD('D', -6, D.DT) || ' - ' || D.DT AS TIMEFRAME
         , SUM(DAILY_INSTALLS) OVER (ORDER BY DT ASC
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)  AS INSTALLS_DURING_TIMEFRAME
         , SUM(PUC_INSTALLS) OVER (ORDER BY DT ASC
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)  AS PUC_INSTALLS_DURING_TIMEFRAME
         , SUM(UCC_INSTALLS) OVER (ORDER BY DT ASC
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)  AS UCC_INSTALLS_DURING_TIMEFRAME
    FROM ALL_INSTALLS,
         RPT.T_DATES AS D
    WHERE D.DT BETWEEN '2013-11-07' AND CURRENT_DATE()
    GROUP BY D.DT
           , D.WEEK_DAY_NUM
    ORDER BY D.DT
)

   , BATCHES AS (
    SELECT DT
         , TIMEFRAME
         , INSTALLS_DURING_TIMEFRAME
         , PUC_INSTALLS_DURING_TIMEFRAME
         , UCC_INSTALLS_DURING_TIMEFRAME
         , ROW_NUMBER() OVER (ORDER BY DT) AS BATCH_NUMBER
    FROM TIMEFRAME_CALCULATION
    WHERE WEEK_DAY_NUM = 3
)

   , FINAL AS (
    SELECT BATCH_NUMBER
         , TIMEFRAME
         , INSTALLS_DURING_TIMEFRAME
         , PUC_INSTALLS_DURING_TIMEFRAME
         , UCC_INSTALLS_DURING_TIMEFRAME
    FROM BATCHES
    WHERE DT >= DATE_TRUNC('Y', CURRENT_DATE())
)

SELECT *
-- FROM FINAL

FROM ALL_INSTALLS
WHERE INSTALL_DATE >= '2018-12-26'
ORDER BY INSTALL_DATE DESC