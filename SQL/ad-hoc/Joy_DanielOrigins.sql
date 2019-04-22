WITH ACCOUNT_LIST AS (
    SELECT ANY_VALUE(O.ACCOUNT_ID)            AS ACCOUNT_ID
         , ANY_VALUE(O.SALES_REP_NAME)        AS SALES_REP_NAME
         , ANY_VALUE(O.SALES_CLOSER_NAME)     AS SALES_CLOSER_NAME
         , P.SERVICE_NAME
         , ANY_VALUE(P.PROJECT_ID)            AS PROJECT_ID
         , ANY_VALUE(P.SERVICE_NUMBER)        AS SERVICE_NUMBER
         , ANY_VALUE(S.SERVICE_STATUS)        AS SERVICE_STATUS
         , ANY_VALUE(P.INSTALLATION_COMPLETE) AS INSTALL_DATE
         , ANY_VALUE(P.PTO_AWARDED)           AS PTO_AWARDED
         , ANY_VALUE(SD.SYSTEM_SIZE)          AS SYSTEM_SIZE

    FROM RPT.T_OPPORTUNITY AS O
             LEFT JOIN
         RPT.T_PROJECT AS P
         ON P.ACCOUNT_ID = O.ACCOUNT_ID

             LEFT JOIN
         RPT.T_SERVICE AS S
         ON S.PROJECT_ID = P.PROJECT_ID

             LEFT JOIN
         RPT.T_SYSTEM_DETAILS_SNAP AS SD
         ON SD.PROJECT_ID = P.PROJECT_ID
    WHERE (
                  O.SALES_REP_EMP_ID = '201830'
                  OR
                  O.SALES_REP_EMP_ID = '97674'
              )
    GROUP BY P.SERVICE_NAME
),

     CASE_TABLE AS (
         SELECT C.PROJECT_ID
              , C.STATUS
              , C.RECORD_TYPE
              , C.CASE_NUMBER
              , C.ORIGIN
         FROM RPT.T_CASE AS C
         WHERE
--		C.STATUS NOT LIKE '%Close%'
C.RECORD_TYPE IN ('Solar - Customer Escalation', 'Solar - Customer Default')
     ),

--SELECT * FROM ACCOUNT_LIST
     T1 AS (
         SELECT A.ACCOUNT_ID
              , A.SALES_REP_NAME
              , A.SALES_CLOSER_NAME
              , A.SERVICE_NAME
              , A.PROJECT_ID
              , A.SERVICE_NUMBER
              , A.SERVICE_STATUS
              , A.INSTALL_DATE
              , A.PTO_AWARDED
              , A.SYSTEM_SIZE
              , C.ORIGIN
              , C.RECORD_TYPE
         FROM ACCOUNT_LIST AS A
                  LEFT JOIN
              CASE_TABLE AS C
              ON C.PROJECT_ID = A.PROJECT_ID
     )
        ,

     T2 AS (

/*
 * Required Fields
 * ---------------
 * X Total Accounts
 * X Total Installed
 * X Total PTO
 * X Total Cancelled Pre-PTO
 * X Total Cancelled Post-PTO
 * X Total Cost of Post-install Cancellations ($4.00 per watt installed)
 * X Total Default
 * X Total BBB
 * X Total Complaints
 * O Breakdown of Complaint Origin
 */

         SELECT SALES_REP_NAME
              , COUNT(SERVICE_NUMBER)                                                                 AS TOTAL_ACCOUNTS
              , COUNT(INSTALL_DATE)                                                                   AS TOTAL_INSTALL
              , COUNT(PTO_AWARDED)                                                                    AS TOTAL_PTO
              , COUNT(
                 CASE WHEN SERVICE_STATUS = 'Cancelled' AND INSTALL_DATE IS NULL THEN 1 END)          AS TOTAL_CANCELLED_PRE_INSTALL
              , COUNT(CASE
                          WHEN SERVICE_STATUS = 'Cancelled' AND INSTALL_DATE IS NOT NULL
                              THEN 1 END)                                                             AS TOTAL_CANCELLED_POST_INSTALL
              , ROUND(SUM(CASE
                              WHEN SERVICE_STATUS = 'Cancelled' AND INSTALL_DATE IS NOT NULL
                                  THEN (SYSTEM_SIZE * 1000) * 4 END),
                      2)                                                                              AS COST_OF_INSTALL_CANCELLATION
         FROM T1
         GROUP BY SALES_REP_NAME
         ORDER BY SALES_REP_NAME
     )

SELECT SALES_REP_NAME
     , ORIGIN
     , RECORD_TYPE
     , COUNT(*) AS TOTAL
FROM T1
WHERE ORIGIN IS NOT NULL
GROUP BY SALES_REP_NAME
       , ORIGIN
       , RECORD_TYPE
ORDER BY SALES_REP_NAME