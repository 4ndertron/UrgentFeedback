WITH T1 AS (
    SELECT C.CASE_NUMBER
         , C.OWNER
         , C.SUBJECT
         , C.STATUS
         , C.CREATED_DATE
         , C.CLOSED_DATE
         , C.CONTACT_ID
         , C.EXECUTIVE_RESOLUTIONS_ACCEPTED
         , Q1.TRANSFER_CASE_NUMBER
         , Q1.HOME_CLOSING_DATE
         , C.PROJECT_ID
    FROM RPT.T_CASE AS C
             LEFT OUTER JOIN
         (
             SELECT C1.CASE_NUMBER       AS TRANSFER_CASE_NUMBER
                  , C1.HOME_CLOSING_DATE AS HOME_CLOSING_DATE
                  , C1.PROJECT_ID        AS PID1
             FROM RPT.T_CASE AS C1
             WHERE C1.RECORD_TYPE = 'Solar - Transfer'
         ) AS Q1
         ON Q1.PID1 = C.PROJECT_ID
    WHERE C.RECORD_TYPE = 'Solar - Customer Escalation'
      AND C.EXECUTIVE_RESOLUTIONS_ACCEPTED <= C.CREATED_DATE
      AND C.CREATED_DATE >= '2019-01-23'
      AND SUBJECT LIKE '%[INT]%'
      AND STATUS != 'In Dispute'
),

     T2 AS (
-- Import project information
         SELECT T1.*
              , P.SERVICE_NUMBER  AS SERVICE
              , P.SERVICE_ADDRESS AS STREET
              , P.SERVICE_CITY    AS CITY
              , P.SERVICE_STATE   AS STATE
              , P.ROC_NAME        AS OFFICE
              , P.CREDIT_REPORT_ID
              , P.CREDIT_REPORT_CREATED_DATE
              , T1.PROJECT_ID     AS T2ID
         FROM T1
            , RPT.T_PROJECT AS P
         WHERE T1.PROJECT_ID = P.PROJECT_ID
     ),

     T3 AS (
--Count Credit Runs
         SELECT T2.*
              , CT.FULL_NAME AS CUSTOMER_NAME
         FROM T2
                  LEFT OUTER JOIN
              RPT.T_CONTACT AS CT
              ON T2.CONTACT_ID = CT.CONTACT_ID
     ),

     T4 AS (
         SELECT T3.*
              , CR.CREATED_DATE AS CREDIT_RUN_DATE
         FROM T3
                  LEFT OUTER JOIN
              RPT.V_SF_CREDIT_REPORT AS CR
              ON CR.CONTACT__C = T3.CONTACT_ID
     )

SELECT CASE_NUMBER
     , ANY_VALUE(SERVICE)                        AS SERVICE
     , ANY_VALUE(CUSTOMER_NAME)                  AS CUSTOMER_NAME
     , ANY_VALUE(OWNER)                          AS OWNER
     , ANY_VALUE(SUBJECT)                        AS SUBJECT
     , ANY_VALUE(STATUS)                         AS STATUS
     , ANY_VALUE(CREATED_DATE)                   AS CREATED_DATE
     , ANY_VALUE(CLOSED_DATE)                    AS CLOSED_DATE
     , ANY_VALUE(EXECUTIVE_RESOLUTIONS_ACCEPTED) AS EXECUTIVE_RESOLUTIONS_ACCEPTED
     , ANY_VALUE(TRANSFER_CASE_NUMBER)           AS TRANSFER_CASE_NUMBER
     , MAX(HOME_CLOSING_DATE)                    AS HOME_CLOSED_DATE
     , ANY_VALUE(STREET)                         AS STREET
     , ANY_VALUE(CITY)                           AS CITY
     , ANY_VALUE(STATE)                          AS STATE
     , ANY_VALUE(OFFICE)                         AS OFFICE
     , COUNT(CREDIT_RUN_DATE)                       CUSTOMER_CREDIT_RUN_DATES
FROM T4
GROUP BY CASE_NUMBER