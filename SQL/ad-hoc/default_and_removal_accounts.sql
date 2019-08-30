WITH DEFAULT_CASES AS (
    SELECT C.CASE_NUMBER          AS DEFAULT_CASE_NUMBER
         , C.PROJECT_ID
         , P.SERVICE_NAME
         , P.PROJECT_NAME
         , CT.FULL_NAME           AS CUSTOMER_NAME
         , C.OWNER                AS DEFAULT_CASE_OWNER
         , C.CREATED_DATE         AS DEFAUL_CASE_CREATED_DATE
         , C.STATUS               AS DEFAULT_CASE_STATUS
         , C.SOLAR_PRIMARY_REASON AS DEFAULT_PRIMARY_REASON
    FROM RPT.T_CASE AS C
             LEFT JOIN
         RPT.T_PROJECT AS P
         ON P.PROJECT_ID = C.PROJECT_ID
             LEFT JOIN
         RPT.T_CONTACT AS CT
         ON CT.CONTACT_ID = C.CONTACT_ID
    WHERE C.RECORD_TYPE = 'Solar - Customer Default'
      AND C.SOLAR_PRIMARY_REASON = 'Foreclosure'
      AND C.SUBJECT NOT ILIKE '%D3%'
      AND C.STATUS NOT ILIKE '%CLOSED%'
      AND C.STATUS NOT ILIKE '%COMPLETE$'
)

   , RR_CASES AS (
    SELECT C.CASE_NUMBER          AS RR_CASE_NUMBER
         , C.PROJECT_ID
         , P.SERVICE_NAME
         , P.PROJECT_NAME
         , CT.FULL_NAME           AS CUSTOMER_NAME
         , C.OWNER                AS RR_CASE_OWNER
         , C.CREATED_DATE         AS RR_CASE_CREATED_DATE
         , C.STATUS               AS RR_CASE_STATUS
         , C.SOLAR_PRIMARY_REASON AS RR_PRIMARY_REASON
    FROM RPT.T_CASE AS C
             LEFT JOIN
         RPT.T_PROJECT AS P
         ON P.PROJECT_ID = C.PROJECT_ID
             LEFT JOIN
         RPT.T_CONTACT AS CT
         ON CT.CONTACT_ID = C.CONTACT_ID
    WHERE C.RECORD_TYPE = 'Solar - Panel Removal'
      AND STATUS NOT ILIKE '%CLOSE%'
)

   , COMBINED_CASES AS (
    SELECT D.*
         , RR.PROJECT_ID AS RR_PID
         , RR.RR_CASE_STATUS
         , RR.RR_CASE_NUMBER
    FROM DEFAULT_CASES AS D
             LEFT JOIN RR_CASES AS RR
                       ON RR.PROJECT_ID = D.PROJECT_ID
)

   , ACCOUNT_LIST AS (
    SELECT SERVICE_NAME
         , CUSTOMER_NAME
         , DEFAULT_CASE_NUMBER
         , DEFAULT_CASE_STATUS
         , RR_CASE_NUMBER
         , RR_CASE_STATUS
    FROM COMBINED_CASES
    WHERE RR_PID IS NOT NULL
)

SELECT *
FROM ACCOUNT_LIST
ORDER BY SERVICE_NAME