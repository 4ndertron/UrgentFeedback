WITH ACTIVE_CASE_LIST AS (
    SELECT C.CASE_NUMBER
         , C.OWNER
         , C.ORIGIN
         , C.EXECUTIVE_RESOLUTIONS_ACCEPTED AS ERA
         , P.SERVICE_NAME
         , P.SOLAR_BILLING_ACCOUNT_NUMBER
         , P.PROJECT_NAME
    FROM RPT.T_CASE AS C
             LEFT JOIN RPT.T_PROJECT AS P
                       ON P.PROJECT_ID = C.PROJECT_ID
    WHERE C.RECORD_TYPE = 'Solar - Customer Escalation'
      AND C.EXECUTIVE_RESOLUTIONS_ACCEPTED IS NOT NULL
      AND C.STATUS NOT ILIKE '%CLOSE%'
)

SELECT DISTINCT SERVICE_NAME
              , SOLAR_BILLING_ACCOUNT_NUMBER
FROM ACTIVE_CASE_LIST