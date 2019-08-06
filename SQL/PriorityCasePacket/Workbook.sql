WITH ERT_CASES AS (
    SELECT C.ORIGIN                                                       AS COMPLAINT_SOURCE
         , P.SERVICE_STATE
         , P.SERVICE_NAME                                                 AS SERVICE_NUMBER
         , C.OWNER
         , CT.FULL_NAME                                                   AS CUSTOMER
         , C.DESCRIPTION                                                  AS COMPLAINT
         , IFF(C.STATUS ILIKE '%CLOSE%', 'This is a resolved case', NULL) AS CURRENT_UPDATE
         , NULL                                                           AS DESIRED_SETTLEMENT
         , IFF(C.STATUS ILIKE '%CLOSE%', 'NA', NULL)                      AS RECOMMENDATION
         , NULL                                                           AS RESOLUTION_COST
         , IFF(C.STATUS ILIKE '%CLOSE%', 'Resolved', NULL)                AS STATUS
         , NULL                                                           AS CHANNEL
         , NULL                                                           AS SOURCE
         , NULL                                                           AS RISK
         , C.PROJECT_ID
    FROM RPT.T_CASE AS C
             LEFT JOIN
         RPT.T_PROJECT AS P
         ON P.PROJECT_ID = C.PROJECT_ID
             LEFT JOIN
         RPT.T_CONTACT AS CT
         ON CT.CONTACT_ID = C.CONTACT_ID
    WHERE C.RECORD_TYPE = 'Solar - Customer Escalation'
      AND C.CREATED_DATE >= DATE_TRUNC('Y', CURRENT_DATE)
      AND C.ORIGIN IN
          ('Legal', 'Online Review', 'CEO Promise', 'Social Media', 'Executive', 'BBB',
           'News Media')
)

   , DAMAGE_CASES AS (
    SELECT C.CASE_NUMBER
         , C.DESCRIPTION
         , C.SETTLEMENT_AMOUNT
         , C.PROJECT_ID
    FROM RPT.T_CASE AS C
    WHERE C.RECORD_TYPE = 'Solar - Damage'
      AND C.CREATED_DATE >= DATE_TRUNC('Y', CURRENT_DATE)
      AND C.CLOSED_DATE IS NULL
)

   , MAIN AS (
    SELECT ER.COMPLAINT_SOURCE
         , ER.SERVICE_STATE     AS COMPLAINT_STATE
         , ER.SERVICE_NUMBER
         , ER.OWNER             AS CASE_OWNER
         , ER.CUSTOMER          AS CUSTOMER_NAME
         , ER.COMPLAINT
         , ER.CURRENT_UPDATE
         , ER.DESIRED_SETTLEMENT
         , ER.RECOMMENDATION
         , ER.RESOLUTION_COST
         , ER.STATUS
         , ER.CHANNEL
         , ER.SOURCE
         , ER.RISK
         , DM.CASE_NUMBER       AS DAMMAGE_CASE
         , DM.DESCRIPTION       AS DAMAGE_SUMMARY
         , DM.SETTLEMENT_AMOUNT AS DAMAGE_COST_TO_RESOLVE
    FROM ERT_CASES AS ER
             LEFT JOIN
         DAMAGE_CASES AS DM
         ON ER.PROJECT_ID = DM.PROJECT_ID
)

SELECT *
FROM MAIN
ORDER BY SERVICE_NUMBER