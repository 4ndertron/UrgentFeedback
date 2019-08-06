WITH VALID_ERT_CASES AS (
    SELECT C.CASE_NUMBER
         , C.ORIGIN                                                           AS COMPLAINT_SOURCE
         , C.OWNER
         , CN.FULL_NAME                                                       AS CUSTOMER
         , C.DESCRIPTION                                                      AS COMPLAINT
         , IFF(C.STATUS ILIKE '%CLOSE%', 'This is a cancelled account', NULL) AS CURRENT_UPDATE
         , NULL                                                               AS DESIRED_SETTLEMENT
         , IFF(C.STATUS ILIKE '%CLOSE%', 'NA', NULL)                          AS RECOMMENDATION
         , NULL                                                               AS RESOLUTION_COST
         , IFF(C.STATUS ILIKE '%CLOSE%', 'Resolved', NULL)                    AS STATUS
         , NULL                                                               AS CHANNEL
         , NULL                                                               AS SOURCE
         , NULL                                                               AS RISK
         , C.PROJECT_ID
         , C.RECORD_TYPE
         , C.ORIGIN
         , TO_DATE(C.EXECUTIVE_RESOLUTIONS_ACCEPTED)                          AS ERA
    FROM RPT.T_CASE AS C
             LEFT JOIN
         RPT.T_CONTACT AS CN
         ON CN.CONTACT_ID = C.CONTACT_ID
    WHERE ERA IS NOT NULL
      AND C.RECORD_TYPE = 'Solar - Customer Escalation'
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

   , CASE_LIST AS (
    SELECT ER.COMPLAINT_SOURCE
         , P.SERVICE_STATE      AS COMPLAINT_STATE
         , P.SERVICE_NAME       AS SERVICE_NUMBER
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
         , DM.CASE_NUMBER       AS DAMAGE_CASE
         , DM.DESCRIPTION       AS DAMAGE_SUMMARY
         , DM.SETTLEMENT_AMOUNT AS DAMAGE_COST_TO_RESOLVE
         , ER.PROJECT_ID
         , ER.ERA
    FROM VALID_ERT_CASES AS ER
             LEFT JOIN
         DAMAGE_CASES AS DM
         ON ER.PROJECT_ID = DM.PROJECT_ID
             LEFT JOIN
         RPT.T_PROJECT AS P
         ON P.PROJECT_ID = ER.PROJECT_ID
)

   , NEW_CORE AS (
    SELECT ANY_VALUE(ERT.COMPLAINT_SOURCE)                        AS COMPLAINT_SOURCE
         , ANY_VALUE(ERT.COMPLAINT_STATE)                         AS COMPLAINT_STATE
         , ERT.SERVICE_NUMBER
         , ANY_VALUE(ERT.CASE_OWNER)                              AS CASE_OWNER
         , ANY_VALUE(ERT.CUSTOMER_NAME)                           AS CUSTOMER_NAME
         , ANY_VALUE(ERT.COMPLAINT)                               AS COMPLAINT
         , ANY_VALUE(ERT.CURRENT_UPDATE)                          AS CURRENT_UPDATE
         , ANY_VALUE(ERT.DESIRED_SETTLEMENT)                      AS DESIRED_SETTLEMENT
         , ANY_VALUE(ERT.RECOMMENDATION)                          AS RECOMMENDATION
         , ANY_VALUE(ERT.RESOLUTION_COST)                         AS RESOLUTION_COST
         , ANY_VALUE(ERT.STATUS)                                  AS STATUS
         , ANY_VALUE(ERT.CHANNEL)                                 AS CHANNEL
         , ANY_VALUE(ERT.SOURCE)                                  AS SOURCE
         , ANY_VALUE(ERT.RISK)                                    AS RISK
         , ANY_VALUE(ERT.DAMAGE_CASE)                             AS DAMAGE_CASE
         , ANY_VALUE(ERT.DAMAGE_SUMMARY)                          AS DAMAGE_SUMMARY
         , ANY_VALUE(ERT.DAMAGE_COST_TO_RESOLVE)                  AS DAMAGE_COST_TO_RESOLVE
         , ANY_VALUE(CAD.SYSTEM_SIZE_ACTUAL)                      AS SYSTEM_SIZE_ACTUAL
         , ROUND(ANY_VALUE(CAD.SYSTEM_SIZE_ACTUAL) * 4 * 1000, 2) AS SYSTEM_VALUE
    FROM CASE_LIST AS ERT
             LEFT JOIN
         RPT.T_CASE AS C
         ON C.PROJECT_ID = ERT.PROJECT_ID
             LEFT JOIN
         RPT.T_SYSTEM_DETAILS_SNAP AS CAD
         ON CAD.PROJECT_ID = ERT.PROJECT_ID
    WHERE C.RECORD_TYPE = 'Solar - Cancellation Request'
      AND ERT.ERA >= DATE_TRUNC('Y', CURRENT_DATE)
    GROUP BY ERT.SERVICE_NUMBER
)

SELECT *
FROM NEW_CORE