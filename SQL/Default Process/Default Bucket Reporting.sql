WITH PRE_DEFAULT_CASES AS (
    SELECT P.SOLAR_BILLING_ACCOUNT_NUMBER                        AS BILLING_ACCOUNT
         , P.SERVICE_NAME
         , C.CASE_NUMBER
         , '<a href="https://vivintsolar.lightning.force.com/lightning/r/Case/' || C.CASE_ID ||
           '/view" target="_blank">' || C.CASE_NUMBER || '</a>'  AS SALESFORCE_CASE
         , C.STATUS
         , C.PRIMARY_REASON
         , C.OWNER
         , C.RECORD_TYPE
         , C.SOLAR_QUEUE
         , C.SUBJECT
         , IFF(P.PTO_AWARDED IS NOT NULL, 'Post-PTO', 'Pre-PTO') AS PTO_INDEX
         , 'Pre-Default'                                         AS CASE_BUCKET
    FROM RPT.T_CASE AS C
             LEFT OUTER JOIN RPT.T_PROJECT AS P
                             ON P.PROJECT_ID = C.PROJECT_ID
    WHERE C.RECORD_TYPE = 'Solar - Customer Escalation'
      AND C.SOLAR_QUEUE = 'Dispute/Evasion'
)

   , DEFAULT_CASES AS (
    SELECT P.SOLAR_BILLING_ACCOUNT_NUMBER                        AS BILLING_ACCOUNT
         , P.SERVICE_NAME
         , C.CASE_NUMBER
         , '<a href="https://vivintsolar.lightning.force.com/lightning/r/Case/' || C.CASE_ID ||
           '/view" target="_blank">' || C.CASE_NUMBER || '</a>'  AS SALESFORCE_CASE
         , C.STATUS
         , C.PRIMARY_REASON
         , C.OWNER
         , C.RECORD_TYPE
         , C.SOLAR_QUEUE
         , C.SUBJECT
         , IFF(P.PTO_AWARDED IS NOT NULL, 'Post-PTO', 'Pre-PTO') AS PTO_INDEX
         , CASE
               WHEN C.CLOSED_DATE IS NOT NULL
                   THEN 'Complete'
               WHEN (C.STATUS ILIKE '%LEGAL%' OR C.STATUS ILIKE '%ESCALATED%')
                   THEN 'Pending Legal Action'
               WHEN C.DESCRIPTION ILIKE '%MBW%'
                   THEN 'Third-party'
               WHEN C.DRA IS NOT NULL
                   THEN 'DRA'
               WHEN C.P_5_LETTER IS NOT NULL
                   THEN 'P5 Letter'
               WHEN C.P_4_LETTER IS NOT NULL
                   THEN 'P4 Letter'
               WHEN C.HOME_VISIT_ONE IS NOT NULL
                   THEN 'Home Visit/Letter'
               ELSE 'Actively Working'
        END                                                      AS CASE_BUCKET
    FROM RPT.T_CASE AS C
             LEFT OUTER JOIN RPT.T_PROJECT AS P
                             ON P.PROJECT_ID = C.PROJECT_ID
    WHERE C.RECORD_TYPE = 'Solar - Customer Default'
      AND C.SUBJECT NOT ILIKE '%D3%'
)

   , MAIN AS (
    SELECT *
    FROM PRE_DEFAULT_CASES AS PC
    UNION
    SELECT *
    FROM DEFAULT_CASES AS DC
)

   , TEST_CTE AS (
    SELECT DISTINCT *
    FROM MAIN
)

SELECT *
     , CURRENT_DATE AS LAST_REFRESHED
FROM MAIN
;