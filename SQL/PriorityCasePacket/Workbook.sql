WITH ERT_CASES AS (
    /*
     Required Fields:
     ----------------
     X Complaint Source
     X Complaint State
     X Service Number
     X Case Owner
     X Customer Name
     X Complaint
     X Current Update
     X Desired Settlement
     X Recommendation
     X Resolution Cost
     X Status
     X Channel
     X Source
     X Risk
     O Damage Case
     O Damage Summary
     O Damage cost to resolve
     */
    SELECT CASE WHEN C.ORIGIN = 'Legal'
        THEN 'Attorney General'
        WHEN C.ORIGIN = ''
        END AS COMPLAINT_SOURCE
         , P.SERVICE_STATE
         , P.SERVICE_NAME AS SERVICE_NUMBER
         , C.OWNER
         , CT.FULL_NAME   AS CUSTOMER
         , C.DESCRIPTION  AS COMPLAINT
         , ''             AS CURRENT_UPDATE
         , ''             AS DESIRED_SETTLEMENT
         , ''             AS RECOMMENDATION
         , ''             AS RESOLUTION_COST
         , C.STATUS       AS STATUS
         , ''             AS CHANNEL
         , ''             AS SOURCE
         , ''             AS RISK
--          , '' AS DAMAGE_CASE
--          , '' AS DAMAGE_SUMMARY
--          , '' AS DAMAGE_COST_TO_RESOLVE
    FROM RPT.T_CASE AS C
             LEFT JOIN
         RPT.T_PROJECT AS P
         ON P.PROJECT_ID = C.PROJECT_ID
             LEFT JOIN
         RPT.T_CONTACT AS CT
         ON CT.CONTACT_ID = C.CONTACT_ID
    WHERE C.RECORD_TYPE = 'Solar - Customer Escalation'
      AND C.CREATED_DATE >= DATE_TRUNC('Y', CURRENT_DATE)
      AND C.ORIGIN IN ('News Media', 'Legal', 'BBB')
)

   , DAMAGE_CASES AS (
    SELECT C.CASE_NUMBER
         , C.DESCRIPTION
         , C.SETTLEMENT_AMOUNT
    FROM RPT.T_CASE AS C
    WHERE C.RECORD_TYPE = 'Solar - Damage'
      AND C.CREATED_DATE >= DATE_TRUNC('Y', CURRENT_DATE)
)

SELECT *
FROM ERT_CASES