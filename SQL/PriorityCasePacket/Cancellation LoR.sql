WITH CANCELLED_ACCOUNTS AS (
    /*
     Turn to look for closed cancellation case in 2019 instad of cancelled accounts in 2019.
     Maybe have the core account list be ert cases created in 2019 with open/closed cancellation cases...
     Core ERT created in 2019. Of those, how many of those have a cancellation case...
     */
    SELECT P.PROJECT_NAME
         , ANY_VALUE(P.SERVICE_NAME)               AS  SERVICE
         , ANY_VALUE(P.SERVICE_STATE)              AS  STATE
         , ANY_VALUE(P.PROJECT_ID)                 AS  PROJECT_ID
         , ANY_VALUE(TO_DATE(P.INSTALLATION_COMPLETE)) INSTALLATION_DATE
         , ANY_VALUE(TO_DATE(P.CANCELLATION_DATE)) AS  CANCELLATION_DATE
         , ANY_VALUE(CAD.SYSTEM_SIZE_ACTUAL)       AS  THIS_SIZE
         , ROUND(THIS_SIZE * 1000 * 4, 2)          AS  SYSTEM_VALUE
    FROM RPT.T_PROJECT AS P
             LEFT JOIN
         RPT.T_SYSTEM_DETAILS_SNAP AS CAD
         ON CAD.PROJECT_ID = P.PROJECT_ID
             LEFT JOIN
         RPT.T_CONTRACT AS CT
         ON CT.CONTRACT_ID = P.PRIMARY_CONTRACT_ID
             LEFT JOIN
         RPT.T_CONTACT AS CN
         ON CN.CONTACT_ID = CT.SIGNER_CONTACT_ID
    WHERE P.PROJECT_STATUS = 'Cancelled'
      AND P.INSTALLATION_COMPLETE IS NOT NULL
      AND P.CANCELLATION_DATE >= DATE_TRUNC('Y', CURRENT_DATE)
    GROUP BY P.PROJECT_NAME
)

   , VALID_ERT_CASES AS (
    SELECT C.CASE_NUMBER
         , CN.FULL_NAME                              AS CUSTOMER
         , C.OWNER
         , C.PROJECT_ID
         , C.RECORD_TYPE
         , C.ORIGIN
         , TO_DATE(C.EXECUTIVE_RESOLUTIONS_ACCEPTED) AS ERA
    FROM RPT.T_CASE AS C
             LEFT JOIN
         RPT.T_CONTACT AS CN
         ON CN.CONTACT_ID = C.CONTACT_ID
    WHERE ERA IS NOT NULL
      AND C.RECORD_TYPE = 'Solar - Customer Escalation'
      AND C.ORIGIN IN ('BBB', 'Legal', 'News Media')
)

   , CANCELLATION_LOR AS (
    SELECT CA.PROJECT_NAME
         , CA.SERVICE
         , ERT.CUSTOMER
         , ERT.OWNER
         , CA.STATE
         , CA.INSTALLATION_DATE
         , CA.CANCELLATION_DATE
         , CA.THIS_SIZE AS SYSTEM_SIZE
         , CA.SYSTEM_VALUE
         , ERT.CASE_NUMBER
         , ERT.RECORD_TYPE
         , ERT.ORIGIN
         , ERT.ERA      AS EXECUTIVE_RESOLUTIONS_ACCEPTED_DATE
    FROM CANCELLED_ACCOUNTS CA
             LEFT JOIN
         VALID_ERT_CASES AS ERT
         ON CA.PROJECT_ID = ERT.PROJECT_ID
    WHERE ERT.CASE_NUMBER IS NOT NULL
    ORDER BY CA.CANCELLATION_DATE
)

   , NEW_CORE AS (
    SELECT ERT.CASE_NUMBER
         , ANY_VALUE(P.SERVICE_STATE)                             AS SERVICE_STATE
         , ANY_VALUE(P.SERVICE_NAME)                              AS SERVICE_NUMBER
         , ANY_VALUE(ERT.CUSTOMER)                                AS CUSTOMER
         , ANY_VALUE(ERT.OWNER)                                   AS OWNER
         , ANY_VALUE(ERT.PROJECT_ID)                              AS PROJECT_ID
         , ANY_VALUE(ERT.RECORD_TYPE)                             AS RECORD_TYPE
         , ANY_VALUE(ERT.ORIGIN)                                  AS ORIGIN
         , ANY_VALUE(ERT.ERA)                                     AS ERA
         , ANY_VALUE(CAD.SYSTEM_SIZE_ACTUAL)                      AS SYSTEM_SIZE_ACTUAL
         , ROUND(ANY_VALUE(CAD.SYSTEM_SIZE_ACTUAL) * 4 * 1000, 2) AS SYSTEM_VALUE
    FROM VALID_ERT_CASES AS ERT
             LEFT JOIN
         RPT.T_CASE AS C
         ON C.PROJECT_ID = ERT.PROJECT_ID
             LEFT JOIN
         RPT.T_SYSTEM_DETAILS_SNAP AS CAD
         ON CAD.PROJECT_ID = ERT.PROJECT_ID
             LEFT JOIN
         RPT.T_PROJECT AS P
         ON C.PROJECT_ID = P.PROJECT_ID
    WHERE C.RECORD_TYPE = 'Solar - Cancellation Request'
      AND ERT.ERA >= DATE_TRUNC('Y', CURRENT_DATE)
    GROUP BY ERT.CASE_NUMBER
)

/*
 bbb Non-active are showing up... somewhere.
 Desired, REcc, Cost, Current Update.
 Source, Channel,

 */

/*
 Current open-active
 Pending complaints... Resolved on TPC, not VSLR
 Resolved
 */

SELECT ORIGIN
     , SERVICE_STATE
     , SERVICE_NUMBER
     , OWNER
     , CUSTOMER
     , SYSTEM_VALUE
FROM NEW_CORE