/*
 * Anchor the table to the project, and left join the rest of the information with ties to the account_id
 * Fields needed
 * -------------
 * X Service Number
 * X Customer Name
 * X Regional Office
 * X Transaction Date
 * X Contract Type
 * X Welcome Call
 * X Site Survey
 * X Funded Date
 * X Installation Date
 * X Inspection Date
 * X PTO Date
 */

SELECT
--	  P.ACCOUNT_ID
    P.SERVICE_NAME                     AS SERVICE
     , ANY_VALUE(P.ROC_NAME)           AS ROC_NAME
     , ANY_VALUE(CT.FULL_NAME)         AS FULL_NAME
     , ANY_VALUE(S.SERVICE_ADDRESS)    AS SERVICE_ADDRESS
     , ANY_VALUE(S.SERVICE_CITY)       AS SERVICE_CITY
     , ANY_VALUE(S.SERVICE_STATE)      AS SERVICE_STATE
     , ANY_VALUE(S.SERVICE_ZIP_CODE)   AS SERVICE_ZIP_CODE
     , ANY_VALUE(CT.EMAIL)             AS EMAIL
     , ANY_VALUE(CT.PHONE)             AS PHONE
     , ANY_VALUE(SD.SYSTEM_SIZE)       AS SYSTEM_SIZE
     , ANY_VALUE(S.OPTY_CONTRACT_TYPE) AS CONTRACT_TYPE
     , ANY_VALUE(C.CONTRACT_VERSION)   AS CONTRACT_VERSION

FROM RPT.T_PROJECT AS P
         LEFT JOIN
     RPT.T_SYSTEM_DETAILS_SNAP AS SD
     ON P.PROJECT_ID = SD.PROJECT_ID

         LEFT JOIN
     RPT.T_CONTACT AS CT
     ON CT.ACCOUNT_ID = P.ACCOUNT_ID

         LEFT JOIN
     RPT.T_OPPORTUNITY AS O
     ON O.ACCOUNT_ID = P.ACCOUNT_ID

         LEFT JOIN
     RPT.T_CONTRACT AS C
     ON C.PROJECT_ID = P.PROJECT_ID

         LEFT JOIN
     RPT.T_SERVICE AS S
     ON S.PROJECT_ID = P.PROJECT_ID

WHERE P.SERVICE_NAME IN
      ('S-5862291', 'S-5994418', 'S-2896715S', 'S-5145109', 'S-5834195', 'S-5280637', 'S-5443793', 'S-5894066')
GROUP BY SERVICE
ORDER BY SERVICE