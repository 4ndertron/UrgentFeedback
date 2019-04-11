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

SELECT P.ACCOUNT_ID
     , ANY_VALUE(P.SERVICE_NAME)                                               AS "Service Number:"
     , ANY_VALUE(P.ROC_NAME)                                                   AS "Regional Office:"
     , ANY_VALUE(P.SITE_SURVEY_COMPLETE)                                          "Site Survey:"
     , ANY_VALUE(P.INSTALLATION_COMPLETE)                                      AS "Installation Date:"
     , ANY_VALUE(P.PTO_AWARDED)                                                AS "PTO Date:"
     , ANY_VALUE(SD.CONTRIBUTION_DATE)                                         AS "Funded Date:"
     , ANY_VALUE(CT.FULL_NAME)                                                 AS "Customer Name:"
     , ANY_VALUE(O.WELCOME_CALL_COMPLETED_DATE)                                AS "Welcome Call:"
     , ANY_VALUE(P.ELEC_INSPECTION_COMPLETE)                                   AS "Inspection Date:"
     , ANY_VALUE(S.OPTY_CONTRACT_TYPE) || ' ' || ANY_VALUE(C.CONTRACT_VERSION) AS "Contract Type:"
     , ANY_VALUE(C.TRANSACTION_DATE)                                           AS "Transaction Date:"

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
      ('S-5972292', 'S-6040195', 'S-5970537', 'S-2975768S', 'S-4456917', 'S-5897704', 'S-5851922', 'S-5473285',
       'S-3593416S', 'S-4628797', 'S-4628797', 'S-4386605', 'S-5941368', 'S-4033154', 'S-5930754', 'S-5957302',
       'S-5384859', 'S-6016732', 'S-4840228', 'S-5855228')
GROUP BY P.ACCOUNT_ID
ORDER BY P.ACCOUNT_ID