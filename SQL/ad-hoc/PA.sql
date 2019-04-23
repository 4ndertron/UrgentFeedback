WITH INFORMATION AS (
    -- Objectives Table
    /*
     TODO: Informational Fields:
        Service Number
        Customer Name
        City
        Install Date
        PTO Date
        Sales Manager
        Transaction Date
     */

    SELECT P.SERVICE_NAME
         , ANY_VALUE(CN.FULL_NAME)            AS FULL_NAME
         , ANY_VALUE(P.SERVICE_CITY)          AS SERVICE_CITY
         , ANY_VALUE(P.INSTALLATION_COMPLETE) AS INSTALLATION_COMPLETE
         , ANY_VALUE(P.PTO_AWARDED)           AS PTO_AWARDED
         , ANY_VALUE(O.SALES_REP_NAME)        AS SALES_REP_NAME
         , ANY_VALUE(CT.TRANSACTION_DATE)     AS TRANSACTION_DATE
    FROM RPT.T_PROJECT AS P
             LEFT OUTER JOIN
         RPT.T_CONTRACT AS CT
         ON CT.CONTRACT_ID = P.PRIMARY_CONTRACT_ID
             LEFT OUTER JOIN
         RPT.T_CONTACT AS CN
         ON CT.SIGNER_CONTACT_ID = CN.CONTACT_ID
             LEFT OUTER JOIN
         RPT.T_OPPORTUNITY AS O
         ON O.OPPORTUNITY_ID = P.OPPORTUNITY_ID
             LEFT OUTER JOIN
         RPT.T_CASE AS C
         ON C.PROJECT_ID = P.PROJECT_ID
    WHERE P.INSTALLATION_COMPLETE IS NOT NULL
      AND P.SERVICE_STATE = 'PA'
      AND P.PROJECT_STATUS NOT LIKE '%Canc%'
    GROUP BY P.SERVICE_NAME
    ORDER BY P.SERVICE_NAME
)
   , CASES AS (
-- Objectives Table
/*
TODO: Fields needed:
    Executive Resolutions Boolean
    Executive Resolutions Description
    BBB Complaint Boolean
    BBB Description
    Escalation Boolean
    Escalation Description
    Service Boolean
    Service Description
    Troubleshooting Boolean
    Troubleshooting Description
    Damage Boolean
    Damage Description
    Damage Primary Reason
 */

    SELECT P.PROJECT_ID
         , C.CASE_NUMBER
         , C.ORIGIN
         , C.RECORD_TYPE
         , C.PRIMARY_REASON
    FROM RPT.T_CASE AS C
             LEFT OUTER JOIN
         RPT.T_PROJECT AS P
         ON P.PROJECT_ID = C.PROJECT_ID

)

SELECT *
FROM INFORMATION
ORDER BY SERVICE_NAME