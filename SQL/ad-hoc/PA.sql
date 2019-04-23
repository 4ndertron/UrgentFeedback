WITH INFORMATION AS (
    -- Objectives Table
    /*
     -- TODO: Informational Fields:
            Service Number
            Customer Name
            City
            Install Date
            PTO Date
            Sales Manager
            Transaction Date
     */

    SELECT P.SERVICE_NAME
         , CN.FULL_NAME
         , P.SERVICE_CITY
         , P.INSTALLATION_COMPLETE
         , PTO_AWARDED
         , O.SALES_REP_NAME || ' (' || O.SALES_REP_EMP_ID || ')' AS SALES_MANAGER
         , CT.TRANSACTION_DATE
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
    WHERE P.INSTALLATION_COMPLETE IS NOT NULL
      AND P.SERVICE_STATE = 'PA'
      AND P.PROJECT_STATUS NOT LIKE '%Canc%'
--       AND C.RECORD_TYPE = 'Solar - Customer Escalation'
--       AND C.EXECUTIVE_RESOLUTIONS_ACCEPTED IS NOT NULL
)
   , CASES AS (
-- Objectives Table
/*
-- TODO: Fields needed:
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

    SELECT *
    FROM RPT.T_CASE AS C
)

SELECT *
FROM INFORMATION