WITH T1 AS (
    SELECT *
    FROM RPT.T_PROJECT AS PR
             LEFT JOIN
         RPT.T_CONTRACT AS CO
         ON CO.CONTRACT_ID = PR.PRIMARY_CONTRACT_ID
)

SELECT *
FROM T1

/*
    Fields needed
    -------------
    Signer | Contract
    Co-signer | Contract
    Office | Project
    ROC | Project
    Transaction Date | Contract
    Install Date | Project
    Funded Date | Fund
    PTO Date | Project
    Fund | Fund
    System Size | CAD
    Contract Type (type version) | Contract
 */