WITH DELINQUENT_CHANGE_LIST AS (
    SELECT CH.BILLING_ACCOUNT
         , CH.PREV_CODE
         , CH.PREV_CODE_ADDED_DATE
         , CH.CURRENT_CODE
         , CH.AS_OF_DATE
         , CH.AGE
         , CH.TOTAL_DELINQUENT_AMOUNT_DUE
    FROM (
             SELECT LDH.BILLING_ACCOUNT
                  , NVL(LDH.COLLECTION_CODE, 'NULL')           AS CURRENT_CODE
                  , NVL(LAG(CURRENT_CODE)
                            OVER (
                                PARTITION BY LDH.BILLING_ACCOUNT
                                ORDER BY LDH.AS_OF_DATE
                                ),
                        'NULL')                                AS PREV_CODE
                  , NVL(LAG(LDH.COLLECTION_DATE)
                            OVER (
                                PARTITION BY LDH.BILLING_ACCOUNT
                                ORDER BY LDH.AS_OF_DATE
                                ),
                        LDH.PTO_DATE)                          AS PREV_CODE_ADDED_DATE
                  , IFF(PREV_CODE = CURRENT_CODE, FALSE, TRUE) AS CODE_CHANGE
                  , LDH.AGE
                  , LDH.TOTAL_DELINQUENT_AMOUNT_DUE
                  , LDH.COLLECTION_DATE
                  , LDH.AS_OF_DATE
                  , ROW_NUMBER()
                     OVER (
                         PARTITION BY LDH.BILLING_ACCOUNT
                         ORDER BY LDH.AS_OF_DATE
                         )                                     AS RN
             FROM LD.T_DAILY_DATA_EXTRACT_HIST AS LDH -- Lease Dimensions History
             WHERE LDH.TOTAL_DELINQUENT_AMOUNT_DUE > 0
         ) AS CH -- Code History
    WHERE CH.CODE_CHANGE
      AND CH.AS_OF_DATE = (SELECT MAX(AS_OF_DATE) FROM LD.T_DAILY_DATA_EXTRACT_HIST)
    ORDER BY CH.BILLING_ACCOUNT
           , CH.AS_OF_DATE
)

SELECT *
FROM DELINQUENT_CHANGE_LIST
;