WITH CORE AS (
    SELECT
/* Required Fields
 * ---------------
 * Service
 * Case Number
 * Case Record Type
 * Category -- Definition required
 * Description
 * Long Island
 * Customer Utility
 * Contract Type
 */
        P.SERVICE_NAME
         , C.CASE_NUMBER
         , C.RECORD_TYPE
         , C.PRIMARY_REASON AS CATEGORY
         , C.DESCRIPTION
         , CASE
               WHEN P.UTILITY_COMPANY = 'PSEGLI'
                   THEN 'Yes'
               ELSE 'No'
        END                 AS LONG_ISLAND
         , P.UTILITY_COMPANY
         , S.OPTY_CONTRACT_TYPE
    FROM RPT.T_CASE AS C

             LEFT OUTER JOIN
         RPT.T_PROJECT AS P
         ON P.PROJECT_ID = C.PROJECT_ID

             LEFT OUTER JOIN
         RPT.T_SERVICE AS S
         ON C.SERVICE_ID = S.SERVICE_ID

    WHERE C.CREATED_DATE BETWEEN '2018-01-01' AND '2019-01-01'
      AND P.SERVICE_STATE = 'NY'
      AND (
            (
                    C.RECORD_TYPE = 'Solar - Customer Default'
                    AND C.SUBJECT NOT LIKE '%D3%'
                ) OR (
                C.RECORD_TYPE = 'Solar - Customer Escalation'
                ) OR (
                C.RECORD_TYPE IN ('Solar Damage Resolutions')
                ) OR (
                C.RECORD_TYPE IN ('Solar - Service')
                )
        )
    ORDER BY 1
)

SELECT *
FROM CORE
ORDER BY 1