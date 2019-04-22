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
         , CASE
               WHEN P.UTILITY_COMPANY = 'PSEGLI'
                   THEN 'Yes'
               ELSE 'No'
        END AS LONG_ISLAND
         , P.SERVICE_COUNTY
         , P.UTILITY_COMPANY
         , P.INSTALLATION_COMPLETE
         , S.OPTY_CONTRACT_TYPE
    FROM RPT.T_PROJECT AS P
             LEFT OUTER JOIN
         RPT.T_SERVICE AS S
         ON S.PROJECT_ID = P.PROJECT_ID

    WHERE S.SERVICE_STATUS NOT ILIKE '%cancel%'
      AND P.INSTALLATION_COMPLETE IS NOT NULL
      AND P.SERVICE_STATE = 'NY'
    ORDER BY 1
)

SELECT UTILITY_COMPANY
     , LONG_ISLAND
     , COUNT(CASE WHEN OPTY_CONTRACT_TYPE = 'PPA' THEN 1 END)   AS PPA_COUNT
     , COUNT(CASE WHEN OPTY_CONTRACT_TYPE = 'LEASE' THEN 1 END) AS LESAE_COUNT
     , COUNT(LONG_ISLAND)                                       AS TOTAL_COUNT
FROM CORE
GROUP BY 2, 1
ORDER BY 2 DESC, 1 ASC