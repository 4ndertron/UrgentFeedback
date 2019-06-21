WITH EMAIL_LIST AS (
    SELECT ANY_VALUE(P.PROJECT_NAME)                   AS PROJECT_NAME
         , ANY_VALUE(P.SERVICE_CITY)                   AS SERVICE_CITY
         , ANY_VALUE(P.SERVICE_STATE)                  AS SERVICE_STATE
         , TO_DATE(ANY_VALUE(P.INSTALLATION_COMPLETE)) AS DATE_SIGNED
         , ANY_VALUE(CN.FIRST_NAME)                    AS FIRST_NAME
         , ANY_VALUE(CN.LAST_NAME)                     AS LAST_NAME
         , ANY_VALUE(CN.EMAIL)                         AS EMAIL
         , ANY_VALUE(CN.PREFERRED_LANGUAGE)            AS PREFERRED_LANGUAGE
    FROM RPT.T_PROJECT AS P
             LEFT JOIN
         RPT.T_CONTRACT AS CT
         ON CT.CONTRACT_ID = P.PRIMARY_CONTRACT_ID
             LEFT JOIN
         RPT.V_SF_ACCOUNT AS A
         ON A.ID = P.ACCOUNT_ID
             LEFT JOIN
         RPT.T_CONTACT AS CN
         ON CN.ACCOUNT_ID = A.ID
    WHERE P.SERVICE_CITY ILIKE '%Parlier%'
      AND P.INSTALLATION_COMPLETE IS NOT NULL
      AND P.CANCELLATION_DATE IS NULL
      AND CN.EMAIL IS NOT NULL
    GROUP BY CN.EMAIL
    ORDER BY 1)

SELECT *
FROM EMAIL_LIST