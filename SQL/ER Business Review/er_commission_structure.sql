WITH CASE_TABLE AS (
    SELECT C.CASE_NUMBER
         , ANY_VALUE(C.CASE_ID)                            AS CASE_ID
         , C.PROJECT_ID
         , ANY_VALUE(LDD.AGE)                              AS AGE_FOR_CASE
         , ANY_VALUE(C.OWNER)                              AS CASE_OWNER
         , ANY_VALUE(C.SUBJECT)                            AS SUBJECT1
         , ANY_VALUE(CAD.SYSTEM_SIZE)                      AS SYSTEM_SIZE
         , ROUND(ANY_VALUE(CAD.SYSTEM_SIZE) * 1000 * 4, 2) AS SYSTEM_VALUE
         , ANY_VALUE(C.STATUS)                             AS STATUS2
         , ANY_VALUE(C.CUSTOMER_TEMPERATURE)               AS CUSTOMER_TEMPERATURE
         , ANY_VALUE(C.CREATED_DATE)                       AS CREATED_DATE1
         , ANY_VALUE(C.CLOSED_DATE)                        AS CLOSED_DATE1
         , ANY_VALUE(C.RECORD_TYPE)                        AS RECORD_TYPE1
         , ANY_VALUE(S.SOLAR_BILLING_ACCOUNT_NUMBER)       AS SOLAR_BILLING_ACCOUNT_NUMBER1
    FROM RPT.T_CASE AS C
             LEFT JOIN
         RPT.T_SERVICE AS S
         ON C.SERVICE_ID = S.SERVICE_ID

             LEFT JOIN
         RPT.T_SYSTEM_DETAILS_SNAP AS CAD
         ON CAD.SERVICE_ID = C.SERVICE_ID

             LEFT JOIN
         LD.T_DAILY_DATA_EXTRACT AS LDD
         ON LDD.BILLING_ACCOUNT = S.SOLAR_BILLING_ACCOUNT_NUMBER
    WHERE C.RECORD_TYPE = 'Solar - Customer Escalation'
      AND C.EXECUTIVE_RESOLUTIONS_ACCEPTED IS NOT NULL
      AND C.SUBJECT NOT ILIKE '%VIP%'
      AND C.SUBJECT NOT ILIKE '%NPS%'
      AND C.SUBJECT NOT ILIKE '%COMP%'
    GROUP BY C.PROJECT_ID
           , C.CASE_NUMBER
    ORDER BY C.PROJECT_ID
)

   , CASE_HISTORY_TABLE AS (
    SELECT CT.*
         , CC.CREATEDATE
         , USR.NAME AS COMMENT_CREATE_BY
         , HR.BUSINESS_TITLE
         , DATEDIFF(s, NVL(LAG(CC.CREATEDATE) OVER (PARTITION BY CT.CASE_NUMBER
        ORDER BY CC.CREATEDATE),
                           CT.CREATED_DATE1),
                    CC.CREATEDATE
               ) / (24 * 60 * 60)
                    AS GAP
    FROM CASE_TABLE AS CT
             LEFT OUTER JOIN
         RPT.V_SF_CASECOMMENT AS CC
         ON CC.PARENTID = CT.CASE_ID
             LEFT JOIN RPT.V_SF_USER AS USR
                       ON USR.ID = CC.CREATEDBYID
             LEFT JOIN HR.T_EMPLOYEE AS HR
                       ON HR.EMPLOYEE_ID = USR.EMPLOYEE_ID__C
    ORDER BY CASE_NUMBER, CC.CREATEDATE
)

   , FULL_CASE AS (
    SELECT CASE_NUMBER
         , ANY_VALUE(CASE_OWNER)           AS CASE_OWNER
         , ANY_VALUE(SUBJECT1)             AS SUBJECT
         , ANY_VALUE(SYSTEM_SIZE)          AS SYSTEM_SIZE
         , ANY_VALUE(SYSTEM_VALUE)         AS SYSTEM_VALUE
         , ANY_VALUE(STATUS2)              AS STATUS1
         , ANY_VALUE(CUSTOMER_TEMPERATURE) AS CUSTOMER_TEMPERATURE
         , ANY_VALUE(AGE_FOR_CASE)         AS AGE_FOR_CASE
         , ANY_VALUE(CREATED_DATE1)        AS CREATED_DATE
         , ANY_VALUE(CLOSED_DATE1)         AS CLOSED_DATE
         , ANY_VALUE(RECORD_TYPE1)         AS RECORD_TYPE1
         , DATEDIFF(dd,
                    TO_DATE(CREATED_DATE),
                    NVL(TO_DATE(CLOSED_DATE),
                        CURRENT_DATE))     AS CASE_AGE
         , SOLAR_BILLING_ACCOUNT_NUMBER1
         , AVG(GAP)                        AS AVERAGE_GAP
    FROM CASE_HISTORY_TABLE
    GROUP BY SOLAR_BILLING_ACCOUNT_NUMBER1
           , CASE_NUMBER
    ORDER BY SOLAR_BILLING_ACCOUNT_NUMBER1
           , CASE_NUMBER
)

   , ER_AGENTS AS (
    SELECT EMPLOYEE_ID
         , ANY_VALUE(COST_CENTER)                   AS COST_CENTER
         , ANY_VALUE(FULL_NAME)                     AS FULL_NAME
         , ANY_VALUE(POSITION_TITLE)                AS POSITION_TITLE
         , ANY_VALUE(SUPERVISOR)                    AS DIRECT_MANAGER
         , ANY_VALUE(SUPERVISORY_ORG)               AS SUPERVISORY_ORG
         , ANY_VALUE(HIRE_DATE1)                    AS HIRE_DATE
         , ANY_VALUE(TERMINATED)                    AS TERMINATED
         , ANY_VALUE(TERMINATION_DATE)              AS TERMINATION_DATE
         , ANY_VALUE(TERMINATION_CATEGORY)          AS TERMINATION_CATEGORY
         , ANY_VALUE(TERMINATION_REASON)            AS TERMINATION_REASON
         , MIN(TEAM_START_DATE)                     AS TEAM_START_DATE
         , MAX(TEAM_END_DATE1)                      AS TEAM_END_DATE
         , DATEDIFF('MM', HIRE_DATE, TEAM_END_DATE) AS MONTH_TENURE
         , MAX(RN)                                  AS RN
         , DIRECTOR_ORG
         , MAX(TRANSFER)                            AS TRANSFER
    FROM (
             SELECT HR.EMPLOYEE_ID
                  , HR.SUPERVISORY_ORG
                  , ANY_VALUE(COST_CENTER)                                                    AS COST_CENTER
                  , ANY_VALUE(POSITION_TITLE)                                                 AS POSITION_TITLE
                  , ANY_VALUE(HR.FULL_NAME)                                                   AS FULL_NAME
                  , ANY_VALUE(HR.SUPERVISOR_NAME_1)
                 || ' (' || ANY_VALUE(HR.SUPERVISOR_BADGE_ID_1) || ')'                        AS SUPERVISOR
                  , DATEDIFF('MM', ANY_VALUE(HR.HIRE_DATE),
                             NVL(ANY_VALUE(HR.TERMINATION_DATE), CURRENT_DATE()))             AS MONTH_TENURE1
                  , ANY_VALUE(HR.HIRE_DATE)                                                   AS HIRE_DATE1
                  , ANY_VALUE(HR.TERMINATED)                                                  AS TERMINATED
                  , ANY_VALUE(HR.TERMINATION_DATE)                                            AS TERMINATION_DATE
                  , ANY_VALUE(HR.TERMINATION_CATEGORY)                                        AS TERMINATION_CATEGORY
                  , ANY_VALUE(HR.TERMINATION_REASON)                                          AS TERMINATION_REASON
                  , MIN(HR.CREATED_DATE)                                                      AS TEAM_START_DATE
                  -- Begin custom fields IN the TABLE
                  , CASE
                        WHEN MAX(HR.EXPIRY_DATE) >= CURRENT_DATE() AND ANY_VALUE(HR.TERMINATION_DATE) IS NOT NULL
                            THEN ANY_VALUE(HR.TERMINATION_DATE)
                        WHEN MAX(HR.EXPIRY_DATE) >= ANY_VALUE(HR.TERMINATION_DATE)
                            THEN ANY_VALUE(HR.TERMINATION_DATE)
                        ELSE MAX(HR.EXPIRY_DATE) END                                          AS TEAM_END_DATE1
                  , ROW_NUMBER() OVER ( PARTITION BY HR.EMPLOYEE_ID ORDER BY TEAM_START_DATE) AS RN
                  , IFF(ANY_VALUE(HR.COST_CENTER_ID) IN ('3400', '3700', '4967-60'),
                        TRUE,
                        FALSE)                                                                AS DIRECTOR_ORG
                  , LEAD(DIRECTOR_ORG, 1, DIRECTOR_ORG) OVER (PARTITION BY HR.EMPLOYEE_ID
                 ORDER BY TEAM_START_DATE)                                                    AS NEXT_DIRECTOR
                  , CASE
                        WHEN ANY_VALUE(HR.TERMINATION_DATE) >= TEAM_END_DATE1 AND NOT NEXT_DIRECTOR THEN TRUE
                        WHEN DIRECTOR_ORG != NEXT_DIRECTOR THEN TRUE
                        ELSE FALSE END                                                        AS TRANSFER
             FROM HR.T_EMPLOYEE_ALL AS HR
             GROUP BY HR.EMPLOYEE_ID
                    , HR.SUPERVISORY_ORG
             ORDER BY HR.EMPLOYEE_ID
                    , TEAM_START_DATE DESC
         ) AS ENTIRE_HISTORY
    WHERE DIRECTOR_ORG
      AND TEAM_END_DATE1 >= TEAM_START_DATE
      AND ENTIRE_HISTORY.SUPERVISOR ILIKE '%208513%'
    GROUP BY EMPLOYEE_ID, DIRECTOR_ORG
)

   , TEST_RESULTS AS (
    SELECT ''
)

   , MAIN AS (
    SELECT CASE_NUMBER
         , CASE_OWNER
         , SUBJECT
         , SYSTEM_SIZE
         , SYSTEM_VALUE
         , STATUS1
         , AGE_FOR_CASE
         , TO_DATE(CREATED_DATE)                     AS CREATED_DATE
         , TO_DATE(CLOSED_DATE)                      AS CLOSED_DATE
         , DATE_TRUNC('MM', TO_DATE(FC.CLOSED_DATE)) AS CLOSED_MONTH
         , RECORD_TYPE1
         , SOLAR_BILLING_ACCOUNT_NUMBER1
         , CUSTOMER_TEMPERATURE
    FROM FULL_CASE AS FC
             LEFT OUTER JOIN ER_AGENTS AS ER
                             ON FC.CASE_OWNER = ER.FULL_NAME
    WHERE NOT ER.TERMINATED
      AND FC.CLOSED_DATE IS NOT NULL
      AND ER.TEAM_END_DATE >= CURRENT_DATE
      AND ER.POSITION_TITLE ILIKE '%EXECUTIVE%'
      AND ER.EMPLOYEE_ID NOT IN (210521, 202038)
      AND FC.SYSTEM_SIZE IS NOT NULL
      AND FC.CLOSED_DATE BETWEEN
        DATE_TRUNC('Y', CURRENT_DATE) AND
        LAST_DAY(DATEADD('MM', -1, CURRENT_DATE))
)

SELECT *
FROM MAIN