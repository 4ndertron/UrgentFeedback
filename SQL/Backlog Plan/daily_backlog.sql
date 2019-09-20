WITH ER_CASES AS (
    SELECT C.PROJECT_ID
         , C.EXECUTIVE_RESOLUTIONS_ACCEPTED AS ERA
         , C.CLOSED_DATE
         , CASE
               WHEN C.CREATED_DATE IS NOT NULL
                   AND A.SUPERVISOR_BADGE_ID_1 = 124126
                   AND C.OWNER_EMPLOYEE_ID NOT IN ('204095')
                   AND NOT A.TERMINATED
                   AND (A.EXPIRY_DATE >= CURRENT_DATE OR A.EXPIRY_DATE IS NULL)
                   THEN 'Executive Resolutions - Tier I'
               WHEN C.CREATED_DATE IS NOT NULL
                   AND A.SUPERVISOR_BADGE_ID_1 = 208513
                   AND C.OWNER_EMPLOYEE_ID NOT IN ('202038', '210521', '119688', '213011')
                   AND NOT A.TERMINATED
                   AND (A.EXPIRY_DATE >= CURRENT_DATE OR A.EXPIRY_DATE IS NULL)
                   AND NOT A.TERMINATED
                   THEN 'Executive Resolutions - Tier II'
        END                                 AS CASE_BUCKET
         , C.OWNER_EMPLOYEE_ID
         , A.FULL_NAME
         , A.SUPERVISOR_NAME_1
         , A.SUPERVISOR_BADGE_ID_1
    FROM RPT.T_CASE AS C
             LEFT OUTER JOIN HR.T_EMPLOYEE_ALL AS A
                             ON A.EMPLOYEE_ID = C.OWNER_EMPLOYEE_ID
    WHERE C.RECORD_TYPE = 'Solar - Customer Escalation'
      AND C.STATUS != 'In Dispute'
      AND C.SUBJECT NOT ILIKE '%VIP%'
      AND C.SUBJECT NOT ILIKE '%COMP%'
      AND C.SUBJECT NOT ILIKE '%NPS%'
)

   , DEFAULT_CASES AS (
    SELECT C.PROJECT_ID
         , C.CREATED_DATE
         , C.CLOSED_DATE
         , CASE
               WHEN C.CREATED_DATE IS NOT NULL
                   AND C.PRIMARY_REASON IN ('Foreclosure', 'Deceased')
                   THEN 'Fore/Deceased'
               WHEN C.CREATED_DATE IS NOT NULL
                   AND C.PRIMARY_REASON NOT IN ('Foreclosure', 'Deceased')
                   THEN 'Default'
        END AS CASE_BUCKET
         , C.OWNER_EMPLOYEE_ID
         , A.FULL_NAME
         , A.SUPERVISOR_NAME_1
         , A.SUPERVISOR_BADGE_ID_1
    FROM RPT.T_CASE AS C
             LEFT OUTER JOIN HR.T_EMPLOYEE_ALL AS A
                             ON A.EMPLOYEE_ID = C.OWNER_EMPLOYEE_ID
    WHERE C.RECORD_TYPE = 'Solar - Customer Default'
      AND C.SUBJECT NOT ILIKE '%D3%'
      AND C.STATUS IN ('Pending Customer Action', 'Pending Corporate Action')
)

   , SYSTEM_DAMAGE_CASES AS (
       SELECT ''
)

   , RELOCATION_CASES AS (
       SELECT ''
)

   , PRE_DEFAULT_CASES AS (
       SELECT ''
)

   , MAIN AS (
    SELECT ''
)

   , TEST_CTE AS (
    SELECT DISTINCT FULL_NAME
                  , OWNER_EMPLOYEE_ID
                  , SUPERVISOR_NAME_1
    FROM DEFAULT_CASES
)

SELECT *
FROM TEST_CTE