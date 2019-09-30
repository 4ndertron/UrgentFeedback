WITH ER_CASES AS (
    SELECT C.PROJECT_ID
         , C.CASE_NUMBER
         , C.STATUS
         , C.DESCRIPTION
         , C.EXECUTIVE_RESOLUTIONS_ACCEPTED AS CREATED_DATE
         , C.CLOSED_DATE
         , CASE
               WHEN C.EXECUTIVE_RESOLUTIONS_ACCEPTED IS NOT NULL
                   AND A.SUPERVISOR_BADGE_ID_1 = 124126
                   AND C.OWNER_EMPLOYEE_ID NOT IN ('204095')
                   AND NOT A.TERMINATED
                   AND (A.EXPIRY_DATE >= CURRENT_DATE OR A.EXPIRY_DATE IS NULL)
                   THEN 'Executive Resolutions - Tier I'
               WHEN C.EXECUTIVE_RESOLUTIONS_ACCEPTED IS NOT NULL
                   AND A.SUPERVISOR_BADGE_ID_1 = 208513
                   AND C.OWNER_EMPLOYEE_ID NOT IN ('202038', '210521', '119688', '213011')
                   AND NOT A.TERMINATED
                   AND (A.EXPIRY_DATE >= CURRENT_DATE OR A.EXPIRY_DATE IS NULL)
                   AND NOT A.TERMINATED
                   THEN 'Executive Resolutions - Tier II'
        END                                 AS CASE_BUCKET
         , C.OWNER_EMPLOYEE_ID
         , A.FULL_NAME
    FROM RPT.T_CASE AS C
             LEFT OUTER JOIN HR.T_EMPLOYEE_ALL AS A
                             ON A.EMPLOYEE_ID = C.OWNER_EMPLOYEE_ID
    WHERE C.RECORD_TYPE = 'Solar - Customer Escalation'
      AND C.EXECUTIVE_RESOLUTIONS_ACCEPTED IS NOT NULL
      AND C.STATUS != 'In Dispute'
      AND C.SUBJECT NOT ILIKE '%VIP%'
      AND C.SUBJECT NOT ILIKE '%COMP%'
      AND C.SUBJECT NOT ILIKE '%NPS%'
)

   , ANNUAL_REVIEW AS (
    /*
     TODO: inflow = 12 months after the resolution accepted date
        The Resolution Accepted date is not in the warehouse right now.
     TODO: outflow = case closed date.
     */
    SELECT C.PROJECT_ID
         , C.CASE_NUMBER
         , C.STATUS
         , C.DESCRIPTION
         , C.CREATED_DATE AS CREATED_DATE
         , C.CLOSED_DATE
    FROM RPT.T_CASE AS C
    WHERE C.RECORD_TYPE = 'Solar - Customer Escalation'
      AND C.EXECUTIVE_RESOLUTIONS_ACCEPTED IS NULL
      AND C.DEPARTMENT = 'Executive Resolutions'
      AND C.SOLAR_QUEUE = 'Advocate Response'
)

   , DEFAULT_CASES AS (
    SELECT C.PROJECT_ID
         , C.CASE_NUMBER
         , C.STATUS
         , C.DESCRIPTION
         , C.CREATED_DATE
         , C.CLOSED_DATE
         , CASE
               WHEN C.CREATED_DATE IS NOT NULL
                   AND C.PRIMARY_REASON IN ('Foreclosure', 'Deceased')
                   THEN 'Pre-default'
               WHEN C.CREATED_DATE IS NOT NULL
                   AND C.PRIMARY_REASON NOT IN ('Foreclosure', 'Deceased')
                   THEN 'Default'
        END AS CASE_BUCKET
         , C.OWNER_EMPLOYEE_ID
         , A.FULL_NAME
    FROM RPT.T_CASE AS C
             LEFT OUTER JOIN HR.T_EMPLOYEE_ALL AS A
                             ON A.EMPLOYEE_ID = C.OWNER_EMPLOYEE_ID
    WHERE C.RECORD_TYPE = 'Solar - Customer Default'
      AND C.SUBJECT NOT ILIKE '%D3%'
)

   , SYSTEM_DAMAGE_CASES AS (
    SELECT C.PROJECT_ID
         , C.CASE_NUMBER
         , C.STATUS
         , C.DESCRIPTION
         , C.CREATED_DATE
         , C.CLOSED_DATE
         , CASE
               WHEN C.CREATED_DATE IS NOT NULL
                   THEN 'Executive Resolutions - Tier I'
        END AS CASE_BUCKET
         , C.OWNER_EMPLOYEE_ID
         , A.FULL_NAME
    FROM RPT.T_CASE AS C
             LEFT OUTER JOIN HR.T_EMPLOYEE_ALL AS A
                             ON A.EMPLOYEE_ID = C.OWNER_EMPLOYEE_ID
    WHERE C.RECORD_TYPE = 'Solar - Service'
      AND C.PRIMARY_REASON = 'System Damage'
)

   , RELOCATION_CASES AS (
    SELECT C.PROJECT_ID
         , C.CASE_NUMBER
         , C.STATUS
         , C.DESCRIPTION
         , C.CREATED_DATE
         , C.CLOSED_DATE
         , CASE
               WHEN C.CREATED_DATE IS NOT NULL
                   THEN 'Executive Resolutions - Tier II'
        END AS CASE_BUCKET
         , C.OWNER_EMPLOYEE_ID
         , A.FULL_NAME
    FROM RPT.T_CASE AS C
             LEFT OUTER JOIN HR.T_EMPLOYEE_ALL AS A
                             ON A.EMPLOYEE_ID = C.OWNER_EMPLOYEE_ID
    WHERE C.RECORD_TYPE = 'Solar - Transfer'
      AND C.PRIMARY_REASON = 'Relocation'
      AND C.OWNER_EMPLOYEE_ID IN ('210140')
)

   , PRE_DEFAULT_CASES AS (
    SELECT C.PROJECT_ID
         , C.CASE_NUMBER
         , C.STATUS
         , C.DESCRIPTION
         , C.CREATED_DATE
         , C.CLOSED_DATE
         , CASE
               WHEN C.CREATED_DATE IS NOT NULL
                   THEN 'Risk Mitigation'
        END AS CASE_BUCKET
         , C.OWNER_EMPLOYEE_ID
         , A.FULL_NAME
    FROM RPT.T_CASE AS C
             LEFT OUTER JOIN HR.T_EMPLOYEE_ALL AS A
                             ON A.EMPLOYEE_ID = C.OWNER_EMPLOYEE_ID
    WHERE c.RECORD_TYPE IN
          ('Solar - Customer Default', 'Solar - Billing', 'Solar - Panel Removal', 'Solar - Service',
           'Solar Damage Resolutions', 'Solar - Customer Escalation', 'Solar - Troubleshooting')
      AND C.SOLAR_QUEUE = 'Dispute/Evasion'
)

   , CASES_OVERALL AS (
    SELECT *
    FROM (
                 (SELECT * FROM ER_CASES)
                 UNION
                 (SELECT * FROM DEFAULT_CASES)
                 UNION
                 (SELECT * FROM PRE_DEFAULT_CASES)
                 UNION
                 (SELECT * FROM RELOCATION_CASES)
                 UNION
                 (SELECT * FROM SYSTEM_DAMAGE_CASES)
         )
    WHERE CASE_BUCKET IS NOT NULL
)

   , DAY_METRICS AS (
    SELECT D.DT
         , CA.CASE_BUCKET
         , COUNT(CASE WHEN TO_DATE(CREATED_DATE) = D.DT THEN 1 END) AS INFLOW
         , COUNT(CASE WHEN TO_DATE(CLOSED_DATE) = D.DT THEN 1 END)  AS OUTFLOW
         , INFLOW - OUTFLOW                                         AS NETFLOW
         , COUNT(CASE
                     WHEN CREATED_DATE <= D.DT AND
                          (CLOSED_DATE > D.DT OR
                           CLOSED_DATE IS NULL)
                         THEN 1 END)                                AS WIP
    FROM RPT.T_DATES AS D
       , CASES_OVERALL AS CA
    WHERE D.DT BETWEEN
              '2019-08-01' AND
              CURRENT_DATE
    GROUP BY D.DT,
             CA.CASE_BUCKET
    ORDER BY CASE_BUCKET
           , D.DT
)

   , ACTIVE_DEFAULT_BUCKET AS (
    SELECT CASE
               WHEN CO.DESCRIPTION ILIKE '%MBW%'
                   THEN 'TPC'
               WHEN CO.STATUS ILIKE '%IN PROG%'
                   THEN 'Letters'
               ELSE 'Actively Working'
        END AS DEFAULT_BUCKET
         , count(*)
    FROM CASES_OVERALL AS CO
    WHERE CLOSED_DATE IS NULL
      AND CASE_BUCKET = 'Default'
    GROUP BY DEFAULT_BUCKET
)

   , MAIN AS (
    SELECT *
    FROM DAY_METRICS
)

   , TEST_CTE AS (
    SELECT *
    FROM ANNUAL_REVIEW
)

SELECT *
FROM TEST_CTE