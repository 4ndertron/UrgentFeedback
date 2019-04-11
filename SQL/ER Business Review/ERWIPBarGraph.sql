WITH AGENTS AS (
    SELECT HR.FULL_NAME
         , MIN(HR.CREATED_DATE) AS TEAM_START_DATE
         , MAX(HR.EXPIRY_DATE)  AS TEAM_END_DATE
    FROM HR.T_EMPLOYEE_ALL AS HR
    WHERE HR.SUPERVISORY_ORG = 'Executive Resolutions'
    GROUP BY HR.FULL_NAME
    ORDER BY TEAM_START_DATE DESC
)

   , PAYMENTS AS (
    SELECT PMT.CREATED_BY
         , PMT.CREATED_DATE
         , PMT.NAME
         , C.CASE_NUMBER
         , P.PROJECT_NUMBER
         , P.PROJECT_ID
         , PMT.RECORD_TYPE
         , PMT.PAYMENT_TYPE
         , PMT.PAYMENT_AMOUNT
         , PMT.APPROVAL_DATE
    FROM RPT.T_PAYMENT AS PMT
             LEFT OUTER JOIN
         AGENTS AS A
         ON PMT.CREATED_BY = A.FULL_NAME
             LEFT OUTER JOIN
         RPT.T_PROJECT AS P
         ON P.PROJECT_ID = PMT.PROJECT_ID
             LEFT OUTER JOIN
         RPT.T_CASE AS C
         ON C.CASE_ID = PMT.CASE_ID
    WHERE PMT.CREATED_DATE >= A.TEAM_START_DATE
      AND PMT.CREATED_DATE <= A.TEAM_END_DATE
      AND A.FULL_NAME IS NOT NULL
      AND PMT.RECORD_TYPE = 'Customer Compensation'
      AND PMT.APPROVAL_DATE IS NOT NULL
)

   , CANCELLED AS (
    SELECT P.PROJECT_NUMBER
         , ANY_VALUE(P.PROJECT_ID)        AS PROJECT_ID
         , ANY_VALUE(CAD.SYSTEM_SIZE)     AS THIS_SIZE
         , ROUND(THIS_SIZE * 1000 * 7, 2) AS SYSTEM_VALUE
    FROM RPT.T_PROJECT AS P
             LEFT OUTER JOIN
         RPT.T_SYSTEM_DETAILS_SNAP AS CAD
         ON CAD.PROJECT_ID = P.PROJECT_ID
    WHERE P.PROJECT_STATUS = 'Cancelled'
      AND P.INSTALLATION_COMPLETE IS NOT NULL
    GROUP BY P.PROJECT_NUMBER
    ORDER BY P.PROJECT_NUMBER
)

   , T1 AS (
    SELECT C.CASE_NUMBER
         , C.ORIGIN
         , C.STATUS
         , C.PROJECT_ID                                                                            AS PID
         , CASE
               WHEN C.ORIGIN IN ('Executive', 'News Media') THEN 'Executive/News Media'
               WHEN C.ORIGIN IN ('BBB', 'Legal') THEN 'Legal/BBB'
               WHEN C.ORIGIN IN ('Online Review') THEN 'Online Review'
               WHEN C.ORIGIN IN ('Social Media') THEN 'Social Media'
               ELSE 'Internal'
        END                                                                                        AS PRIORITY_BUCKET
         , CASE
               WHEN C.ORIGIN IN ('Executive', 'News Media') THEN 'Executive/News Media'
               WHEN C.ORIGIN IN ('BBB', 'Legal') THEN 'Legal/BBB'
               WHEN C.ORIGIN IN ('Online Review') THEN 'Online Review'
               WHEN C.ORIGIN IN ('Social Media') THEN 'Social Media'
               ELSE 'Internal'
        END                                                                                        AS PRIORITY_TABLE
         , C.OWNER
         , C.OWNER_ID
         , C.CREATED_DATE
         , DATE_TRUNC('D', C.CREATED_DATE)                                                         AS DAY_CREATED
         , DATE_TRUNC('W', C.CREATED_DATE)                                                         AS WEEK_CREATED
         , DATE_TRUNC('month', C.CREATED_DATE)                                                     AS MONTH_CREATED
         , C.CLOSED_DATE
         , DATE_TRUNC('D', C.CLOSED_DATE)                                                          AS DAY_CLOSED
         , DATE_TRUNC('W', C.CLOSED_DATE)                                                          AS WEEK_CLOSED
         , DATE_TRUNC('month', C.CLOSED_DATE)                                                      AS MONTH_CLOSED
         , NVL(C.EXECUTIVE_RESOLUTIONS_ACCEPTED, CC.CREATEDATE)                                    AS ERA
         , DATE_TRUNC('D', ERA)                                                                    AS ER_ACCEPTED_DAY
         , DATE_TRUNC('W', ERA)                                                                    AS ER_ACCEPTED_WEEK
         , DATE_TRUNC('month', ERA)                                                                AS ER_ACCEPTED_MONTH
         , CC.CREATEDATE
         , DATEDIFF(S,
                    CC.CREATEDATE,
                    NVL(LEAD(CC.CREATEDATE) OVER(PARTITION BY C.CASE_NUMBER
                             ORDER BY CC.CREATEDATE),
                        CURRENT_TIMESTAMP())) / (24 * 60 * 60)
                                                                                                   AS GAP
         , ROW_NUMBER() OVER(PARTITION BY C.CASE_NUMBER ORDER BY CC.CREATEDATE)                    AS COVERAGE
         , IFF(
            CC.CREATEDATE >= DATEADD('D', -30, CURRENT_DATE()),
            DATEDIFF(S,
                     CC.CREATEDATE,
                     NVL(LEAD(CC.CREATEDATE) OVER(
                              PARTITION BY C.CASE_NUMBER
                              ORDER BY CC.CREATEDATE
                             ),
                         CURRENT_TIMESTAMP())) / (24 * 60 * 60),
            NULL
        )
                                                                                                   AS LAST_30_DAY_GAP
         , IFF(CC.CREATEDATE >= DATEADD('D', -30, CURRENT_DATE()),
               1,
               NULL)
                                                                                                   AS LAST_30_DAY_COVERAGE_TALLY
         , CC.CREATEDBYID
         , CASE
               WHEN
                       c.CREATED_DATE IS NOT NULL
                       AND
                       c.CLOSED_DATE IS NULL
                   THEN
                   1
        END                                                                                        AS WIP_kpi
         , CASE
               WHEN
                   CREATEDBYID = OWNER_ID
                   THEN
                           DATEDIFF(S, C.CREATED_DATE, nvl(cc.CREATEDATE, CURRENT_TIMESTAMP())) / (60 * 60)
                       - (DATEDIFF(WK, C.CREATED_DATE, CC.CREATEDATE) * 2)
                       - (CASE WHEN DAYNAME(C.CREATED_DATE) = 'Sun' THEN 24 ELSE 0 END)
                       - (CASE WHEN DAYNAME(C.CREATED_DATE) = 'Sat' THEN 24 ELSE 0 END)
               ELSE
                       DATEDIFF('S', C.CREATED_DATE, nvl(cc.CREATEDATE, CURRENT_TIMESTAMP())) / (60 * 60)
        END                                                                                        AS HOURLY_RESPONSE_TAT
         , DATEDIFF('s', C.CREATED_DATE, NVL(C.CLOSED_DATE, CURRENT_TIMESTAMP())) / (24 * 60 * 60) AS CASE_AGE
         , CASE
               WHEN
                   HOURLY_RESPONSE_TAT <= 24
                   THEN
                   1
        END                                                                                        AS RESPONSE_SLA
         , CASE
               WHEN
                   HOURLY_RESPONSE_TAT <= 2
                   THEN
                   1
        END                                                                                        AS PRIORITY_RESPONSE_SLA
         , CASE
               WHEN
                   C.CLOSED_DATE IS NOT NULL AND CASE_AGE <= 15
                   THEN
                   1
        END                                                                                        AS CLOSED_15_DAY_SLA
         , CASE
               WHEN
                   C.CLOSED_DATE IS NOT NULL AND CASE_AGE <= 30
                   THEN
                   1
        END                                                                                        AS CLOSED_30_DAY_SLA
    FROM RPT.T_CASE AS C
             LEFT OUTER JOIN
         RPT.V_SF_CASECOMMENT AS CC
         ON C.CASE_ID = CC.PARENTID
    WHERE RECORD_TYPE = 'Solar - Customer Escalation'
      AND EXECUTIVE_RESOLUTIONS_ACCEPTED IS NOT NULL
      AND SUBJECT NOT ILIKE '[NPS]%'
      AND SUBJECT NOT ILIKE '%VIP%'
      AND ORIGIN != 'NPS'
      AND STATUS != 'In Dispute'
)

   , T2 AS (
    SELECT T1.*
         , P.SERVICE_STATE
         , P.PROJECT_ID
         , P.SOLAR_BILLING_ACCOUNT_NUMBER
         , CASE
               WHEN T1.ORIGIN IN ('BBB') THEN T1.ORIGIN
               ELSE P.SERVICE_STATE
        END                                       AS PRIORITY_CASE_BUCKET
         , CAD.SYSTEM_SIZE
         , ROUND((CAD.SYSTEM_SIZE * 1000) * 7, 2) AS SYSTEM_VALUE
    FROM T1
             LEFT OUTER JOIN
         RPT.T_PROJECT AS P
         ON P.PROJECT_ID = T1.PID

             LEFT OUTER JOIN
         RPT.T_SYSTEM_DETAILS_SNAP AS CAD
         ON CAD.PROJECT_ID = T1.PID

             LEFT OUTER JOIN
         RPT.T_PAYMENT AS PMT
         ON PMT.PROJECT_ID = T1.PID
)

   , T3 AS (
    SELECT CREATED_DATE
         , ANY_VALUE(CASE_NUMBER)                  AS CASE_NUMBER
         , ANY_VALUE(PROJECT_ID)                   AS PROJECT_ID
         , ANY_VALUE(STATUS)                       AS CASE_STATUS
         , ANY_VALUE(SOLAR_BILLING_ACCOUNT_NUMBER) AS SOLAR_BILLING_ACCOUNT_NUMBER
         , ANY_VALUE(SYSTEM_SIZE)                  AS SYSTEM_SIZE
         , ANY_VALUE(SYSTEM_VALUE)                 AS SYSTEM_VALUE
         , ANY_VALUE(SERVICE_STATE)                AS SERVICE_STATE
         , ANY_VALUE(OWNER)                        AS OWNER
         , ANY_VALUE(ORIGIN)                       AS ORIGIN
         , ANY_VALUE(PRIORITY_BUCKET)              AS PRIORITY_BUCKET
         , ANY_VALUE(PRIORITY_TABLE)               AS PRIORITY_TABLE
         , ANY_VALUE(DAY_CREATED)                  AS DAY_CREATED
         , ANY_VALUE(WEEK_CREATED)                 AS WEEK_CREATED
         , ANY_VALUE(MONTH_CREATED)                AS MONTH_CREATED
         , ANY_VALUE(CLOSED_DATE)                  AS CLOSED_DATE
         , ANY_VALUE(DAY_CLOSED)                   AS DAY_CLOSED
         , ANY_VALUE(WEEK_CLOSED)                  AS WEEK_CLOSED
         , ANY_VALUE(MONTH_CLOSED)                 AS MONTH_CLOSED
         , ANY_VALUE(ERA)                          AS EXECUTIVE_RESOLUTIONS_ACCEPTED
         , ANY_VALUE(ER_ACCEPTED_DAY)              AS ER_ACCEPTED_DAY
         , ANY_VALUE(ER_ACCEPTED_WEEK)             AS ER_ACCEPTED_WEEK
         , ANY_VALUE(ER_ACCEPTED_MONTH)            AS ER_ACCEPTED_MONTH
         , ANY_VALUE(CASE_AGE)                     AS CASE_AGE
         , ANY_VALUE(PRIORITY_CASE_BUCKET)         AS PRIORITY_CASE_BUCKET
         , MAX(WIP_KPI)                            AS WIP_KPI
         , AVG(GAP)                                AS CASE_AVERAGE_GAP
         , MAX(COVERAGE)                           AS CASE_COVERAGE
         , AVG(LAST_30_DAY_GAP)                    AS AVERAGE_30_DAY_GAP
         , SUM(LAST_30_DAY_COVERAGE_TALLY)         AS AVERAGE_30_DAY_COVERAGE
         , MAX(RESPONSE_SLA)                       AS RESPONSE_SLA
         , MAX(PRIORITY_RESPONSE_SLA)              AS PRIORITY_RESPONSE_SLA
         , MAX(CLOSED_15_DAY_SLA)                  AS CLOSED_15_DAY_SLA
         , MAX(CLOSED_30_DAY_SLA)                  AS CLOSED_30_DAY_SLA
         , MIN(HOURLY_RESPONSE_TAT)                AS HOURLY_RESPONSE_TAT
    FROM T2
    GROUP BY CREATED_DATE
)

   , MERGE AS (
    SELECT T3.CREATED_DATE
         , ANY_VALUE(T3.CASE_NUMBER)                                                                 AS CASE_NUMBER
         , ANY_VALUE(T3.PROJECT_ID)                                                                  AS PROJECT_ID
         , ANY_VALUE(T3.CASE_STATUS)                                                                 AS CASE_STATUS
         , ANY_VALUE(T3.SOLAR_BILLING_ACCOUNT_NUMBER)                                                AS SOLAR_BILLING_ACCOUNT_NUMBER
         , ANY_VALUE(T3.SYSTEM_SIZE)                                                                 AS SYSTEM_SIZE
         , ANY_VALUE(T3.SYSTEM_VALUE)                                                                AS SYSTEM_VALUE
         , ANY_VALUE(T3.SERVICE_STATE)                                                               AS SERVICE_STATE
         , ANY_VALUE(T3.OWNER)                                                                       AS OWNER
         , ANY_VALUE(T3.ORIGIN)                                                                      AS ORIGIN
         , ANY_VALUE(T3.PRIORITY_BUCKET)                                                             AS PRIORITY_BUCKET
         , ANY_VALUE(T3.PRIORITY_TABLE)                                                              AS PRIORITY_TABLE
         , ANY_VALUE(T3.DAY_CREATED)                                                                 AS DAY_CREATED
         , ANY_VALUE(T3.WEEK_CREATED)                                                                AS WEEK_CREATED
         , ANY_VALUE(T3.MONTH_CREATED)                                                               AS MONTH_CREATED
         , ANY_VALUE(T3.CLOSED_DATE)                                                                 AS CLOSED_DATE
         , ANY_VALUE(T3.DAY_CLOSED)                                                                  AS DAY_CLOSED
         , ANY_VALUE(T3.WEEK_CLOSED)                                                                 AS WEEK_CLOSED
         , ANY_VALUE(T3.MONTH_CLOSED)                                                                AS MONTH_CLOSED
         , ANY_VALUE(T3.EXECUTIVE_RESOLUTIONS_ACCEPTED)                                              AS EXECUTIVE_RESOLUTIONS_ACCEPTED
         , ANY_VALUE(T3.ER_ACCEPTED_DAY)                                                             AS ER_ACCEPTED_DAY
         , ANY_VALUE(T3.ER_ACCEPTED_WEEK)                                                            AS ER_ACCEPTED_WEEK
         , ANY_VALUE(T3.ER_ACCEPTED_MONTH)                                                           AS ER_ACCEPTED_MONTH
         , ANY_VALUE(T3.CASE_AGE)                                                                    AS CASE_AGE
         , ANY_VALUE(T3.PRIORITY_CASE_BUCKET)                                                        AS PRIORITY_CASE_BUCKET
         , ANY_VALUE(T3.WIP_KPI)                                                                     AS WIP_KPI
         , ANY_VALUE(T3.CASE_AVERAGE_GAP)                                                            AS CASE_AVERAGE_GAP
         , ANY_VALUE(T3.CASE_COVERAGE)                                                               AS CASE_COVERAGE
         , ANY_VALUE(T3.AVERAGE_30_DAY_GAP)                                                          AS AVERAGE_30_DAY_GAP
         , ANY_VALUE(T3.AVERAGE_30_DAY_COVERAGE)                                                     AS AVERAGE_30_DAY_COVERAGE
         , ANY_VALUE(T3.RESPONSE_SLA)                                                                AS RESPONSE_SLA
         , ANY_VALUE(T3.PRIORITY_RESPONSE_SLA)                                                       AS PRIORITY_RESPONSE_SLA
         , ANY_VALUE(T3.CLOSED_15_DAY_SLA)                                                           AS CLOSED_15_DAY_SLA
         , ANY_VALUE(T3.CLOSED_30_DAY_SLA)                                                           AS CLOSED_30_DAY_SLA
         , ANY_VALUE(T3.HOURLY_RESPONSE_TAT)                                                         AS HOURLY_RESPONSE_TAT
         , NVL(SUM(CASE WHEN P.CREATED_DATE >= T3.CREATED_DATE THEN P.PAYMENT_AMOUNT ELSE 0 END), 0) AS COMPENSATION
         , NVL(SUM(C.SYSTEM_VALUE), 0)                                                               AS CANCELLATION_COSTS
         , COMPENSATION + CANCELLATION_COSTS                                                         AS ESCALATION_COST
    FROM T3
             LEFT OUTER JOIN
         PAYMENTS AS P
         ON P.PROJECT_ID = T3.PROJECT_ID
             LEFT OUTER JOIN
         CANCELLED AS C
         ON C.PROJECT_ID = T3.PROJECT_ID
    GROUP BY T3.CREATED_DATE
)

   , T4 AS (
    SELECT D.DT
         , D.WEEK_DAY_NUM
         , SUM(CASE
                   WHEN T3.ER_ACCEPTED_DAY <= D.DT AND (T3.DAY_CLOSED >= D.DT OR T3.DAY_CLOSED IS NULL)
                       THEN 1 END)                                              AS ALL_WIP
         , SUM(CASE
                   WHEN T3.ER_ACCEPTED_DAY <= D.DT AND (T3.DAY_CLOSED >= D.DT OR T3.DAY_CLOSED IS NULL) AND
                        T3.PRIORITY_BUCKET = 'Executive/News Media' THEN 1 END) AS P1_WIP
         , SUM(CASE
                   WHEN T3.ER_ACCEPTED_DAY <= D.DT AND (T3.DAY_CLOSED >= D.DT OR T3.DAY_CLOSED IS NULL) AND
                        T3.PRIORITY_BUCKET = 'Legal/BBB' THEN 1 END)            AS P2_WIP
         , SUM(CASE
                   WHEN T3.ER_ACCEPTED_DAY <= D.DT AND (T3.DAY_CLOSED >= D.DT OR T3.DAY_CLOSED IS NULL) AND
                        T3.PRIORITY_BUCKET = 'Online Review' THEN 1 END)        AS P3_WIP
         , SUM(CASE
                   WHEN T3.ER_ACCEPTED_DAY <= D.DT AND (T3.DAY_CLOSED >= D.DT OR T3.DAY_CLOSED IS NULL) AND
                        T3.PRIORITY_BUCKET = 'Social Media' THEN 1 END)         AS P4_WIP
         , SUM(CASE
                   WHEN T3.ER_ACCEPTED_DAY <= D.DT AND (T3.DAY_CLOSED >= D.DT OR T3.DAY_CLOSED IS NULL) AND
                        T3.PRIORITY_BUCKET = 'Internal' THEN 1 END)             AS P5_WIP
    FROM MERGE AS T3
       , RPT.T_DATES AS D
    WHERE D.DT BETWEEN DATEADD('y', -1, DATE_TRUNC('month', CURRENT_DATE())) AND CURRENT_DATE()
    GROUP BY D.DT
           , D.WEEK_DAY_NUM
    ORDER BY D.DT
)

SELECT DT
     , ALL_WIP
     , P1_WIP AS EXECUTIVE_WIP
     , P2_WIP AS LEGAL_WIP
     , P3_WIP AS REVIEW_WIP
     , P4_WIP AS SOCIAL_WIP
     , P5_WIP AS INTERNAL_WIP
FROM T4
WHERE T4.DT = CURRENT_DATE
   OR T4.DT = LAST_DAY(T4.DT)