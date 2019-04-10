WITH T1 AS (
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
      AND C.CREATED_DATE >= DATEADD('y', -1, DATE_TRUNC('month', CURRENT_DATE()))
),

     T2 AS (
         SELECT T1.*
              , P.SERVICE_STATE
              , P.SOLAR_BILLING_ACCOUNT_NUMBER
              , CASE
                    WHEN T1.ORIGIN IN ('BBB') THEN T1.ORIGIN
                    ELSE P.SERVICE_STATE
             END                                       AS PRIORITY_CASE_BUCKET
              , CAD.SYSTEM_SIZE
              , ROUND((CAD.SYSTEM_SIZE * 1000) * 4, 2) AS SYSTEM_VALUE
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
     ),

     T3 AS (
         SELECT CREATED_DATE
              , ANY_VALUE(CASE_NUMBER)                  AS CASE_NUMBER
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

SELECT *
FROM T3
ORDER BY 1