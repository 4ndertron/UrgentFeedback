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
         , NVL(C.EXECUTIVE_RESOLUTIONS_ACCEPTED, CC.CREATEDATE)                  AS ERA
         , DATE_TRUNC('D', ERA)                                                  AS ER_ACCEPTED_DAY
         , DATE_TRUNC('W', ERA)                                                  AS ER_ACCEPTED_WEEK
         , DATE_TRUNC('month', ERA)                                              AS ER_ACCEPTED_MONTH
         , CC.CREATEDATE
         , DATEDIFF(S,
                    CC.CREATEDATE,
                    NVL(LEAD(CC.CREATEDATE) OVER (PARTITION BY C.CASE_NUMBER
                        ORDER BY CC.CREATEDATE),
                        CURRENT_TIMESTAMP())) / (24 * 60 * 60)
                                                                                 AS GAP
         , ROW_NUMBER() OVER (PARTITION BY C.CASE_NUMBER ORDER BY CC.CREATEDATE) AS COVERAGE
         , IFF(
            CC.CREATEDATE >= DATEADD('D', -30, CURRENT_DATE()),
            DATEDIFF(S,
                     CC.CREATEDATE,
                     NVL(LEAD(CC.CREATEDATE) OVER (
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
        END                                                                      AS WIP_kpi
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
        END                                                                      AS CLOSED_15_DAY_SLA
         , CASE
               WHEN
                   C.CLOSED_DATE IS NOT NULL AND CASE_AGE <= 30
                   THEN
                   1
        END                                                                      AS CLOSED_30_DAY_SLA
    FROM RPT.T_CASE AS C
             LEFT OUTER JOIN
         RPT.V_SF_CASECOMMENT AS CC
         ON C.CASE_ID = CC.PARENTID
             LEFT JOIN
         RPT.T_SERVICE AS S
         ON S.SERVICE_ID = C.SERVICE_ID
    WHERE (RECORD_TYPE = 'Solar - Customer Escalation'
        AND EXECUTIVE_RESOLUTIONS_ACCEPTED IS NOT NULL
        AND SUBJECT NOT ILIKE '[NPS]%'
        AND SUBJECT NOT ILIKE '%VIP%'
        AND ORIGIN != 'NPS')
       OR S.SERVICE_NAME IN
          ('S-2594389S', 'S-2698033S', 'S-2747837S', 'S-2786055S', 'S-3342623S', 'S-3404713S', 'S-3456975S',
           'S-3504359S', 'S-3547131S', 'S-3584006S', 'S-3654413', 'S-3696176', 'S-3738658', 'S-3755738', 'S-3777046',
           'S-3777969', 'S-3792484', 'S-3825780', 'S-3882409', 'S-4116378', 'S-4123071', 'S-4163168', 'S-4167427',
           'S-4241242', 'S-4274306', 'S-4353148', 'S-4409576', 'S-4419580', 'S-4424965', 'S-4581661', 'S-4589735',
           'S-4611213', 'S-4655882', 'S-4659855', 'S-4673442', 'S-4674276', 'S-4678871', 'S-4684539', 'S-4696062',
           'S-4705941', 'S-4706168', 'S-4760459', 'S-4784041', 'S-4789750', 'S-4843946', 'S-4876226', 'S-4890180',
           'S-4894004', 'S-4896772', 'S-4942996', 'S-4961513', 'S-5013934', 'S-5036787', 'S-5065771', 'S-5166266',
           'S-5249060', 'S-5266113', 'S-5277676', 'S-5279905', 'S-5280637', 'S-5304616', 'S-5332373', 'S-5342134',
           'S-5363434', 'S-5395325', 'S-5403838', 'S-5437036', 'S-5437451', 'S-5443793', 'S-5444941', 'S-5445374',
           'S-5508218', 'S-5533018', 'S-5555922', 'S-5575502', 'S-5579020', 'S-5586318', 'S-5590700', 'S-5656454',
           'S-5685061', 'S-5697181', 'S-5734010', 'S-5800896', 'S-5806896', 'S-5807860', 'S-5815580', 'S-5820472',
           'S-5823544', 'S-5824820', 'S-5834195', 'S-5840717', 'S-5847658', 'S-5849091', 'S-5849814', 'S-5850905',
           'S-5852804', 'S-5854806', 'S-5857562', 'S-5862387', 'S-5863477', 'S-5871962', 'S-5875238', 'S-5876533',
           'S-5877631', 'S-5878382', 'S-5882018', 'S-5891630', 'S-5903730', 'S-5903845', 'S-5913186', 'S-5913340',
           'S-5916848', 'S-5929485', 'S-5932574', 'S-5934675', 'S-5937866', 'S-5937984', 'S-5952354', 'S-5956162',
           'S-5957701', 'S-5958634', 'S-5961043', 'S-5964786', 'S-5965802', 'S-5973449', 'S-5981933', 'S-5985877',
           'S-5996665', 'S-6002377', 'S-6007954', 'S-6008019', 'S-6013867', 'S-6014845', 'S-6018125', 'S-6023012',
           'S-6025786', 'S-6025908', 'S-6036779', 'S-6038002', 'S-6038152', 'S-6046548', 'S-6056509', 'S-6090681',
           'S-6098495', 'S-6111463')
--       AND STATUS != 'In Dispute'
)

   , T2 AS (
    SELECT T1.*
         , P.SERVICE_STATE
         , P.SERVICE_NAME
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
         , ANY_VALUE(SERVICE_NAME)                 AS SERVICE_NUMBER
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
         , ANY_VALUE(SERVICE_NUMBER)                                                                 AS SERVICE_NUMBER
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

SELECT TO_DATE(CREATED_DATE) AS CREATED_DATE
     , CASE_NUMBER
     , SERVICE_NUMBER
     , SERVICE_STATE
     , OWNER
     , ORIGIN
     , PRIORITY_BUCKET
FROM MERGE
WHERE CREATED_DATE >= '2019-08-26'