WITH T1 AS (
    SELECT C.CASE_NUMBER
         , C.ORIGIN
         , C.PROJECT_ID                                                                            AS PID
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
         , C.EXECUTIVE_RESOLUTIONS_ACCEPTED
         , DATE_TRUNC('D', C.EXECUTIVE_RESOLUTIONS_ACCEPTED)                                       AS ER_ACCEPTED_DAY
         , DATE_TRUNC('W', C.EXECUTIVE_RESOLUTIONS_ACCEPTED)                                       AS ER_ACCEPTED_WEEK
         , DATE_TRUNC('month', C.EXECUTIVE_RESOLUTIONS_ACCEPTED)                                   AS ER_ACCEPTED_MONTH
         , CC.CREATEDATE
         , CC.CREATEDBYID
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
      AND STATUS != 'IN Dispute'
      AND C.CREATED_DATE >= DATEADD('y', -1, DATE_TRUNC('month', CURRENT_DATE()))
),

     T2 AS (
         SELECT T1.*
              , P.SERVICE_STATE
              , CASE
                    WHEN T1.ORIGIN IN ('BBB') THEN T1.ORIGIN
                    WHEN P.SERVICE_STATE IN ('NM', 'CA', 'NJ', 'NY', 'MA') THEN P.SERVICE_STATE
             END AS PRIORITY_BUCKET
         FROM T1
                  LEFT OUTER JOIN
              RPT.T_PROJECT AS P
              ON P.PROJECT_ID = T1.PID
     ),

     T3 AS (
         SELECT CREATED_DATE
              , ANY_VALUE(CASE_NUMBER)                    AS CASE_NUMBER
              , ANY_VALUE(SERVICE_STATE)                  AS SERVICE_STATE
              , ANY_VALUE(OWNER)                          AS OWNER
              , ANY_VALUE(ORIGIN)                         AS ORIGIN
              , ANY_VALUE(PRIORITY_BUCKET)                AS PRIORITY_BUCKET
              , ANY_VALUE(DAY_CREATED)                    AS DAY_CREATED
              , ANY_VALUE(WEEK_CREATED)                   AS WEEK_CREATED
              , ANY_VALUE(MONTH_CREATED)                  AS MONTH_CREATED
              , ANY_VALUE(CLOSED_DATE)                    AS CLOSED_DATE
              , ANY_VALUE(DAY_CLOSED)                     AS DAY_CLOSED
              , ANY_VALUE(WEEK_CLOSED)                    AS WEEK_CLOSED
              , ANY_VALUE(MONTH_CLOSED)                   AS MONTH_CLOSED
              , ANY_VALUE(EXECUTIVE_RESOLUTIONS_ACCEPTED) AS EXECUTIVE_RESOLUTIONS_ACCEPTED
              , ANY_VALUE(ER_ACCEPTED_DAY)                AS ER_ACCEPTED_DAY
              , ANY_VALUE(ER_ACCEPTED_WEEK)               AS ER_ACCEPTED_WEEK
              , ANY_VALUE(ER_ACCEPTED_MONTH)              AS ER_ACCEPTED_MONTH
              , ANY_VALUE(CASE_AGE)                       AS CASE_AGE
              , MAX(RESPONSE_SLA)                         AS RESPONSE_SLA
              , MAX(CLOSED_15_DAY_SLA)                    AS CLOSED_15_DAY_SLA
              , MAX(CLOSED_30_DAY_SLA)                    AS CLOSED_30_DAY_SLA
              , MIN(HOURLY_RESPONSE_TAT)                  AS HOURLY_RESPONSE_TAT
         FROM T2
         GROUP BY CREATED_DATE
     )

SELECT DATE_TRUNC('MM', D.DT)                            AS MONTH1_TREND
     , PRIORITY_BUCKET                                   AS PRIORITY_BUCKET_TREND
     , COUNT(CASE WHEN T3.DAY_CREATED = D.DT THEN 1 END) AS INFLOW
     , COUNT(CASE WHEN T3.DAY_CLOSED = D.DT THEN 1 END)  AS OUTFLOW
     , INFLOW - OUTFLOW                                  AS NET
FROM T3
   , RPT.T_DATES AS D
WHERE D.DT BETWEEN DATEADD('MM', -2, DATE_TRUNC('MM', CURRENT_DATE())) AND CURRENT_DATE()
  AND PRIORITY_BUCKET_TREND IS NOT NULL
GROUP BY MONTH1_TREND
       , PRIORITY_BUCKET_TREND
ORDER BY PRIORITY_BUCKET_TREND
       , MONTH1_TREND