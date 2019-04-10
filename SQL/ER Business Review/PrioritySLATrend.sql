WITH T1 AS (
-- Collect the Cases, and their CASE Comments for the
-- Risk Management Business Report IN Microstrategy.
----------------- Start of the Core Query --------------------------
    SELECT C.CASE_NUMBER
         , C.ORIGIN
         , C.PROJECT_ID                                                                            AS PID
         , CASE
               WHEN C.ORIGIN IN ('Executive', 'News Media') THEN 'Executive/News Media'
               WHEN C.ORIGIN IN ('BBB', 'Legal') THEN 'Legal/BBB'
               WHEN C.ORIGIN IN ('Online Review') THEN 'Online Review'
               WHEN C.ORIGIN IN ('Social Media') THEN 'Social Media'
               ELSE 'Internal'
        END                                                                                        AS PRIORITY_BUCKET
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
                       PRIORITY_BUCKET LIKE '%Executive%'
                       AND
                       HOURLY_RESPONSE_TAT <= 4
                   THEN
                   1
               WHEN
                       PRIORITY_BUCKET LIKE '%Legal%'
                       AND
                       HOURLY_RESPONSE_TAT <= 24
                   THEN
                   1
               WHEN
                       PRIORITY_BUCKET LIKE '%Review%'
                       AND
                       HOURLY_RESPONSE_TAT <= 24
                   THEN
                   1
               WHEN
                       PRIORITY_BUCKET LIKE '%Social%'
                       AND
                       HOURLY_RESPONSE_TAT <= 24
                   THEN
                   1
               WHEN
                       PRIORITY_BUCKET LIKE '%Internal%'
                       AND
                       HOURLY_RESPONSE_TAT <= 24
                   THEN
                   1
        END                                                                                        AS RESPONSE_SLA
         , CASE
               WHEN
                   HOURLY_RESPONSE_TAT <= 4
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
-- Add the State to the TABLE
         SELECT T1.*
              , P.SERVICE_STATE
         FROM T1
                  LEFT OUTER JOIN
              RPT.T_PROJECT AS P
              ON P.PROJECT_ID = T1.PID
     ),

     T3 AS (
-- De-duplicate the CASE numbers, leaving the first comment date In the TABLE
         SELECT CREATED_DATE
              , ANY_VALUE(CASE_NUMBER)     AS CASE_NUMBER
              , ANY_VALUE(SERVICE_STATE)   AS SERVICE_STATE
              , ANY_VALUE(OWNER)           AS OWNER
              , ANY_VALUE(ORIGIN)          AS ORIGIN
              , ANY_VALUE(PRIORITY_BUCKET) AS PRIORITY_BUCKET
              , ANY_VALUE(DAY_CREATED)     AS DAY_CREATED
              , ANY_VALUE(WEEK_CREATED)    AS WEEK_CREATED
              , ANY_VALUE(MONTH_CREATED)   AS MONTH_CREATED
              , ANY_VALUE(CLOSED_DATE)     AS CLOSED_DATE
              , ANY_VALUE(DAY_CLOSED)      AS DAY_CLOSED
              , ANY_VALUE(WEEK_CLOSED)     AS WEEK_CLOSED
              , ANY_VALUE(MONTH_CLOSED)    AS MONTH_CLOSED
              , MIN(ERA)                   AS ERA
              , MIN(ER_ACCEPTED_DAY)       AS ER_ACCEPTED_DAY
              , MIN(ER_ACCEPTED_WEEK)      AS ER_ACCEPTED_WEEK
              , MIN(ER_ACCEPTED_MONTH)     AS ER_ACCEPTED_MONTH
              , MIN(CASE_AGE)              AS CASE_AGE
              , MAX(RESPONSE_SLA)          AS RESPONSE_SLA
              , MAX(PRIORITY_RESPONSE_SLA) AS PRIORITY_RESPONSE_SLA
              , MAX(CLOSED_15_DAY_SLA)     AS CLOSED_15_DAY_SLA
              , MAX(CLOSED_30_DAY_SLA)     AS CLOSED_30_DAY_SLA
              , MIN(HOURLY_RESPONSE_TAT)   AS HOURLY_RESPONSE_TAT
         FROM T2
         GROUP BY CREATED_DATE
         ORDER BY CREATED_DATE
     ),

----------------- End of the Core Query --------------------------


----------------- Start of the Goal-specific Query --------------------------
----------------- Objective: Find the 30 Day SLA   --------------------------
-----------------            Over the last 8 Weeks --------------------------

     T4 AS (
-- GROUP the TABLE BY a date, and calculate the total count, and count within SLA for
-- each of the PRIORITY_BUCKET values.
         SELECT D.DT
              , D.WEEK_DAY_NUM
              , PRIORITY_BUCKET
              , CASE
                    WHEN
                        PRIORITY_BUCKET = 'SOS' THEN NULL
                    ELSE
                        SUM(CASE WHEN T3.ER_ACCEPTED_DAY = D.DT THEN 1 END)
             END AS ALL_IN
              , CASE
                    WHEN
                        PRIORITY_BUCKET = 'SOS' THEN NULL
                    ELSE
                        SUM(CASE WHEN T3.ER_ACCEPTED_DAY = D.DT AND RESPONSE_SLA = 1 THEN 1 END)
             END AS RESPONSE_SLA_COUNT
         FROM T3
            , RPT.T_DATES AS D
         WHERE D.DT BETWEEN DATEADD('y', -1, DATE_TRUNC('month', CURRENT_DATE())) AND CURRENT_DATE()
         GROUP BY D.DT
                , D.WEEK_DAY_NUM
                , PRIORITY_BUCKET
         ORDER BY D.DT, D.WEEK_DAY_NUM, PRIORITY_BUCKET
     ),

     T5 AS (
----------  *********     Start of the result variance     *********     -----------------------
-- Add up all the PRIORITY_BUCKET VALUES over the last 30 days (for the 3 bucket VALUES)
-- and compare that to the number of cases within SLA over the past 30 days (for the 3 bucket VALUES)
         SELECT DT                                                           AS DT_LINE
              , WEEK_DAY_NUM
              , PRIORITY_BUCKET                                              AS PRIORITY_BUCKET_LINE
              , SUM(NVL(RESPONSE_SLA_COUNT, 0))
                    OVER(
                    PARTITION BY PRIORITY_BUCKET_LINE
                    ORDER BY DT_LINE
                    ROWS BETWEEN 30 PRECEDING AND CURRENT ROW
             )
                                                                             AS PRIORITY_LIST_SLA_LINE
              , SUM(NVL(ALL_IN, 0))
                    OVER(
                    PARTITION BY PRIORITY_BUCKET_LINE
                    ORDER BY DT_LINE
                    ROWS BETWEEN 30 PRECEDING AND CURRENT ROW
             )
                                                                             AS PRIORITY_LIST_COUNT_LINE
              , PRIORITY_LIST_SLA_LINE / NULLIF(PRIORITY_LIST_COUNT_LINE, 0) AS PRIORITY_LIST_RATIO_LINE
         FROM T4
--    WHERE
--        -- Monday Criteria
--    (DATE_PART('dw',CURRENT_DATE()) = 1
--        AND WEEK_DAY_NUM = 6)
--        OR
--        -- Non-Monday Criteria
--    (DATE_PART('dw',CURRENT_DATE()) != 1
--        AND WEEK_DAY_NUM = DATE_PART('dw',CURRENT_DATE()))
     )

-- Remove the Week Day Number FROM the final results
SELECT DT_LINE
     , PRIORITY_BUCKET_LINE
     , PRIORITY_LIST_SLA_LINE
     , PRIORITY_LIST_COUNT_LINE
     , PRIORITY_LIST_RATIO_LINE
FROM T5
WHERE DT_LINE >= DATE_TRUNC('WK', DATEADD('D', -56, CURRENT_DATE()))
ORDER BY DT_LINE
       , PRIORITY_BUCKET_LINE

----------------- End of the Goal-specific Query --------------------------