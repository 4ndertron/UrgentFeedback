WITH ACTIVITY_LIST AS (
    SELECT T.SUBJECT
         , T.STATUS
         , T.PROJECT_ID
         , T.TASK_ID
         , T.TASK
         , TO_DATE(T.CREATED_DATE)                            AS TASK_CREATED
         , TO_DATE(NVL(T.CLOSE_DATE_TIME, T.CLOSE_DATE_TIME)) AS TASK_CLOSED
         , TO_DATE(P.CANCELLATION_DATE)                       AS CANCELLATION_DATE

    FROM RPT.T_TASK AS T
             LEFT JOIN RPT.T_PROJECT AS P
                       ON P.PROJECT_ID = T.PROJECT_ID
    WHERE T.SUBJECT ILIKE '%BK07%'
       OR T.SUBJECT ILIKE '%BK13%'
)

   , ACTIVITY_ION AS (
    SELECT IFF(LAST_DAY(D.DT) >= CURRENT_DATE, CURRENT_DATE, LAST_DAY(D.DT)) AS MONTH
--          , A.BK_BUCKET
         , COUNT(CASE
                     WHEN A.TASK_CREATED = D.DT
                         THEN 1 END)                                         AS BK_INFLOW
         , COUNT(CASE
                     WHEN A.TASK_CLOSED = D.DT
                         THEN 1 END)                                         AS BK_OUTFLOW
         , COUNT(CASE
                     WHEN A.TASK_CLOSED = D.DT
                         AND CANCELLATION_DATE IS NULL
                         THEN 1 END)                                         AS BK_CLOSED_WON
         , COUNT(CASE
                     WHEN A.TASK_CLOSED = D.DT
                         AND CANCELLATION_DATE IS NOT NULL
                         THEN 1 END)                                         AS BK_CLOSED_LOST
         , BK_INFLOW - BK_OUTFLOW                                            AS BK_NET
    FROM RPT.T_DATES AS D
       , ACTIVITY_LIST AS A
    WHERE D.DT BETWEEN
              DATE_TRUNC('Y', DATEADD('Y', -1, CURRENT_DATE)) AND
              CURRENT_DATE
    GROUP BY MONTH
    ORDER BY MONTH
)

   , ACTIVITY_WIP AS (
    SELECT D.DT
         , COUNT(CASE
                     WHEN A.TASK_CREATED <= D.DT AND
                          (A.TASK_CLOSED > D.DT OR A.TASK_CLOSED IS NULL)
                         THEN 1 END) AS BK_WIP
    FROM RPT.T_DATES AS D
       , ACTIVITY_LIST AS A
    WHERE D.DT BETWEEN
        DATE_TRUNC('Y', DATEADD('Y', -1, CURRENT_DATE)) AND
        CURRENT_DATE
    GROUP BY D.DT
    ORDER BY D.DT
)

   , MAIN AS (
    SELECT AI.MONTH
         , AI.BK_INFLOW
         , AI.BK_CLOSED_LOST
         , AI.BK_CLOSED_WON
         , AW.BK_WIP
    FROM ACTIVITY_ION AS AI
       , ACTIVITY_WIP AS AW
    WHERE AW.DT = AI.MONTH
    ORDER BY AI.MONTH
)

   , TEST_CTE AS (
    SELECT *
    FROM MAIN
)

SELECT *
FROM TEST_CTE