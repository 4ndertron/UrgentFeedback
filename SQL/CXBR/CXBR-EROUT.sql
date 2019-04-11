WITH CORE_TABLE AS (
    SELECT DATE_TRUNC('month', D.DT)                                  AS MONTH_1
         , CASE
               WHEN C.ORIGIN IN ('Executive', 'News Media') THEN 'Executive/News Media'
               WHEN C.ORIGIN IN ('BBB', 'Legal') THEN 'Legal/BBB'
               WHEN C.ORIGIN IN ('Online Review') THEN 'Online Review'
               WHEN C.ORIGIN IN ('Social Media') THEN 'Social Media'
               ELSE 'Internal'
        END                                                           AS PRIORITY_BUCKET
         , COUNT(CASE WHEN TO_DATE(C.CREATED_DATE) = D.DT THEN 1 END) AS PRIORITY_CREATED
         , COUNT(CASE WHEN TO_DATE(C.CLOSED_DATE) = D.DT THEN 1 END)  AS PRIORITY_CLOSED
         , PRIORITY_CREATED - PRIORITY_CLOSED                         AS NET
    FROM RPT.T_dates AS D,
         RPT.T_CASE AS c,
         RPT.T_PROJECT AS P
    WHERE D.DT BETWEEN DATEADD('y', -1, DATE_TRUNC('MM', CURRENT_DATE()))
        AND CURRENT_DATE()
      AND c.RECORD_TYPE = 'Solar - Customer Escalation'
      AND c.EXECUTIVE_RESOLUTIONS_ACCEPTED IS NOT NULL
      AND C.STATUS != 'In Dispute'
      AND P.PROJECT_ID = C.PROJECT_ID
    GROUP BY MONTH_1
           , PRIORITY_BUCKET
    ORDER BY PRIORITY_BUCKET
           , MONTH_1
)

SELECT MONTH_1
     , SUM(PRIORITY_CREATED) AS INFLOW
     , SUM(PRIORITY_CLOSED)  AS OUTFLOW
     , SUM(NET)              AS NETFLOW
FROM CORE_TABLE AS CT
GROUP BY MONTH_1
ORDER BY MONTH_1