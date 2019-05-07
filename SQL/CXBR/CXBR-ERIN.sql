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
    WHERE D.DT BETWEEN DATEADD('y', -2, DATE_TRUNC('MM', CURRENT_DATE()))
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
     , YEAR(MONTH_1)                                                                     AS YEAR_1
     , SUM(CASE WHEN PRIORITY_BUCKET = 'Executive/News Media' THEN PRIORITY_CREATED END) AS EXECUTIVE
     , SUM(CASE WHEN PRIORITY_BUCKET = 'Legal/BBB' THEN PRIORITY_CREATED END)            AS LEGAL
     , EXECUTIVE + LEGAL                                                                 AS TOTAL
FROM CORE_TABLE AS CT
WHERE PRIORITY_BUCKET IN ('Executive/News Media', 'Legal/BBB')
GROUP BY MONTH_1
ORDER BY MONTH_1