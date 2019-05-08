WITH CORE_TABLE AS (
    SELECT DATE_TRUNC('month', D.DT)                                  AS MONTH_1
         , CASE
               WHEN C.ORIGIN IN ('Executive', 'News Media') THEN 'Executive/News Media'
               WHEN C.ORIGIN IN ('BBB', 'Legal') THEN 'Legal/BBB'
               WHEN C.ORIGIN IN ('Online Review') THEN 'Online Review'
               WHEN C.ORIGIN IN ('Social Media') THEN 'Social Media'
               WHEN C.ORIGIN IN ('Credit Dispute') OR C.SUBJECT ILIKE '%CRED%' THEN 'Credit Dispute'
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

   , INFLOW_TABLE AS (
    SELECT MONTH_1
         , YEAR(MONTH_1)                                                                     AS YEAR_1
         , SUM(PRIORITY_CREATED)                                                             AS PRIORITY_CREATED
--          , SUM(PRIORITY_CLOSED)                                                              AS PRIORITY_CLOSED
         , SUM(CASE WHEN PRIORITY_BUCKET = 'Executive/News Media' THEN PRIORITY_CREATED END) AS EXECUTIVE
         , SUM(CASE WHEN PRIORITY_BUCKET = 'Legal/BBB' THEN PRIORITY_CREATED END)            AS LEGAL
         , SUM(CASE WHEN PRIORITY_BUCKET = 'Online Review' THEN PRIORITY_CREATED END)        AS REVIEW
         , SUM(CASE WHEN PRIORITY_BUCKET = 'Social Media' THEN PRIORITY_CREATED END)         AS SOCIAL
         , SUM(CASE WHEN PRIORITY_BUCKET = 'Credit Dispute' THEN PRIORITY_CREATED END)       AS CREDIT
         , SUM(CASE WHEN PRIORITY_BUCKET = 'Internal' THEN PRIORITY_CREATED END)             AS INTERNAL
         , EXECUTIVE + LEGAL                                                                 AS OVERVIEW_TOTAL
    FROM CORE_TABLE AS CT
    GROUP BY MONTH_1
    ORDER BY MONTH_1
)

SELECT *
FROM INFLOW_TABLE