WITH T1 AS (
    SELECT C.CREATED_DATE
         , C.CASE_NUMBER
         , C.STATUS
         , C.PRIMARY_REASON
         , CAD.SYSTEM_SIZE_ACTUAL_KW        AS SYSTEM_SIZE
         , ROUND(SYSTEM_SIZE * 1000 * 7, 2) AS SYSTEM_VALUE
    FROM RPT.T_CASE AS C
             LEFT OUTER JOIN
         RPT.T_CAD AS CAD
         ON CAD.PROJECT_ID = C.PROJECT_ID
    WHERE C.RECORD_TYPE = 'Solar - Customer Default'
      AND C.PRIMARY_REASON IN ('Customer Deceased', 'Foreclosure')
      AND C.CREATED_DATE >= '2018-12-01'
      AND CREATED_DATE < '2019-03-01'
      AND C.STATUS ILIKE '%CLOSE%'
      AND C.STATUS NOT ILIKE '%VOID%'
)

SELECT SUM(CASE WHEN STATUS = 'Closed - Saved' THEN 1 ELSE 0 END)     AS SAVED_CT
     , SUM(CASE WHEN STATUS = 'Closed - Saved' THEN SYSTEM_VALUE END) AS SAVED_VALUE
     , SUM(CASE WHEN STATUS = 'Closed' THEN 1 ELSE 0 END)             AS LOST_CT
     , SUM(CASE WHEN STATUS = 'Closed' THEN SYSTEM_VALUE END)         AS LOST_VALUE
     , SAVED_VALUE - LOST_VALUE                                       AS NET
FROM T1
ORDER BY T1.CREATED_DATE
