WITH CASE_LIST AS (
    SELECT C.CASE_NUMBER
         , P.PROJECT_NAME          AS CASE_PROJECT_NAME
         , TO_DATE(C.CREATED_DATE) AS CASE_CREATED
         , TO_DATE(C.CLOSED_DATE)  AS CASE_CLOSED
         , CASE
               WHEN C.STATUS = 'In Progress'
                   THEN 'Default Letter or 3rd Party'
               WHEN C.STATUS = 'Escalated'
                   THEN 'Pending Legal Recommendation'
               WHEN C.STATUS = 'Pending Customer Action'
                   THEN 'Working with Customer/Owner/Bank'
               WHEN C.STATUS = 'Pending Corporate Action'
                   THEN 'Pending Customer Contact'
               ELSE 'Other'
        END                        AS FORE_BUCKET
    FROM RPT.T_CASE AS C
             LEFT JOIN RPT.T_PROJECT AS P
                       ON P.PROJECT_ID = C.PROJECT_ID
--       AND NEWVALUE IN ('In Progress', 'Escalated', 'Pending Customer Action', 'Pending Corporate Action')
    WHERE C.RECORD_TYPE = 'Solar - Customer Default'
      AND C.SUBJECT NOT ILIKE '%D3%'
      AND C.PRIMARY_REASON = 'Foreclosure'
)

   , LD_CODES AS (
    SELECT 'SP-' || LD.PROJECT_NUMBER AS LD_PROJECT_NAME
         , LD.COLLECTION_CODE
         , LD.COLLECTION_DATE
    FROM LD.T_DAILY_DATA_EXTRACT AS LD
    WHERE LD.COLLECTION_CODE = 'FORE'
)

   , PRE_DEFAULT_LD AS (
    SELECT LD.LD_PROJECT_NAME
         , C.CASE_PROJECT_NAME
         , C.CASE_NUMBER
         , LD.COLLECTION_CODE
         , LD.COLLECTION_DATE
    FROM LD_CODES AS LD
             LEFT OUTER JOIN CASE_LIST AS C
                             ON C.CASE_PROJECT_NAME = LD.LD_PROJECT_NAME
    WHERE C.CASE_NUMBER IS NULL
)

   , CASE_ION AS (
    SELECT IFF(LAST_DAY(D.DT) >= CURRENT_DATE, CURRENT_DATE, LAST_DAY(D.DT)) AS MONTH
         , C.FORE_BUCKET
         , COUNT(CASE
                     WHEN C.CASE_CREATED = D.DT
                         THEN 1 END)                                         AS FORE_INFLOW
         , COUNT(CASE
                     WHEN C.CASE_CLOSED = D.DT
                         THEN 1 END)                                         AS FORE_OUTFLOW
         , FORE_INFLOW - FORE_OUTFLOW                                        AS FORE_NET
    FROM RPT.T_DATES AS D
       , CASE_LIST AS C
    WHERE D.DT BETWEEN
              DATE_TRUNC('Y', DATEADD('Y', -1, CURRENT_DATE)) AND
              CURRENT_DATE
    GROUP BY MONTH
           , C.FORE_BUCKET
    ORDER BY C.FORE_BUCKET
           , MONTH
)

   , CASE_WIP AS (
    SELECT D.DT
         , C.FORE_BUCKET
         , COUNT(CASE
                     WHEN C.CASE_CREATED <= D.DT AND
                          (C.CASE_CLOSED > D.DT OR C.CASE_CLOSED IS NULL)
                         THEN 1 END) AS FORE_WIP
    FROM RPT.T_DATES AS D
       , CASE_LIST AS C
    WHERE D.DT BETWEEN
              DATE_TRUNC('Y', DATEADD('Y', -1, CURRENT_DATE)) AND
              CURRENT_DATE
    GROUP BY D.DT
           , C.FORE_BUCKET
    ORDER BY C.FORE_BUCKET
           , D.DT
)

   , MAIN AS (
    SELECT CI.MONTH
         , CI.FORE_BUCKET
         , CI.FORE_INFLOW
         , CI.FORE_OUTFLOW
         , CW.FORE_WIP
    FROM CASE_ION AS CI
       , CASE_WIP AS CW
    WHERE CW.DT = CI.MONTH
      AND CW.FORE_BUCKET = CI.FORE_BUCKET
    ORDER BY CI.FORE_BUCKET
           , CI.MONTH
)

   , TEST_CTE AS (
    SELECT *
    FROM MAIN
)

SELECT *
FROM TEST_CTE