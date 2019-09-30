WITH CODE_TABLE AS (
    SELECT LD.BILLING_ACCOUNT
         , LD.AGE
         , LD.COLLECTION_CODE
         , CASE
               WHEN LD.AGE BETWEEN 1 AND 30
                   THEN 30
               WHEN LD.AGE BETWEEN 31 AND 60
                   THEN 60
               WHEN LD.AGE BETWEEN 61 AND 90
                   THEN 90
               WHEN LD.AGE BETWEEN 91 AND 120
                   THEN 120
               WHEN LD.AGE >= 121
                   THEN 121
        END AS AGE_BUCKET
         , CASE
               WHEN AGE_BUCKET = 30
                   THEN '1 to 30'
               WHEN AGE_BUCKET = 60
                   THEN '31 to 60'
               WHEN AGE_BUCKET = 90
                   THEN '61 to 90'
               WHEN AGE_BUCKET = 120
                   THEN '91 to 120'
               WHEN AGE_BUCKET = 121
                   THEN '121+'
        END AS AGE_BUCKET_TEXT
    FROM LD.T_DAILY_DATA_EXTRACT AS LD
    WHERE LD.TOTAL_DELINQUENT_AMOUNT_DUE > 0
      AND LD.COLLECTION_CODE IN ('FORE', 'FORP', 'BK07', 'BK13', 'DCNT', 'CORP')
)

   , CASE_TABLE AS (
    SELECT P.SOLAR_BILLING_ACCOUNT_NUMBER AS BILLING_ACCOUNT
         , C.CASE_NUMBER
         , CASE
               WHEN C.CLOSED_DATE IS NOT NULL
                   THEN 'Pending Legal'
               WHEN C.DESCRIPTION ILIKE '%MBW%'
                   THEN 'Third-party'
               WHEN C.DRA IS NOT NULL
                   THEN 'DRA1'
               WHEN C.P_5_LETTER IS NOT NULL
                   THEN 'P5 Letter'
               WHEN C.P_4_LETTER IS NOT NULL
                   THEN 'P4 Letter'
               WHEN C.HOME_VISIT_ONE IS NOT NULL
                   THEN 'Home Visit/Letter'
               ELSE 'Unknown'
        END                               AS CASE_BUCKET
    FROM RPT.T_CASE AS C
             LEFT OUTER JOIN RPT.T_PROJECT AS P
                             ON P.PROJECT_ID = C.PROJECT_ID
    WHERE C.RECORD_TYPE = 'Solar - Customer Default'
      AND C.SUBJECT NOT ILIKE '%D3%'
)

   , ACCOUNT_TABLE AS (
    SELECT CT.*
         , CS.CASE_NUMBER
         , CS.CASE_BUCKET
    FROM CODE_TABLE AS CT
             INNER JOIN CASE_TABLE AS CS
                        ON CS.BILLING_ACCOUNT = CT.BILLING_ACCOUNT
)

   , DATA_TABLE AS (
    SELECT AT.AGE_BUCKET
         , AT.AGE_BUCKET_TEXT
         , AT.COLLECTION_CODE
         , AT.CASE_BUCKET
         , COUNT(*) AS VOLUME
    FROM ACCOUNT_TABLE AS AT
    GROUP BY AT.AGE_BUCKET
           , AT.AGE_BUCKET_TEXT
           , AT.COLLECTION_CODE
           , AT.CASE_BUCKET
    ORDER BY AT.AGE_BUCKET
           , AT.AGE_BUCKET_TEXT
           , AT.COLLECTION_CODE
           , AT.CASE_BUCKET
)

   , TEST_CTE AS (
    SELECT *
    FROM DATA_TABLE
)

SELECT *
FROM TEST_CTE