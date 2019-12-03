WITH PRE_DEFAULT_CASES AS (
    SELECT C.PROJECT_ID
         , C.CASE_ID
         , C.CASE_NUMBER
         , C.RECORD_TYPE
         , C.STATUS
         , C.SOLAR_QUEUE
         , C.PRIMARY_REASON
         , C.OWNER
         , C.SUBJECT
         , C.CREATED_DATE
         , C.CLOSED_DATE
         , C.ORIGIN
         , IFF(C.PRIMARY_REASON IN ('Customer Refusal', 'Evasion/Avoidance'), C.PRIMARY_REASON, 'NA') AS DEFAULT_TYPE
         , 'Pre-Default'                                                                              AS CASE_TYPE
    FROM RPT.T_CASE AS C
    WHERE C.RECORD_TYPE = 'Solar - Customer Escalation'
      AND C.SOLAR_QUEUE ILIKE '%EVASION%'
      AND C.CLOSED_DATE IS NOT NULL
)
   , FORECLOSURE_CASES AS (
    SELECT C.PROJECT_ID
         , C.CASE_ID
         , C.CASE_NUMBER
         , C.RECORD_TYPE
         , C.STATUS
         , C.SOLAR_QUEUE
         , C.PRIMARY_REASON
         , C.OWNER
         , C.SUBJECT
         , C.CREATED_DATE
         , C.CLOSED_DATE
         , C.ORIGIN
         , 'NA'          AS DEFAULT_TYPE
         , 'Foreclosure' AS CASE_TYPE
    FROM RPT.T_CASE AS C
    WHERE C.RECORD_TYPE = 'Solar - Customer Default'
      AND C.PRIMARY_REASON IN ('Foreclosure')
      AND C.CLOSED_DATE IS NOT NULL
)

   , DECEASED_CASES AS (
    SELECT C.PROJECT_ID
         , C.CASE_ID
         , C.CASE_NUMBER
         , C.RECORD_TYPE
         , C.STATUS
         , C.SOLAR_QUEUE
         , C.PRIMARY_REASON
         , C.OWNER
         , C.SUBJECT
         , C.CREATED_DATE
         , C.CLOSED_DATE
         , C.ORIGIN
         , 'NA'       AS DEFAULT_TYPE
         , 'Deceased' AS CASE_TYPE
    FROM RPT.T_CASE AS C
    WHERE C.RECORD_TYPE = 'Solar - Customer Default'
      AND C.PRIMARY_REASON IN ('Customer Deceased')
      AND C.CLOSED_DATE IS NOT NULL
)

   , DEFAULT_CASES AS (
    SELECT C.PROJECT_ID
         , C.CASE_ID
         , C.CASE_NUMBER
         , C.RECORD_TYPE
         , C.STATUS
         , C.SOLAR_QUEUE
         , C.PRIMARY_REASON
         , C.OWNER
         , C.SUBJECT
         , C.CREATED_DATE
         , C.CLOSED_DATE
         , C.ORIGIN
         , IFF(C.PRIMARY_REASON IN ('Customer Refusal', 'Evasion/Avoidance'), C.PRIMARY_REASON, 'NA') AS DEFAULT_TYPE
         , 'Default'                                                                                  AS CASE_TYPE
    FROM RPT.T_CASE AS C
    WHERE C.RECORD_TYPE = 'Solar - Customer Default'
      AND C.PRIMARY_REASON NOT IN ('Foreclosure', 'Customer Deceased')
      AND C.CLOSED_DATE IS NOT NULL
)

   , ALL_CASES AS (
    SELECT *
    FROM (
                 (SELECT * FROM PRE_DEFAULT_CASES)
                 UNION
                 (SELECT * FROM FORECLOSURE_CASES)
                 UNION
                 (SELECT * FROM DECEASED_CASES)
                 UNION
                 (SELECT * FROM DEFAULT_CASES)
         )
)

   , CASE_COMMENTS AS (
    SELECT AC.*
         , CC.CREATEDATE
         , CC.CREATEDBYID
         , COUNT(AC.CASE_ID) OVER (PARTITION BY AC.CASE_ID) AS COMMENTS
    FROM ALL_CASES AS AC
             LEFT OUTER JOIN RPT.V_SF_CASECOMMENT AS CC
                             ON CC.PARENTID = AC.CASE_ID
)

   , UPDATES AS (
    SELECT CASE_ID
         , ANY_VALUE(PROJECT_ID)     AS PROJECT_ID
         , ANY_VALUE(CASE_NUMBER)    AS CASE_NUMBER
         , ANY_VALUE(RECORD_TYPE)    AS RECORD_TYPE
         , ANY_VALUE(STATUS)         AS STATUS
         , ANY_VALUE(SOLAR_QUEUE)    AS SOLAR_QUEUE
         , ANY_VALUE(PRIMARY_REASON) AS PRIMARY_REASON
         , ANY_VALUE(OWNER)          AS OWNER
         , ANY_VALUE(SUBJECT)        AS SUBJECT
         , ANY_VALUE(CREATED_DATE)   AS CREATED_DATE
         , ANY_VALUE(CLOSED_DATE)    AS CLOSED_DATE
         , ANY_VALUE(ORIGIN)         AS ORIGIN
         , ANY_VALUE(DEFAULT_TYPE)   AS DEFAULT_TYPE
         , ANY_VALUE(CASE_TYPE)      AS CASE_TYPE
         , ANY_VALUE(CREATEDATE)     AS CREATEDATE
         , ANY_VALUE(CREATEDBYID)    AS CREATEDBYID
         , ANY_VALUE(COMMENTS)       AS COMMENTS
    FROM CASE_COMMENTS
    GROUP BY CASE_ID
    ORDER BY CASE_ID DESC
)

   , TOUCHES AS (
    SELECT CASE_TYPE
         , DEFAULT_TYPE
         , AVG(COMMENTS)   AS AVG_TOUCHES
         , COUNT(COMMENTS) AS VOLUME
    FROM UPDATES
    GROUP BY CASE_TYPE, DEFAULT_TYPE
    ORDER BY CASE_TYPE, DEFAULT_TYPE
)

SELECT *
FROM TOUCHES
;