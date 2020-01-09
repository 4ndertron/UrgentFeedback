WITH DATES AS (
    SELECT D.DT
    FROM RPT.T_DATES AS D
    WHERE D.DT BETWEEN
              DATE_TRUNC(Y, DATEADD(Y, -2, CURRENT_DATE)) AND
              CURRENT_DATE
)

   , DEFAULT_BUCKET AS (
    /*
     Salesforce Source:
     https://vivintsolar.lightning.force.com/lightning/r/Report/00O1M000007qbHa/view
     */
    SELECT C.CASE_NUMBER
         , C.CASE_ID
         , C.PROJECT_ID
         , C.RECORD_TYPE
         , C.STATUS
         , C.DESCRIPTION
         , C.SUBJECT
         , C.SOLAR_QUEUE
         , TO_DATE(C.CREATED_DATE)                                   AS BUCKET_START
         , TO_DATE(C.CLOSED_DATE)                                    AS BUCKET_END
         , DATEDIFF(dd, BUCKET_START, NVL(BUCKET_END, CURRENT_DATE)) AS BUCKET_AGE
         , 'Default'                                                 AS PROCESS_BUCKET
         , 1                                                         AS BUCKET_PRIORITY
    FROM RPT.T_CASE AS C
    WHERE C.RECORD_TYPE = 'Solar - Customer Default'
      AND C.SUBJECT NOT ILIKE '%D3%'
)

   , PRE_DEFAULT_BUCKET AS (
    /*
     Salesforce Source:
     https://vivintsolar.lightning.force.com/lightning/r/Report/00O1M000007qqVr/view
     https://vivintsolar.lightning.force.com/lightning/r/Report/00O1M000007qorb/view
     */
    SELECT C.CASE_NUMBER
         , C.CASE_ID
         , C.PROJECT_ID
         , C.RECORD_TYPE
         , C.STATUS
         , C.DESCRIPTION
         , C.SUBJECT
         , C.SOLAR_QUEUE
         , TO_DATE(C.CREATED_DATE)                                   AS BUCKET_START
         , TO_DATE(C.CLOSED_DATE)                                    AS BUCKET_END
         , DATEDIFF(dd, BUCKET_START, NVL(BUCKET_END, CURRENT_DATE)) AS BUCKET_AGE
         , 'Pre-Default'                                             AS PROCESS_BUCKET
         , 2                                                         AS BUCKET_PRIORITY
    FROM RPT.T_CASE AS C
    WHERE C.RECORD_TYPE = 'Solar - Customer Escalation'
      AND C.SOLAR_QUEUE = 'Dispute/Evasion'
      AND C.PRIORITY IN ('1', '2', '3')
)

   , AUDIT_BUCKET AS (
    SELECT Q.CASE_NUMBER
         , Q.CASE_ID
         , Q.PROJECT_ID
         , Q.RECORD_TYPE
         , Q.STATUS
         , Q.DESCRIPTION
         , Q.SUBJECT
         , Q.SOLAR_QUEUE
         , TO_DATE(Q.QUEUE_START)                                    AS BUCKET_START
         , CASE
               WHEN Q.NEXT_START IS NOT NULL
                   THEN TO_DATE(Q.QUEUE_START)
               WHEN Q.CASE_CLOSED_DAY IS NOT NULL
                   THEN TO_DATE(Q.CASE_CLOSED_DAY)
               ELSE TO_DATE(Q.NEXT_START) END                        AS BUCKET_END
         , DATEDIFF(dd, BUCKET_START, NVL(BUCKET_END, CURRENT_DATE)) AS BUCKET_AGE
         , 'Audit'                                                   AS TEAM
         , 3                                                         AS BUCKET_PRIORITY
         , NULL                                                      AS GAP
    FROM (
             SELECT C.CASE_ID
                  , C.PROJECT_ID
                  , C.CASE_NUMBER
                  , C.RECORD_TYPE
                  , C.STATUS
                  , C.DESCRIPTION
                  , C.SUBJECT
                  , C.SOLAR_QUEUE
                  , TO_DATE(C.CLOSED_DATE)                                  AS CASE_CLOSED_DAY
                  , CH.FIELD                                                AS CASE_FIELD_CHANGE
                  , CH.NEWVALUE                                             AS QUEUE_VALUE
                  , LEAD(CH.NEWVALUE) OVER
                 (PARTITION BY CH.CASEID, CH.FIELD ORDER BY CH.CREATEDDATE) AS NEXT_VALUE
                  , CH.CREATEDDATE                                          AS QUEUE_START
                  , LEAD(CH.CREATEDDATE) OVER
                 (PARTITION BY CH.CASEID, CH.FIELD ORDER BY CH.CREATEDDATE) AS NEXT_START
             FROM RPT.V_SF_CASEHISTORY AS CH
                      LEFT JOIN RPT.T_CASE AS C
                                ON C.CASE_ID = CH.CASEID
                      LEFT JOIN D_POST_INSTALL.T_EMPLOYEE_MASTER AS E
                                ON E.SALESFORCE_ID = CH.CREATEDBYID
             WHERE C.RECORD_TYPE NOT IN ('Solar - Customer Default')
               AND CH.FIELD = 'Solar_Queue__c'
         ) AS Q
    WHERE Q.QUEUE_VALUE = 'Dispute/Evasion'
)

   , DEFAULT_HISTORY AS (
    SELECT C.*
         , NVL(LAG(CC.CREATEDATE) OVER (PARTITION BY C.CASE_NUMBER ORDER BY CC.CREATEDATE),
               C.BUCKET_START)                          AS PREVIOUS_COMMENT_DATE
         , CC.CREATEDATE                                AS CURRENT_COMMENT_DATE
         , NVL(LEAD(CC.CREATEDATE) OVER (PARTITION BY C.CASE_NUMBER ORDER BY CC.CREATEDATE),
               NVL(C.BUCKET_END,
                   CURRENT_DATE))                       AS NEXT_COMMENT_DATE
         , USR.NAME                                     AS COMMENT_CREATE_BY
         , HR.BUSINESS_TITLE
         , DATEDIFF(s, NVL(LAG(CC.CREATEDATE) OVER (PARTITION BY C.CASE_NUMBER
        ORDER BY CC.CREATEDATE),
                           C.BUCKET_START),
                    CC.CREATEDATE
               ) / (24 * 60 * 60)
                                                        AS LAG_GAP
         , DATEDIFF(s, CC.CREATEDATE,
                    NVL(LEAD(CC.CREATEDATE) OVER (PARTITION BY C.CASE_NUMBER ORDER BY CC.CREATEDATE),
                        C.BUCKET_END)) / (24 * 60 * 60) AS LEAD_GAP
    FROM (SELECT * FROM DEFAULT_BUCKET UNION SELECT * FROM PRE_DEFAULT_BUCKET) AS C
             LEFT OUTER JOIN RPT.V_SF_CASECOMMENT AS CC
                             ON CC.PARENTID = C.CASE_ID
             LEFT JOIN RPT.V_SF_USER AS USR
                       ON USR.ID = CC.CREATEDBYID
             LEFT JOIN HR.T_EMPLOYEE AS HR
                       ON HR.EMPLOYEE_ID = USR.EMPLOYEE_ID__C
    WHERE CC.CREATEDATE <= NVL(C.BUCKET_END, CURRENT_DATE)
    ORDER BY CASE_NUMBER, CC.CREATEDATE
)

   , FULL_CASE AS (
    SELECT C.CASE_NUMBER
         , ANY_VALUE(C.CASE_ID)        AS CASE_ID
         , ANY_VALUE(C.PROJECT_ID)     AS PROJECT_ID
         , ANY_VALUE(C.RECORD_TYPE)    AS RECORD_TYPE
         , ANY_VALUE(C.STATUS)         AS STATUS
         , ANY_VALUE(C.DESCRIPTION)    AS DESCRIPTION
         , ANY_VALUE(C.SUBJECT)        AS SUBJECT
         , ANY_VALUE(C.SOLAR_QUEUE)    AS SOLAR_QUEUE
         , ANY_VALUE(C.BUCKET_START)   AS BUCKET_START
         , ANY_VALUE(C.BUCKET_END)     AS BUCKET_END
         , ANY_VALUE(C.BUCKET_AGE)     AS BUCKET_AGE
         , ANY_VALUE(C.PROCESS_BUCKET) AS TEAM
         , MIN(BUCKET_PRIORITY)        AS BUCKET_PRIORITY
         , AVG(LAG_GAP)                AS AVERAGE_GAP
    FROM DEFAULT_HISTORY AS C
    GROUP BY C.CASE_NUMBER
)

   , CXBR_DEFAULT AS (
    SELECT Q.*
         , ROUND(SYS.AS_BUILT_SYSTEM_SIZE * 1000 * 4, 2) AS SYSTEM_VALUE
    FROM (SELECT *
          FROM (
                       (SELECT * FROM FULL_CASE)
                       UNION
                       (SELECT * FROM AUDIT_BUCKET)
               )
              QUALIFY ROW_NUMBER() OVER (PARTITION BY CASE_NUMBER ORDER BY BUCKET_PRIORITY ASC) = 1
         ) AS Q
             LEFT JOIN (SELECT DISTINCT PROJECT_ID, AS_BUILT_SYSTEM_SIZE FROM RPT.T_NV_PV_DSAB_CALCULATIONS) AS SYS
                       ON SYS.PROJECT_ID = Q.PROJECT_ID
)

   , RAW_UPDATES AS (
    SELECT DH.CASE_NUMBER
         , DH.PROCESS_BUCKET
         , DH.CURRENT_COMMENT_DATE AS DAY_UPDATED
    FROM DEFAULT_HISTORY AS DH
)

   , UPDATES_DAY AS (
    SELECT D.DT
         , COUNT(CASE
                     WHEN TO_DATE(U.DAY_UPDATED) = D.DT
                         THEN 1 END)                  AS UPDATES
         , IFF(DAYNAME(D.DT) IN ('Sat', 'Sun'), 0, 1) AS WORKDAY
    FROM DATES AS D
       , RAW_UPDATES AS U
    GROUP BY D.DT
    ORDER BY D.DT
)

   , CASE_DAY_WIP AS (
    SELECT D.DT
         , COUNT(CASE
                     WHEN TO_DATE(FC.BUCKET_START) <= D.DT AND
                          (TRUE /* Replace with "Status Bucket = "Working with Customer" */) AND
                          (TO_DATE(FC.BUCKET_END) >= D.DT OR FC.BUCKET_END IS NULL)
                         THEN 1 END) AS CASE_ACTIVE_WIP
         , COUNT(CASE
                     WHEN TO_DATE(FC.BUCKET_START) <= D.DT AND
                          (TO_DATE(FC.BUCKET_END) >= D.DT OR FC.BUCKET_END IS NULL)
                         THEN 1 END) AS BUCKET_TOTAL_WIP
         , COUNT(CASE
                     WHEN TO_DATE(FC.BUCKET_START) <= D.DT AND
                          FC.SUBJECT NOT ILIKE '%BANK%' AND
                          FC.STATUS NOT ILIKE '%LEGAL%' AND
                          (FC.DESCRIPTION NOT ILIKE '%MBW%' OR FC.SUBJECT NOT ILIKE '%COLL%') AND
                          FC.STATUS NOT ILIKE '%ESCALATED%' AND
                          FC.TEAM NOT ILIKE '%AUDIT%' AND
--                           FC.TEAM NOT ILIKE '%PRE%' AND
                          (TO_DATE(FC.BUCKET_END) >= D.DT OR FC.BUCKET_END IS NULL)
                         THEN 1 END) AS COVERAGE_WIP
    FROM DATES AS D
       , CXBR_DEFAULT AS FC
    GROUP BY D.DT
    ORDER BY D.DT
)

   , CASE_MONTH_WIP AS (
    SELECT CW.DT
         , CW.BUCKET_TOTAL_WIP
         , CW.COVERAGE_WIP
    FROM CASE_DAY_WIP CW
    WHERE (CW.DT = LAST_DAY(CW.DT) OR CW.DT = CURRENT_DATE)
)

   , UPDATES_MONTH AS (
    SELECT IFF(LAST_DAY(U.DT) > CURRENT_DATE, CURRENT_DATE, LAST_DAY(U.DT)) AS MONTH
         , SUM(U.UPDATES)                                                   AS TOTAL_UPDATES
         , SUM(U.WORKDAY)                                                   AS WORKDAYS
    FROM UPDATES_DAY AS U
    GROUP BY MONTH
)

   , ION AS (
    SELECT IFF(LAST_DAY(D.DT) > CURRENT_DATE, CURRENT_DATE, LAST_DAY((D.DT))) AS MONTH
         , ROUND(COUNT(CASE
                           WHEN TO_DATE(BUCKET_END) = D.DT
                               AND TEAM = 'Default'
                               THEN 1 END), 2)                                AS DEFAULT_TOTAL_CLOSED
         , ROUND(COUNT(CASE -- error influence
                           WHEN TO_DATE(BUCKET_END) = D.DT
                               THEN 1 END), 2)                                AS OUT
         , ROUND(COUNT(CASE -- error influence
                           WHEN TO_DATE(BUCKET_END) = D.DT
                               AND STATUS = 'Closed - Saved'
                               THEN 1 END), 2)                                AS TOTAL_CLOSED_WON
         , ROUND(TOTAL_CLOSED_WON / DEFAULT_TOTAL_CLOSED, 4)                  AS CLOSED_WON_RATIO
         , ROUND(COUNT(CASE
                           WHEN TO_DATE(BUCKET_START) = D.DT
                               THEN 1 END), 2)                                AS TOTAL_CREATED
         , ROUND(SUM(CASE
                         WHEN TO_DATE(BUCKET_END) = D.DT AND STATUS = 'Closed - Saved'
                             THEN SYSTEM_VALUE END), 2)                       AS TOTAL_AMOUNT_SAVED
         , ROUND(AVG(CASE
                         WHEN TO_DATE(BUCKET_START) <= D.DT AND
                              (TO_DATE(BUCKET_END) > D.DT OR
                               BUCKET_END IS NULL)
                             AND (DESCRIPTION NOT ILIKE ('%MBW%')
                                 OR DESCRIPTION NOT ILIKE ('%COLLECTION%'))
                             AND STATUS NOT ILIKE '%ESCALATED%'
                             THEN DATEDIFF(dd, TO_DATE(FC.BUCKET_START), D.DT)
        END), 2)                                                              AS AVG_OPEN_AGE
         , MAX(CASE
                   WHEN TO_DATE(BUCKET_START) <= D.DT AND
                        (TO_DATE(BUCKET_END) > D.DT OR
                         BUCKET_END IS NULL)
                       AND STATUS NOT ILIKE '%DISPUTE%'
                       THEN DATEDIFF(dd, TO_DATE(FC.BUCKET_START), D.DT)
        END)                                                                  AS MAX_MONTH_AGE
         , ROUND(AVG(CASE
                         WHEN TO_DATE(BUCKET_END) = D.DT
                             THEN BUCKET_AGE END), 2)                         AS AVG_CLOSED_AGE
    FROM CXBR_DEFAULT AS FC
       , DATES AS D
    GROUP BY MONTH
    ORDER BY MONTH
)

   , COVERAGE AS (
    SELECT MONTH
         , U.TOTAL_UPDATES
         , C.COVERAGE_WIP
         , ROUND(U.TOTAL_UPDATES / C.COVERAGE_WIP, 2) AS X_COVERAGE
    FROM UPDATES_MONTH AS U
       , CASE_MONTH_WIP AS C
    WHERE C.DT = U.MONTH
)

   , TEST_RESULTS AS (
    SELECT *
    FROM RAW_UPDATES
    ORDER BY 3 DESC
)

   , MAIN AS (
    SELECT ION.MONTH
         , ION.TOTAL_CREATED           AS "In"         -- Metric 1
         , ION.OUT                     AS "Out"        -- Metric 1
         , CASE_MONTH_WIP.COVERAGE_WIP AS WIP          -- Metric 2
         , COVERAGE.X_COVERAGE         AS "X Coverage" -- Metric 3
         , ION.CLOSED_WON_RATIO        AS WL           -- Metric 4
         , ION.TOTAL_AMOUNT_SAVED      AS "Saved"      -- Metric 5
    FROM ION
       , CASE_MONTH_WIP
       , COVERAGE
    WHERE CASE_MONTH_WIP.DT = ION.MONTH
      AND COVERAGE.MONTH = ION.MONTH
      AND ION.MONTH != CURRENT_DATE
    ORDER BY ION.MONTH
)

SELECT *
FROM MAIN