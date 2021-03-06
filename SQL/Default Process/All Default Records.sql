WITH DEFAULT_BUCKET AS (
    SELECT C.CASE_NUMBER
         , C.CASE_ID
         , C.PROJECT_ID
         , C.RECORD_TYPE
         , C.STATUS
         , C.DESCRIPTION
         , C.OWNER
         , C.PRIMARY_REASON
         , C.P_4_LETTER
         , C.P_5_LETTER
         , IFF(P.PTO_AWARDED IS NOT NULL, 'Post-PTO', 'Pre-PTO')     AS PTO_INDEX
         , P.SERVICE_NAME
         , P.SERVICE_STATE
         , P.SERVICE_COUNTY
         , P.SERVICE_CITY
         , C.DRA
         , C.PRIORITY
         , C.HOME_VISIT_ONE
         , C.SUBJECT
         , '<a href="https://vivintsolar.lightning.force.com/lightning/r/Case/' || C.CASE_ID ||
           '/view" target="_blank">' || C.CASE_NUMBER || '</a>'      AS SALESFORCE_CASE
         , C.SOLAR_QUEUE
         , TO_DATE(C.CREATED_DATE)                                   AS BUCKET_START
         , TO_DATE(C.CLOSED_DATE)                                    AS BUCKET_END
         , DATEDIFF(dd, BUCKET_START, NVL(BUCKET_END, CURRENT_DATE)) AS BUCKET_AGE
         , 'Default'                                                 AS PROCESS_BUCKET
         , 1                                                         as Bucket_Priority
    FROM RPT.T_CASE AS C
             LEFT JOIN RPT.T_PROJECT AS P
                       ON P.PROJECT_ID = C.PROJECT_ID
    WHERE C.RECORD_TYPE = 'Solar - Customer Default'
      AND C.SUBJECT NOT ILIKE '%D3%'
)

   , PRE_DEFAULT_BUCKET AS (
    SELECT C.CASE_NUMBER
         , C.CASE_ID
         , C.PROJECT_ID
         , C.RECORD_TYPE
         , C.STATUS
         , C.DESCRIPTION
         , C.OWNER
         , C.PRIMARY_REASON
         , C.P_4_LETTER
         , C.P_5_LETTER
         , IFF(P.PTO_AWARDED IS NOT NULL, 'Post-PTO', 'Pre-PTO')     AS PTO_INDEX
         , P.SERVICE_NAME
         , P.SERVICE_STATE
         , P.SERVICE_COUNTY
         , P.SERVICE_CITY
         , C.DRA
         , C.PRIORITY
         , C.HOME_VISIT_ONE
         , C.SUBJECT
         , '<a href="https://vivintsolar.lightning.force.com/lightning/r/Case/' || C.CASE_ID ||
           '/view" target="_blank">' || C.CASE_NUMBER || '</a>'      AS SALESFORCE_CASE
         , C.SOLAR_QUEUE
         , TO_DATE(C.CREATED_DATE)                                   AS BUCKET_START
         , TO_DATE(C.CLOSED_DATE)                                    AS BUCKET_END
         , DATEDIFF(dd, BUCKET_START, NVL(BUCKET_END, CURRENT_DATE)) AS BUCKET_AGE
         , 'Pre-Default'                                             AS PROCESS_BUCKET
         , 2                                                         as Bucket_Priority
    FROM RPT.T_CASE AS C
             LEFT JOIN RPT.T_PROJECT AS P
                       ON P.PROJECT_ID = C.PROJECT_ID
    WHERE C.RECORD_TYPE = 'Solar - Customer Escalation'
      AND C.SOLAR_QUEUE = 'Dispute/Evasion'
)

   , AUDIT_BUCKET AS (
    SELECT C.CASE_NUMBER
         , C.CASE_ID
         , C.PROJECT_ID
         , C.RECORD_TYPE
         , C.STATUS
         , C.DESCRIPTION
         , C.OWNER
         , C.PRIMARY_REASON
         , C.P_4_LETTER
         , C.P_5_LETTER
         , IFF(P.PTO_AWARDED IS NOT NULL, 'Post-PTO', 'Pre-PTO')     AS PTO_INDEX
         , P.SERVICE_NAME
         , P.SERVICE_STATE
         , P.SERVICE_COUNTY
         , P.SERVICE_CITY
         , C.DRA
         , C.PRIORITY
         , C.HOME_VISIT_ONE
         , C.SUBJECT
         , '<a href="https://vivintsolar.lightning.force.com/lightning/r/Case/' || C.CASE_ID ||
           '/view" target="_blank">' || C.CASE_NUMBER || '</a>'      AS SALESFORCE_CASE
         , C.SOLAR_QUEUE
         , TO_DATE(C.QUEUE_START)                                    AS BUCKET_START
         , CASE
               WHEN C.NEXT_START IS NOT NULL
                   THEN TO_DATE(C.QUEUE_START)
               WHEN C.CASE_CLOSED_DAY IS NOT NULL
                   THEN TO_DATE(C.CASE_CLOSED_DAY)
               ELSE TO_DATE(C.NEXT_START) END                        AS BUCKET_END
         , DATEDIFF(dd, BUCKET_START, NVL(BUCKET_END, CURRENT_DATE)) AS BUCKET_AGE
         , 'Audit'                                                   AS PROCESS_BUCKET
         , 3                                                         as Bucket_Priority
    FROM (
             SELECT C.CASE_ID
                  , C.PROJECT_ID
                  , C.CASE_NUMBER
                  , C.RECORD_TYPE
                  , C.STATUS
                  , C.DESCRIPTION
                  , C.OWNER
                  , C.PRIMARY_REASON
                  , C.P_4_LETTER
                  , C.P_5_LETTER
                  , C.DRA
                  , C.PRIORITY
                  , C.HOME_VISIT_ONE
                  , C.SUBJECT
                  , C.SOLAR_QUEUE
                  , TO_DATE(C.CLOSED_DATE)                                                               AS CASE_CLOSED_DAY
                  , CH.FIELD                                                                             AS CASE_FIELD_CHANGE
                  , CH.NEWVALUE                                                                          AS QUEUE_VALUE
                  , LEAD(CH.NEWVALUE) OVER (PARTITION BY CH.CASEID, CH.FIELD ORDER BY CH.CREATEDDATE)    AS NEXT_VALUE
                  , CH.CREATEDDATE                                                                       AS QUEUE_START
                  , LEAD(CH.CREATEDDATE) OVER (PARTITION BY CH.CASEID, CH.FIELD ORDER BY CH.CREATEDDATE) AS NEXT_START
             FROM RPT.V_SF_CASEHISTORY AS CH
                      LEFT JOIN RPT.T_CASE AS C
                                ON C.CASE_ID = CH.CASEID
             WHERE C.RECORD_TYPE NOT IN ('Solar - Customer Default')
               AND CH.FIELD = 'Solar_Queue__c'
         ) AS C
             LEFT JOIN RPT.T_PROJECT AS P
                       ON P.PROJECT_ID = C.PROJECT_ID
    WHERE C.QUEUE_VALUE = 'Dispute/Evasion'
)

   , MAIN AS (
    /*
     88 Misfits
     ----------

     */
    SELECT C.*
         , CASE

        -- NON-WIP
               WHEN C.BUCKET_END IS NOT NULL
                   THEN 'Complete'
               WHEN C.SUBJECT ILIKE '%BANKRUP%'
                   AND C.PROCESS_BUCKET = 'Pre-Default'
                   THEN 'Bankruptcy'
               WHEN C.STATUS ILIKE '%LEGAL%'
                   AND C.SUBJECT NOT ILIKE '%BANKR%'
                   THEN 'Pending Legal Action'
               WHEN (C.DESCRIPTION ILIKE '%MBW%' OR C.SUBJECT ILIKE '%COLL%')
                   AND C.PROCESS_BUCKET = 'Default'
                   THEN 'Third-party'
               WHEN C.STATUS ILIKE '%ESCALATED%'
                   AND C.PROCESS_BUCKET = 'Default'
                   THEN 'Legal'

        -- LETTERS
               WHEN C.DRA IS NOT NULL
                   AND C.PROCESS_BUCKET = 'Default'
                   AND C.STATUS ILIKE '%PROGRESS%'
                   THEN 'DRA'
               WHEN C.P_5_LETTER IS NOT NULL
                   AND C.PROCESS_BUCKET = 'Default'
                   AND C.STATUS ILIKE '%PROGRESS%'
                   THEN 'P5 Letter'
               WHEN C.P_4_LETTER IS NOT NULL
                   AND C.PROCESS_BUCKET = 'Default'
                   AND C.STATUS ILIKE '%PROGRESS%'
                   THEN 'P4 Letter'
               WHEN (C.HOME_VISIT_ONE IS NOT NULL OR C.STATUS = 'In Progress')
                   AND C.PROCESS_BUCKET = 'Default'
                   AND C.STATUS ILIKE '%PROGRESS%'
                   THEN 'Home Visit/Letter'

        -- AGENTS
               WHEN C.STATUS ILIKE '%PENDING%ACTION%'
                   AND C.PROCESS_BUCKET = 'Default'
                   THEN 'Default - Active'
               WHEN C.PRIORITY IN ('1', '2', '3')
                   AND C.PROCESS_BUCKET = 'Pre-Default'
                   THEN 'Pre-Default'
               WHEN C.PROCESS_BUCKET = 'Pre-Default'
                   THEN 'Pre-Default'
               WHEN C.PROCESS_BUCKET = 'Audit'
                   AND C.RECORD_TYPE NOT ILIKE '%PANEL%'
                   AND C.RECORD_TYPE NOT ILIKE '%ESCALATION%'
                   THEN 'Audit'
               ELSE 'Lost'
        END             AS CASE_BUCKET
         , CURRENT_DATE AS LAST_REFRESHED
    FROM (
                 (SELECT * FROM DEFAULT_BUCKET)
                 UNION All
                 (SELECT * FROM PRE_DEFAULT_BUCKET)
                 UNION ALL
                 (SELECT * FROM AUDIT_BUCKET)
         ) AS C
)

   , TEST_RESULTS AS (
    SELECT CASE_BUCKET
         , COUNT(*)
    FROM MAIN
    GROUP BY CASE_BUCKET
    ORDER BY 1
)

SELECT *
FROM MAIN
    qualify Row_Number() over (partition by CASE_NUMBER order by Bucket_Priority) = 1