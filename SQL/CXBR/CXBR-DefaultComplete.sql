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
         , C.OWNER
         , C.PRIMARY_REASON
         , C.P_4_LETTER
         , C.P_5_LETTER
         , IFF(P.PTO_AWARDED IS NOT NULL, 'Post-PTO', 'Pre-PTO')     AS PTO_INDEX
         , P.SERVICE_NAME
         , C.DRA
         , C.PRIORITY
         , C.HOME_VISIT_ONE
         , C.MANAGER_CALL
         , C.SUBJECT
         , '<a href="https://vivintsolar.lightning.force.com/lightning/r/Case/' || C.CASE_ID ||
           '/view" target="_blank">' || C.CASE_NUMBER || '</a>'      AS SALESFORCE_CASE
         , C.SOLAR_QUEUE
         , TO_DATE(C.CREATED_DATE)                                   AS BUCKET_START
         , TO_DATE(C.CLOSED_DATE)                                    AS BUCKET_END
         , DATEDIFF(dd, BUCKET_START, NVL(BUCKET_END, CURRENT_DATE)) AS BUCKET_AGE
         , 'Default'                                                 AS PROCESS_BUCKET
         , 1                                                         AS BUCKET_PRIORITY
    FROM RPT.T_CASE AS C
             LEFT JOIN RPT.T_PROJECT AS P
                       ON P.PROJECT_ID = C.PROJECT_ID
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
         , C.OWNER
         , C.PRIMARY_REASON
         , C.P_4_LETTER
         , C.P_5_LETTER
         , IFF(P.PTO_AWARDED IS NOT NULL, 'Post-PTO', 'Pre-PTO')     AS PTO_INDEX
         , P.SERVICE_NAME
         , C.DRA
         , C.PRIORITY
         , C.HOME_VISIT_ONE
         , C.MANAGER_CALL
         , C.SUBJECT
         , '<a href="https://vivintsolar.lightning.force.com/lightning/r/Case/' || C.CASE_ID ||
           '/view" target="_blank">' || C.CASE_NUMBER || '</a>'      AS SALESFORCE_CASE
         , C.SOLAR_QUEUE
         , TO_DATE(C.CREATED_DATE)                                   AS BUCKET_START
         , TO_DATE(C.CLOSED_DATE)                                    AS BUCKET_END
         , DATEDIFF(dd, BUCKET_START, NVL(BUCKET_END, CURRENT_DATE)) AS BUCKET_AGE
         , 'Pre-Default'                                             AS PROCESS_BUCKET
         , 2                                                         AS BUCKET_PRIORITY
    FROM RPT.T_CASE AS C
             LEFT JOIN RPT.T_PROJECT AS P
                       ON P.PROJECT_ID = C.PROJECT_ID
    WHERE C.RECORD_TYPE = 'Solar - Customer Escalation'
      AND C.SOLAR_QUEUE = 'Dispute/Evasion'
      AND C.PRIORITY IN ('1', '2', '3')
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
         , C.DRA
         , C.PRIORITY
         , C.HOME_VISIT_ONE
         , C.SUBJECT
         , C.MANAGER_CALL
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
         , 'Audit'                                                   AS TEAM
         , 3                                                         AS BUCKET_PRIORITY
         , NULL                                                      AS AVERAGE_GAP
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
                  , C.MANAGER_CALL
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
         ) AS C
             LEFT JOIN RPT.T_PROJECT AS P
                       ON P.PROJECT_ID = C.PROJECT_ID
    WHERE C.QUEUE_VALUE = 'Dispute/Evasion'
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
         , ANY_VALUE(C.CASE_ID)         AS CASE_ID
         , ANY_VALUE(C.PROJECT_ID)      AS PROJECT_ID
         , ANY_VALUE(C.RECORD_TYPE)     AS RECORD_TYPE
         , ANY_VALUE(C.STATUS)          AS STATUS
         , ANY_VALUE(C.DESCRIPTION)     AS DESCRIPTION
         , ANY_VALUE(C.OWNER)           AS OWNER
         , ANY_VALUE(C.PRIMARY_REASON)  AS PRIMARY_REASON
         , ANY_VALUE(C.P_4_LETTER)      AS P_4_LETTER
         , ANY_VALUE(C.P_5_LETTER)      AS P_5_LETTER
         , ANY_VALUE(C.PTO_INDEX)       AS PTO_INDEX
         , ANY_VALUE(C.SERVICE_NAME)    AS SERVICE_NAME
         , ANY_VALUE(C.DRA)             AS DRA
         , ANY_VALUE(C.PRIORITY)        AS PRIORITY
         , ANY_VALUE(C.HOME_VISIT_ONE)  AS HOME_VISIT_ONE
         , ANY_VALUE(C.SUBJECT)         AS SUBJECT
         , ANY_VALUE(C.MANAGER_CALL)    AS MANAGER_CALL
         , ANY_VALUE(C.SALESFORCE_CASE) AS SALESFORCE_CASE
         , ANY_VALUE(C.SOLAR_QUEUE)     AS SOLAR_QUEUE
         , ANY_VALUE(C.BUCKET_START)    AS BUCKET_START
         , ANY_VALUE(C.BUCKET_END)      AS BUCKET_END
         , ANY_VALUE(C.BUCKET_AGE)      AS BUCKET_AGE
         , ANY_VALUE(C.PROCESS_BUCKET)  AS TEAM
         , MIN(BUCKET_PRIORITY)         AS BUCKET_PRIORITY
         , AVG(LAG_GAP)                 AS AVERAGE_GAP
    FROM DEFAULT_HISTORY AS C
    GROUP BY C.CASE_NUMBER
)

   , CXBR_DEFAULT AS (
    SELECT C.*
         , ROUND(SYS.AS_BUILT_SYSTEM_SIZE * 1000 * 4, 2) AS SYSTEM_VALUE
         , CASE
               WHEN C.TEAM = 'Pre-Default' AND
                    C.BUCKET_END IS NOT NULL
                   THEN 'Pre-Default'
               WHEN (C.DESCRIPTION ILIKE '%MBW%' OR C.SUBJECT ILIKE '%COLL%')
                   THEN 'Collections'
               WHEN C.STATUS = 'In Progress'
                   AND C.MANAGER_CALL IS NOT NULL
                   THEN 'Letters'
               WHEN PTO_INDEX = 'Pre-PTO'
                   THEN 'Pre-PTO'
               WHEN PTO_INDEX = 'Post-PTO'
                   THEN 'Post-PTO'
        END                                              AS CASE_BUCKET
    FROM (SELECT *
          FROM (
                       (SELECT * FROM FULL_CASE)
                       UNION
                       (SELECT * FROM AUDIT_BUCKET)
               )
              QUALIFY ROW_NUMBER() OVER (PARTITION BY CASE_NUMBER ORDER BY BUCKET_PRIORITY) = 1
         ) AS C
             LEFT JOIN (SELECT DISTINCT PROJECT_ID, AS_BUILT_SYSTEM_SIZE FROM RPT.T_NV_PV_DSAB_CALCULATIONS) AS SYS
                       ON SYS.PROJECT_ID = C.PROJECT_ID
)

   , GAP_LIST AS (
    SELECT CHT.CASE_NUMBER
         , TO_DATE(CHT.PREVIOUS_COMMENT_DATE)       AS PREVIOUS_COMMENT_DATE
         , TO_DATE(CHT.CURRENT_COMMENT_DATE)        AS CURRENT_COMMENT_DATE
         , TO_DATE(CHT.NEXT_COMMENT_DATE)           AS NEXT_COMMENT_DATE
         , DATEDIFF(dd,
                    TO_DATE(CHT.CURRENT_COMMENT_DATE),
                    TO_DATE(CHT.NEXT_COMMENT_DATE)) AS LEAD_GAP
         , ANY_VALUE(CHT.COMMENT_CREATE_BY)         AS COMMENT_CREATED_BY_NAME
         , ANY_VALUE(CHT.BUSINESS_TITLE)            AS BUSINESS_TITLE
         , MAX(CHT.LAG_GAP)                         AS MAIN_GAP
         , ANY_VALUE(CHT.BUCKET_START)              AS BUCKET_START
         , ANY_VALUE(CHT.BUCKET_END)                AS BUCKET_END
         , ANY_VALUE(CHT.STATUS)                    AS STATUS
         , ANY_VALUE(CHT.RECORD_TYPE)               AS RECORD_TYPE
         , ANY_VALUE(CHT.CASE_ID)                   AS CASE_ID
         , ANY_VALUE(CHT.PROJECT_ID)                AS PROJECT_ID
         , ANY_VALUE(CHT.SUBJECT)                   AS SUBJECT
         , D.DT
         , ROW_NUMBER()
            OVER (PARTITION BY CHT.CASE_NUMBER
                , TO_DATE(CHT.CURRENT_COMMENT_DATE)
                ORDER BY D.DT)                      AS ACTIVE_COMMENT_AGE
    FROM DEFAULT_HISTORY AS CHT
             INNER JOIN RPT.T_DATES AS D
                        ON D.DT BETWEEN CHT.CURRENT_COMMENT_DATE AND CHT.NEXT_COMMENT_DATE
    WHERE (CHT.DESCRIPTION NOT ILIKE ('%MBW%')
        OR CHT.DESCRIPTION NOT ILIKE ('%COLLECTION%'))
    GROUP BY CASE_NUMBER,
             CURRENT_COMMENT_DATE,
             PREVIOUS_COMMENT_DATE,
             NEXT_COMMENT_DATE,
             D.DT
)

   , DEFAULT_AGENTS AS (
    SELECT EMPLOYEE_ID
         , ANY_VALUE(COST_CENTER)                   AS COST_CENTER
         , ANY_VALUE(FULL_NAME)                     AS FULL_NAME
         , ANY_VALUE(POSITION_TITLE)                AS POSITION_TITLE
         , ANY_VALUE(SUPERVISOR_NAME_1)             AS DIRECT_MANAGER
         , ANY_VALUE(SUPERVISORY_ORG)               AS SUPERVISORY_ORG
         , ANY_VALUE(HIRE_DATE1)                    AS HIRE_DATE
         , ANY_VALUE(TERMINATED)                    AS TERMINATED
         , ANY_VALUE(TERMINATION_DATE)              AS TERMINATION_DATE
         , ANY_VALUE(TERMINATION_CATEGORY)          AS TERMINATION_CATEGORY
         , ANY_VALUE(TERMINATION_REASON)            AS TERMINATION_REASON
         , MIN(TEAM_START_DATE)                     AS TEAM_START_DATE
         , MAX(TEAM_END_DATE1)                      AS TEAM_END_DATE
         , DATEDIFF('MM', HIRE_DATE, TEAM_END_DATE) AS MONTH_TENURE
         , MAX(RN)                                  AS RN
         , DIRECTOR_ORG
         , MAX(TRANSFER)                            AS TRANSFER
    FROM (
             SELECT HR.EMPLOYEE_ID
                  , HR.SUPERVISORY_ORG
                  , ANY_VALUE(COST_CENTER)                                                   AS COST_CENTER
                  , ANY_VALUE(HR.FULL_NAME)                                                  AS FULL_NAME
                  , ANY_VALUE(HR.POSITION_TITLE)                                             AS POSITION_TITLE
                  , ANY_VALUE(HR.SUPERVISOR_NAME_1)                                          AS SUPERVISOR_NAME_1
                  , DATEDIFF('MM', ANY_VALUE(HR.HIRE_DATE),
                             NVL(ANY_VALUE(HR.TERMINATION_DATE), CURRENT_DATE()))            AS MONTH_TENURE1
                  , ANY_VALUE(HR.HIRE_DATE)                                                  AS HIRE_DATE1
                  , ANY_VALUE(HR.TERMINATED)                                                 AS TERMINATED
                  , ANY_VALUE(HR.TERMINATION_DATE)                                           AS TERMINATION_DATE
                  , ANY_VALUE(HR.TERMINATION_CATEGORY)                                       AS TERMINATION_CATEGORY
                  , ANY_VALUE(HR.TERMINATION_REASON)                                         AS TERMINATION_REASON
                  , MIN(HR.CREATED_DATE)                                                     AS TEAM_START_DATE
                  -- Begin custom fields IN the TABLE
                  , CASE
                        WHEN MAX(HR.EXPIRY_DATE) >= CURRENT_DATE() AND ANY_VALUE(HR.TERMINATION_DATE) IS NOT NULL
                            THEN ANY_VALUE(HR.TERMINATION_DATE)
                        WHEN MAX(HR.EXPIRY_DATE) >= ANY_VALUE(HR.TERMINATION_DATE)
                            THEN ANY_VALUE(HR.TERMINATION_DATE)
                        ELSE MAX(HR.EXPIRY_DATE) END                                         AS TEAM_END_DATE1
                  , ROW_NUMBER() OVER (PARTITION BY HR.EMPLOYEE_ID ORDER BY TEAM_START_DATE) AS RN
                  , IFF(ANY_VALUE(HR.COST_CENTER_ID) IN
                        ('3400', '3700', '4967-60'), TRUE, FALSE)                            AS DIRECTOR_ORG
                  , LEAD(DIRECTOR_ORG, 1, DIRECTOR_ORG) OVER (PARTITION BY HR.EMPLOYEE_ID
                 ORDER BY TEAM_START_DATE)                                                   AS NEXT_DIRECTOR
                  , CASE
                        WHEN ANY_VALUE(HR.TERMINATION_DATE) >= TEAM_END_DATE1 AND NOT NEXT_DIRECTOR THEN TRUE
                        WHEN DIRECTOR_ORG != NEXT_DIRECTOR THEN TRUE
                        ELSE FALSE END                                                       AS TRANSFER
             FROM HR.T_EMPLOYEE_ALL AS HR
             GROUP BY HR.EMPLOYEE_ID
                    , HR.SUPERVISORY_ORG
             ORDER BY HR.EMPLOYEE_ID
                    , TEAM_START_DATE DESC
         ) AS ENTIRE_HISTORY
    WHERE DIRECTOR_ORG
      AND TEAM_END_DATE1 >= TEAM_START_DATE
      AND (ENTIRE_HISTORY.SUPERVISOR_NAME_1 ILIKE '%PERC%' OR
           ENTIRE_HISTORY.SUPERVISOR_NAME_1 ILIKE '%VALERIA MENDOZA%')
    GROUP BY EMPLOYEE_ID, DIRECTOR_ORG
)

   , QA_CALABRIO AS (
    SELECT DISTINCT p.EMPLOYEE_ID             AS agent_badge
                  , rc.AGENT_DISPLAY_ID       AS agent_evaluated
                  , rc.TEAM_NAME
                  , rc.EVALUATION_EVALUATED   AS evaluation_date
                  , rc.RECORDING_CONTACT_ID   AS contact_id
                  , rc.EVALUATION_TOTAL_SCORE AS qa_score
                  , rc.EVALUATOR_DISPLAY_ID   AS evaluator
                  , rc.EVALUATOR_USER_NAME    AS evaluator_email
    FROM CALABRIO.T_RECORDING_CONTACT rc
             LEFT JOIN CALABRIO.T_PERSONS p
                       ON p.ACD_ID = rc.AGENT_ACD_ID
    WHERE rc.EVALUATION_EVALUATED BETWEEN
              DATE_TRUNC('Y', DATEADD('Y', -1, CURRENT_DATE)) AND
              DATE_TRUNC(w, CURRENT_DATE)
)

   , QA_INCONTACT AS (
    SELECT QA.QSCORE                                 AS QA_SCORE
         , QA.EVALUATION_DATE
         , QA.EVALUATION_TYPE
         , QA.AGENT_FIRST_NAME || QA.AGENT_LAST_NAME AS AGENT_EVALUATED
    FROM D_POST_INSTALL.T_NE_AGENT_QSCORE AS QA
    WHERE QA.EVALUATION_DATE BETWEEN
              DATE_TRUNC('Y', DATEADD('Y', -1, CURRENT_DATE)) AND
              DATE_TRUNC(w, CURRENT_DATE)
)

   , QA AS (
    SELECT DATE_TRUNC(w, D.DT) AS WEEK
         , ROUND(AVG(CASE
                         WHEN
                             TO_DATE(QA.evaluation_date) = D.DT
                             THEN QA.qa_score
        END), 2)               AS AVG_QA
    FROM RPT.T_DATES AS D
       , (
        SELECT C.QA_SCORE
             , C.agent_evaluated
             , C.evaluation_date
             , DA.direct_manager
        FROM QA_CALABRIO AS C
                 LEFT JOIN
             DEFAULT_AGENTS AS DA
             ON DA.FULL_NAME = C.agent_evaluated
        UNION ALL
        SELECT I.QA_SCORE
             , I.AGENT_EVALUATED
             , I.EVALUATION_DATE
             , DA.direct_manager
        FROM QA_INCONTACT AS I
                 LEFT JOIN
             DEFAULT_AGENTS AS DA
             ON DA.FULL_NAME || ' ' = I.agent_evaluated
    ) AS QA
    WHERE D.DT BETWEEN
        DATE_TRUNC('Y', DATEADD('Y', -1, CURRENT_DATE)) AND
        DATE_TRUNC(w, CURRENT_DATE)
      AND QA.direct_manager IS NOT NULL
    GROUP BY WEEK
    ORDER BY WEEK
)

   , RAW_UPDATES AS (
    SELECT DH.CASE_NUMBER
         , DH.PROCESS_BUCKET
         , DH.CURRENT_COMMENT_DATE AS DAY_UPDATED
    FROM DEFAULT_HISTORY AS DH
)
/*
 End core tables
 */

/*
 Start day wip tables
 */
   , DEFAULT_AGENT_DAY_WIP AS (
    SELECT D.DT
         , COUNT(CASE
                     WHEN DA.TEAM_START_DATE <= D.DT AND
                          DA.TEAM_END_DATE > D.DT AND
                          DA.POSITION_TITLE NOT ILIKE '%SPECIAL%'
                         THEN 1 END) AS ACTIVE_AGENTS
    FROM RPT.T_DATES AS D
       , DEFAULT_AGENTS AS DA
    WHERE D.DT BETWEEN
              DATE_TRUNC('Y', DATEADD('Y', -1, CURRENT_DATE)) AND
              DATE_TRUNC(w, CURRENT_DATE)
    GROUP BY D.DT
    ORDER BY D.DT
)

   , UPDATES_DAY AS (
    SELECT D.DT
         , COUNT(CASE
                     WHEN TO_DATE(U.DAY_UPDATED) = D.DT
                         THEN 1 END)                  AS UPDATES
         , IFF(DAYNAME(D.DT) IN ('Sat', 'Sun'), 0, 1) AS WORKDAY
         , DW.ACTIVE_AGENTS
    FROM DATES AS D
       , RAW_UPDATES AS U
       , DEFAULT_AGENT_DAY_WIP AS DW
    WHERE D.DT = DW.DT
    GROUP BY D.DT, DW.ACTIVE_AGENTS
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
/*
 End day wip tables
 */

/*
 Start month wip tables
 */
   , CASE_MONTH_WIP AS (
    SELECT CW.DT
         , CW.BUCKET_TOTAL_WIP
         , CW.COVERAGE_WIP
         , DW.ACTIVE_AGENTS
    FROM CASE_DAY_WIP CW
       , DEFAULT_AGENT_DAY_WIP AS DW
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

   , DEBUG_CTE AS (
    SELECT *
    FROM RAW_UPDATES
    ORDER BY 3 DESC
)

SELECT *
FROM MAIN