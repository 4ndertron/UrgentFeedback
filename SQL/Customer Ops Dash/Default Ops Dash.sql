WITH DEFAULT_BUCKET AS ( -- GROUP BY CLEAR -- TIMESTAMP CLEAR
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
    FROM RPT.T_CASE AS C
             LEFT JOIN RPT.T_PROJECT AS P
                       ON P.PROJECT_ID = C.PROJECT_ID
    WHERE C.RECORD_TYPE = 'Solar - Customer Default'
      AND C.SUBJECT NOT ILIKE '%D3%'
)

   , PRE_DEFAULT_BUCKET AS ( -- GROUP BY CLEAR -- TIMESTAMP CLEAR
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
    FROM RPT.T_CASE AS C
             LEFT JOIN RPT.T_PROJECT AS P
                       ON P.PROJECT_ID = C.PROJECT_ID
    WHERE C.RECORD_TYPE = 'Solar - Customer Escalation'
      AND C.SOLAR_QUEUE = 'Dispute/Evasion'
)

   , AUDIT_BUCKET AS ( -- GROUP BY CLEAR -- TIMESTAMP CLEAR
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
         , 'Audit'                                                   AS PROCESS_BUCKET
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

   , DEFAULT_HISTORY AS ( -- GROUP BY CLEAR -- TIMESTAMP CLEAR
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

   , FULL_CASE AS ( -- GROUP BY CLEAR -- TIMESTAMP CLEAR
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
         , ANY_VALUE(C.PROCESS_BUCKET)  AS PROCESS_BUCKET
         , AVG(LAG_GAP)                 AS AVERAGE_GAP
    FROM DEFAULT_HISTORY AS C
    GROUP BY C.CASE_NUMBER
)

   , CXBR_DEFAULT AS ( -- GROUP BY ERROR -- TIMESTAMP ERROR
    SELECT C.*
         , ROUND(SYS.AS_BUILT_SYSTEM_SIZE * 1000 * 4, 2) AS SYSTEM_VALUE
         , CASE
               WHEN C.PROCESS_BUCKET = 'Pre-Default' AND
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
    FROM (
                 (SELECT * FROM FULL_CASE)
                 UNION
                 (SELECT * FROM AUDIT_BUCKET)
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
    SELECT U.*
    FROM (
             (SELECT CC.ID
                   , CC.CREATEDATE  AS CREATED_DATE
                   , CC.PARENTID    AS PARENT
                   , E.EMPLOYEE_ID
                   , 'Case Comment' AS UPDATE_TYPE
              FROM RPT.V_SF_CASECOMMENT AS CC
                       LEFT OUTER JOIN D_POST_INSTALL.T_EMPLOYEE_MASTER AS E
                                       ON E.SALESFORCE_ID = CC.CREATEDBYID)
             UNION
             (SELECT T.TASK_ID       AS ID
                   , T.CREATED_DATE  AS CREATED_DATE
                   , T.PROJECT_ID    AS PARENT
                   , E.EMPLOYEE_ID
                   , 'Task Creation' AS UPDATE_TYPE
              FROM RPT.T_TASK AS T
                       LEFT OUTER JOIN D_POST_INSTALL.T_EMPLOYEE_MASTER AS E
                                       ON E.SALESFORCE_ID = T.CREATED_BY_ID)
         ) AS U
             LEFT OUTER JOIN DEFAULT_AGENTS AS A
                             ON A.EMPLOYEE_ID = U.EMPLOYEE_ID
    WHERE A.EMPLOYEE_ID IS NOT NULL
      AND U.CREATED_DATE >= A.TEAM_START_DATE
      AND U.CREATED_DATE <= A.TEAM_END_DATE
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
                     WHEN TO_DATE(U.CREATED_DATE) = D.DT
                         THEN 1 END)                  AS UPDATES
         , IFF(DAYNAME(D.DT) IN ('Sat', 'Sun'), 0, 1) AS WORKDAY
         , DW.ACTIVE_AGENTS
    FROM RAW_UPDATES AS U
       , RPT.T_DATES AS D
       , DEFAULT_AGENT_DAY_WIP AS DW
    WHERE D.DT BETWEEN
        DATE_TRUNC('Y', DATEADD('Y', -1, CURRENT_DATE)) AND
        DATE_TRUNC(w, CURRENT_DATE)
      AND DW.DT = D.DT
    GROUP BY D.DT, DW.ACTIVE_AGENTS
    ORDER BY D.DT
)

   , CASE_DAY_WIP AS (
    SELECT D.DT
         , COUNT(CASE
                     WHEN TO_DATE(FC.BUCKET_START) <= D.DT AND
                          (TO_DATE(FC.BUCKET_END) >= D.DT OR FC.BUCKET_END IS NULL) AND
                          FC.CASE_BUCKET = 'Pre-PTO'
                         THEN 1 END) AS PRE_PTO_WIP
         , COUNT(CASE
                     WHEN TO_DATE(FC.BUCKET_START) <= D.DT AND
                          (TO_DATE(FC.BUCKET_END) >= D.DT OR FC.BUCKET_END IS NULL) AND
                          FC.CASE_BUCKET = 'Pre-Default'
                         THEN 1 END) AS PRE_DEFAULT_WIP
         , COUNT(CASE
                     WHEN TO_DATE(FC.BUCKET_START) <= D.DT AND
                          (TO_DATE(FC.BUCKET_END) >= D.DT OR FC.BUCKET_END IS NULL) AND
                          FC.CASE_BUCKET = 'Post-PTO'
                         THEN 1 END) AS POST_PTO_WIP
         , COUNT(CASE
                     WHEN TO_DATE(FC.BUCKET_START) <= D.DT AND
                          (TO_DATE(FC.BUCKET_END) >= D.DT OR FC.BUCKET_END IS NULL) AND
                          FC.CASE_BUCKET = 'Collections'
                         THEN 1 END) AS COLLECTIONS_WIP
         , COUNT(CASE
                     WHEN TO_DATE(FC.BUCKET_START) <= D.DT AND
                          (TO_DATE(FC.BUCKET_END) >= D.DT OR FC.BUCKET_END IS NULL) AND
                          FC.CASE_BUCKET = 'Letters'
                         THEN 1 END) AS LETTERS_WIP
         , COUNT(CASE
                     WHEN TO_DATE(FC.BUCKET_START) <= D.DT AND
                          (TO_DATE(FC.BUCKET_END) >= D.DT OR FC.BUCKET_END IS NULL)
                         THEN 1 END) AS CASE_TOTAL_WIP
    FROM RPT.T_DATES AS D
       , CXBR_DEFAULT AS FC
    WHERE D.DT BETWEEN
        DATE_TRUNC('Y', DATEADD('Y', -1, CURRENT_DATE)) AND
        DATE_TRUNC(w, CURRENT_DATE)
      AND FC.SUBJECT NOT ILIKE '%#COLL%'
    GROUP BY D.DT
    ORDER BY D.DT
)
/*
 End day wip tables
 */

/*
 Start month wip tables
 */
   , CASE_WEEK_WIP AS (
    SELECT CW.DT
         , ROUND(CW.CASE_TOTAL_WIP / DW.ACTIVE_AGENTS, 2) AS AVERAGE_AGENT_WIP
         , CW.PRE_DEFAULT_WIP
         , CW.PRE_PTO_WIP
         , CW.POST_PTO_WIP
         , CW.CASE_TOTAL_WIP
         , CW.LETTERS_WIP
         , CW.COLLECTIONS_WIP
         , DW.ACTIVE_AGENTS
    FROM CASE_DAY_WIP CW
       , DEFAULT_AGENT_DAY_WIP AS DW
    WHERE DAYOFWEEK(DW.DT) = 1
      AND DW.DT = CW.DT
)

   , GAP_WEEK_TABLE AS (
    SELECT DATE_TRUNC(w, D.DT)                         AS WEEK
         , AVG(CASE
                   WHEN GL.DT = WEEK
                       THEN GL.ACTIVE_COMMENT_AGE END) AS AVG_GAP
         , MAX(CASE
                   WHEN GL.DT = WEEK
                       THEN GL.ACTIVE_COMMENT_AGE END) AS MAX_GAP
    FROM GAP_LIST AS GL
       , RPT.T_DATES AS D
    WHERE D.DT BETWEEN
              DATE_TRUNC('Y', DATEADD('Y', -1, CURRENT_DATE)) AND
              DATE_TRUNC(w, CURRENT_DATE)
    GROUP BY WEEK
    ORDER BY WEEK
)

   , UPDATES_WEEK AS (
    SELECT DATE_TRUNC(w, U.DT)                            AS WEEK
         , SUM(U.UPDATES)                                 AS TOTAL_UPDATES
         , SUM(U.WORKDAY)                                 AS WORKDAYS
         , ROUND(TOTAL_UPDATES / WORKDAYS, 2)             AS AVG_DAY_UPDATES
         , ROUND(TOTAL_UPDATES / MIN(U.ACTIVE_AGENTS), 2) AS AVG_AGENT_DAY_UPDATES
    FROM UPDATES_DAY AS U
    GROUP BY WEEK
)

   , ION AS (
    SELECT DATE_TRUNC(w, D.DT)                                             AS WEEK
         , ROUND(COUNT(CASE
                           WHEN TO_DATE(BUCKET_END) = D.DT
                               THEN 1 END) / DW.ACTIVE_AGENTS, 2)          AS AVERAGE_CLOSED
         , ROUND(COUNT(CASE
                           WHEN TO_DATE(BUCKET_END) = D.DT
                               THEN 1 END), 2)                             AS TOTAL_CLOSED
         , ROUND(COUNT(CASE
                           WHEN TO_DATE(BUCKET_END) = D.DT AND
                                FC.PROCESS_BUCKET = 'Default'
                               THEN 1 END), 2)                             AS DEFAULT_TOTAL_CLOSED
         , ROUND(COUNT(CASE
                           WHEN TO_DATE(BUCKET_END) = D.DT
                               AND STATUS = 'Closed - Saved'
                               THEN 1 END), 2)                             AS TOTAL_CLOSED_WON
         , ROUND(TOTAL_CLOSED_WON / DEFAULT_TOTAL_CLOSED, 4)               AS CLOSED_WON_RATIO
         , ROUND(COUNT(CASE
                           WHEN TO_DATE(BUCKET_START) = D.DT
                               THEN 1 END) / DW.ACTIVE_AGENTS, 2)          AS AVERAGE_CREATED
         , ROUND(COUNT(CASE
                           WHEN TO_DATE(BUCKET_START) = D.DT
                               THEN 1 END), 2)                             AS TOTAL_CREATED
         , ROUND(COUNT(CASE
                           WHEN TO_DATE(BUCKET_END) = D.DT AND STATUS = 'Closed - Saved'
                               THEN 1 END) / DW.ACTIVE_AGENTS,
                 2)                                                        AS AVERAGE_CLOSED_WON_CASES
         , ROUND(SUM(CASE
                         WHEN TO_DATE(BUCKET_END) = D.DT AND STATUS = 'Closed - Saved'
                             THEN SYSTEM_VALUE END) / DW.ACTIVE_AGENTS, 2) AS AVERAGE_AMOUNT_SAVED
         , ROUND(SUM(CASE
                         WHEN TO_DATE(BUCKET_END) = D.DT AND STATUS = 'Closed - Saved'
                             THEN SYSTEM_VALUE END), 2)                    AS TOTAL_AMOUNT_SAVED
         , ROUND(AVG(CASE
                         WHEN TO_DATE(BUCKET_START) <= D.DT AND
                              (TO_DATE(BUCKET_END) > D.DT OR
                               BUCKET_END IS NULL)
                             AND (DESCRIPTION NOT ILIKE '%COLL%' OR
                                  STATUS NOT ILIKE '%COLL%' OR
                                  DESCRIPTION NOT ILIKE '%MBW%' OR
                                  STATUS NOT ILIKE '%MBW%')
                             AND STATUS NOT ILIKE '%ESCALATED%'
                             THEN DATEDIFF(dd, TO_DATE(FC.BUCKET_START), D.DT)
        END), 2)                                                           AS AVG_OPEN_AGE
         , MAX(CASE
                   WHEN TO_DATE(BUCKET_START) <= D.DT AND
                        (TO_DATE(BUCKET_END) > D.DT OR
                         BUCKET_END IS NULL)
                       AND STATUS NOT ILIKE '%DISPUTE%'
                       THEN DATEDIFF(dd, TO_DATE(FC.BUCKET_START), D.DT)
        END)                                                               AS MAX_MONTH_AGE
         , ROUND(AVG(CASE
                         WHEN TO_DATE(BUCKET_END) = D.DT
                             THEN BUCKET_AGE END), 2)                      AS AVG_CLOSED_AGE
         , DW.ACTIVE_AGENTS
    FROM CXBR_DEFAULT AS FC
       , RPT.T_DATES AS D
       , DEFAULT_AGENT_DAY_WIP AS DW
    WHERE D.DT BETWEEN
        DATE_TRUNC('Y', DATEADD('Y', -1, CURRENT_DATE)) AND
        DATE_TRUNC(w, CURRENT_DATE)
      AND DW.DT = WEEK
    GROUP BY WEEK, DW.ACTIVE_AGENTS
    ORDER BY WEEK, DW.ACTIVE_AGENTS
)

   , COVERAGE AS (
    SELECT WEEK
         , U.TOTAL_UPDATES
         , C.CASE_TOTAL_WIP
         , ROUND(U.TOTAL_UPDATES / C.CASE_TOTAL_WIP, 4) AS X_COVERAGE
    FROM UPDATES_WEEK AS U
       , CASE_WEEK_WIP AS C
    WHERE C.DT = U.WEEK
)
/*
 End month wip tables
 */

   , FIND_CASE_BY_GAP AS (
    SELECT CASE_NUMBER
         , MAX(GL.ACTIVE_COMMENT_AGE) AS MAX_GAP
         , ANY_VALUE(GL.BUCKET_START) AS CREATED_DATE
         , ANY_VALUE(GL.BUCKET_END)   AS CLOSED_DATE
    FROM GAP_LIST AS GL
    WHERE GL.DT = DATE_TRUNC(w, CURRENT_DATE)
    GROUP BY CASE_NUMBER
    ORDER BY MAX_GAP DESC
)

   , TEST_RESULTS AS (
    SELECT *
    FROM ION
    ORDER BY WEEK
)

   , MAIN AS (
    /*
     Last run duration:
     7 s 783 ms
     ( •̀ ω •́ )y
     */
    SELECT ION.WEEK
         , ION.AVG_CLOSED_AGE                           AS DAYS_TO_RESOLVE -- Age of cases closed in the week
         , COVERAGE.X_COVERAGE                          AS COVERAGE
         , ION.TOTAL_CLOSED_WON
         , ION.DEFAULT_TOTAL_CLOSED
         , ION.TOTAL_CREATED - ION.DEFAULT_TOTAL_CLOSED AS NET_CASE_FLOW
         , UPDATES_WEEK.AVG_AGENT_DAY_UPDATES           AS AVG_AGENT_CONTACTS
         , QA.AVG_QA                                    AS QUALITY
         , CASE_WEEK_WIP.PRE_DEFAULT_WIP                AS PRE_DEFAULT_WIP
         , CASE_WEEK_WIP.PRE_PTO_WIP                    AS PRE_PTO_DEFAULT_WIP
         , CASE_WEEK_WIP.POST_PTO_WIP                   AS POST_PTO_DEFAULT_WIP
         , CASE_WEEK_WIP.LETTERS_WIP                    AS LETTERS_WIP
         , CASE_WEEK_WIP.COLLECTIONS_WIP                AS COLLECTIONS_WIP
         , ION.AVG_OPEN_AGE                             AS AGE_OF_TOTAL_WIP
         , UPDATES_WEEK.TOTAL_UPDATES
         , ION.CLOSED_WON_RATIO
         , ION.TOTAL_AMOUNT_SAVED
         , CASE_WEEK_WIP.ACTIVE_AGENTS
    FROM ION
       , CASE_WEEK_WIP
       , GAP_WEEK_TABLE
       , UPDATES_WEEK
       , COVERAGE
       , QA
    WHERE CASE_WEEK_WIP.DT = ION.WEEK
      AND GAP_WEEK_TABLE.WEEK = ION.WEEK
      AND UPDATES_WEEK.WEEK = ION.WEEK
      AND COVERAGE.WEEK = ION.WEEK
      AND QA.WEEK = ION.WEEK
      AND ION.WEEK != DATE_TRUNC(wk, CURRENT_DATE)
    ORDER BY ION.WEEK DESC
)

SELECT *
FROM MAIN