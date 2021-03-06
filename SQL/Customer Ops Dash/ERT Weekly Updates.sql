/*
 Start core tables
 */
WITH ESCALATION_CASES AS (
    SELECT C.CASE_NUMBER
         , C.CASE_ID
         , C.PROJECT_ID
         , C.SUBJECT
         , C.STATUS
         , C.CUSTOMER_TEMPERATURE
         , TO_DATE(C.CREATED_DATE) AS CASE_CREATED_DATE
         , C.CREATED_DATE          AS CASE_CREATED_DATETIME
         , TO_DATE(C.CLOSED_DATE)  AS CASE_CLOSED_DATE
         , C.CLOSED_DATE           AS CASE_CLOSED_DATETIME
         , C.RECORD_TYPE
    FROM RPT.T_CASE AS C
    WHERE C.RECORD_TYPE = 'Solar - Customer Escalation'
      AND C.EXECUTIVE_RESOLUTIONS_ACCEPTED IS NOT NULL
      AND C.SUBJECT NOT ILIKE '%VIP%'
      AND C.SUBJECT NOT ILIKE '%NPS%'
      AND C.SUBJECT NOT ILIKE '%COMP%'
    ORDER BY C.PROJECT_ID
)

   , SYSTEM_DAMAGE_CASES AS (
    SELECT C.CASE_NUMBER
         , C.CASE_ID
         , C.PROJECT_ID
         , C.SUBJECT
         , C.STATUS
         , C.CUSTOMER_TEMPERATURE
         , TO_DATE(C.CREATED_DATE) AS CREATED_DATE
         , TO_DATE(C.CLOSED_DATE)  AS CLOSED_DATE
         , C.RECORD_TYPE
    FROM RPT.T_CASE AS C
    WHERE C.RECORD_TYPE = 'Solar - Service'
      AND C.PRIMARY_REASON = 'System Damage'
)

   , RELOCATION_CASES AS (
    SELECT C.CASE_NUMBER
         , C.CASE_ID
         , C.PROJECT_ID
         , C.SUBJECT
         , C.STATUS
         , C.CUSTOMER_TEMPERATURE
         , TO_DATE(C.CREATED_DATE) AS CREATED_DATE
         , TO_DATE(C.CLOSED_DATE)  AS CLOSED_DATE
         , C.RECORD_TYPE
    FROM RPT.T_CASE AS C
    WHERE C.RECORD_TYPE = 'Solar - Transfer'
      AND C.PRIMARY_REASON = 'Relocation'
      AND C.OWNER_EMPLOYEE_ID IN ('210140')
)

   , CASES_OVERALL AS (
    SELECT *
    FROM (
                 (SELECT * FROM ESCALATION_CASES)
--                  UNION
--                  (SELECT * FROM RELOCATION_CASES)
--                  UNION
--                  (SELECT * FROM SYSTEM_DAMAGE_CASES)
         ) AS C
)

   , CASE_HISTORY_TABLE AS (
    SELECT CO.*
         , NVL(LAG(CC.CREATEDATE) OVER (PARTITION BY CO.CASE_NUMBER ORDER BY CC.CREATEDATE),
               CO.CASE_CREATED_DATETIME)                            AS PREVIOUS_COMMENT_DATE
         , CC.CREATEDATE                                            AS CURRENT_COMMENT_DATE
         , NVL(LEAD(CC.CREATEDATE) OVER (PARTITION BY CO.CASE_NUMBER ORDER BY CC.CREATEDATE),
               NVL(CO.CASE_CLOSED_DATETIME,
                   CURRENT_TIMESTAMP))                              AS NEXT_COMMENT_DATE
         , USR.NAME                                                 AS COMMENT_CREATE_BY
         , HR.BUSINESS_TITLE
         , DATEDIFF(s, NVL(LAG(CC.CREATEDATE) OVER (PARTITION BY CO.CASE_NUMBER
        ORDER BY CC.CREATEDATE),
                           CO.CASE_CREATED_DATETIME),
                    CC.CREATEDATE
               ) / (24 * 60 * 60)
                                                                    AS LAG_GAP
         , DATEDIFF(s, CC.CREATEDATE,
                    NVL(LEAD(CC.CREATEDATE) OVER (PARTITION BY CO.CASE_NUMBER ORDER BY CC.CREATEDATE),
                        CO.CASE_CREATED_DATETIME)) / (24 * 60 * 60) AS LEAD_GAP
         , IFF(PREVIOUS_COMMENT_DATE = CO.CASE_CREATED_DATETIME,
               DATEDIFF(s, CO.CASE_CREATED_DATETIME, CC.CREATEDATE),
               NULL) / (24 * 60 * 60)                               AS INTIAL_RESPONSE
    FROM CASES_OVERALL AS CO
             LEFT OUTER JOIN RPT.V_SF_CASECOMMENT AS CC
                             ON CC.PARENTID = CO.CASE_ID
             LEFT JOIN RPT.V_SF_USER AS USR
                       ON USR.ID = CC.CREATEDBYID
             LEFT JOIN HR.T_EMPLOYEE AS HR
                       ON HR.EMPLOYEE_ID = USR.EMPLOYEE_ID__C
    WHERE CC.CREATEDATE <= NVL(CO.CASE_CLOSED_DATE, CURRENT_TIMESTAMP)
    ORDER BY CASE_NUMBER, CC.CREATEDATE
)

   , FULL_CASE AS (
    SELECT CASE_NUMBER
         , ANY_VALUE(SUBJECT)                                                AS SUBJECT
         , ANY_VALUE(CAD.SYSTEM_SIZE)                                        AS SYSTEM_SIZE
         , ANY_VALUE(CAD.SYSTEM_SIZE) * (1000 * 4)                           AS SYSTEM_VALUE
         , ANY_VALUE(STATUS)                                                 AS STATUS
         , ANY_VALUE(CUSTOMER_TEMPERATURE)                                   AS CUSTOMER_TEMPERATURE
         , ANY_VALUE(CHT.CASE_CREATED_DATE)                                  AS CREATED_DATE
         , ANY_VALUE(CHT.CASE_CLOSED_DATE)                                   AS CLOSED_DATE
         , ANY_VALUE(RECORD_TYPE)                                            AS RECORD_TYPE
         , DATEDIFF(dd, TO_DATE(ANY_VALUE(CASE_CREATED_DATE)),
                    NVL(TO_DATE(ANY_VALUE(CASE_CLOSED_DATE)), CURRENT_DATE)) AS CASE_AGE
         , AVG(LAG_GAP)                                                      AS AVERAGE_GAP
         , ANY_VALUE(INTIAL_RESPONSE)                                        AS INITIAL_RESPONSE
    FROM CASE_HISTORY_TABLE AS CHT
             LEFT OUTER JOIN RPT.T_SYSTEM_DETAILS_SNAP AS CAD
                             ON CAD.PROJECT_ID = CHT.PROJECT_ID
    GROUP BY CASE_NUMBER
    ORDER BY CASE_NUMBER
)

   , ER_AGENTS AS (
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
                  , ANY_VALUE(POSITION_TITLE)                                                AS POSITION_TITLE
                  , ANY_VALUE(HR.FULL_NAME)                                                  AS FULL_NAME
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
                  , IFF(ANY_VALUE(HR.COST_CENTER_ID) IN ('3400', '3700', '4967-60'),
                        TRUE,
                        FALSE)                                                               AS DIRECTOR_ORG
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
      AND (ENTIRE_HISTORY.SUPERVISOR_NAME_1 ILIKE '%OB AZEV%'
        OR ENTIRE_HISTORY.SUPERVISOR_NAME_1 ILIKE '%ALFONSO C%')
    GROUP BY EMPLOYEE_ID, DIRECTOR_ORG
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
         , ANY_VALUE(CHT.CASE_CREATED_DATE)         AS CREATED_DATE
         , ANY_VALUE(CHT.CASE_CLOSED_DATE)          AS CLOSED_DATE
         , ANY_VALUE(CHT.STATUS)                    AS STATUS
         , ANY_VALUE(CHT.RECORD_TYPE)               AS RECORD_TYPE
         , ANY_VALUE(CHT.CASE_ID)                   AS CASE_ID
         , ANY_VALUE(CHT.PROJECT_ID)                AS PROJECT_ID
         , ANY_VALUE(CHT.SUBJECT)                   AS SUBJECT
         , ANY_VALUE(CHT.CUSTOMER_TEMPERATURE)      AS CUSTOMER_TEMPERATURE
         , D.DT
         , ROW_NUMBER()
            OVER (PARTITION BY CHT.CASE_NUMBER
                , TO_DATE(CHT.CURRENT_COMMENT_DATE)
                ORDER BY D.DT)                      AS ACTIVE_COMMENT_AGE
    FROM CASE_HISTORY_TABLE AS CHT
             INNER JOIN RPT.T_DATES AS D
                        ON D.DT BETWEEN CHT.CURRENT_COMMENT_DATE AND CHT.NEXT_COMMENT_DATE
    WHERE STATUS NOT ILIKE '%DISPUTE%'
      AND COMMENT_CREATE_BY NOT ILIKE '%OB AZE%'
      AND COMMENT_CREATE_BY NOT ILIKE '%NSO CON%'
    GROUP BY CASE_NUMBER,
             CURRENT_COMMENT_DATE,
             PREVIOUS_COMMENT_DATE,
             NEXT_COMMENT_DATE,
             D.DT
)

   , DAY_GAP_LIST AS (
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
         , ANY_VALUE(CHT.CASE_CREATED_DATE)         AS CREATED_DATE
         , ANY_VALUE(CHT.CASE_CLOSED_DATE)          AS CLOSED_DATE
         , ANY_VALUE(CHT.STATUS)                    AS STATUS
         , ANY_VALUE(CHT.RECORD_TYPE)               AS RECORD_TYPE
         , ANY_VALUE(CHT.CASE_ID)                   AS CASE_ID
         , ANY_VALUE(CHT.PROJECT_ID)                AS PROJECT_ID
         , ANY_VALUE(CHT.SUBJECT)                   AS SUBJECT
         , ANY_VALUE(CHT.CUSTOMER_TEMPERATURE)      AS CUSTOMER_TEMPERATURE
         , D.DT
         , ROW_NUMBER()
            OVER (PARTITION BY CHT.CASE_NUMBER
                , TO_DATE(CHT.CURRENT_COMMENT_DATE)
                ORDER BY D.DT)                      AS ACTIVE_COMMENT_AGE
    FROM CASE_HISTORY_TABLE AS CHT
             INNER JOIN RPT.T_DATES AS D
                        ON D.DT BETWEEN CHT.CURRENT_COMMENT_DATE AND CHT.NEXT_COMMENT_DATE
    WHERE STATUS NOT ILIKE '%DISPUTE%'
      AND COMMENT_CREATE_BY NOT ILIKE '%OB AZE%'
      AND COMMENT_CREATE_BY NOT ILIKE '%NSO CON%'
    GROUP BY CASE_NUMBER,
             CURRENT_COMMENT_DATE,
             PREVIOUS_COMMENT_DATE,
             NEXT_COMMENT_DATE,
             D.DT
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
             ER_AGENTS AS DA
             ON DA.FULL_NAME = C.agent_evaluated
        UNION ALL
        SELECT I.QA_SCORE
             , I.AGENT_EVALUATED
             , I.EVALUATION_DATE
             , DA.direct_manager
        FROM QA_INCONTACT AS I
                 LEFT JOIN
             ER_AGENTS AS DA
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
             LEFT OUTER JOIN ER_AGENTS AS A
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
                          DA.TEAM_END_DATE > D.DT
                         THEN 1 END) AS ACTIVE_AGENTS
    FROM RPT.T_DATES AS D
       , ER_AGENTS AS DA
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
                     WHEN TO_DATE(FC.CREATED_DATE) <= D.DT AND
                          FC.STATUS NOT ILIKE '%DISPUTE%' AND
                          (TO_DATE(FC.CLOSED_DATE) >= D.DT OR FC.CLOSED_DATE IS NULL)
                         THEN 1 END) AS CASE_ACTIVE_WIP
    FROM RPT.T_DATES AS D
       , FULL_CASE AS FC
    WHERE D.DT BETWEEN
              DATE_TRUNC('Y', DATEADD('Y', -1, CURRENT_DATE)) AND
              DATE_TRUNC(w, CURRENT_DATE)
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
         , ROUND(CW.CASE_ACTIVE_WIP / DW.ACTIVE_AGENTS, 2) AS AVERAGE_AGENT_WIP
         , CW.CASE_ACTIVE_WIP
         , DW.ACTIVE_AGENTS
    FROM CASE_DAY_WIP CW
       , DEFAULT_AGENT_DAY_WIP AS DW
    WHERE DAYOFWEEK(CW.DT) = 1
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
      AND GL.STATUS NOT ILIKE '%DISPUTE%'
    GROUP BY WEEK
    ORDER BY WEEK
)

   , UPDATES_MONTH AS (
    SELECT DATE_TRUNC(w, U.DT)                            AS WEEK
         , SUM(U.UPDATES)                                 AS TOTAL_UPDATES
         , SUM(U.WORKDAY)                                 AS WORKDAYS
         , ROUND(TOTAL_UPDATES / WORKDAYS, 2)             AS AVG_DAY_UPDATES
         , ROUND(TOTAL_UPDATES / MIN(U.ACTIVE_AGENTS), 2) AS AVG_AGENT_DAY_UPDATES
    FROM UPDATES_DAY AS U
    GROUP BY WEEK
)

   , ION AS (
    SELECT DATE_TRUNC(w, D.DT)                                    AS WEEK
         , ROUND(COUNT(CASE
                           WHEN TO_DATE(CLOSED_DATE) = D.DT
                               THEN 1 END) / DW.ACTIVE_AGENTS, 2) AS AVERAGE_CLOSED
         , ROUND(COUNT(CASE
                           WHEN TO_DATE(CLOSED_DATE) = D.DT
                               THEN 1 END), 2)                    AS TOTAL_CLOSED
         , ROUND(SUM(CASE
                         WHEN TO_DATE(CLOSED_DATE) = D.DT
                             THEN SYSTEM_VALUE END), 2)           AS TOTAL_CLOSED_SAVED
         , ROUND(SUM(CASE
                         WHEN TO_DATE(CLOSED_DATE) = D.DT
                             AND STATUS ILIKE '%SAVE%'
                             THEN SYSTEM_VALUE END), 2)           AS CLOSED_WON_SAVED
         , ROUND(CLOSED_WON_SAVED / TOTAL_CLOSED_SAVED, 4)        AS CLOSED_WON_RATIO
         , ROUND(COUNT(CASE
                           WHEN TO_DATE(CREATED_DATE) = D.DT
                               THEN 1 END) / DW.ACTIVE_AGENTS, 2) AS AVERAGE_CREATED
         , ROUND(COUNT(CASE
                           WHEN TO_DATE(CREATED_DATE) = D.DT
                               THEN 1 END), 2)                    AS TOTAL_CREATED
         , ROUND(AVG(CASE
                         WHEN TO_DATE(CREATED_DATE) <= D.DT AND
                              (TO_DATE(CLOSED_DATE) > D.DT OR
                               CLOSED_DATE IS NULL)
                             AND STATUS NOT ILIKE '%DISPUTE%'
                             THEN DATEDIFF(dd, TO_DATE(FC.CREATED_DATE), D.DT)
        END), 2)                                                  AS AVG_OPEN_AGE
         , ROUND(AVG(CASE
                         WHEN TO_DATE(CLOSED_DATE) = D.DT
                             THEN CASE_AGE END), 2)               AS AVG_CLOSED_AGE
         , ROUND(AVG(CASE
                         WHEN TO_DATE(CLOSED_DATE) >= DATEADD(DAY, -7, CURRENT_DATE)
                             THEN DATEDIFF(dd, TO_DATE(FC.CREATED_DATE), TO_DATE(FC.CLOSED_DATE))
        END), 2)                                                  AS AVG_7_DAY_CLOSED_AGE
         , MAX(CASE
                   WHEN TO_DATE(CREATED_DATE) <= D.DT AND
                        (TO_DATE(CLOSED_DATE) > D.DT OR
                         CLOSED_DATE IS NULL)
                       AND STATUS NOT ILIKE '%DISPUTE%'
                       THEN DATEDIFF(dd, TO_DATE(FC.CREATED_DATE), D.DT)
        END)                                                      AS MAX_MONTH_AGE
         , ROUND(COUNT(CASE
                           WHEN TO_DATE(CLOSED_DATE) = D.DT AND CUSTOMER_TEMPERATURE != 'Escalated'
                               THEN 1 END) / DW.ACTIVE_AGENTS,
                 2)                                               AS AVERAGE_CLOSED_WON_CASES
         , ROUND(AVG(CASE
                         WHEN TO_DATE(CREATED_DATE) = D.DT
                             THEN INITIAL_RESPONSE END), 2)       AS INITIAL_RESPONSE
         , DW.ACTIVE_AGENTS
    FROM FULL_CASE AS FC
       , RPT.T_DATES AS D
       , DEFAULT_AGENT_DAY_WIP AS DW
    WHERE D.DT BETWEEN
        DATE_TRUNC('Y', DATEADD('Y', -1, CURRENT_DATE)) AND
        DATE_TRUNC(w, CURRENT_DATE)
      AND DW.DT = WEEK
    GROUP BY WEEK, DW.ACTIVE_AGENTS
    ORDER BY WEEK, DW.ACTIVE_AGENTS
)
/*
 End month wip tables
 */

   , FIND_CASE_BY_GAP AS (
    SELECT CASE_NUMBER
         , MAX(GL.ACTIVE_COMMENT_AGE) AS MAX_GAP
         , ANY_VALUE(GL.CREATED_DATE) AS CREATED_DATE
         , ANY_VALUE(GL.CLOSED_DATE)  AS CLOSED_DATE
    FROM GAP_LIST AS GL
    WHERE GL.DT = CURRENT_DATE
    GROUP BY CASE_NUMBER
    ORDER BY MAX_GAP DESC
)

   , TEST_RESULTS AS (
    SELECT *
    FROM FULL_CASE
    WHERE DATE_TRUNC(w, CREATED_DATE) = DATE_TRUNC(w, DATEADD(dd, -7, CURRENT_DATE))
)

   , MAIN AS (
    SELECT ION.WEEK
         , ION.AVG_CLOSED_AGE                   AS DAYS_TO_RESOLVE
         , GAP_WEEK_TABLE.AVG_GAP               AS UPDATE_GAP
         , ION.TOTAL_CREATED - ION.TOTAL_CLOSED AS NET_CASE_FLOW
         , UPDATES_MONTH.AVG_AGENT_DAY_UPDATES  AS AVG_AGENT_CONTACTS
         , QA.AVG_QA                            AS QUALITY
         , CASE_WEEK_WIP.CASE_ACTIVE_WIP        AS WIP
         , ION.AVG_OPEN_AGE                     AS AGE_OF_WIP
         , ION.INITIAL_RESPONSE
         , UPDATES_MONTH.TOTAL_UPDATES
         , CASE_WEEK_WIP.ACTIVE_AGENTS
    FROM ION
       , CASE_WEEK_WIP
       , GAP_WEEK_TABLE
       , UPDATES_MONTH
       , QA
    WHERE CASE_WEEK_WIP.DT = ION.WEEK
      AND GAP_WEEK_TABLE.WEEK = ION.WEEK
      AND UPDATES_MONTH.WEEK = ION.WEEK
      AND QA.WEEK = ION.WEEK
      AND ION.WEEK != DATE_TRUNC(wk, CURRENT_DATE)
    ORDER BY ION.WEEK DESC
)

SELECT *
FROM MAIN
