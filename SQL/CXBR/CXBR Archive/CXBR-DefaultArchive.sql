/*
 Start core tables
 */
WITH DEFAULT_CASES AS (
    SELECT C.CASE_NUMBER
         , ANY_VALUE(C.CASE_ID)                            AS CASE_ID
         , C.PROJECT_ID
         , ANY_VALUE(C.SUBJECT)                            AS SUBJECT1
         , ANY_VALUE(CAD.SYSTEM_SIZE)                      AS SYSTEM_SIZE
         , ROUND(ANY_VALUE(CAD.SYSTEM_SIZE) * 1000 * 4, 2) AS SYSTEM_VALUE
         , ANY_VALUE(C.STATUS)                             AS STATUS2
         , TO_DATE(ANY_VALUE(C.CREATED_DATE))              AS CREATED_DATE1
         , TO_DATE(ANY_VALUE(C.CLOSED_DATE))               AS CLOSED_DATE1
         , ANY_VALUE(C.RECORD_TYPE)                        AS RECORD_TYPE1
         , ANY_VALUE(S.SOLAR_BILLING_ACCOUNT_NUMBER)       AS SOLAR_BILLING_ACCOUNT_NUMBER1
         , CASE
               WHEN STATUS2 = 'Escalated' AND RECORD_TYPE1 = 'Solar - Customer Default' THEN 'Pending Legal'
               WHEN RECORD_TYPE1 = 'Solar - Customer Default'
                   AND ANY_VALUE(C.DESCRIPTION) ILIKE '%MBW%'
                   AND ANY_VALUE(C.DESCRIPTION) ILIKE '%COLLECTION%'
                   THEN 'MBW'
               WHEN RECORD_TYPE1 = 'Solar - Customer Default' AND SUBJECT1 ILIKE '%D1%' THEN 'D1'
               WHEN RECORD_TYPE1 = 'Solar - Customer Default' AND SUBJECT1 ILIKE '%D2%' THEN 'D2'
               WHEN RECORD_TYPE1 = 'Solar - Customer Default' AND SUBJECT1 ILIKE '%D4%' THEN 'D4'
               WHEN RECORD_TYPE1 = 'Solar - Customer Default' AND SUBJECT1 ILIKE '%D5%' THEN 'D5'
               WHEN RECORD_TYPE1 = 'Solar - Customer Default' AND SUBJECT1 ILIKE '%CORP%' THEN 'CORP-Default'
        END                                                AS DEFAULT_BUCKET
    FROM RPT.T_CASE AS C
             LEFT JOIN RPT.T_SERVICE AS S
                       ON C.SERVICE_ID = S.SERVICE_ID
             LEFT JOIN RPT.T_SYSTEM_DETAILS_SNAP AS CAD
                       ON CAD.SERVICE_ID = C.SERVICE_ID
             LEFT JOIN LD.T_DAILY_DATA_EXTRACT AS LDD
                       ON LDD.BILLING_ACCOUNT = S.SOLAR_BILLING_ACCOUNT_NUMBER
    WHERE (
                  (C.RECORD_TYPE = 'Solar - Customer Default'
                      AND C.SUBJECT NOT ILIKE '%D3%')
                  OR
                  (C.RECORD_TYPE IN
                   ('Solar - Customer Default', 'Solar - Billing', 'Solar - Panel Removal', 'Solar - Service',
                    'Solar Damage Resolutions', 'Solar - Customer Escalation', 'Solar - Troubleshooting')
                      AND C.SOLAR_QUEUE = 'Dispute/Evasion')
              )
    GROUP BY C.PROJECT_ID
           , C.CASE_NUMBER
    ORDER BY C.PROJECT_ID
)

   , CASE_HISTORY_TABLE AS (
    SELECT CT.*
         , NVL(LAG(CC.CREATEDATE) OVER (PARTITION BY CT.CASE_NUMBER ORDER BY CC.CREATEDATE),
               CT.CREATED_DATE1)                            AS PREVIOUS_COMMENT_DATE
         , CC.CREATEDATE                                    AS CURRENT_COMMENT_DATE
         , NVL(LEAD(CC.CREATEDATE) OVER (PARTITION BY CT.CASE_NUMBER ORDER BY CC.CREATEDATE),
               NVL(CT.CLOSED_DATE1,
                   CURRENT_DATE))                           AS NEXT_COMMENT_DATE
         , USR.NAME                                         AS COMMENT_CREATE_BY
         , HR.BUSINESS_TITLE
         , DATEDIFF(s, NVL(LAG(CC.CREATEDATE) OVER (PARTITION BY CT.CASE_NUMBER
        ORDER BY CC.CREATEDATE),
                           CT.CREATED_DATE1),
                    CC.CREATEDATE
               ) / (24 * 60 * 60)
                                                            AS LAG_GAP
         , DATEDIFF(s, CC.CREATEDATE,
                    NVL(LEAD(CC.CREATEDATE) OVER (PARTITION BY CT.CASE_NUMBER ORDER BY CC.CREATEDATE),
                        CT.CREATED_DATE1)) / (24 * 60 * 60) AS LEAD_GAP
    FROM DEFAULT_CASES AS CT
             LEFT OUTER JOIN RPT.V_SF_CASECOMMENT AS CC
                             ON CC.PARENTID = CT.CASE_ID
             LEFT JOIN RPT.V_SF_USER AS USR
                       ON USR.ID = CC.CREATEDBYID
             LEFT JOIN HR.T_EMPLOYEE AS HR
                       ON HR.EMPLOYEE_ID = USR.EMPLOYEE_ID__C
    WHERE CC.CREATEDATE <= NVL(CT.CLOSED_DATE1, CURRENT_DATE)
    ORDER BY CASE_NUMBER, CC.CREATEDATE
)

   , FULL_CASE AS (
    SELECT CASE_NUMBER
         , ANY_VALUE(SUBJECT1)                                                          AS SUBJECT
         , ANY_VALUE(SYSTEM_SIZE)                                                       AS SYSTEM_SIZE
         , ANY_VALUE(SYSTEM_VALUE)                                                      AS SYSTEM_VALUE
         , ANY_VALUE(STATUS2)                                                           AS STATUS1
         , ANY_VALUE(CREATED_DATE1)                                                     AS CREATED_DATE
         , ANY_VALUE(CLOSED_DATE1)                                                      AS CLOSED_DATE
         , DATEDIFF(dd, TO_DATE(CREATED_DATE), NVL(TO_DATE(CLOSED_DATE), CURRENT_DATE)) AS CASE_AGE
         , ANY_VALUE(RECORD_TYPE1)                                                      AS RECORD_TYPE1
         , SOLAR_BILLING_ACCOUNT_NUMBER1
         , ANY_VALUE(DEFAULT_BUCKET)                                                    AS DEFAULT_BUCKET1
         , CASE
               WHEN STATUS1 IN ('In Progress') AND DEFAULT_BUCKET1 NOT IN ('MBW', 'Pending Legal')
                   THEN 'P4/P5/DRA Letters'
               WHEN STATUS1 IN ('Pending Customer Action') AND
                    DEFAULT_BUCKET1 NOT IN ('MBW', 'Pending Legal') AND
                    ANY_VALUE(CASE_HISTORY_TABLE.RECORD_TYPE1) = 'Solar - Customer Default'
                   THEN 'Working with Customer' END                                     AS STATUS_BUCKET
         , AVG(LAG_GAP)                                                                 AS AVERAGE_GAP
    FROM CASE_HISTORY_TABLE
    GROUP BY SOLAR_BILLING_ACCOUNT_NUMBER1
           , CASE_NUMBER
    ORDER BY SOLAR_BILLING_ACCOUNT_NUMBER1
           , CASE_NUMBER
)

   , GAP_LIST AS (
    SELECT CHT.CASE_NUMBER
         , TO_DATE(CHT.PREVIOUS_COMMENT_DATE)           AS PREVIOUS_COMMENT_DATE
         , TO_DATE(CHT.CURRENT_COMMENT_DATE)            AS CURRENT_COMMENT_DATE
         , TO_DATE(CHT.NEXT_COMMENT_DATE)               AS NEXT_COMMENT_DATE
         , DATEDIFF(dd,
                    TO_DATE(CHT.CURRENT_COMMENT_DATE),
                    TO_DATE(CHT.NEXT_COMMENT_DATE))     AS LEAD_GAP
         , ANY_VALUE(CHT.COMMENT_CREATE_BY)             AS COMMENT_CREATED_BY_NAME
         , ANY_VALUE(CHT.BUSINESS_TITLE)                AS BUSINESS_TITLE
         , MAX(CHT.LAG_GAP)                             AS MAIN_GAP
         , ANY_VALUE(CHT.CREATED_DATE1)                 AS CREATED_DATE
         , ANY_VALUE(CHT.CLOSED_DATE1)                  AS CLOSED_DATE
         , ANY_VALUE(CHT.STATUS2)                       AS STATUS
         , ANY_VALUE(CHT.RECORD_TYPE1)                  AS RECORD_TYPE
         , ANY_VALUE(CHT.SOLAR_BILLING_ACCOUNT_NUMBER1) AS BILLING_ACCOUNTS
         , ANY_VALUE(CHT.CASE_ID)                       AS CASE_ID
         , ANY_VALUE(CHT.PROJECT_ID)                    AS PROJECT_ID
         , ANY_VALUE(CHT.SUBJECT1)                      AS SUBJECT
         , ANY_VALUE(CHT.SYSTEM_SIZE)                   AS SYSTEM_SIZE
         , ANY_VALUE(CHT.SYSTEM_VALUE)                  AS SYSTEM_VALUE
         , D.DT
         , ROW_NUMBER()
            OVER (PARTITION BY CHT.CASE_NUMBER
                , TO_DATE(CHT.CURRENT_COMMENT_DATE)
                ORDER BY D.DT)                          AS ACTIVE_COMMENT_AGE
    FROM CASE_HISTORY_TABLE AS CHT
             INNER JOIN RPT.T_DATES AS D
                        ON D.DT BETWEEN CHT.CURRENT_COMMENT_DATE AND CHT.NEXT_COMMENT_DATE
    WHERE CHT.STATUS2 IN ('Pending Customer Action')
      AND CHT.DEFAULT_BUCKET NOT IN ('MBW', 'Pending Legal')
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
      AND ENTIRE_HISTORY.SUPERVISOR_NAME_1 ILIKE '%PERC%'
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
              CURRENT_DATE
)

   , QA_INCONTACT AS (
    SELECT QA.QSCORE                                 AS QA_SCORE
         , QA.EVALUATION_DATE
         , QA.EVALUATION_TYPE
         , QA.AGENT_FIRST_NAME || QA.AGENT_LAST_NAME AS AGENT_EVALUATED
    FROM D_POST_INSTALL.T_NE_AGENT_QSCORE AS QA
    WHERE QA.EVALUATION_DATE BETWEEN
              DATE_TRUNC('Y', DATEADD('Y', -1, CURRENT_DATE)) AND
              CURRENT_DATE
)

   , QA AS (
    SELECT LAST_DAY(D.DT) AS MONTH1
         , ROUND(AVG(CASE
                         WHEN
                             TO_DATE(QA.evaluation_date) = D.DT
                             THEN QA.qa_score
        END), 2)          AS AVG_QA
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
        CURRENT_DATE
      AND QA.direct_manager IS NOT NULL
    GROUP BY MONTH1
    ORDER BY MONTH1
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
              CURRENT_DATE
    GROUP BY D.DT
    ORDER BY D.DT
)

   , UPDATES_DAY AS (
    SELECT D.DT
         , COUNT(CASE
                     WHEN TO_DATE(CHT.CURRENT_COMMENT_DATE) = D.DT
                         THEN 1 END)                  AS UPDATES
         , IFF(DAYNAME(D.DT) IN ('Sat', 'Sun'), 0, 1) AS WORKDAY
         , DW.ACTIVE_AGENTS
    FROM CASE_HISTORY_TABLE AS CHT
       , RPT.T_DATES AS D
       , DEFAULT_AGENT_DAY_WIP AS DW
    WHERE D.DT BETWEEN
        DATE_TRUNC('Y', DATEADD('Y', -1, CURRENT_DATE)) AND
        CURRENT_DATE
      AND DW.DT = D.DT
    GROUP BY D.DT, DW.ACTIVE_AGENTS
    ORDER BY D.DT
)

   , CASE_DAY_WIP AS (
    SELECT D.DT
         , COUNT(CASE
                     WHEN TO_DATE(FC.CREATED_DATE) <= D.DT AND
                          STATUS_BUCKET = 'Working with Customer' AND
                          (TO_DATE(FC.CLOSED_DATE) >= D.DT OR FC.CLOSED_DATE IS NULL)
                         THEN 1 END) AS CASE_ACTIVE_WIP
         , COUNT(CASE
                     WHEN TO_DATE(FC.CREATED_DATE) <= D.DT AND
                          (TO_DATE(FC.CLOSED_DATE) >= D.DT OR FC.CLOSED_DATE IS NULL)
                         THEN 1 END) AS CASE_TOTAL_WIP
    FROM RPT.T_DATES AS D
       , FULL_CASE AS FC
    WHERE D.DT BETWEEN
              DATE_TRUNC('Y', DATEADD('Y', -1, CURRENT_DATE)) AND
              CURRENT_DATE
--       AND STATUS_BUCKET = 'Working with Customer'
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
         , ROUND(CW.CASE_TOTAL_WIP / DW.ACTIVE_AGENTS, 2) AS AVERAGE_AGENT_WIP
         , CW.CASE_ACTIVE_WIP
         , CW.CASE_TOTAL_WIP
         , DW.ACTIVE_AGENTS
    FROM CASE_DAY_WIP CW
       , DEFAULT_AGENT_DAY_WIP AS DW
    WHERE (CW.DT = LAST_DAY(CW.DT) OR CW.DT = CURRENT_DATE)
      AND DW.DT = CW.DT
)

   , GAP_MONTH_TABLE AS (
    SELECT IFF(LAST_DAY(D.DT) > CURRENT_DATE, CURRENT_DATE, LAST_DAY(D.DT)) AS MONTH
         , AVG(CASE
                   WHEN GL.DT = MONTH
                       THEN GL.ACTIVE_COMMENT_AGE END)                      AS AVG_GAP
         , MAX(CASE
                   WHEN GL.DT = MONTH
                       THEN GL.ACTIVE_COMMENT_AGE END)                      AS MAX_GAP
    FROM GAP_LIST AS GL
       , RPT.T_DATES AS D
    WHERE D.DT BETWEEN
              DATE_TRUNC('Y', DATEADD('Y', -1, CURRENT_DATE)) AND
              CURRENT_DATE
    GROUP BY MONTH
    ORDER BY MONTH
)

   , UPDATES_MONTH AS (
    SELECT IFF(LAST_DAY(U.DT) > CURRENT_DATE, CURRENT_DATE, LAST_DAY(U.DT)) AS MONTH
         , SUM(U.UPDATES)                                                   AS TOTAL_UPDATES
         , SUM(U.WORKDAY)                                                   AS WORKDAYS
         , ROUND(TOTAL_UPDATES / WORKDAYS, 2)                               AS AVG_DAY_UPDATES
         , ROUND(AVG_DAY_UPDATES / MIN(U.ACTIVE_AGENTS), 2)                 AS AVG_AGENT_DAY_UPDATES
    FROM UPDATES_DAY AS U
    GROUP BY MONTH
)

   , ION AS (
    SELECT IFF(LAST_DAY(D.DT) > CURRENT_DATE, CURRENT_DATE, LAST_DAY((D.DT))) AS MONTH1
         , ROUND(COUNT(CASE
                           WHEN TO_DATE(CLOSED_DATE) = D.DT
                               THEN 1 END) / DW.ACTIVE_AGENTS, 2)             AS AVERAGE_CLOSED
         , ROUND(COUNT(CASE
                           WHEN TO_DATE(CLOSED_DATE) = D.DT
                               THEN 1 END), 2)                                AS TOTAL_CLOSED
         , ROUND(COUNT(CASE
                           WHEN TO_DATE(CLOSED_DATE) = D.DT
                               AND STATUS1 = 'Closed - Saved'
                               THEN 1 END), 2)                                AS TOTAL_CLOSED_WON
         , ROUND(TOTAL_CLOSED_WON / TOTAL_CLOSED, 4)                          AS CLOSED_WON_RATIO
         , ROUND(COUNT(CASE
                           WHEN TO_DATE(CREATED_DATE) = D.DT
                               THEN 1 END) / DW.ACTIVE_AGENTS, 2)             AS AVERAGE_CREATED
         , ROUND(COUNT(CASE
                           WHEN TO_DATE(CREATED_DATE) = D.DT
                               THEN 1 END), 2)                                AS TOTAL_CREATED
         , ROUND(COUNT(CASE
                           WHEN TO_DATE(CLOSED_DATE) = D.DT AND STATUS1 = 'Closed - Saved'
                               THEN 1 END) / DW.ACTIVE_AGENTS,
                 2)                                                           AS AVERAGE_CLOSED_WON_CASES
         , ROUND(SUM(CASE
                         WHEN TO_DATE(CLOSED_DATE) = D.DT AND STATUS1 = 'Closed - Saved'
                             THEN SYSTEM_VALUE END) / DW.ACTIVE_AGENTS, 2)    AS AVERAGE_AMOUNT_SAVED
         , ROUND(SUM(CASE
                         WHEN TO_DATE(CLOSED_DATE) = D.DT AND STATUS1 = 'Closed - Saved'
                             THEN SYSTEM_VALUE END), 2)                       AS TOTAL_AMOUNT_SAVED
         , ROUND(AVG(CASE
                         WHEN TO_DATE(CREATED_DATE) <= D.DT AND
                              (TO_DATE(CLOSED_DATE) > D.DT OR
                               CLOSED_DATE IS NULL)
                             AND DEFAULT_BUCKET1 NOT IN ('MBW', 'Pending Legal')
                             AND STATUS1 NOT ILIKE '%ESCALATED%'
                             THEN DATEDIFF(dd, TO_DATE(FC.CREATED_DATE), D.DT)
        END), 2)                                                              AS AVG_OPEN_AGE
         , MAX(CASE
                   WHEN TO_DATE(CREATED_DATE) <= D.DT AND
                        (TO_DATE(CLOSED_DATE) > D.DT OR
                         CLOSED_DATE IS NULL)
                       AND STATUS1 NOT ILIKE '%DISPUTE%'
                       THEN DATEDIFF(dd, TO_DATE(FC.CREATED_DATE), D.DT)
        END)                                                                  AS MAX_MONTH_AGE
         , ROUND(AVG(CASE
                         WHEN TO_DATE(CLOSED_DATE) = D.DT
                             THEN CASE_AGE END), 2)                           AS AVG_CLOSED_AGE
         , DW.ACTIVE_AGENTS
    FROM FULL_CASE AS FC
       , RPT.T_DATES AS D
       , DEFAULT_AGENT_DAY_WIP AS DW
    WHERE D.DT BETWEEN
        DATE_TRUNC('Y', DATEADD('Y', -1, CURRENT_DATE)) AND
        CURRENT_DATE
      AND DW.DT = MONTH1
    GROUP BY MONTH1, DW.ACTIVE_AGENTS
    ORDER BY MONTH1, DW.ACTIVE_AGENTS
)

   , COVERAGE AS (
    SELECT MONTH
         , U.TOTAL_UPDATES
         , C.CASE_TOTAL_WIP
         , ROUND(U.TOTAL_UPDATES / C.CASE_TOTAL_WIP, 2) AS X_COVERAGE
    FROM UPDATES_MONTH AS U
       , CASE_MONTH_WIP AS C
    WHERE C.DT = U.MONTH
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
    WHERE GL.DT = current_date
    GROUP BY CASE_NUMBER
    ORDER BY MAX_GAP DESC
)

   , TEST_RESULTS AS (
    SELECT *
    FROM COVERAGE
    ORDER BY MONTH
)

   , MAIN AS (
    /*
     Last run duration:
     7 s 783 ms
     ( •̀ ω •́ )y
     */
    SELECT ION.MONTH1
         , ION.TOTAL_CREATED             AS "In"         -- Metric 1
         , ION.TOTAL_CLOSED              AS "Out"        -- Metric 1
         , CASE_MONTH_WIP.CASE_TOTAL_WIP AS WIP          -- Metric 2
         , COVERAGE.TOTAL_UPDATES        AS "Updates"    -- Information
         , COVERAGE.X_COVERAGE           AS "X Coverage" -- Metric 3
         , ION.CLOSED_WON_RATIO          AS WL           -- Metric 4
         , ION.TOTAL_AMOUNT_SAVED        AS "Saved"      -- Metric 5
         , CASE_MONTH_WIP.ACTIVE_AGENTS                  -- Indirect Reference Point
    FROM ION
       , CASE_MONTH_WIP
       , GAP_MONTH_TABLE
       , UPDATES_MONTH
       , COVERAGE
    WHERE CASE_MONTH_WIP.DT = ION.MONTH1
      AND GAP_MONTH_TABLE.MONTH = ION.MONTH1
      AND UPDATES_MONTH.MONTH = ION.MONTH1
      AND COVERAGE.MONTH = ION.MONTH1
      AND ION.MONTH1 != DATE_TRUNC('MM', CURRENT_DATE)
    ORDER BY ION.MONTH1
)

SELECT *
FROM MAIN
