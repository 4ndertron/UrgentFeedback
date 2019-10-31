/*
 "
 To provide the sales team with an understanding of call:
 1) types -- Types of sales account.. Corporate, door to home, retail, etc.
 2) issues -- Types of sales calls?
 3) training gaps -- More definition is needed
 4) A decrease in call volume and upset customers -- Expecting an outcome is not a requirement of a report.
 "

 Sales wants to know data. Call data, volume, NIS and Door to Home, Which customer calls are which types of acocunts.
 Top three call volumes related to Sales. Customer metions of Sales.
 Sales is pushing back hard on the claims.
 Tali wants hard data tos how 30% is Door to Home, these are the locations.
 Ratio of those calls against total call volume.

 When is the next meeting?
11/7/2019

 Breakdown rolling 30 days
 Ratio of sales calls.
 Breakdown of top three call types.

 TODO: tie call events to case comments and activities created in the same day by agent to find the "call types."

 */

WITH CALL_VIEW AS (
    /*
     With Calabrio, do agents still disposition a call?
     If they don't, then they "why" can still be found by either the activity subject code OR
     by the parent case of the case comment left by the call.
     */

    /*
     114837 DISTINCT NUMBERS
     73826 DISTINCT MAPPED NUMBERS
     64.2876% OF ALL NUMBERS ARE MAPPED WITH THIS METHOD
     */
    SELECT C.SESSION_ID
         , ANY_VALUE(C.DATE)           AS DATE        -- WHEN
         , ANY_VALUE(E.EMPLOYEE_ID)    AS EMPLOYEE_ID -- WHO
         , ANY_VALUE(C.ANI)            AS ANI
         , ANY_VALUE(C.AGENT_1_ACD_ID) AS AGENT_1_ACD_ID
         , ANY_VALUE(AC.CONTACT_PHONE) AS LD_CONTACT
         , ANY_VALUE(CT.CLEAN_PHONE)   AS SF_CONTACT
    FROM D_POST_INSTALL.T_CJP_CDR_TEMP AS C
             LEFT OUTER JOIN D_POST_INSTALL.T_EMPLOYEE_MASTER AS E
                             ON E.CJP_ID = C.AGENT_1_ACD_ID
             LEFT OUTER JOIN RPT.T_CONTACT AS CT
                             ON CT.CLEAN_PHONE = C.ANI
             LEFT OUTER JOIN (
        SELECT REGEXP_SUBSTR(BILL_TO_CONTACT_PHONE, '\\d+', 1, 1, 'e') AS CONTACT_PHONE
        FROM LD.T_ACCT_CONV) AS AC
                             ON AC.CONTACT_PHONE = C.ANI
    GROUP BY C.SESSION_ID
    ORDER BY DATE DESC
)

   , TASK_VIEW AS (
    /*
     TODO: JOIN TO CALLS ON WHO AND WHEN
     */
    SELECT T.TASK_ID
         , E.EMPLOYEE_ID  -- WHO
         , T.CREATED_DATE -- WHEN
    FROM RPT.T_TASK AS T
             LEFT OUTER JOIN D_POST_INSTALL.T_EMPLOYEE_MASTER AS E
                             ON E.SALESFORCE_ID = T.CREATED_BY_ID
             LEFT OUTER JOIN HR.T_EMPLOYEE AS HR
                             ON HR.EMPLOYEE_ID = E.EMPLOYEE_ID
)

   , CASE_COMMENT_VIEW AS (
    /*
     TODO: JOIN TO CALLS ON WHO AND WHEN
     */
    SELECT CC.ID
         , HR.EMPLOYEE_ID                -- WHO
         , CC.CREATEDATE AS CREATED_DATE -- WHEN
    FROM RPT.V_SF_CASECOMMENT AS CC
             LEFT OUTER JOIN D_POST_INSTALL.T_EMPLOYEE_MASTER AS E
                             ON E.SALESFORCE_ID = cc.CREATEDBYID
             LEFT OUTER JOIN HR.T_EMPLOYEE AS HR
                             ON HR.EMPLOYEE_ID = E.EMPLOYEE_ID
)

   , MERGE AS (
    /*
     TODO: CHECK THIS METHOD AGAINST THE MAPPED CALLS
     */
    SELECT CV.SESSION_ID
         , ANY_VALUE(CV.EMPLOYEE_ID)   AS CV_EMPLOYEE_ID
         , ANY_VALUE(CV.DATE)          AS CV_DATE
         , TV.TASK_ID
         , ANY_VALUE(TV.EMPLOYEE_ID)   AS TV_EMPLOYEE_ID
         , ANY_VALUE(TV.CREATED_DATE)  AS TV_DATE
         , CCV.ID                      AS CASE_COMMENT_ID
         , ANY_VALUE(CCV.EMPLOYEE_ID)  AS CCV_EMPLOYEE_ID
         , ANY_VALUE(CCV.CREATED_DATE) AS CCV_DATE
    FROM CALL_VIEW AS CV
             LEFT OUTER JOIN TASK_VIEW AS TV
                             ON TV.EMPLOYEE_ID = CV.EMPLOYEE_ID AND
                                TV.CREATED_DATE = CV.DATE
             LEFT OUTER JOIN CASE_COMMENT_VIEW AS CCV
                             ON CCV.EMPLOYEE_ID = CV.EMPLOYEE_ID AND
                                CCV.CREATED_DATE = CV.DATE
    WHERE CV.DATE = DATEADD(d, -5, CURRENT_DATE)
      AND TV.TASK_ID IS NOT NULL
      AND CCV.ID IS NOT NULL
    GROUP BY CV.SESSION_ID
           , TV.TASK_ID
           , CCV.ID
)

   , MAIN AS (
    SELECT COUNT(SESSION_ID)          AS CALL_RECORDS
         , COUNT(DISTINCT SESSION_ID) AS UNIQUE_CALLS
         , COUNT(CV_EMPLOYEE_ID)      AS CV_EMPLOYEE_CT
         , COUNT(TV_EMPLOYEE_ID)      AS TV_EMPLOYEE_CT
         , COUNT(CCV_EMPLOYEE_ID)     AS CCV_EMPLOYEE_CT
    FROM MERGE
)

   , QUEUES AS (
    SELECT Q.ORG_NAME
         , Q.TEAM_NAME
         , Q.QUEUE_NAME
    FROM D_POST_INSTALL.V_CJP_QUEUES AS Q
    WHERE Q.TEAM_NAME NOT ILIKE '%PERSONAL%'
      AND Q.QUEUE_NAME IS NOT NULL
)

   , TEST_TASK AS (
    SELECT COUNT(TV.TASK_ID)          AS ALL_TASKS
         , COUNT(DISTINCT TV.TASK_ID) AS UNIQUE_TASKS
         , COUNT(TV.EMPLOYEE_ID)      AS TV_EMPLOYEE_COUNT
    FROM TASK_VIEW AS TV
    WHERE TV.CREATED_DATE = DATEADD(d, -5, CURRENT_DATE)
)

   , TEST_CASE_COMMENT AS (
    SELECT COUNT(CCV.ID)          AS ALL_CASE_COMMENTS
         , COUNT(DISTINCT CCV.ID) AS UNIQUE_CASE_COMMENTS
         , COUNT(CCV.EMPLOYEE_ID) AS CCV_EMPLOYEE_COUNT
    FROM CASE_COMMENT_VIEW AS CCV
    WHERE CCV.CREATED_DATE = DATEADD(d, -5, CURRENT_DATE)
)

   , TEST_CALLS AS (
    SELECT COUNT(CV.SESSION_ID)          AS ALL_CALLS
         , COUNT(DISTINCT CV.SESSION_ID) AS DISTINCT_CALLS
         , COUNT(CV.EMPLOYEE_ID)         AS CV_EMPLOYEE_COUNT
    FROM CALL_VIEW AS CV
    WHERE CV.DATE = DATEADD(d, -5, CURRENT_DATE)
)

SELECT *
FROM MAIN