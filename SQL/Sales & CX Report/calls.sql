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

 */

WITH CALL_VIEW AS (
    SELECT C.SESSION_ID
         , ANY_VALUE(C.DATE)           AS DATE        -- WHEN
         , ANY_VALUE(C.CALL_START)     AS CALL_START  -- WHEN
         , ANY_VALUE(C.CALL_END)       AS CALL_END    -- WHEN
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
    WHERE C.QUEUE_1 NOT ILIKE '%OUT%'
    GROUP BY C.SESSION_ID
    ORDER BY DATE DESC
)

   , TASK_VIEW AS (
    SELECT T.TASK_ID
         , E.EMPLOYEE_ID                           -- WHO
         , TO_DATE(T.CREATED_DATE) AS CREATED_DATE -- WHEN
         , TO_TIME(T.CREATED_DATE) AS CREATED_TIME -- WHEN
         , T.SUBJECT
         , CASE
               WHEN T.SUBJECT ILIKE '%SP.%'
                   THEN 1 END      AS SALES_TALLY
    FROM RPT.T_TASK AS T
             LEFT OUTER JOIN D_POST_INSTALL.T_EMPLOYEE_MASTER AS E
                             ON E.SALESFORCE_ID = T.CREATED_BY_ID
             LEFT OUTER JOIN HR.T_EMPLOYEE AS HR
                             ON HR.EMPLOYEE_ID = E.EMPLOYEE_ID
    WHERE T.CREATED_DATE >= DATE_TRUNC('Y', CURRENT_DATE)
)

   , CASE_COMMENT_VIEW AS (
    SELECT CC.ID
         , HR.EMPLOYEE_ID                         -- WHO
         , TO_DATE(CC.CREATEDATE) AS CREATED_DATE -- WHEN
         , TO_TIME(CC.CREATEDATE) AS CREATED_TIME -- WHEN
         , C.CASE_NUMBER
         , C.RECORD_TYPE
         , C.PRIMARY_REASON
         , C.SOLAR_QUEUE
         , CASE
               WHEN C.PRIMARY_REASON ILIKE '%SALE%'
                   THEN 1 END     AS SALES_TALLY
    FROM RPT.V_SF_CASECOMMENT AS CC
             LEFT OUTER JOIN D_POST_INSTALL.T_EMPLOYEE_MASTER AS E
                             ON E.SALESFORCE_ID = cc.CREATEDBYID
             LEFT OUTER JOIN HR.T_EMPLOYEE AS HR
                             ON HR.EMPLOYEE_ID = E.EMPLOYEE_ID
             LEFT OUTER JOIN RPT.T_CASE AS C
                             ON CC.PARENTID = C.CASE_ID
             LEFT OUTER JOIN RPT.T_PAYMENT AS PT
                             ON PT.CASE_ID = CC.PARENTID
    WHERE CC.CREATEDATE >= DATE_TRUNC('Y', CURRENT_DATE)
)

   , MERGE AS (
    SELECT CV.SESSION_ID
         , ANY_VALUE(CV.EMPLOYEE_ID)     AS CV_EMPLOYEE_ID
         , ANY_VALUE(CV.DATE)            AS CV_DATE
         , ANY_VALUE(CV.CALL_START)      AS CV_START
         , ANY_VALUE(CV.CALL_END)        AS CV_END
         , TV.TASK_ID
         , ANY_VALUE(TV.EMPLOYEE_ID)     AS TV_EMPLOYEE_ID
         , ANY_VALUE(TV.CREATED_DATE)    AS TV_DATE
         , ANY_VALUE(TV.CREATED_TIME)    AS TV_TIME
         , ANY_VALUE(TV.SUBJECT)         AS TV_SUBJECT
         , SUM(TV.SALES_TALLY)           AS TV_SALES_TALLY
         , CCV.ID                        AS CASE_COMMENT_ID
         , ANY_VALUE(CCV.EMPLOYEE_ID)    AS CCV_EMPLOYEE_ID
         , ANY_VALUE(CCV.CREATED_DATE)   AS CCV_DATE
         , ANY_VALUE(CCV.CREATED_TIME)   AS CCV_TIME
         , ANY_VALUE(CCV.PRIMARY_REASON) AS CCV_PRIMARY_REASON
         , SUM(CCV.SALES_TALLY)          AS CCV_SALES_TALLY
    FROM CALL_VIEW AS CV
             LEFT OUTER JOIN TASK_VIEW AS TV
                             ON TV.EMPLOYEE_ID = CV.EMPLOYEE_ID AND
                                TV.CREATED_DATE = CV.DATE AND
                                TV.CREATED_TIME BETWEEN
                                    CV.CALL_START AND
                                    TIMEADD(mi, 5, CV.CALL_END)
             LEFT OUTER JOIN CASE_COMMENT_VIEW AS CCV
                             ON CCV.EMPLOYEE_ID = CV.EMPLOYEE_ID AND
                                CCV.CREATED_DATE = CV.DATE AND
                                CCV.CREATED_TIME BETWEEN
                                    CV.CALL_START AND
                                    TIMEADD(mi, 5, CV.CALL_END)
    GROUP BY CV.SESSION_ID
           , TV.TASK_ID
           , CCV.ID
)


   , MAIN AS (
    SELECT D.DT
         , COUNT(DISTINCT SESSION_ID)                         AS TOTAL_CALLS
         , NVL(SUM(TV_SALES_TALLY) + SUM(CCV_SALES_TALLY), 0) AS SALES_CALLS
         , SALES_CALLS / TOTAL_CALLS                          AS SALES_CALL_RATIO
         , CURRENT_DATE                                       AS LAST_REFRESHED
    FROM RPT.T_DATES AS D
             INNER JOIN MERGE AS M
                        ON M.CV_DATE = D.DT
    GROUP BY D.DT
    ORDER BY D.DT DESC
)

   , TEST_CTE AS (
    SELECT MAX(SALES_CALL_RATIO)    AS MAX_RATIO
         , AVG(SALES_CALL_RATIO)    AS AVT_RATIO
         , MEDIAN(SALES_CALL_RATIO) AS MEDIAN_RATIO
    FROM MAIN
)

SELECT *
FROM MAIN