/*
 "
 To provide the sales team with an understanding of call:
 1) types
 2) issues
 3) training gaps.
 4) A decrease in call volume and upset customers.
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
 Brakdown of top three call types.

 TODO: tie call events to case comments and activities created in the same day by agent.

 */

WITH CALL_VIEW AS (
    /*
     With Calabrio, do agents still disposition a call?
     If they don't, then they "why" can still be found by either the activity subject code OR
     by the parent case of the case comment left by the call.
     */
    SELECT C.DATE
         , C.QUEUE_1
         , C.AGENT_1
         , TP.TEAM
         , CT.ACCOUNT_ID
         , P.SERVICE_STATE
    FROM D_POST_INSTALL.T_CJP_CDR_TEMP AS C
             LEFT JOIN RPT.T_CONTACT AS CT
                       ON NVL(CLEAN_MOBILE_PHONE, CLEAN_PHONE) = C.ANI
             LEFT JOIN RPT.T_PROJECT AS P
                       ON P.ACCOUNT_ID = CT.ACCOUNT_ID
             LEFT JOIN CALABRIO.T_PERSONS AS TP
                       ON TP.ACD_ID = C.AGENT_1_ACD_ID
             LEFT JOIN D_POST_INSTALL.T_EMPLOYEE_MASTER AS E
                       ON E.CJP_ID = TP.USER_ID
             LEFT JOIN RPT.V_SF_CASECOMMENT AS CC
                       ON CC.CREATEDBYID = E.SALESFORCE_ID
    WHERE C.session_id IS NOT NULL
      AND C.contact_type != 'Master'
      AND C.connected > 0
      AND C.QUEUE_1 NOT ILIKE '%OUTDIAL%'
    ORDER BY C.DATE DESC
)

   , TASKS AS (
    SELECT LD.BILLING_ACCOUNT
         , LD.COLLECTION_CODE
         , LD.COLLECTION_DATE
         , LD.TOTAL_DELINQUENT_AMOUNT_DUE
         , LD.AGE
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
        END                                                                AS AGE_BUCKET
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
        END                                                                AS AGE_BUCKET_TEXT
         , 'Task Creation'                                                 AS UPDATE_TYPE
         , 'https://vivintsolar.my.salesforce.com/' || T.PROJECT_ID        AS PARENT_OBJECT
         , P.PROJECT_NAME                                                  AS PARENT_NAME
         , 'https://vivintsolar.my.salesforce.com/' || T.TASK_ID           AS SELF
         , T.SUBJECT                                                       AS SELF_NAME
         , 'Task Type'                                                     AS SELF_KEY_ATTRIBUTE
         , T.TYPE                                                          AS SELF_KEY_ATTRIBUTE_VALUE
         , USR.NAME                                                        AS CREATED_BY
         , HR.SUPERVISOR_NAME_1 || ' (' || HR.SUPERVISOR_BADGE_ID_1 || ')' AS DIRECT_MANAGER
         , HR.SUPERVISORY_ORG
         , T.CREATED_DATE
    FROM RPT.T_TASK AS T
             LEFT OUTER JOIN RPT.T_PROJECT AS P
                             ON T.PROJECT_ID = P.PROJECT_ID
             LEFT OUTER JOIN LD.T_DAILY_DATA_EXTRACT AS LD
                             ON LD.BILLING_ACCOUNT = P.SOLAR_BILLING_ACCOUNT_NUMBER
             LEFT OUTER JOIN D_POST_INSTALL.T_EMPLOYEE_MASTER AS E
                             ON E.SALESFORCE_ID = T.CREATED_BY_ID
             LEFT OUTER JOIN HR.T_EMPLOYEE AS HR
                             ON HR.EMPLOYEE_ID = E.EMPLOYEE_ID
             LEFT OUTER JOIN RPT.V_SF_USER AS USR
                             ON USR.ID = T.CREATED_BY_ID
    WHERE LD.TOTAL_DELINQUENT_AMOUNT_DUE > 0
)

   , CASE_COMMENTS AS (
    SELECT LD.BILLING_ACCOUNT
         , LD.COLLECTION_CODE
         , LD.COLLECTION_DATE
         , LD.TOTAL_DELINQUENT_AMOUNT_DUE
         , LD.AGE
         , 'Case Comment Creation'                                         AS UPDATE_TYPE
         , 'https://vivintsolar.my.salesforce.com/' || cc.PARENTID         AS PARENT_OBJECT
         , C.CASE_NUMBER                                                   AS PARENT_NAME
         , CC.ID                                                           AS SELF
         , CC.CREATEDBYID                                                  AS SELF_NAME
         , 'Comment Body'                                                  AS SELF_KEY_ATTRIBUTE
         , CC.COMMENTBODY                                                  AS SELF_KEY_ATTRIBUTE_VALUE
         , USR.NAME                                                        AS CREATED_BY
         , HR.SUPERVISOR_NAME_1 || ' (' || HR.SUPERVISOR_BADGE_ID_1 || ')' AS DIRECT_MANAGER
         , HR.SUPERVISORY_ORG
         , CC.CREATEDATE                                                   AS CREATED_DATE
    FROM RPT.V_SF_CASECOMMENT AS CC
             LEFT OUTER JOIN RPT.T_CASE AS C
                             ON C.CASE_ID = CC.PARENTID
             LEFT OUTER JOIN RPT.T_PROJECT AS P
                             ON P.PROJECT_ID = C.PROJECT_ID
             LEFT OUTER JOIN LD.T_DAILY_DATA_EXTRACT AS LD
                             ON LD.BILLING_ACCOUNT = P.SOLAR_BILLING_ACCOUNT_NUMBER
             LEFT OUTER JOIN D_POST_INSTALL.T_EMPLOYEE_MASTER AS E
                             ON E.SALESFORCE_ID = cc.CREATEDBYID
             LEFT OUTER JOIN HR.T_EMPLOYEE AS HR
                             ON HR.EMPLOYEE_ID = E.EMPLOYEE_ID
             LEFT OUTER JOIN RPT.V_SF_USER AS USR
                             ON USR.ID = CC.CREATEDBYID
    WHERE LD.TOTAL_DELINQUENT_AMOUNT_DUE > 0
)

   , MAIN AS (
    SELECT *
    FROM CALL_VIEW
)

   , QUEUES AS (
    SELECT Q.ORG_NAME
         , Q.TEAM_NAME
         , Q.QUEUE_NAME
    FROM D_POST_INSTALL.V_CJP_QUEUES AS Q
    WHERE Q.TEAM_NAME NOT ILIKE '%PERSONAL%'
      AND Q.QUEUE_NAME IS NOT NULL
)

SELECT *
FROM QUEUES