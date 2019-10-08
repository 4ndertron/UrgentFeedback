WITH AGENTS AS (
    SELECT E.FULL_NAME
         , E.EMPLOYEE_ID
         , E.BUSINESS_TITLE
         , E.SUPERVISORY_ORG
    FROM HR.T_EMPLOYEE AS E
    WHERE E.SUPERVISOR_BADGE_ID_1 = '67600'
      AND E.EMPLOYEE_ID NOT IN ('209513')
      AND NOT TERMINATED
)

   , TIMEFRAME AS (
    SELECT D.DT
         , DAYOFWEEK(D.DT) AS DAY
    FROM RPT.T_DATES AS D
    WHERE D.DT BETWEEN
              DATEADD(DD, -30, CURRENT_DATE) AND
              DATEADD(DD, -1, CURRENT_DATE)
)

   , CASES AS (
    SELECT P.SOLAR_BILLING_ACCOUNT_NUMBER                                  AS BILLING_ACCOUNT
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
         , 'Case Creation'                                                 AS UPDATE_TYPE
         , 'https://vivintsolar.my.salesforce.com/' || C.PROJECT_ID        AS PARENT_OBJECT
         , P.PROJECT_NAME                                                  AS PARENT_NAME
         , 'https://vivintsolar.my.salesforce.com/' || C.CASE_ID           AS SELF
         , C.CASE_NUMBER                                                   AS SELF_NAME
         , 'Record Type'                                                   AS SELF_KEY_ATTRIBUTE
         , C.RECORD_TYPE                                                   AS SELF_KEY_ATTRIBUTE_VALUE
         , USR.NAME                                                        AS CREATED_BY
         , HR.SUPERVISOR_NAME_1 || ' (' || HR.SUPERVISOR_BADGE_ID_1 || ')' AS DIRECT_MANAGER
         , HR.SUPERVISORY_ORG
         , C.CREATED_DATE
    FROM RPT.T_CASE AS C
             LEFT OUTER JOIN RPT.T_PROJECT AS P
                             ON P.PROJECT_ID = C.PROJECT_ID
             LEFT OUTER JOIN LD.T_DAILY_DATA_EXTRACT AS LD
                             ON LD.BILLING_ACCOUNT = P.SOLAR_BILLING_ACCOUNT_NUMBER
             LEFT OUTER JOIN D_POST_INSTALL.T_EMPLOYEE_MASTER AS E
                             ON E.SALESFORCE_ID = C.CREATED_BY_ID
             LEFT OUTER JOIN HR.T_EMPLOYEE AS HR
                             ON HR.EMPLOYEE_ID = E.EMPLOYEE_ID
             LEFT OUTER JOIN RPT.V_SF_USER AS USR
                             ON USR.ID = C.CREATED_BY_ID
    WHERE LD.TOTAL_DELINQUENT_AMOUNT_DUE > 0
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

   , CASE_HISTORY AS (
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
         , 'Case Field Change'                                             AS UPDATE_TYPE
         , 'https://vivintsolar.my.salesforce.com/' || ch.CASEID           AS PARENT_OBJECT
         , C.CASE_NUMBER                                                   AS PARENT_NAME
         , CH.ID                                                           AS SELF
         , CH.FIELD                                                        AS SELF_NAME
         , 'New Value'                                                     AS SELF_KEY_ATTRIBUTE
         , CH.NEWVALUE                                                     AS SELF_KEY_ATTRIBUTE_VALUE
         , USR.NAME                                                        AS CREATED_BY
         , HR.SUPERVISOR_NAME_1 || ' (' || HR.SUPERVISOR_BADGE_ID_1 || ')' AS DIRECT_MANAGER
         , HR.SUPERVISORY_ORG
         , CH.CREATEDDATE                                                  AS CREATED_DATE
    FROM RPT.V_SF_CASEHISTORY AS CH
             LEFT OUTER JOIN RPT.T_CASE AS C
                             ON C.CASE_ID = CH.CASEID
             LEFT OUTER JOIN RPT.T_PROJECT AS P
                             ON P.PROJECT_ID = C.PROJECT_ID
             LEFT OUTER JOIN LD.T_DAILY_DATA_EXTRACT AS LD
                             ON LD.BILLING_ACCOUNT = P.SOLAR_BILLING_ACCOUNT_NUMBER
             LEFT OUTER JOIN D_POST_INSTALL.T_EMPLOYEE_MASTER AS E
                             ON E.SALESFORCE_ID = CH.CREATEDBYID
             LEFT OUTER JOIN HR.T_EMPLOYEE AS HR
                             ON E.EMPLOYEE_ID = HR.EMPLOYEE_ID
             LEFT OUTER JOIN RPT.V_SF_USER AS USR
                             ON USR.ID = CH.CREATEDBYID
    WHERE LD.TOTAL_DELINQUENT_AMOUNT_DUE > 0
      AND CH.FIELD NOT IN ('created')
)

   , EVENTS AS (
    SELECT C.*
         , T.DT
         , A.EMPLOYEE_ID
    FROM (
                 (SELECT * FROM CASES)
                 UNION
                 (SELECT * FROM TASKS)
                 UNION
                 (SELECT * FROM CASE_COMMENTS)
                 UNION
                 (SELECT * FROM CASE_HISTORY)
         ) AS C
             LEFT OUTER JOIN TIMEFRAME AS T
                             ON T.DT = TO_DATE(C.CREATED_DATE)
             LEFT OUTER JOIN AGENTS AS A
                             ON A.FULL_NAME = C.CREATED_BY
    ORDER BY BILLING_ACCOUNT
           , CREATED_DATE
)

   , MAIN AS (
    SELECT E.CREATED_BY
         , E.EMPLOYEE_ID
         , E.DT
         , COUNT(CASE WHEN TO_DATE(E.CREATED_DATE) = E.DT THEN 1 END) AS UPDATES
    FROM EVENTS AS E
    WHERE DT IS NOT NULL
      AND EMPLOYEE_ID IS NOT NULL
    GROUP BY E.CREATED_BY, E.EMPLOYEE_ID, E.DT
    ORDER BY E.CREATED_BY, E.DT
)

SELECT *
FROM MAIN
