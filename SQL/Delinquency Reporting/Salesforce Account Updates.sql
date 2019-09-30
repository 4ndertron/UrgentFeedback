/*
 TODO: Look for created_by, created_date, creation_type, parent_object, account
    in cases, case comments, tasks, case_history, etc.
    Union those, and make a timeline of changes/creations on a given account.
 */

WITH CASES AS (
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
    SELECT *
    FROM (
                 (SELECT * FROM CASES)
                 UNION
                 (SELECT * FROM TASKS)
                 UNION
                 (SELECT * FROM CASE_COMMENTS)
                 UNION
                 (SELECT * FROM CASE_HISTORY)
         ) AS C
    WHERE TO_DATE(C.CREATED_DATE) = (SELECT IFF(DAYOFWEEK(CURRENT_DATE) = 1,
                                                DATEADD(DAY, -3, CURRENT_DATE),
                                                CURRENT_DATE))
    ORDER BY BILLING_ACCOUNT
           , CREATED_DATE
)

   , OWNERSHIP_PARSE AS (
    SELECT LD.BILLING_ACCOUNT
         , LD.TOTAL_DELINQUENT_AMOUNT_DUE
         , C.CASE_NUMBER
         , C.RECORD_TYPE
         , C.CREATED_DATE
         , C.CLOSED_DATE
         , CASE
               WHEN C.RECORD_TYPE = 'Solar - Cancellation'
                   AND C.OWNER_EMPLOYEE_ID IN
                       ('121126', '207396', '208297', '208853', '209336', '209990', '211343', '213132', '214556')
                   THEN 'ERT'
               WHEN C.RECORD_TYPE = 'Solar - Customer Escalation'
                   AND C.EXECUTIVE_RESOLUTIONS_ACCEPTED IS NOT NULL
                   THEN 'ERT'
               WHEN C.RECORD_TYPE = 'Solar - Service'
                   AND C.PRIMARY_REASON ILIKE '%SYSTEM DAMAGE%'
                   THEN 'ERT'
               WHEN C.RECORD_TYPE = 'Solar - Customer Default'
                   AND C.SUBJECT ILIKE '%D3%'
                   THEN 'Collections'
               WHEN C.RECORD_TYPE = 'Solar - Customer Default'
                   AND C.SUBJECT NOT ILIKE '%D3%'
                   THEN 'Default'
               WHEN C.RECORD_TYPE = 'Solar - Customer Escalation'
                   AND C.EXECUTIVE_RESOLUTIONS_ACCEPTED IS NULL
                   THEN 'CX'
               WHEN C.RECORD_TYPE = 'Solar - Service'
                   AND C.PRIMARY_REASON NOT ILIKE '%SYSTEM DAMAGE%'
                   THEN 'CX'
               WHEN C.RECORD_TYPE = 'Solar - Panel Removal'
                   THEN 'CX'
               WHEN C.RECORD_TYPE = 'Solar - Transfer'
                   THEN 'CX'
               WHEN C.RECORD_TYPE = 'Solar - Troubleshooting'
                   AND C.SOLAR_QUEUE IN ('Tier II', 'Outbound')
                   THEN 'CX'
               WHEN C.RECORD_TYPE = 'Solar - Troubleshooting'
                   AND C.SOLAR_QUEUE NOT IN ('Inbound', 'Tier II')
                   THEN 'SPC'
               WHEN C.RECORD_TYPE = 'Solar - Troubleshooting'
                   AND C.SOLAR_QUEUE IS NULL
                   THEN 'SPC'
               WHEN C.RECORD_TYPE = 'Solar Damage Resolutions'
                   THEN 'Damage'
               ELSE 'No Owner'
        END                              AS ORG_OWNER
         , CASE
               WHEN ORG_OWNER = 'ERT'
                   THEN 1
               WHEN ORG_OWNER = 'Collections'
                   THEN 2
               WHEN ORG_OWNER = 'Default'
                   THEN 3
               WHEN ORG_OWNER = 'Damage'
                   THEN 4
               WHEN ORG_OWNER = 'Transfers'
                   THEN 5
               WHEN ORG_OWNER = 'CX'
                   THEN 6
               WHEN ORG_OWNER = 'SPC'
                   THEN 7
               ELSE 8 END                AS ORG_PRIORITY
         , ROW_NUMBER()
            OVER (
                PARTITION BY LD.BILLING_ACCOUNT
                ORDER BY C.CREATED_DATE) AS CASE_COUNT
    FROM LD.T_DAILY_DATA_EXTRACT AS LD
             LEFT OUTER JOIN RPT.T_PROJECT AS P
                             ON P.SOLAR_BILLING_ACCOUNT_NUMBER = LD.BILLING_ACCOUNT
             LEFT OUTER JOIN RPT.T_CASE AS C
                             ON C.PROJECT_ID = P.PROJECT_ID
    WHERE LD.TOTAL_DELINQUENT_AMOUNT_DUE > 0
    ORDER BY BILLING_ACCOUNT
           , ORG_PRIORITY
--            , C.CREATED_DATE
)

   , OWNERSHIP_COMPILE AS (
    SELECT BILLING_ACCOUNT
         , ORG_OWNER
         , ORG_PRIORITY
         , ROW_NUMBER() OVER (PARTITION BY BILLING_ACCOUNT ORDER BY ORG_PRIORITY) AS RN
    FROM OWNERSHIP_PARSE
    WHERE (CLOSED_DATE IS NULL AND ORG_PRIORITY < 8)
       OR (ORG_PRIORITY = 8)
    GROUP BY BILLING_ACCOUNT
           , ORG_OWNER
           , ORG_PRIORITY
    ORDER BY BILLING_ACCOUNT
           , ORG_PRIORITY
)

   , DELINQUENT_OWNERS AS (
    SELECT BILLING_ACCOUNT
         , ORG_OWNER
         , ORG_PRIORITY
    FROM OWNERSHIP_COMPILE
    WHERE RN = 1
)

   , MAIN AS (
    SELECT E.*
         , DO.ORG_OWNER
         , DO.ORG_PRIORITY
    FROM EVENTS AS E
             LEFT OUTER JOIN DELINQUENT_OWNERS AS DO
                             ON DO.BILLING_ACCOUNT = E.BILLING_ACCOUNT
    ORDER BY E.AGE_BUCKET
)

SELECT *
FROM MAIN
