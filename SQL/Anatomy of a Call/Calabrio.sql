WITH CJP AS (
    SELECT C.DATE -- MSTG Attributes
         , C.QUEUE_1
         , C.AGENT_1
         , C.ANI
         , HR.FULL_NAME || ' (' || HR.EMPLOYEE_ID || ')'                   AS EMPLOYEE
         , HR.SUPERVISOR_NAME_1 || ' (' || HR.SUPERVISOR_BADGE_ID_1 || ')' AS SUPERVISOR
         , COALESCE(HR.MGR_NAME_5, HR.MGR_NAME_4, HR.MGR_NAME_3) ||
           ' (' || COALESCE(HR.MGR_ID_5, HR.MGR_ID_4, HR.MGR_ID_3) || ')'  AS MANAGER
         , HR.BUSINESS_SITE_NAME                                           AS LOCATION
         , DATEDIFF(DAY, HR.HIRE_DATE, CURRENT_DATE)                       AS HIRE_TENURE
         , CASE
               WHEN HIRE_TENURE BETWEEN 0 AND 7 THEN 7
               WHEN HIRE_TENURE BETWEEN 7 AND 14 THEN 14
               WHEN HIRE_TENURE BETWEEN 14 AND 30 THEN 30
               WHEN HIRE_TENURE BETWEEN 30 AND 60 THEN 60
               WHEN HIRE_TENURE BETWEEN 60 AND 90 THEN 90
               WHEN HIRE_TENURE BETWEEN 90 AND 120 THEN 120
               WHEN HIRE_TENURE BETWEEN 120 AND 365 THEN 365
               WHEN HIRE_TENURE BETWEEN 365 AND 730 THEN 730
               WHEN HIRE_TENURE BETWEEN 730 AND 1460 THEN 1460
               WHEN HIRE_TENURE >= 1460 THEN 1461
        END                                                                AS HIRE_TENURE_BUCKET
         , CASE
               WHEN HIRE_TENURE_BUCKET = 7 THEN '0-7'
               WHEN HIRE_TENURE_BUCKET = 14 THEN '7-14'
               WHEN HIRE_TENURE_BUCKET = 30 THEN '14-30'
               WHEN HIRE_TENURE_BUCKET = 60 THEN '30-60'
               WHEN HIRE_TENURE_BUCKET = 90 THEN '60-90'
               WHEN HIRE_TENURE_BUCKET = 120 THEN '90-120'
               WHEN HIRE_TENURE_BUCKET = 365 THEN '120-365'
               WHEN HIRE_TENURE_BUCKET = 730 THEN '1-2 Years'
               WHEN HIRE_TENURE_BUCKET = 1460 THEN '2-4 years'
               WHEN HIRE_TENURE_BUCKET = 1461 THEN '4+ Years'
        END                                                                AS HIRE_TENURE_BUCKET_NAMES
         , DATEDIFF(DAY, HR.LOAD_DATE, CURRENT_DATE)                       AS TEAM_TENURE
         , CASE
               WHEN TEAM_TENURE BETWEEN 0 AND 7 THEN 7
               WHEN TEAM_TENURE BETWEEN 7 AND 14 THEN 14
               WHEN TEAM_TENURE BETWEEN 14 AND 30 THEN 30
               WHEN TEAM_TENURE BETWEEN 30 AND 60 THEN 60
               WHEN TEAM_TENURE BETWEEN 60 AND 90 THEN 90
               WHEN TEAM_TENURE BETWEEN 90 AND 120 THEN 120
               WHEN TEAM_TENURE BETWEEN 120 AND 365 THEN 365
               WHEN TEAM_TENURE BETWEEN 365 AND 730 THEN 730
               WHEN TEAM_TENURE BETWEEN 730 AND 1460 THEN 1460
               WHEN TEAM_TENURE >= 1460 THEN 1461
        END                                                                AS TEAM_TENURE_BUCKET
         , CASE
               WHEN TEAM_TENURE_BUCKET = 7 THEN '0-7'
               WHEN TEAM_TENURE_BUCKET = 14 THEN '7-14'
               WHEN TEAM_TENURE_BUCKET = 30 THEN '14-30'
               WHEN TEAM_TENURE_BUCKET = 60 THEN '30-60'
               WHEN TEAM_TENURE_BUCKET = 90 THEN '60-90'
               WHEN TEAM_TENURE_BUCKET = 120 THEN '90-120'
               WHEN TEAM_TENURE_BUCKET = 365 THEN '120-365'
               WHEN TEAM_TENURE_BUCKET = 730 THEN '1-2 Years'
               WHEN TEAM_TENURE_BUCKET = 1460 THEN '2-4 years'
               WHEN TEAM_TENURE_BUCKET = 1461 THEN '4+ Years'
        END                                                                AS TEAM_TENURE_BUCKET_NAMES
         , nvl(regexp_substr(E.TEAM,
                             ' [\\(\\[]([\\w\\s\\,&]*)[\\)\\]] ?',
                             1, 1, 'e'),
               E.TEAM)                                                     AS TEAM
         -------------------
         -- Metric Fields --
         -------------------
         , C.ON_HOLD
         , C.WRAPUP
         , C.MAX_DELAY
         , DATEDIFF(S, C.CALL_START, C.CALL_END) + C.WRAPUP - C.MAX_DELAY  AS HANDLE_TIME
    FROM D_POST_INSTALL.T_CJP_CDR_TEMP AS C
             LEFT OUTER JOIN CALABRIO.T_PERSONS AS E
                             ON E.ACD_ID = C.AGENT_1_ACD_ID
             LEFT OUTER JOIN HR.T_EMPLOYEE AS HR
                             ON HR.EMPLOYEE_ID = E.EMPLOYEE_ID
    WHERE C.ANI != '0'
      AND C.QUEUE_1 != 'Q_Test'
)

   , AGENT_CALL_METRICS AS (
    /*
    TODO: 90 Day trend of the following calabrio metrics:
       Abandon Rate
       "ASA by interval" -- More clarification will be needed
       Inbound Volume
       Outbound Volume
       Adherence
       Occupancy
       ATC
    */
    SELECT
         -- ATTRIBUTES
        C.DATE                       AS DAY
         , C.QUEUE_1
         , C.EMPLOYEE
         , C.SUPERVISOR
         , C.MANAGER
         , C.LOCATION
         , C.HIRE_TENURE_BUCKET
         , C.HIRE_TENURE_BUCKET_NAMES
         , C.TEAM_TENURE_BUCKET
         , C.TEAM_TENURE_BUCKET_NAMES
         , C.TEAM

         -- METRICS
         , AVG(HANDLE_TIME)          AS AHT
         , AVG(ON_HOLD)              AS HOLD
         , AVG(C.WRAPUP)             AS ACW
         , AVG(C.MAX_DELAY)          AS ASA
         , COUNT(CASE
                     WHEN NVL(C.QUEUE_1, 'No_Queue') NOT ILIKE '%OUT%' AND
                          C.AGENT_1 IS NOT NULL
                         THEN 1 END) AS INBOUND_VOLUME
         , COUNT(CASE
                     WHEN NVL(C.QUEUE_1, 'No_Queue') ILIKE '%OUT%'
                         THEN 1 END) AS OUTBOUND_VOLUME
         , COUNT(CASE
                     WHEN NVL(AGENT_1, 'Abandoned Call') = 'Abandoned Call' AND
                          NVL(C.QUEUE_1, 'No_Queue') NOT ILIKE '%OUT%'
                         THEN 1 END) AS ABANDONDED_CALLS
         , COUNT(C.ANI)              AS CALL_VOLUME
         , CURRENT_DATE              AS LAST_REFRESHED
    FROM CJP AS C
    WHERE ANI != '0'
    GROUP BY C.DATE
           , C.QUEUE_1
           , C.EMPLOYEE
           , C.SUPERVISOR
           , C.MANAGER
           , C.LOCATION
           , C.HIRE_TENURE_BUCKET
           , C.HIRE_TENURE_BUCKET_NAMES
           , C.TEAM_TENURE_BUCKET
           , C.TEAM_TENURE_BUCKET_NAMES
           , C.TEAM
    ORDER BY C.DATE, C.QUEUE_1
)

SELECT *
FROM AGENT_CALL_METRICS