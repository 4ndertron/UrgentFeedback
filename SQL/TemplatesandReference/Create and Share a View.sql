CREATE OR REPLACE VIEW D_POST_INSTALL.V_AGENT_ADH_TREND AS ( -- Create the view
    WITH CALABRIO_VIEW AS (
        SELECT ADH.ADH_ACD_AGENT_ID
             , E.EMPLOYEE_ID
             , ADH.DETAIL_SCHEDULED_ACTIVITY_TYPE
             , ADH.DETAIL_SCHEDULED_ACTIVITY_START_TIME
             , ADH.ADH_SCHEDULED_SECONDS
             , ADH.ADH_ACTUAL_IN_SERVICE_SECONDS
             , ADH.DETAIL_IN_ADHERENCE_SECONDS
             , ADH.DETAIL_OUT_OF_ADHERENCE_SECONDS
        FROM CALABRIO.T_AGENT_ADHERENCE AS ADH -- I don't think this has a '1001735-0' value
                 LEFT JOIN D_POST_INSTALL.T_EMPLOYEE_MASTER AS E
                           ON E.CJP_ID = ADH.ADH_ACD_AGENT_ID
    )

       , AGENT_STATES AS (
        SELECT CP.EMPLOYEE_ID
             , A.START_TIMESTAMP
             , A.END_TIMESTAMP
             , IFF(A.STATE IN
                   ('HOLD', 'RINGING', 'CONNECTEDCONSULTING', 'CONNECTED', 'ON_HOLD', 'CONFERENCING', 'CONSULT_ANSWER',
                    'CONSULT_REQUEST'),
                   'ON CALL',
                   'NOT CALL')                                                 AS STATE_BUCKET
             , TO_DATE(A.START_TIMESTAMP)                                      AS START_DAY
             , HR.SUPERVISOR_NAME_1 || ' (' || HR.SUPERVISOR_BADGE_ID_1 || ')' AS SUPERVISOR
             , HR.SUPERVISOR_NAME_2 || ' (' || HR.SUPERVISOR_BADGE_ID_2 || ')' AS MANAGER
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
             , nvl(regexp_substr(CP.TEAM,
                                 ' [\\(\\[]([\\w\\s\\,&]*)[\\)\\]] ?',
                                 1, 1, 'e'), CP.TEAM)                          AS TEAM

            /*
             Metric Fields
             */
             , TIMESTAMPDIFF(S, A.START_TIMESTAMP, A.END_TIMESTAMP)            AS TIME_DELTA_SECONDS
        FROM CJP.V_AGENT_ACTIVITY AS A
                 LEFT JOIN D_POST_INSTALL.T_EMPLOYEE_MASTER AS E
                           ON E.CJP_ID = A.AGENT_ACD_ID
                 LEFT OUTER JOIN CALABRIO.T_PERSONS AS CP
                                 ON CP.ACD_ID = A.AGENT_ACD_ID
                 LEFT OUTER JOIN HR.T_EMPLOYEE AS HR
                                 ON HR.EMPLOYEE_ID = CP.EMPLOYEE_ID
    )

       , ADHERENCE AS (
        SELECT EMPLOYEE_ID
             , TO_DATE(DETAIL_SCHEDULED_ACTIVITY_START_TIME)                   AS DAY
             , SUM(DISTINCT DETAIL_IN_ADHERENCE_SECONDS)                       AS ADHERENCE_NUM
             , MAX(ADH_SCHEDULED_SECONDS)                                      AS ADHERENCE_DENOM
             , IFF(ADHERENCE_DENOM = 0, NULL, ADHERENCE_NUM / ADHERENCE_DENOM) AS ADHERENCE
        FROM CALABRIO_VIEW
        GROUP BY EMPLOYEE_ID
               , DAY
        ORDER BY EMPLOYEE_ID, DAY
    )

       , ATC AS (
        SELECT EMPLOYEE_ID
             , TO_DATE(DETAIL_SCHEDULED_ACTIVITY_START_TIME)                                       AS DAY
             , MAX(ADH_ACTUAL_IN_SERVICE_SECONDS)                                                  AS ATC_NUM
             , SUM(DISTINCT IFF(DETAIL_SCHEDULED_ACTIVITY_TYPE = 'in_service',
                                DETAIL_IN_ADHERENCE_SECONDS + DETAIL_OUT_OF_ADHERENCE_SECONDS, 0)) AS ATC_DENOM
             , IFF(ATC_DENOM = 0, NULL, ATC_NUM / ATC_DENOM)                                       AS ATC
        FROM CALABRIO_VIEW
        GROUP BY EMPLOYEE_ID
               , DAY
        ORDER BY EMPLOYEE_ID, DAY
    )


       , OCCUPANCY AS (
        SELECT
            /*
             ATTRIBUTE FIELDS
             */
            EMPLOYEE_ID
             , START_DAY
             , ANY_VALUE(SUPERVISOR)                    AS SUPERVISOR
             , ANY_VALUE(MANAGER)                       AS MANAGER
             , ANY_VALUE(LOCATION)                      AS LOCATION
             , ANY_VALUE(HIRE_TENURE)                   AS HIRE_TENURE
             , ANY_VALUE(HIRE_TENURE_BUCKET)            AS HIRE_TENURE_BUCKET
             , ANY_VALUE(HIRE_TENURE_BUCKET_NAMES)      AS HIRE_TENURE_BUCKET_NAMES
             , ANY_VALUE(TEAM_TENURE)                   AS TEAM_TENURE
             , ANY_VALUE(TEAM_TENURE_BUCKET)            AS TEAM_TENURE_BUCKET
             , ANY_VALUE(TEAM_TENURE_BUCKET_NAMES)      AS TEAM_TENURE_BUCKET_NAMES
             , ANY_VALUE(TEAM)                          AS TEAM

            /*
             METRIC FIELDS
             */
             , SUM(CASE
                       WHEN STATE_BUCKET = 'ON CALL'
                           THEN TIME_DELTA_SECONDS END) AS SECONDS_ON_CALL
             , SUM(CASE
                       WHEN STATE_BUCKET = 'NOT CALL'
                           THEN TIME_DELTA_SECONDS END) AS SECONDS_OFF_CALL
             , SECONDS_ON_CALL / SECONDS_OFF_CALL       AS OCCUPANCY_SCORE
        FROM AGENT_STATES
        WHERE TIME_DELTA_SECONDS > 0
        GROUP BY EMPLOYEE_ID, START_DAY
        ORDER BY EMPLOYEE_ID, START_DAY
    )

       , AGENT_METRICS AS (
        SELECT
            /*
             FINAL ATTRIBUTES
             */
            ADH.EMPLOYEE_ID
             , ADH.DAY
             , ANY_VALUE(OCC.SUPERVISOR)               AS SUPERVISOR
             , ANY_VALUE(OCC.MANAGER)                  AS MANAGER
             , ANY_VALUE(OCC.LOCATION)                 AS LOCATION
             , ANY_VALUE(OCC.HIRE_TENURE)              AS HIRE_TENURE
             , ANY_VALUE(OCC.HIRE_TENURE_BUCKET)       AS HIRE_TENURE_BUCKET
             , ANY_VALUE(OCC.HIRE_TENURE_BUCKET_NAMES) AS HIRE_TENURE_BUCKET_NAMES
             , ANY_VALUE(OCC.TEAM_TENURE)              AS TEAM_TENURE
             , ANY_VALUE(OCC.TEAM_TENURE_BUCKET)       AS TEAM_TENURE_BUCKET
             , ANY_VALUE(OCC.TEAM_TENURE_BUCKET_NAMES) AS TEAM_TENURE_BUCKET_NAMES
             , ANY_VALUE(OCC.TEAM)                     AS TEAM

            /*
             METRICS
             */
             , ANY_VALUE(ADH.ADHERENCE)                AS ADHERENCE
             , ANY_VALUE(AC.ATC)                       AS ATC
             , ANY_VALUE(OCC.OCCUPANCY_SCORE)          AS OCCUPANCY_SCORE
        FROM ADHERENCE AS ADH
                 INNER JOIN ATC AS AC
                            ON AC.EMPLOYEE_ID = ADH.EMPLOYEE_ID AND
                               AC.DAY = ADH.DAY
                 INNER JOIN OCCUPANCY AS OCC
                            ON OCC.EMPLOYEE_ID = ADH.EMPLOYEE_ID AND
                               OCC.START_DAY = ADH.DAY
        GROUP BY ADH.EMPLOYEE_ID
               , ADH.DAY
    )

    SELECT *
    FROM AGENT_METRICS
);

GRANT SELECT ON VIEW D_POST_INSTALL.V_AGENT_ADH_TREND TO GENERAL_REPORTING_R -- Share the view