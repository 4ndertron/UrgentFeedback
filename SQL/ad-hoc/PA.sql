WITH INFORMATION AS (
    -- Objectives Table
    /*
        Informational Fields:
        Service Number
        Customer Name
        City
        Install Date
        PTO Date
        Sales Manager
        Transaction Date
     */

    SELECT ANY_VALUE(CN.FULL_NAME)                     AS FULL_NAME
         , P.SERVICE_NAME
         , NULL                                        AS SERVICE_2
         , ANY_VALUE(P.SERVICE_CITY)                   AS SERVICE_CITY
         , TO_DATE(ANY_VALUE(P.INSTALLATION_COMPLETE)) AS INSTALLATION_COMPLETE
         , TO_DATE(ANY_VALUE(P.PTO_AWARDED))           AS PTO_AWARDED
         , ANY_VALUE(O.SALES_REP_NAME)                 AS SALES_REP_NAME
         , TO_DATE(ANY_VALUE(CT.TRANSACTION_DATE))     AS TRANSACTION_DATE
         , ANY_VALUE(P.PROJECT_ID)                     AS PROJECT_ID
    FROM RPT.T_PROJECT AS P
             LEFT OUTER JOIN
         RPT.T_CONTRACT AS CT
         ON CT.CONTRACT_ID = P.PRIMARY_CONTRACT_ID
             LEFT OUTER JOIN
         RPT.T_CONTACT AS CN
         ON P.CONTRACT_SIGNER = CN.CONTACT_ID
             LEFT OUTER JOIN
         RPT.T_OPPORTUNITY AS O
         ON O.OPPORTUNITY_ID = P.OPPORTUNITY_ID
             LEFT OUTER JOIN
         RPT.T_CASE AS C
         ON C.PROJECT_ID = P.PROJECT_ID
    WHERE P.INSTALLATION_COMPLETE IS NOT NULL
      AND P.SERVICE_STATE = 'PA'
      AND P.PROJECT_STATUS NOT LIKE '%Canc%'
    GROUP BY P.SERVICE_NAME
    ORDER BY P.SERVICE_NAME
)

   , TASKS AS (
    SELECT T.PROJECT_ID
         , T.CREATED_DATE
         , ROW_NUMBER() OVER(PARTITION BY T.PROJECT_ID ORDER BY T.CREATED_DATE DESC) AS RN
         , CASE
               WHEN SUBJECT ILIKE '%QX1%'
                   THEN TRUE END                                                     AS QX1BOOL
         , CASE
               WHEN SUBJECT ILIKE '%QX1%'
                   THEN T.DESCRIPTION END                                            AS QX1DESC
    FROM RPT.T_TASK AS T
    WHERE T.SUBJECT ILIKE '%QX1%'
)

   , CASES AS (
-- Objectives Table
/*
TODO: Fields needed:
    Executive Resolutions Boolean
    Executive Resolutions Description
    BBB Complaint Boolean
    BBB Description
    Escalation Boolean
    Escalation Description
    Service Boolean
    Service Description
    Troubleshooting Boolean
    Troubleshooting Description
    Damage Boolean
    Damage Description
    Damage Primary Reason
 */

    SELECT C.PROJECT_ID
         , C.RECORD_TYPE
         , C.CREATED_DATE
         , ROW_NUMBER() OVER(PARTITION BY C.PROJECT_ID, C.RECORD_TYPE ORDER BY C.CREATED_DATE DESC) AS RN
         , CASE
               WHEN C.RECORD_TYPE = 'Solar - Customer Escalation' AND
                    C.EXECUTIVE_RESOLUTIONS_ACCEPTED IS NOT NULL
                   THEN TRUE END                                                                    AS ER_BOOL
         , CASE
               WHEN C.RECORD_TYPE = 'Solar - Customer Escalation' AND
                    C.EXECUTIVE_RESOLUTIONS_ACCEPTED IS NOT NULL
                   THEN C.ORIGIN END                                                                AS ER_ORIGIN
         , CASE
               WHEN C.RECORD_TYPE = 'Solar - Customer Escalation' AND
                    C.EXECUTIVE_RESOLUTIONS_ACCEPTED IS NOT NULL
                   THEN C.DESCRIPTION END                                                           AS ER_DESC
         -- TODO: NPS, and split that into their own column and page into the PA
         -- TODO: QX1 activities... use RPT.T_TASK
         , CASE
               WHEN C.RECORD_TYPE = 'Solar - Customer Escalation' AND
                    C.EXECUTIVE_RESOLUTIONS_ACCEPTED IS NOT NULL AND
                    C.ORIGIN = 'BBB'
                   THEN TRUE END                                                                    AS BBB_BOOL
         , CASE
               WHEN C.RECORD_TYPE = 'Solar - Customer Escalation' AND
                    C.EXECUTIVE_RESOLUTIONS_ACCEPTED IS NOT NULL AND
                    C.ORIGIN = 'BBB'
                   THEN C.DESCRIPTION END                                                           AS BBB_DESC
         , CASE
               WHEN C.RECORD_TYPE = 'Solar - Customer Escalation' AND
                    C.EXECUTIVE_RESOLUTIONS_ACCEPTED IS NOT NULL AND
                    (C.ORIGIN = 'NPS' OR C.SUBJECT ILIKE 'NPS')
                   THEN TRUE END                                                                    AS NPS_BOOL
         , CASE
               WHEN C.RECORD_TYPE = 'Solar - Customer Escalation' AND
                    C.EXECUTIVE_RESOLUTIONS_ACCEPTED IS NOT NULL AND
                    (C.ORIGIN = 'NPS' OR C.SUBJECT ILIKE 'NPS')
                   THEN C.DESCRIPTION END                                                           AS NPS_DESC

         , CASE
               WHEN C.RECORD_TYPE = 'Solar - Customer Escalation' AND
                    C.EXECUTIVE_RESOLUTIONS_ACCEPTED IS NULL
                   THEN TRUE END                                                                    AS ESCALATION_BOOL

         , CASE
               WHEN C.RECORD_TYPE = 'Solar - Customer Escalation' AND
                    C.EXECUTIVE_RESOLUTIONS_ACCEPTED IS NULL
                   THEN C.DESCRIPTION END                                                           AS ESCALATION_DESC
         , CASE
               WHEN C.RECORD_TYPE = 'Solar - Service'
                   THEN TRUE END                                                                    AS SERVICE_BOOL
         , CASE
               WHEN C.RECORD_TYPE = 'Solar - Service'
                   THEN C.DESCRIPTION END                                                           AS SERVICE_DESC
         , CASE
               WHEN C.RECORD_TYPE = 'Solar - Troubleshooting' AND C.SOLAR_QUEUE = 'SPC'
                   THEN TRUE END                                                                    AS SPC_BOOL
         , CASE
               WHEN C.RECORD_TYPE = 'Solar - Troubleshooting' AND C.SOLAR_QUEUE = 'SPC'
                   THEN C.DESCRIPTION END                                                           AS SPC_DESC
         , CASE
               WHEN C.RECORD_TYPE = 'Solar - Troubleshooting' AND C.SOLAR_QUEUE != 'SPC'
                   THEN TRUE END                                                                    AS TS_BOOL
         , CASE
               WHEN C.RECORD_TYPE = 'Solar - Troubleshooting' AND C.SOLAR_QUEUE != 'SPC'
                   THEN C.DESCRIPTION END                                                           AS TS_DESC
         , CASE
               WHEN C.RECORD_TYPE = 'Solar - Customer Default' AND C.SUBJECT NOT ILIKE '%D3%'
                   THEN TRUE END                                                                    AS DEFAULT_BOOL
         , CASE
               WHEN C.RECORD_TYPE = 'Solar - Customer Default' AND C.SUBJECT NOT ILIKE '%D3%'
                   THEN C.DESCRIPTION END                                                           AS DEFAULT_DESC
         , CASE
               WHEN C.RECORD_TYPE = 'Solar Damage Resolutions'
                   THEN TRUE END                                                                    AS DAMAGE_BOOL
         , CASE
               WHEN C.RECORD_TYPE = 'Solar Damage Resolutions'
                   THEN C.DESCRIPTION END                                                           AS DAMAGE_DESC
         , CASE
               WHEN C.RECORD_TYPE = 'Solar Damage Resolutions'
                   THEN C.DAMAGE_TYPE END                                                AS DAMAGE_TYPE
    FROM RPT.T_CASE AS C
    WHERE C.RECORD_TYPE IN
          ('Solar - Troubleshooting', 'Solar - Customer Escalation', 'Solar Damage Resolutions', 'Solar - Service',
           'Solar - Customer Default')
)

   , CASE_CLEANUP AS (
    SELECT PROJECT_ID
         , MAX(ER_BOOL)         AS ER_BOOL
         , MAX(ER_DESC)         AS ER_DESC
         , MAX(BBB_BOOL)        AS BBB_BOOL
         , MAX(BBB_DESC)        AS BBB_DESC
         , MAX(ESCALATION_BOOL) AS ESCALATION_BOOL
         , MAX(ER_ORIGIN)       AS ER_ORIGIN
         , MAX(ESCALATION_DESC) AS ESCALATION_DESC
         , MAX(NPS_BOOL)        AS NPS_BOOL
         , MAX(NPS_DESC)        AS NPS_DESC
         , MAX(SERVICE_BOOL)    AS SERVICE_BOOL
         , MAX(SERVICE_DESC)    AS SERVICE_DESC
         , MAX(TS_BOOL)         AS TS_BOOL
         , MAX(TS_DESC)         AS TS_DESC
         , MAX(SPC_BOOL)        AS SPC_BOOL
         , MAX(SPC_DESC)        AS SPC_DESC
         , MAX(DEFAULT_BOOL)    AS DEFAULT_BOOL
         , MAX(DEFAULT_DESC)    AS DEFAULT_DESC
         , MAX(DAMAGE_BOOL)     AS DAMAGE_BOOL
         , MAX(DAMAGE_DESC)     AS DAMAGE_DESC
         , MAX(DAMAGE_TYPE)     AS DAMAGE_TYPE
    FROM CASES
    WHERE RN = 1
    GROUP BY PROJECT_ID
    ORDER BY PROJECT_ID
)

   , TASK_CLEANUP AS (
    SELECT PROJECT_ID
         , MAX(QX1BOOL) AS QX1BOOL
         , MAX(QX1DESC) AS QX1DESC
    FROM TASKS
    WHERE RN = 1
    GROUP BY PROJECT_ID
    ORDER BY PROJECT_ID
)

   , TOTAL_TABLE AS (
    SELECT I.FULL_NAME
         , I.SERVICE_NAME
         , I.SERVICE_2
         , I.SERVICE_CITY
         , I.INSTALLATION_COMPLETE
         , I.PTO_AWARDED
         , I.SALES_REP_NAME
         , I.TRANSACTION_DATE
         , CC.ER_BOOL
         , CC.ER_ORIGIN
         , CC.ER_DESC
         , CC.BBB_DESC
         , CC.NPS_BOOL
         , CC.NPS_DESC
         , CC.ESCALATION_BOOL
         , CC.ESCALATION_DESC
         , TA.QX1BOOL
         , TA.QX1DESC
         , CC.SERVICE_BOOL
         , CC.SERVICE_DESC
         , CC.TS_BOOL
         , CC.TS_DESC
         , CC.SPC_BOOL
         , CC.SPC_DESC
         , CC.DEFAULT_BOOL
         , CC.DEFAULT_DESC
         , CC.DAMAGE_BOOL
         , CC.DAMAGE_DESC
         , CC.DAMAGE_TYPE
    FROM INFORMATION AS I
             LEFT OUTER JOIN
         CASE_CLEANUP AS CC
         ON CC.PROJECT_ID = I.PROJECT_ID
             LEFT OUTER JOIN
         TASK_CLEANUP AS TA
         ON TA.PROJECT_ID = I.PROJECT_ID
    ORDER BY I.SERVICE_NAME
)

SELECT *
FROM TOTAL_TABLE
WHERE ER_BOOL