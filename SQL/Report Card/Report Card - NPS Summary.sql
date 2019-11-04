/*
 Needed Fields:
 DT
 ORG_BUCKET
 SERVICE_STATE
 */
WITH NPS_VIEW AS (
    SELECT NPS.SURVEY_TYPE
         , NPS.PROJECT_ID
         , NPS.NPS_SCORE
         , P.SERVICE_STATE
         , NPS.SURVEY_ENDED_AT
         , CASE
               WHEN NPS.SURVEY_TYPE IN ('320 - Post-Install Process', '330 - Customer Success Managers')
                   THEN 'ACE'
               WHEN NPS.SURVEY_TYPE IN ('520 - Customer Service', '500 - PTO')
                   THEN 'CX'
               WHEN NPS.SURVEY_TYPE IN ('550 - At-Fault Home Damage')
                   THEN 'Damage'
               WHEN NPS.SURVEY_TYPE IN ('530 - Executive Resolutions')
                   THEN 'ERT'
               WHEN NPS.SURVEY_TYPE IN ('540 - Field Service')
                   THEN 'SPC'
        END AS ORG_BUCKET
    FROM D_POST_INSTALL.T_NPS_SURVEY_RESPONSE AS NPS
             LEFT OUTER JOIN RPT.T_PROJECT AS P
                             ON P.PROJECT_ID = NPS.PROJECT_ID
)

   , METRIC AS (
    SELECT D.DT
         , NPS.SERVICE_STATE
         , NPS.ORG_BUCKET
         , AVG(NPS.NPS_SCORE) OVER
        (PARTITION BY NPS.SERVICE_STATE, NPS.ORG_BUCKET
        ORDER BY D.DT ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING) AS ROLLING_30_DAY_NPS
    FROM RPT.T_DATES AS D
             INNER JOIN NPS_VIEW AS NPS
                        ON TO_DATE(NPS.SURVEY_ENDED_AT) = D.DT
    WHERE NPS.ORG_BUCKET IS NOT NULL
      AND D.DT BETWEEN
        DATE_TRUNC('Y', DATEADD('Y', -1, CURRENT_DATE)) AND
        CURRENT_DATE
)

   , MAIN AS (
    SELECT *
         , CURRENT_DATE AS LAST_REFRESHED
    FROM METRIC
    WHERE DAY(DT) = DAY(CURRENT_DATE)
)

SELECT *
FROM MAIN