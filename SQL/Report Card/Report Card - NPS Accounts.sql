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

   , MAIN AS (
    SELECT *
    FROM NPS_VIEW
    WHERE ORG_BUCKET IS NOT NULL
)

SELECT *
FROM MAIN