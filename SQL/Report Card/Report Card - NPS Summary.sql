/*
 Needed Fields:
 DT
 ORG_BUCKET
 SERVICE_STATE
 */
WITH NPS_VIEW AS (
    SELECT NPS.SURVEY_TYPE
         , TO_DATE(NPS.SURVEY_ENDED_AT)  AS SURVEY_COMPLETED_DATE
         , NPS.PROJECT_ID
         , NPS.NPS_SCORE
         , P.SERVICE_STATE
         , 1                             AS SURVEY_TALLY
         , IFF(NPS.NPS_SCORE >= 9, 1, 0) AS PROMOTER_TALLY
         , IFF(NPS.NPS_SCORE <= 6, 1, 0) AS DETRACTOR_TALLY
    FROM D_POST_INSTALL.T_NPS_SURVEY_RESPONSE AS NPS
             LEFT OUTER JOIN RPT.T_PROJECT AS P
                             ON P.PROJECT_ID = NPS.PROJECT_ID
    WHERE NPS.SURVEY_TYPE ILIKE '%600%'
      AND NPS.NPS_SCORE IS NOT NULL
)

   , METRIC AS (
    SELECT D.DT
         , NPS.SERVICE_STATE
         , SUM(COUNT(CASE
                         WHEN SURVEY_COMPLETED_DATE = D.DT
                             THEN NPS.PROJECT_ID END)) OVER
                   (PARTITION BY NPS.SERVICE_STATE
                   ORDER BY D.DT ROWS BETWEEN 90 PRECEDING AND 1 PRECEDING)                 AS ROLLING_RATIO_DENOM
         , SUM(SUM(CASE
                       WHEN SURVEY_COMPLETED_DATE = D.DT
                           THEN PROMOTER_TALLY END)) OVER
                   (PARTITION BY NPS.SERVICE_STATE
                   ORDER BY D.DT ROWS BETWEEN 90 PRECEDING AND 1 PRECEDING)                 AS ROLLING_PROMOTERS_NUM
         , SUM(SUM(CASE
                       WHEN SURVEY_COMPLETED_DATE = D.DT
                           THEN DETRACTOR_TALLY END)) OVER
                   (PARTITION BY NPS.SERVICE_STATE
                   ORDER BY D.DT ROWS BETWEEN 90 PRECEDING AND 1 PRECEDING)                 AS ROLLING_DETRACTORS_NUM
         , IFF(ROLLING_RATIO_DENOM = 0, NULL, ROLLING_PROMOTERS_NUM / ROLLING_RATIO_DENOM)  AS ROLLING_PROMOTERS_RATIO
         , IFF(ROLLING_RATIO_DENOM = 0, NULL, ROLLING_DETRACTORS_NUM / ROLLING_RATIO_DENOM) AS ROLLING_DETRACTORS_RATIO
         , (ROLLING_PROMOTERS_RATIO - ROLLING_DETRACTORS_RATIO) * 100                       AS NPS_METRIC_DEFAULT
    FROM RPT.T_DATES AS D
       , NPS_VIEW AS NPS
    WHERE D.DT BETWEEN
              DATE_TRUNC('Y', DATEADD('Y', -1, CURRENT_DATE)) AND
              CURRENT_DATE
    GROUP BY D.DT
           , NPS.SERVICE_STATE
)

   , MAIN AS (
    SELECT *
         , CURRENT_DATE AS LAST_REFRESHED
    FROM METRIC
    WHERE DAY(DT) = DAY(CURRENT_DATE)
)

   , TEST_CTE AS (
    SELECT DT
         , SUM(ROLLING_RATIO_DENOM)                                                       AS SURVEYS
         , SUM(ROLLING_DETRACTORS_NUM)                                                    AS DETRACTORS
         , SUM(ROLLING_PROMOTERS_NUM)                                                     AS PROMOTERS
         , AVG(NPS_METRIC_DEFAULT)                                                        AS NPS1
         , IFF(SURVEYS = 0, NULL, ((PROMOTERS / SURVEYS) - (DETRACTORS / SURVEYS)) * 100) AS NPS2
    FROM METRIC
    WHERE DAY(DT) = DAY(CURRENT_DATE)
    GROUP BY DT
    ORDER BY DT DESC
)

SELECT *
FROM MAIN
ORDER BY DT DESC