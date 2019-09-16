WITH PROJECTS_RAW AS (
    SELECT PROJECT_ID
         , PROJECT_NAME                               AS PROJECT_NUMBER
         , SERVICE_NAME                               AS SERVICE_NUMBER
         , CASE
               WHEN INSTALLATION_COMPLETE IS NOT NULL
                   THEN 'ACE' END                     AS ORG_BUCKET
         , NVL(SERVICE_STATE, '[blank]')              AS STATE_NAME
         , TO_DATE(INSTALLATION_COMPLETE)             AS INSTALLATION_DATE
         , TO_DATE(CANCELLATION_DATE)                 AS CANCELLATION_DATE
         , TO_DATE(NVL(IN_SERVICE_DATE, PTO_AWARDED)) AS IN_SERVICE_DATE
    FROM RPT.T_PROJECT
    WHERE INSTALLATION_COMPLETE IS NOT NULL
)

   , MAIN_TEST_1 AS (
    SELECT D.DT        AS MONTH
         , YEAR(MONTH) AS YEAR
         , PR.*
    FROM RPT.T_DATES AS D
             INNER JOIN PROJECTS_RAW AS PR
                        ON PR.INSTALLATION_DATE <= D.DT AND (
                                NVL(PR.CANCELLATION_DATE, CURRENT_DATE + 1) > D.DT
                                OR
                                NVL(PR.IN_SERVICE_DATE, CURRENT_DATE + 1) > D.DT
                            )
    WHERE D.DT BETWEEN
        DATE_TRUNC('Y', DATEADD('Y', -1, CURRENT_DATE)) AND
        CURRENT_DATE
      AND DAY(D.DT) = DAY(CURRENT_DATE)
)

SELECT *
FROM PROJECTS_RAW
;