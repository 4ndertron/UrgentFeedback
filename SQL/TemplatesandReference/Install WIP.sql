WITH PROJECTS_RAW AS (
    SELECT P.SERVICE_NAME
         , P.PROJECT_NAME
         , TO_DATE(P.INSTALLATION_COMPLETE) AS INSTALL_DATE
         , P.SERVICE_STATE
         , P.SERVICE_CITY
         , P.SERVICE_COUNTY
         , P.SALES_OFFICE
         , P.ROC_NAME
         , TO_DATE(P.CANCELLATION_DATE)     AS CANCELLATION_DATE
    FROM RPT.T_PROJECT AS P
    WHERE P.INSTALLATION_COMPLETE IS NOT NULL
)

   , I_DAY_WIP AS (
    SELECT D.DT                                              AS MONTH
         , PR.SERVICE_STATE
         , PR.SERVICE_CITY
         , SUM(CASE WHEN PR.INSTALL_DATE <= D.DT THEN 1 END) AS ACTIVE_INSTALLS
    FROM RPT.T_DATES AS D
       , PROJECTS_RAW AS PR
    WHERE D.DT BETWEEN
              DATE_TRUNC('Y', CURRENT_DATE) AND
              LAST_DAY(DATEADD('MM', -1, CURRENT_DATE))
    GROUP BY MONTH
           , PR.SERVICE_STATE
           , PR.SERVICE_CITY
    ORDER BY PR.SERVICE_CITY
           , PR.SERVICE_STATE
           , MONTH
)

   , I_MONTH_WIP AS (
    SELECT *
    FROM I_DAY_WIP
    WHERE MONTH = LAST_DAY(MONTH)
)

   , I_MONTH_STATE_WIP AS (
    SELECT MONTH
         , SERVICE_STATE
         , SUM(ACTIVE_INSTALLS) AS WIP
    FROM I_DAY_WIP
    WHERE MONTH = LAST_DAY(MONTH)
    GROUP BY MONTH, SERVICE_STATE
       ORDER BY SERVICE_STATE, MONTH
)

SELECT *
FROM I_MONTH_STATE_WIP

-- SELECT MONTH
--      , SERVICE_STATE
--      , SUM(ACTIVE_INSTALLS)
-- FROM I_WIP
-- GROUP BY MONTH
--        , SERVICE_STATE
-- ORDER BY MONTH