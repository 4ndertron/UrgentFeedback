WITH PROJECTS_RAW AS (
    SELECT PROJECT_ID
         , NVL(SERVICE_STATE, '[blank]')              AS STATE_NAME
         , CASE
               WHEN INSTALLATION_COMPLETE IS NOT NULL
                   THEN 'ACE' END                     AS ORG_BUCKET
         , TO_DATE(INSTALLATION_COMPLETE)             AS INSTALLATION_DATE
         , TO_DATE(CANCELLATION_DATE)                 AS CANCELLATION_DATE
         , TO_DATE(NVL(IN_SERVICE_DATE, PTO_AWARDED)) AS IN_SERVICE_DATE
    FROM RPT.T_PROJECT
    WHERE INSTALLATION_COMPLETE IS NOT NULL
)

   , I_DAY_WIP AS (
    SELECT D.DT
         , STATE_NAME
         , ORG_BUCKET
         , COUNT(CASE
                     WHEN PR.INSTALLATION_DATE <= D.DT AND
                          (PR.CANCELLATION_DATE >= D.DT OR
                           PR.CANCELLATION_DATE IS NULL)
                         THEN 1 END) AS ACTIVE_INSTALLS
         , COUNT(CASE
                     WHEN PR.INSTALLATION_DATE <= D.DT AND
                          (PR.IN_SERVICE_DATE > D.DT OR
                           PR.IN_SERVICE_DATE IS NULL)
                         THEN 1 END) AS ACTIVE_PRE_PTO
    FROM PROJECTS_RAW AS PR
       , RPT.T_DATES AS D
    WHERE D.DT BETWEEN
              DATE_TRUNC('Y', DATEADD('Y', -1, CURRENT_DATE)) AND
              CURRENT_DATE
    GROUP BY D.DT
           , STATE_NAME
           , ORG_BUCKET
    ORDER BY STATE_NAME
           , ORG_BUCKET
           , D.DT
)

   , I_MONTH_WIP AS (
    SELECT IW.DT       AS MONTH
         , YEAR(MONTH) AS YEAR
         , IW.STATE_NAME
         , IW.ORG_BUCKET
         , IW.ACTIVE_INSTALLS
         , IW.ACTIVE_PRE_PTO
    FROM I_DAY_WIP IW
    WHERE DAY(DT) = DAY(CURRENT_DATE)
    ORDER BY STATE_NAME
           , ORG_BUCKET
           , DT
)


   , INSTALL_ION AS (
    SELECT DATE_TRUNC('MM', TO_DATE(D.DT)) + DAY(CURRENT_DATE) - 1 AS MONTH
         , YEAR(MONTH)                                             AS YEAR
         , PR.STATE_NAME
         , PR.ORG_BUCKET
         , COUNT(CASE WHEN PR.INSTALLATION_DATE = D.DT THEN 1 END) AS INSTALL_INFLOW
         , COUNT(CASE WHEN PR.CANCELLATION_DATE = D.DT THEN 1 END) AS INSTALLATION_OUTFLOW
         , INSTALL_INFLOW - INSTALLATION_OUTFLOW                   AS INSTALLATION_NET
         , COUNT(CASE WHEN PR.IN_SERVICE_DATE = D.DT THEN 1 END)   AS PRE_PTO_OUTFLOW
         , INSTALL_INFLOW - PRE_PTO_OUTFLOW                        AS PRE_PTO_NET
    FROM PROJECTS_RAW AS PR
       , RPT.T_DATES AS D
    WHERE D.DT BETWEEN
              DATE_TRUNC('Y', DATEADD('Y', -1, CURRENT_DATE)) AND
              CURRENT_DATE
    GROUP BY MONTH
           , STATE_NAME
           , ORG_BUCKET
    ORDER BY STATE_NAME
           , ORG_BUCKET
           , MONTH
)

   , METRIC_MERGE AS (
    SELECT ION.MONTH
         , ION.YEAR
         , ION.STATE_NAME
         , ION.ORG_BUCKET
         , ION.INSTALL_INFLOW
         , ION.INSTALLATION_OUTFLOW
         , ION.INSTALLATION_NET
         , WIP.ACTIVE_INSTALLS
         , ION.PRE_PTO_OUTFLOW
         , ION.PRE_PTO_NET
         , WIP.ACTIVE_PRE_PTO
    FROM INSTALL_ION AS ION
             INNER JOIN I_MONTH_WIP AS WIP
                        ON WIP.MONTH = ION.MONTH
                            AND WIP.STATE_NAME = ION.STATE_NAME
                            AND WIP.ORG_BUCKET = ION.ORG_BUCKET
                            AND WIP.YEAR = ION.YEAR
    ORDER BY ION.STATE_NAME
           , ION.MONTH
)

SELECT *
FROM METRIC_MERGE
;