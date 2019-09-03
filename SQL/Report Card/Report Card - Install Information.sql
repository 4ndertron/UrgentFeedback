WITH PROJECTS_RAW AS (
    SELECT PROJECT_ID
         , NVL(SERVICE_STATE, '[blank]')  AS STATE_NAME
         , TO_DATE(INSTALLATION_COMPLETE) AS INSTALL_DATE
         , TO_DATE(CANCELLATION_DATE)     AS CANCELLATION_DATE
         , TO_DATE(PTO_AWARDED)           AS PTO_DATE
    FROM RPT.T_PROJECT
    WHERE INSTALLATION_COMPLETE IS NOT NULL
)

   , I_DAY_WIP AS (
    SELECT D.DT
         , STATE_NAME
         , COUNT(CASE
                     WHEN PR.INSTALL_DATE <= D.DT AND
                          (PR.CANCELLATION_DATE >= D.DT OR
                           PR.CANCELLATION_DATE IS NULL)
                         THEN 1 END) AS ACTIVE_INSTALLS
         , COUNT(CASE
                     WHEN PR.INSTALL_DATE <= D.DT AND
                          (PR.PTO_DATE > D.DT OR
                           PR.PTO_DATE IS NULL)
                         THEN 1 END) AS ACTIVE_PRE_PTO
    FROM PROJECTS_RAW AS PR
       , RPT.T_DATES AS D
    WHERE D.DT BETWEEN
              DATE_TRUNC('Y', DATEADD('Y', -1, CURRENT_DATE)) AND
              CURRENT_DATE
    GROUP BY D.DT
           , STATE_NAME
    ORDER BY STATE_NAME
           , D.DT
)

   , I_MONTH_WIP AS (
    SELECT IW.DT       AS MONTH
         , YEAR(MONTH) AS YEAR
         , IW.STATE_NAME
         , IW.ACTIVE_INSTALLS
         , IW.ACTIVE_PRE_PTO
    FROM I_DAY_WIP IW
    WHERE DT = LAST_DAY(DT)
       OR DT = CURRENT_DATE()
    ORDER BY STATE_NAME, DT
)


   , INSTALL_ION AS (
    SELECT LAST_DAY(D.DT)                                          AS MONTH
         , YEAR(MONTH)                                             AS YEAR
         , PR.STATE_NAME
         , COUNT(CASE WHEN PR.INSTALL_DATE = D.DT THEN 1 END)      AS INSTALL_INFLOW
         , COUNT(CASE WHEN PR.CANCELLATION_DATE = D.DT THEN 1 END) AS INSTALLATION_OUTFLOW
         , INSTALL_INFLOW - INSTALLATION_OUTFLOW                   AS INSTALLATION_NET
         , COUNT(CASE WHEN PR.PTO_DATE = D.DT THEN 1 END)          AS PRE_PTO_OUTFLOW
         , INSTALL_INFLOW - PRE_PTO_OUTFLOW                        AS PRE_PTO_NET
    FROM PROJECTS_RAW AS PR
       , RPT.T_DATES AS D
    WHERE D.DT BETWEEN
              DATE_TRUNC('Y', DATEADD('Y', -1, CURRENT_DATE)) AND
              CURRENT_DATE
    GROUP BY MONTH
           , STATE_NAME
    ORDER BY STATE_NAME
           , MONTH
)

   , METRIC_MERGE AS (
    SELECT ION.MONTH
         , ION.YEAR
         , ION.STATE_NAME
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
                            AND WIP.YEAR = ION.YEAR
    ORDER BY ION.STATE_NAME
           , ION.MONTH
)

SELECT *
FROM METRIC_MERGE
;