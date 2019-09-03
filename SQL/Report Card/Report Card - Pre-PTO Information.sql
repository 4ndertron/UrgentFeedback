WITH PROJECTS_RAW AS (
    SELECT PROJECT_ID
         , NVL(SERVICE_STATE, '[blank]')  AS STATE_NAME
         , TO_DATE(INSTALLATION_COMPLETE) AS INSTALL_DATE
         , TO_DATE(PTO_AWARDED)           AS PTO_DATE
    FROM RPT.T_PROJECT
    WHERE INSTALLATION_COMPLETE IS NOT NULL
)

   , DAY_PRE_PTO AS (
    SELECT D.DT
         , PR.STATE_NAME
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
           , PR.STATE_NAME
    ORDER BY PR.STATE_NAME
           , D.DT
)

   , MONTH_PRE_PTO AS (
    SELECT DPP.DT      AS MONTH
         , YEAR(MONTH) AS YEAR
         , DPP.STATE_NAME
         , DPP.ACTIVE_PRE_PTO
    FROM DAY_PRE_PTO DPP
    WHERE DPP.DT = LAST_DAY(DPP.DT)
       OR DPP.DT = CURRENT_DATE()
    ORDER BY DPP.STATE_NAME,
             MONTH
)

   , PRE_PTO_ION AS (
    SELECT LAST_DAY(D.DT)                                     AS MONTH
         , YEAR(MONTH)                                        AS YEAR
         , PR.STATE_NAME
         , COUNT(CASE WHEN PR.INSTALL_DATE = D.DT THEN 1 END) AS PRE_PTO_INFLOW
         , COUNT(CASE WHEN PR.PTO_DATE = D.DT THEN 1 END)     AS PRE_PTO_OUTFLOW
         , PRE_PTO_INFLOW - PRE_PTO_OUTFLOW                   AS PRE_PTO_NET
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
         , ION.PRE_PTO_INFLOW
         , ION.PRE_PTO_OUTFLOW
         , ION.PRE_PTO_NET
         , WIP.ACTIVE_PRE_PTO
    FROM PRE_PTO_ION AS ION
             INNER JOIN MONTH_PRE_PTO AS WIP
                        ON WIP.MONTH = ION.MONTH
                            AND WIP.STATE_NAME = ION.STATE_NAME
                            AND WIP.YEAR = ION.YEAR
    ORDER BY ION.STATE_NAME
           , ION.MONTH
)

SELECT *
FROM METRIC_MERGE
;