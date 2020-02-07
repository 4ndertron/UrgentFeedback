WITH ORIGINAL_METRICS AS (
    /*
     Pull in the case metric master view as the starting point.
     */
    SELECT *
    FROM D_POST_INSTALL.T_ERT_CASE_METRICS_MASTER AS ECM
    WHERE ECM.DT BETWEEN
              DATE_TRUNC(Y, DATEADD(Y, -1, CURRENT_DATE)) AND
              LAST_DAY(DATEADD(MM, -1, CURRENT_DATE))
)

   , MAIN AS (
    SELECT LAST_DAY(DT)                                          AS MONTH
         , SUM("In")                                             AS "Inflow"  -- 1
         , SUM("Out")                                            AS "Outflow" -- 1
         , AVG("Age of WIP")                                     AS AGE       -- 2
         , SUM(CASE WHEN DT = LAST_DAY(DT) THEN "Total WIP" END) AS WIP       -- 3
         , AVG("Average Gap")                                    AS GAP       -- 4
    FROM ORIGINAL_METRICS
    GROUP BY LAST_DAY(DT)
    ORDER BY 1 DESC
)

   , DEBUG_CTE AS (
    SELECT *
--     FROM ORIGINAL_METRICS
    FROM MAIN
)

SELECT MONTH
     , "Inflow"
     , "Outflow"
     , AGE
     , WIP
     , GAP
FROM MAIN
ORDER BY 1
;