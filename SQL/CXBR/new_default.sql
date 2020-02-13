WITH ORIGINAL_METRICS AS (
    /*
     Pull in the case metric master view as the starting point.
     */
    SELECT *
    FROM D_POST_INSTALL.T_DEFAULT_CASE_METRICS_MASTER AS DCM
    WHERE DCM.DT BETWEEN
        DATE_TRUNC(Y, DATEADD(Y, -1, CURRENT_DATE)) AND
        LAST_DAY(DATEADD(MM, -1, CURRENT_DATE))
      AND PROCESS_BUCKET != 'Audit'
)

   , MAIN AS (
    SELECT LAST_DAY(DT)                                          AS MONTH
         , SUM("In")                                             AS "Inflow"     -- 1
         , SUM("Out")                                            AS "Outflow"    -- 2
         , SUM(CASE WHEN DT = LAST_DAY(DT) THEN "Total WIP" END) AS WIP          -- 3
         , SUM("Total Contacts")                                 AS UPDATES
         , ROUND(UPDATES / WIP, 1)                               AS "X Coverage" -- 4
         , SUM("Closed-Won-Num")                                 AS CLOSED_WON_NUM
         , SUM("Closed-Won-Denom")                               AS CLOSED_WON_DENOM
         , ROUND(CLOSED_WON_NUM / CLOSED_WON_DENOM, 4)           AS WL           -- 5
         , SUM("Total Savings")                                  AS "Saved"      -- 6
    FROM ORIGINAL_METRICS -- Proper aggregation
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
     , WIP
     , "X Coverage"
     , WL
     , "Saved"
FROM MAIN
ORDER BY 1
