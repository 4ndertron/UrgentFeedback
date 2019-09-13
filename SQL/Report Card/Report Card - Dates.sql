SELECT D.DT                                                    AS MONTH
--      , DATE_TRUNC('MM', TO_DATE(D.DT)) + DAY(CURRENT_DATE) - 1 AS MONTH -- Month to use when setting up an ION Table
     , YEAR(MONTH)                                             AS YEAR
FROM RPT.T_DATES AS D
WHERE D.DT BETWEEN
    DATE_TRUNC('Y', DATEADD('Y', -1, CURRENT_DATE)) AND
    CURRENT_DATE
  AND DAY(D.DT) = DAY(CURRENT_DATE)