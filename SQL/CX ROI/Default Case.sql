/*
 Initial Labor
    - Employee development or training neeeds
 Maintenance Labor
    - Number of agents affected
    - Time spent per day
    - Agent Pay
    - 66 Workdays per Quarter

 The purpose of the project is to solidify the default process into IT support and then have Credit Reporting available?
 titanium credit solutions project, and help find the ROI of that.
 Potentially reach out to Cody and Steve Burt and Kent. They should have some ROI data...


 */

WITH CASES AS (
    SELECT P.PROJECT_NAME
         , C.CASE_NUMBER
         , C.CREATED_DATE
         , DATEDIFF(dd,
                    TO_DATE(C.CREATED_DATE),
                    LEAD(TO_DATE(C.CREATED_DATE))
                         OVER (PARTITION BY P.PROJECT_NAME ORDER BY C.CREATED_DATE)) AS TIME_LOST
    FROM RPT.T_PROJECT AS P
             LEFT JOIN
         RPT.T_CASE AS C
         ON C.PROJECT_ID = P.PROJECT_ID
    WHERE C.RECORD_TYPE = 'Solar - Customer Default'
      AND C.CREATED_DATE >= DATE_TRUNC('MM', DATEADD('MM', -3, CURRENT_DATE))
)

   , ACCOUNTS AS (
    SELECT PROJECT_NAME
         , COUNT(CASE_NUMBER) AS CASE_TALLY
         , MAX(TIME_LOST)     AS TIME_LOSS
    FROM CASES
    GROUP BY PROJECT_NAME
    ORDER BY PROJECT_NAME
)

   , AGENTS AS (
    SELECT *
    FROM HR.T_EMPLOYEE_ALL
)

SELECT AVG(TIME_LOSS) AS AVG_LOSS
FROM ACCOUNTS
WHERE CASE_TALLY > 1
AND TIME_LOSS < 90