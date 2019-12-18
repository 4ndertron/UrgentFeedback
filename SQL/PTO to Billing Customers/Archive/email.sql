/*
 Brand new customer call volume after receiving their introductory email.
 Jessica Kelly has the emails.
 Has some quick information.
 PTO TO BILLING EMAILS.
 The last just went out on 10/8
 NO emails were sent out in September.
 Monthly refresh rate at the beginning of the month.
 They're on a schedule after the fisrt, through the time of the billing on the 7-10th.
 */

WITH CALL_VIEW AS (
    SELECT C.DATE
         , C.QUEUE_1
         , CT.ACCOUNT_ID
         , TO_DATE(P.PTO_AWARDED)                                  AS PTO_DATE
         , DATEADD('MM', 1, LAST_DAY(TO_DATE(P.PTO_AWARDED))) + 10 AS FIRST_BILLING_PERIOD_END
    FROM D_POST_INSTALL.T_CJP_CDR_TEMP AS C
             LEFT JOIN RPT.T_CONTACT AS CT
                       ON NVL(CLEAN_MOBILE_PHONE, CLEAN_PHONE) = C.ANI
             LEFT JOIN RPT.T_PROJECT AS P
                       ON P.ACCOUNT_ID = CT.ACCOUNT_ID
    WHERE C.session_id IS NOT NULL
      AND C.contact_type != 'Master'
      AND C.connected > 0
      AND C.QUEUE_1 NOT ILIKE '%OUTDIAL%'
    ORDER BY C.DATE DESC
)

   , MAIN AS (
    SELECT DATE_TRUNC('MM', DATE)                                                   AS MONTH
         , COUNT(CASE
                     WHEN NVL(ACCOUNT_ID, 'NONE') != 'NONE'
                         THEN 1 END)                                                AS CONNECTION
         , COUNT(CASE
                     WHEN NVL(ACCOUNT_ID, 'NONE') = 'NONE'
                         THEN 1 END)                                                AS NO_CONNECTION
         , CONNECTION + NO_CONNECTION                                               AS TOTAL_CALLS
         , IFF(CONNECTION = 0, 1, ROUND(NO_CONNECTION / CONNECTION, 4))             AS ERROR_VARIANCE
         , COUNT(CASE
                     WHEN DATE BETWEEN PTO_DATE AND FIRST_BILLING_PERIOD_END
                         THEN 1 END)                                                AS CONNECTION_FIRST_BILLING_CYCLE
         , IFF(CONNECTION_FIRST_BILLING_CYCLE = 0, 0,
               ROUND(CONNECTION_FIRST_BILLING_CYCLE / CONNECTION, 4))               AS KNOWN_FIRST_CONNECTION_RATIO
         , ROUND(CONNECTION_FIRST_BILLING_CYCLE +
                 (NO_CONNECTION * KNOWN_FIRST_CONNECTION_RATIO), 0)                 AS ESTIMATED_FIRST_CONNECTION
         , IFF(ESTIMATED_FIRST_CONNECTION = 0, 0,
               ROUND(ESTIMATED_FIRST_CONNECTION / (CONNECTION + NO_CONNECTION), 4)) AS ESTIMATED_FIRST_CONNECTION_RATIO
    FROM CALL_VIEW
    GROUP BY MONTH
    ORDER BY MONTH DESC
)

SELECT *
FROM CALL_VIEW