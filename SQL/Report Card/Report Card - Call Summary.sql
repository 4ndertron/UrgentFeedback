WITH CALL_VIEW AS (
    SELECT C.DATE
         , C.QUEUE_1
         , CT.ACCOUNT_ID
         , UPPER(P.SERVICE_STATE)                                  AS SERVICE_STATE
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
    SELECT DATE_TRUNC('MM', TO_DATE(DATE)) + DAY(CURRENT_DATE) - 1                 AS MONTH
         , SERVICE_STATE
         , COUNT(CASE
                     WHEN NVL(ACCOUNT_ID, 'NONE') != 'NONE'
                         THEN 1 END)                                               AS KNOWN_CALLS
         , SUM(IFF(SERVICE_STATE IS NULL, 0, KNOWN_CALLS)) OVER (PARTITION BY MONTH) AS MONTHLY_KNOWN_CALLS
         , SUM(KNOWN_CALLS) OVER (PARTITION BY MONTH)                              AS MONTHLY_CALLS
         , KNOWN_CALLS / MONTHLY_KNOWN_CALLS                                       AS STATE_KNOWN_CALL_RATIO
         , KNOWN_CALLS / MONTHLY_CALLS                                             AS STATE_CALL_RATIO
    FROM CALL_VIEW
    GROUP BY MONTH, SERVICE_STATE
    ORDER BY SERVICE_STATE, MONTH DESC
)

SELECT *
FROM MAIN