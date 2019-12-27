WITH ORIGINAL_LIST AS ( -- Raw data list
    SELECT P.PROJECT_NAME
         , P.SERVICE_NUMBER
         , TO_DATE(P.PTO_AWARDED)                                                      AS PTO_DATE
         , DATEADD(dd, 30, TO_DATE(P.PTO_AWARDED))                                     AS FIRST_BILLING_PERIOD_END
         , D.DT
         , B.START_BILLING_DATE
         , DATEADD(dd, 8, B.START_BILLING_DATE)                                        AS FIRST_INVOICE_DATE
         , CL.SESSION_ID                                                               AS CALL_ID
         , COUNT(CL.SESSION_ID) OVER (PARTITION BY P.PROJECT_NAME)                     AS CUSTOMER_CALLS
         , CL.DATE                                                                     AS CALL_DATE
         , DATEDIFF(dd, FIRST_BILLING_PERIOD_END, CALL_DATE)                           AS BILLING_TO_CALL_GAP
         , DATEDIFF(dd,
                    LAG(CL.DATE) OVER (PARTITION BY P.PROJECT_NAME ORDER BY CL.DATE),
                    CL.DATE)                                                           AS PREVIOUS_CALL_GAP
         , DATEDIFF(dd,
                    CL.DATE,
                    LEAD(CL.DATE) OVER (PARTITION BY P.PROJECT_NAME ORDER BY CL.DATE)) AS NEXT_CALL_GAP
         , CL.QUEUE_1
         , IFF(REGEXP_LIKE(CL.QUEUE_1, 'Q_\\d+', 'i'),
               'Personal',
               REGEXP_SUBSTR(CL.QUEUE_1, '([^Q\\_]+)_', 1, 1, 'ie'))                   AS QUEUE_TYPE
         , CT.EMAIL
         , CT.FULL_NAME
         , P.SERVICE_STATE
         , CURRENT_DATE                                                                AS LAST_REFRESHED
    FROM RPT.T_DATES AS D
             INNER JOIN RPT.T_PROJECT AS P -- Salesforce Project Records
                        ON D.DT = TO_DATE(P.PTO_AWARDED)
             LEFT JOIN (SELECT DISTINCT PROJECT_ID
                                      , START_BILLING_DATE
                        FROM RPT.T_NV_PV_DSAB_ACCOUNT_DETAILS) AS B
                       ON B.PROJECT_ID = P.PROJECT_ID
             LEFT OUTER JOIN RPT.V_SF_ACCOUNT AS A -- Salesforce Account Records
                             ON A.ID = P.ACCOUNT_ID
             LEFT OUTER JOIN RPT.T_CONTACT AS CT -- Salesforce Contact Records
                             ON CT.CONTACT_ID = A.PRIMARY_CONTACT_ID
             LEFT OUTER JOIN (SELECT DISTINCT SESSION_ID
                                            , ANI
                                            , DATE
                                            , QUEUE_1
                              FROM D_POST_INSTALL.T_CJP_CDR_TEMP) AS CL -- CJP Call Sessions
                             ON CL.DATE BETWEEN P.PTO_AWARDED AND DATEADD(dd, 180, TO_DATE(P.PTO_AWARDED)) AND
                                CL.ANI = NVL(CLEAN_MOBILE_PHONE, CLEAN_PHONE)
    WHERE P.PTO_AWARDED IS NOT NULL
      AND D.DT BETWEEN
        '2019-04-01' AND
        CURRENT_DATE
)

   , EO AS (
    SELECT EO.SUBSCRIBER_KEY
         , EO.EMAIL_ADDRESS
         , EO.SERVICE_NUMBER
         , MIN(DATE_OPENED)       AS FIRST_OPEN
         , MAX(DATE_OPENED)       AS LAST_OPEN
         , ANY_VALUE(TOTAL_OPENS) AS TOTAL_OPENS
    FROM (SELECT *
               , COUNT(SUBSCRIBER_KEY)
                       OVER (PARTITION BY SUBSCRIBER_KEY) AS TOTAL_OPENS
          FROM D_POST_INSTALL.T_PTO_TO_BILLING_TMP_EMAIL_OPENS) AS EO
    GROUP BY EO.SUBSCRIBER_KEY, EO.EMAIL_ADDRESS, EO.SERVICE_NUMBER
)

   , EC AS (
    SELECT EC.SUBSCRIBER_KEY
         , EC.EMAIL_ADDRESS
         , EC.SERVICE_NUMBER
         , MIN(DATE_CLICKED)       AS FIRST_CLICK
         , MAX(DATE_CLICKED)       AS LAST_CLICK
         , ANY_VALUE(TOTAL_CLICKS) AS TOTAL_CLICKS
    FROM (SELECT *
               , COUNT(SUBSCRIBER_KEY)
                       OVER (PARTITION BY SUBSCRIBER_KEY) AS TOTAL_CLICKS
          FROM D_POST_INSTALL.T_PTO_TO_BILLING_TMP_EMAIL_LINK_CLICKS) AS EC
    GROUP BY EC.SUBSCRIBER_KEY, EC.EMAIL_ADDRESS, EC.SERVICE_NUMBER
)

   , MAIN AS (
    SELECT OL.*
         , EO.FIRST_OPEN
         , EO.LAST_OPEN
         , EO.TOTAL_OPENS
         , DATEDIFF(dd, OL.FIRST_BILLING_PERIOD_END, EO.FIRST_OPEN)  AS FIRST_OPEN_GAP
         , EC.FIRST_CLICK
         , EC.LAST_CLICK
         , EC.TOTAL_CLICKS
         , DATEDIFF(dd, OL.FIRST_BILLING_PERIOD_END, EC.FIRST_CLICK) AS FIRST_CLICK_GAP
         , DATEDIFF(dd, EO.FIRST_OPEN, OL.CALL_DATE)                 AS EMAIL_OPEN_TO_CALL_GAP
         , DATEDIFF(dd, EC.FIRST_CLICK, OL.CALL_DATE)                AS LINK_CLICK_TO_CALL_GAP
    FROM ORIGINAL_LIST AS OL
             LEFT JOIN EO
                       ON EO.EMAIL_ADDRESS = OL.EMAIL AND
                          EO.FIRST_OPEN >= OL.FIRST_BILLING_PERIOD_END
             LEFT JOIN EC
                       ON EC.EMAIL_ADDRESS = OL.EMAIL AND
                          EC.FIRST_CLICK >= OL.FIRST_BILLING_PERIOD_END
    ORDER BY PROJECT_NAME, CALL_DATE
)

SELECT *
FROM MAIN

/*
 Of the customers who have
 attributes for things that compare.
 pre-campaign
 post-campaign
 relative to pto date.
 how long and often after pto customers have called
 stack and look side by side.
 frequency 30% after 7 days
 pto post frequently more and sooner,
 and topics of calls pre and post campaign

 turn the metrics into a lead instead of a lag

 remove account center.

 proactive adjustments for unforseen data anomolies.
 sync up and follow-up next week.

 Setup another 15-20 minute meeting to walk through the data and understand the data.
 If there are changes to be made.

 Take PTO date,
 datediff between datediff from pto to datetrunc 'y',
 avg duration to first call.

 att of iff pto is before or after 10/1
 to identify in and out of campaign customers

 normalize everything to same start date
 with and without on left axis
 one will be campaign and one will not be.
 Then check out behavior

 normalize time.

 take all over times, then align them to themselves


 todo: Move focus to billing date. The objective is to observe customer behavior in their first billing week.
    email is sent on 1, 3, 5 th of every month.
    Billing Day in LD Table.

 */