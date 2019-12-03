WITH ORIGINAL_LIST AS ( -- Raw data list
    SELECT P.PROJECT_NAME
         , P.SERVICE_NUMBER
         , TO_DATE(P.PTO_AWARDED)                                    AS PTO_DATE
         , DATEADD(dd, 30, TO_DATE(P.PTO_AWARDED))                   AS FIRST_BILLING_PERIOD_END
         , D.DT
         , CL.SESSION_ID                                             AS CALL_ID
         , CL.DATE                                                   AS CALL_DATE
         , CL.QUEUE_1
         , IFF(REGEXP_LIKE(CL.QUEUE_1, 'Q_\\d+', 'i'),
               'Personal',
               REGEXP_SUBSTR(CL.QUEUE_1, '([^Q\\_]+)_', 1, 1, 'ie')) AS QUEUE_TYPE
         , IFF(D.DT BETWEEN
                   P.PTO_AWARDED AND
                   FIRST_BILLING_PERIOD_END, 1, 0)                   AS ACTIVE_TALLY
         , CT.EMAIL
         , CT.FULL_NAME
         , P.SERVICE_STATE
         , CURRENT_DATE                                              AS LAST_REFRESHED
    FROM RPT.T_DATES AS D
             INNER JOIN RPT.T_PROJECT AS P -- Salesforce Project Records
                        ON D.DT = TO_DATE(P.PTO_AWARDED)
             LEFT OUTER JOIN RPT.V_SF_ACCOUNT AS A -- Salesforce Account Records
                             ON A.ID = P.ACCOUNT_ID
             LEFT OUTER JOIN RPT.T_CONTACT AS CT -- Salesforce Contact Records
                             ON CT.CONTACT_ID = A.PRIMARY_CONTACT_ID
             LEFT OUTER JOIN (SELECT DISTINCT SESSION_ID
                                            , ANI
                                            , DATE
                                            , QUEUE_1
                              FROM D_POST_INSTALL.T_CJP_CDR_TEMP) AS CL -- CJP Call Sessions
                             ON CL.DATE = D.DT AND
                                CL.ANI = NVL(CLEAN_MOBILE_PHONE, CLEAN_PHONE)
    WHERE P.PTO_AWARDED IS NOT NULL
      AND D.DT BETWEEN
        DATE_TRUNC('Y', CURRENT_DATE) AND
        CURRENT_DATE
)


SELECT *
FROM ORIGINAL_LIST

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

 */