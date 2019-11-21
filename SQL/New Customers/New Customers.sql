WITH LIST AS ( -- Raw data list
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
                        ON D.DT BETWEEN
                            TO_DATE(P.PTO_AWARDED) AND
                            DATEADD(dd, 30, TO_DATE(P.PTO_AWARDED))
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
                                 --                                 CL.DATE BETWEEN
--                                     TO_DATE(P.PTO_AWARDED) AND
--                                     DATEADD(dd, 30, TO_DATE(P.PTO_AWARDED)) AND
                                CL.ANI = NVL(CLEAN_MOBILE_PHONE, CLEAN_PHONE)
    WHERE P.PTO_AWARDED IS NOT NULL
      AND D.DT BETWEEN
        DATE_TRUNC('Y', CURRENT_DATE) AND
        CURRENT_DATE
)

   , TEST_RESULTS AS ( -- Query troubleshooting
    SELECT DISTINCT QUEUE_TYPE
    FROM LIST
    ORDER BY 1
)

SELECT *
FROM LIST
