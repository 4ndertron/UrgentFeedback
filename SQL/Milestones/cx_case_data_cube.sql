WITH FIELD_HISTORY AS (
    SELECT CH.ID
         , CH.CASEID
         , CH.FIELD
         , CH.OLDVALUE
         , CH.NEWVALUE
         , CH.ISDELETED
         , CH.CREATEDBYID
         , CH.CREATEDDATE AS CREATED_DATE
    FROM RPT.V_SF_CASEHISTORY AS CH
)

   , COMMENT_HISTORY AS (
    SELECT CC.ID
         , CC.PARENTID                                    AS CASEID
         , 'Case_Comment'                                 AS FIELD
         , LAG(CC.COMMENTBODY) OVER
        (PARTITION BY CC.PARENTID ORDER BY CC.CREATEDATE) AS OLD_VALUE
         , CC.COMMENTBODY                                 AS NEW_VALUE
         , CC.ISDELETED
         , CC.CREATEDBYID
         , CC.CREATEDATE                                  AS CREATED_DATE
    FROM RPT.V_SF_CASECOMMENT AS CC
)
   -- Keeping as reference for later
--          , IFF((H.NEWVALUE ILIKE '%PENDING%ACTION' AND
--                 H.NEWVALUE NOT ILIKE '%LEGAL%' AND
--                 C.RECORD_TYPE = 'Solar - Customer Default')
--                    OR
--                (C.RECORD_TYPE ILIKE 'Solar - Customer Escalation' AND
--                 C.EXECUTIVE_RESOLUTIONS_ACCEPTED IS NOT NULL AND
--                 (H.NEWVALUE ILIKE '%EXECUT%' OR
--                  H.NEWVALUE ILIKE '%CORPORAT%')),
--                H.CREATED_DATE,
--                NULL)                                                  AS WORKING_WITH_CUSTOMER

   , CASE_LIFETIME AS (
    SELECT C.CASE_NUMBER
         , C.CASE_ID
         , '<a href="https://vivintsolar.lightning.force.com/lightning/r/Case/' || C.CASE_ID ||
           '/view" target="_blank">' || C.CASE_NUMBER || '</a>'            AS SALESFORCE_CASE_LINK
         , P.SERVICE_STATE
         , P.SERVICE_COUNTY
         , P.SERVICE_CITY
         , C.RECORD_TYPE
         , IFF(EC.CLOSED_WON ILIKE 'YES', 'Closed - Saved', C.STATUS)      AS STATUS
         , C.CREATED_DATE                                                  AS CASE_CREATED_DATE
         , CASE
               WHEN H.FIELD = 'Case_Comment'
                   AND H.OLDVALUE IS NULL
                   THEN DATEDIFF(sec,
                                 C.CREATED_DATE,
                                 H.CREATED_DATE)
        END                                                                AS CASE_CREATED_TO_COMMENT_GAP
         , C.CLOSED_DATE                                                   AS CASE_CLOSED_DATE
         , C.OWNER
         , C.OWNER_EMPLOYEE_ID
         , C.HOME_VISIT_ONE
         , C.P_4_LETTER
         , C.P_5_LETTER
         , C.DRA
         , C.EXECUTIVE_RESOLUTIONS_ACCEPTED
         , C.PRIORITY
         , C.SOLAR_QUEUE
         , C.PRIMARY_REASON
         , C.ORIGIN
         , H.FIELD                                                         AS FIELD_CHANGED
         , E.FULL_NAME                                                     AS FIELD_CHANGED_BY
         , E.EMPLOYEE_ID                                                   AS FIELD_CHANGED_BY_EMPLOYEE_ID
         , H.CREATED_DATE                                                  AS FIELD_CHANGE_DATE
         , H.OLDVALUE                                                      AS OLD_FIELD_VALUE
         , LAG(H.CREATED_DATE) OVER
        (PARTITION BY C.CASE_NUMBER, H.FIELD ORDER BY H.CREATED_DATE)      AS OLD_FIELD_DATE
         , H.NEWVALUE                                                      AS NEW_FIELD_VALUE
         , LEAD(H.NEWVALUE) OVER
        (PARTITION BY C.CASE_NUMBER, H.FIELD ORDER BY H.CREATED_DATE)      AS NEXT_FIELD_VALUE
         , LEAD(H.CREATED_DATE) OVER
        (PARTITION BY C.CASE_NUMBER, H.FIELD ORDER BY H.CREATED_DATE)      AS NEXT_FIELD_DATE
         , DATEDIFF(sec,
                    NVL(OLD_FIELD_DATE,
                        C.CREATED_DATE),
                    H.CREATED_DATE
        )                                                                  AS LAG_FIELD_GAP
         , DATEDIFF(sec,
                    H.CREATED_DATE,
                    NVL(NEXT_FIELD_DATE,
                        NVL(C.CLOSED_DATE,
                            CURRENT_DATE))
        )                                                                  AS LEAD_FIELD_GAP
         , DATEDIFF(sec, C.CREATED_DATE, NVL(C.CLOSED_DATE, CURRENT_DATE)) AS CASE_AGE_SECONDS
         , IFF(C.CLOSED_DATE IS NOT NULL, COUNT(DISTINCT C.CASE_NUMBER) OVER
        (PARTITION BY C.RECORD_TYPE, OWNER), NULL)                         AS CLOSED_CASES
    FROM (
                 (SELECT * FROM FIELD_HISTORY)
                 UNION ALL
                 (SELECT * FROM COMMENT_HISTORY)
         ) H -- Histories
             LEFT JOIN RPT.T_CASE AS C
                       ON C.CASE_ID = H.CASEID
             LEFT JOIN D_POST_INSTALL.T_EMPLOYEE_MASTER AS E
                       ON E.SALESFORCE_ID = H.CREATEDBYID
             LEFT JOIN RPT.T_PROJECT AS P
                       ON P.PROJECT_ID = C.PROJECT_ID
             LEFT JOIN D_POST_INSTALL.T_ERT_CLOSED_WON_CASES AS EC
                       ON EC.SERVICE_NAME = P.SERVICE_NAME
    WHERE C.RECORD_TYPE IN ('Solar - Customer Default', 'Solar - Billing', 'Solar - Panel Removal', 'Solar - Service',
                            'Solar Damage Resolutions', 'Solar - Customer Escalation', 'Solar - Troubleshooting')
      AND NVL(C.CLOSED_DATE, CURRENT_DATE) >= DATEADD(mm, -4, CURRENT_DATE)
    ORDER BY C.CASE_NUMBER DESC
           , H.CREATED_DATE
)


SELECT *, 999 as SLA_LINK, Current_Date() as LAST_REFRESH_DT
FROM CASE_LIFETIME
ORDER BY CASE_NUMBER DESC
       , FIELD_CHANGE_DATE
