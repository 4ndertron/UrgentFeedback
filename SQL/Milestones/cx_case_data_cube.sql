WITH VERTICAL_CASES AS (
    SELECT C.CASE_NUMBER
         , C.CASE_ID
         , D.DT
         , IFF(CH.NEWVALUE = 'Dispute/Evasion', CH.CREATEDDATE, NULL)  AS AUDIT
         , IFF(CH.NEWVALUE = 'Foreclosure', CH.CREATEDDATE, NULL)      AS FORECLOSURE
         , IFF(CH.NEWVALUE = 'Deceased', CH.CREATEDDATE, NULL)         AS DECEASED
         , IFF((CH.NEWVALUE ILIKE '%PENDING%ACTION' AND
                CH.NEWVALUE NOT ILIKE '%LEGAL%' AND
                C.RECORD_TYPE = 'Solar - Customer Default')
                   OR
               (C.RECORD_TYPE ILIKE 'Solar - Customer Escalation' AND
                C.EXECUTIVE_RESOLUTIONS_ACCEPTED IS NOT NULL AND
                (CH.NEWVALUE ILIKE '%EXECUT%' OR
                 CH.NEWVALUE ILIKE '%CORPORAT%')),
               CH.CREATEDDATE,
               NULL)                                                   AS WORKING_WITH_CUSTOMER
         , IFF(CH.NEWVALUE ILIKE '%ESCALATED%' AND
               C.RECORD_TYPE = 'Solar - Custom Default',
               CH.CREATEDDATE,
               NULL)                                                   AS PENDING_LEGAL_FEEDBACK
         , IFF(CH.NEWVALUE ILIKE '%LEGAL%' AND
               C.RECORD_TYPE = 'Solar - Custom Default',
               CH.CREATEDDATE,
               NULL)                                                   AS RETURNED_FROM_THIRD_PARTY
         , C.HOME_VISIT_ONE
         , C.P_4_LETTER
         , C.P_5_LETTER
         , C.DRA
         , C.EXECUTIVE_RESOLUTIONS_ACCEPTED
         , C.PRIORITY
         , C.RECORD_TYPE
         , C.OWNER
         , C.OWNER_EMPLOYEE_ID
         , C.CREATED_DATE
         , C.CLOSED_DATE
         , C.SOLAR_QUEUE
         , C.PRIMARY_REASON
         , C.ORIGIN
         , IFF(EC.CLOSED_WON ILIKE 'YES', 'Closed - Saved', C.STATUS)  AS STATUS
         , CH.FIELD                                                    AS FIELD_CHANGED
         , E.FULL_NAME                                                 AS FIELD_CHANGED_BY
         , E.EMPLOYEE_ID                                               AS FIELD_CHANGED_BY_EMPLOYEE_ID
         , CH.CREATEDDATE                                              AS FIELD_CHANGE_DATE
         , CH.OLDVALUE                                                 AS OLD_FIELD_VALUE
         , CH.NEWVALUE                                                 AS NEW_FIELD_VALUE
         , LEAD(CH.NEWVALUE) OVER
        (PARTITION BY C.CASE_NUMBER, CH.FIELD ORDER BY CH.CREATEDDATE) AS NEXT_FIELD_VALUE
         , LEAD(CH.CREATEDDATE) OVER
        (PARTITION BY C.CASE_NUMBER, CH.FIELD ORDER BY CH.CREATEDDATE) AS NEXT_FIELD_DATE
         , DATEDIFF(s, C.CREATED_DATE, CH.CREATEDDATE)                 AS FIELD_CHANGE_STAGE_SECONDS
         , COUNT(C.CASE_ID) OVER (PARTITION BY C.CASE_ID)              AS TOTAL_UPDATES

    FROM RPT.T_CASE AS C
             LEFT JOIN RPT.V_SF_CASEHISTORY AS CH
                       ON CH.CASEID = C.CASE_ID
             LEFT JOIN D_POST_INSTALL.T_EMPLOYEE_MASTER AS E
                       ON E.SALESFORCE_ID = CH.CREATEDBYID
             INNER JOIN RPT.T_DATES AS D
                        ON D.DT = DATE_TRUNC(dd, C.CREATED_DATE)
             LEFT JOIN RPT.T_PROJECT AS P
                       ON P.PROJECT_ID = C.PROJECT_ID
             LEFT JOIN D_POST_INSTALL.T_ERT_CLOSED_WON_CASES AS EC
                       ON EC.SERVICE_NAME = P.SERVICE_NAME
    WHERE C.RECORD_TYPE IN ('Solar - Customer Default', 'Solar - Billing', 'Solar - Panel Removal', 'Solar - Service',
                            'Solar Damage Resolutions', 'Solar - Customer Escalation', 'Solar - Troubleshooting')
      AND D.DT BETWEEN
        DATEADD(mm, -12, DATE_TRUNC(mm, CURRENT_DATE)) AND
        CURRENT_DATE
    ORDER BY CASE_NUMBER
        DESC, CH.CREATEDDATE
)

   , HORIZONTAL_CASES AS (
    SELECT VC.CASE_NUMBER
         , VC.CASE_ID
         , VC.DT -- Add "Open Cases" per day per agent..... But this is a case record.
         , ANY_VALUE(VC.EXECUTIVE_RESOLUTIONS_ACCEPTED)                              AS EXECUTIVE_RESOLUTIONS_ACCEPTED
         , ANY_VALUE(VC.PRIORITY)                                                    AS PRIORITY
         , ANY_VALUE(VC.RECORD_TYPE)                                                 AS RECORD_TYPE
         , ANY_VALUE(VC.OWNER)                                                       AS OWNER
         , ANY_VALUE(VC.OWNER_EMPLOYEE_ID)                                           AS OWNER_EMPLOYEE_ID
         , ANY_VALUE(VC.CREATED_DATE)                                                AS CASE_CREATED_DATE
         , ANY_VALUE(VC.CLOSED_DATE)                                                 AS CLOSED_DATE
         , ANY_VALUE(VC.SOLAR_QUEUE)                                                 AS SOLAR_QUEUE
         , ANY_VALUE(VC.PRIMARY_REASON)                                              AS PRIMARY_REASON
         , ANY_VALUE(VC.ORIGIN)                                                      AS ORIGIN
         , ANY_VALUE(VC.STATUS)                                                      AS STATUS
         , MAX(VC.AUDIT)                                                             AS AUDIT
         , IFF(ANY_VALUE(VC.RECORD_TYPE) = 'Solar - Customer Escalation' AND
               ANY_VALUE(VC.EXECUTIVE_RESOLUTIONS_ACCEPTED) IS NULL AND
               ANY_VALUE(VC.PRIORITY = '3'),
               ANY_VALUE(VC.CREATED_DATE),
               NULL)                                                                 AS EVASION_AVOIDANCE
         , IFF(ANY_VALUE(VC.RECORD_TYPE) = 'Solar - Customer Escalation' AND
               ANY_VALUE(VC.EXECUTIVE_RESOLUTIONS_ACCEPTED) IS NULL AND
               ANY_VALUE(VC.PRIORITY IN ('2', '1')),
               ANY_VALUE(VC.CREATED_DATE),
               NULL)                                                                 AS REFUSALS
         , MAX(FORECLOSURE)                                                          AS FORECLOSURE
         , MAX(DECEASED)                                                             AS DECEASED
         , MAX(WORKING_WITH_CUSTOMER)                                                AS WORKING_WITH_CUSTOMER
         , MAX(VC.HOME_VISIT_ONE)                                                    AS HOME_VISIT_LETTER
         , MAX(VC.P_4_LETTER)                                                        AS P_4_LETTER
         , MAX(VC.P_5_LETTER)                                                        AS P_5_LETTER
         , MAX(VC.DRA)                                                               AS DRA
         , MAX(PENDING_LEGAL_FEEDBACK)                                               AS PENDING_LEGAL_FEEDBACK
         , NULL                                                                      AS COLLECTIONS
         , MAX(RETURNED_FROM_THIRD_PARTY)                                            AS RETURNED_FROM_THIRD_PARTY
         , DATEDIFF(sec, ANY_VALUE(VC.CREATED_DATE), MAX(VC.AUDIT))                  AS AUDIT_STAGE
         , DATEDIFF(sec, ANY_VALUE(VC.CREATED_DATE), EVASION_AVOIDANCE)              AS EVASION_AVOIDANCE_STAGE
         , DATEDIFF(sec, ANY_VALUE(VC.CREATED_DATE), REFUSALS)                       AS REFUSALS_STAGE
         , DATEDIFF(sec, ANY_VALUE(VC.CREATED_DATE), MAX(VC.FORECLOSURE))            AS FORECLOSURE_STAGE
         , DATEDIFF(sec, ANY_VALUE(VC.CREATED_DATE), MAX(VC.DECEASED))               AS DECEASED_STAGE
         , DATEDIFF(sec, ANY_VALUE(VC.CREATED_DATE), MAX(VC.WORKING_WITH_CUSTOMER))  AS WORKING_WITH_CUSTOMER_STAGE
         , DATEDIFF(sec, ANY_VALUE(VC.CREATED_DATE), HOME_VISIT_LETTER)              AS HOME_VISIT_LETTER_STAGE
         , DATEDIFF(sec, ANY_VALUE(VC.CREATED_DATE), ANY_VALUE(VC.P_4_LETTER))       AS P_4_LETTER_STAGE
         , DATEDIFF(sec, ANY_VALUE(VC.CREATED_DATE), ANY_VALUE(VC.P_5_LETTER))       AS P_5_LETTER_STAGE
         , DATEDIFF(sec, ANY_VALUE(VC.CREATED_DATE), ANY_VALUE(VC.DRA))              AS DRA_STAGE
         , DATEDIFF(sec, ANY_VALUE(VC.CREATED_DATE), MAX(VC.PENDING_LEGAL_FEEDBACK)) AS PENDING_LEGAL_FEEDBACK_STAGE
         , DATEDIFF(sec, ANY_VALUE(VC.CREATED_DATE), COLLECTIONS)                    AS COLLECTIONS_STAGE
         , DATEDIFF(sec, ANY_VALUE(VC.CREATED_DATE),
                    MAX(VC.RETURNED_FROM_THIRD_PARTY))                               AS RETURNED_FROM_THIRD_PARTY_STAGE
         , DATEDIFF(sec,
                    ANY_VALUE(VC.CREATED_DATE),
                    NVL(ANY_VALUE(VC.CLOSED_DATE), CURRENT_TIMESTAMP))               AS CASE_AGE_SECONDS
    FROM VERTICAL_CASES AS VC
    WHERE VC.FIELD_CHANGED IN ('Solar_Queue__c', 'Status', 'Solar_Queue__c', 'Primary_Reason__c',
                               'Solar_Primary_Reason__c', 'Solar_Secondary_Reason__c', 'Solar_Tertiary_Reason__c',
                               'Service__c', 'Owner', 'created', 'closed')
    GROUP BY VC.CASE_NUMBER
           , VC.CASE_ID
           , VC.DT
)

SELECT *
FROM HORIZONTAL_CASES
ORDER BY CASE_NUMBER DESC
