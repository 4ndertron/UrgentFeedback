/*
TODO: required fields
    Active Cases
    Avg Open Case Age
    Last 30 Day Coverage
    Last 30 Day Closed Case
    Last 30 Day Avg Daily Comments
*/

WITH ALL_DEFAULT AS (
    SELECT HR.FULL_NAME
         , ANY_VALUE(HR.FIRST_NAME)            AS FIRST_NAME
         , ANY_VALUE(HR.LAST_NAME)             AS LAST_NAME
         , ANY_VALUE(HR.SUPERVISOR_BADGE_ID_1) AS SUPERVISOR
         , MIN(HR.CREATED_DATE)                AS TEAM_START_DATE
         , MAX(HR.EXPIRY_DATE)                 AS TEAM_END_DATE
         , ANY_VALUE(HR.TERMINATED)            AS TERMINATED
         , ANY_VALUE(HR.SF_REP_ID)             AS SF_ID
    FROM HR.T_EMPLOYEE_ALL AS HR
    WHERE HR.SUPERVISORY_ORG = 'Default Managers'
    GROUP BY HR.FULL_NAME
    ORDER BY TEAM_START_DATE DESC
)

   , ACTIVE_DEFAULT AS (
    SELECT *
    FROM ALL_DEFAULT
    WHERE NOT TERMINATED
      AND SUPERVISOR = '67600'
      AND TEAM_END_DATE > CURRENT_DATE()
)

   , CASE_TABLE AS (
    -- Collect all the open Default 1,2,4,5 and Escalation Cases in Salesforce --
    SELECT C.CASE_NUMBER
         , ANY_VALUE(C.CASE_ID)                                                                          AS CASE_ID
         , C.PROJECT_ID
         , ANY_VALUE(LDD.AGE)                                                                            AS AGE_FOR_CASE
         , ANY_VALUE(C.SUBJECT)                                                                          AS SUBJECT1
         , ANY_VALUE(C.OWNER)                                                                            AS OWNER
         , ANY_VALUE(C.OWNER_ID)                                                                         AS OWNER_ID
         , ANY_VALUE(CAD.SYSTEM_SIZE)                                                                    AS SYSTEM_SIZE
         , ROUND(ANY_VALUE(CAD.SYSTEM_SIZE) * 1000 * 4, 2)                                               AS SYSTEM_VALUE
         , ANY_VALUE(C.STATUS)                                                                           AS STATUS2
         , ANY_VALUE(C.CREATED_DATE)                                                                     AS CREATED_DATE1
         , ANY_VALUE(C.CLOSED_DATE)                                                                      AS CLOSED_DATE1
         , ANY_VALUE(C.EXECUTIVE_RESOLUTIONS_ACCEPTED)                                                   AS EXECUTIVE_RESOLUTIONS_ACCEPTED
         , ANY_VALUE(C.RECORD_TYPE)                                                                      AS RECORD_TYPE1
         , ANY_VALUE(S.SOLAR_BILLING_ACCOUNT_NUMBER)                                                     AS SOLAR_BILLING_ACCOUNT_NUMBER1
         , CASE
               WHEN RECORD_TYPE1 = 'Solar - Customer Default' THEN 1
               WHEN RECORD_TYPE1 = 'Solar Damage Resolutions' THEN 2
               WHEN RECORD_TYPE1 = 'Solar - Customer Escalation' THEN 3
               WHEN RECORD_TYPE1 = 'Solar - Cancellation Request' THEN 4
               WHEN RECORD_TYPE1 = 'Solar - Panel Removal' THEN 5
               WHEN RECORD_TYPE1 = 'Solar - Service' THEN 6
               WHEN RECORD_TYPE1 = 'Solar - Troubleshooting' THEN 7
               WHEN RECORD_TYPE1 = 'Solar - Panel Removal' THEN 8
        END                                                                                              AS RECORD_PRIORITY1
         , ROW_NUMBER() OVER(PARTITION BY C.PROJECT_ID ORDER BY RECORD_PRIORITY1 ASC,CREATED_DATE1 DESC) AS RN
         , CASE
               WHEN STATUS2 = 'Escalated' AND RECORD_TYPE1 = 'Solar - Customer Default' THEN 'Pending Legal'
               WHEN RECORD_TYPE1 = 'Solar - Customer Default' AND ANY_VALUE(C.DESCRIPTION) ILIKE '%MBW%' THEN 'MBW'
               WHEN RECORD_TYPE1 = 'Solar - Customer Default' AND SUBJECT1 ILIKE '%D1%' THEN 'D1'
               WHEN RECORD_TYPE1 = 'Solar - Customer Default' AND SUBJECT1 ILIKE '%D2%' THEN 'D2'
               WHEN RECORD_TYPE1 = 'Solar - Customer Default' AND SUBJECT1 ILIKE '%D4%' THEN 'D4'
               WHEN RECORD_TYPE1 = 'Solar - Customer Default' AND SUBJECT1 ILIKE '%D5%' THEN 'D5'
               WHEN RECORD_TYPE1 = 'Solar - Customer Default' AND SUBJECT1 ILIKE '%CORP%' THEN 'CORP-Default'
        END                                                                                              AS DEFAULT_BUCKET
         , CASE WHEN CLOSED_DATE1 IS NULL THEN 1 END                                                     AS CASE_WIP_KPI
    FROM RPT.T_CASE AS C
             LEFT OUTER JOIN
         RPT.T_SERVICE AS S
         ON C.SERVICE_ID = S.SERVICE_ID

             LEFT OUTER JOIN
         RPT.T_SYSTEM_DETAILS_SNAP AS CAD
         ON CAD.SERVICE_ID = C.SERVICE_ID

             LEFT OUTER JOIN
         LD.T_DAILY_DATA_EXTRACT AS LDD
         ON LDD.BILLING_ACCOUNT = S.SOLAR_BILLING_ACCOUNT_NUMBER
    WHERE (
            C.RECORD_TYPE = 'Solar - Customer Default'
            AND C.SUBJECT NOT LIKE '%D3%'
        )
       OR (
        (C.RECORD_TYPE = 'Solar - Customer Escalation' AND C.EXECUTIVE_RESOLUTIONS_ACCEPTED IS NOT NULL)
        )
       OR (
        C.RECORD_TYPE IN ('Solar Damage Resolutions')
        )
       OR (
            C.RECORD_TYPE IN ('Solar - Service', 'Solar - Troubleshooting') OR
            (C.RECORD_TYPE = 'Solar - Customer Escalation' AND C.EXECUTIVE_RESOLUTIONS_ACCEPTED IS NULL)
        )
       OR (
        C.RECORD_TYPE IN ('Solar - Panel Removal')
        )
    GROUP BY C.PROJECT_ID
           , C.CASE_NUMBER
    ORDER BY C.PROJECT_ID
           , RECORD_PRIORITY1
)

   , CASE_HISTORY_TABLE AS (
    SELECT CT.*
         , CH.CREATEDDATE
         , DATEDIFF(S, CREATEDDATE,
                    NVL(LEAD(CREATEDDATE) OVER(PARTITION BY CT.CASE_NUMBER ORDER BY CH.CREATEDDATE),
                        CURRENT_TIMESTAMP())) / (24 * 60 * 60)                    AS GAP
         , ROW_NUMBER() OVER(PARTITION BY CT.CASE_NUMBER ORDER BY CH.CREATEDDATE) AS COVERAGE
         , IFF(CH.CREATEDDATE >= DATEADD('D', -30, CURRENT_DATE()),
               DATEDIFF(S,
                        CREATEDDATE,
                        NVL(LEAD(CREATEDDATE) OVER(PARTITION BY CT.CASE_NUMBER
                                 ORDER BY CH.CREATEDDATE),
                            CURRENT_TIMESTAMP())) / (24 * 60 * 60), NULL)
                                                                                  AS LAST_30_DAY_GAP
         , IFF(CH.CREATEDDATE >= DATEADD('D', -30, CURRENT_DATE()),
               1,
               NULL)
                                                                                  AS LAST_30_DAY_COVERAGE_TALLY
    FROM CASE_TABLE CT
             LEFT OUTER JOIN
         RPT.V_SF_CASEHISTORY AS CH
         ON CH.CASEID = CT.CASE_ID
    WHERE RN = 1
    ORDER BY CASE_NUMBER, CREATEDDATE
)

   , COVERAGE AS (
    SELECT CASE_NUMBER
         , CREATEDDATE
         , ROW_NUMBER() OVER(PARTITION BY CASE_NUMBER ORDER BY CREATEDDATE) AS LAST_30_DAY_COVERAGE
    FROM CASE_HISTORY_TABLE
    WHERE CREATEDDATE >= DATEADD('D', -30, CURRENT_DATE())
)

   , FULL_CASE AS (
    SELECT CHT.CASE_NUMBER
         , ANY_VALUE(CHT.SUBJECT1)                                                               AS SUBJECT
         , ANY_VALUE(CHT.SYSTEM_SIZE)                                                            AS SYSTEM_SIZE
         , ANY_VALUE(CHT.SYSTEM_VALUE)                                                           AS SYSTEM_VALUE
         , ANY_VALUE(CHT.STATUS2)                                                                AS STATUS1
         , ANY_VALUE(CHT.OWNER)                                                                  AS OWNER
         , ANY_VALUE(CHT.OWNER_ID)                                                               AS OWNER_ID
         , ANY_VALUE(CHT.AGE_FOR_CASE)                                                           AS AGE_FOR_CASE
         , ANY_VALUE(CHT.CREATED_DATE1)                                                          AS CREATED_DATE
         , ANY_VALUE(CHT.RECORD_PRIORITY1)                                                       AS RECORD_PRIORITY
         , ANY_VALUE(CHT.CLOSED_DATE1)                                                           AS CLOSED_DATE
         , ANY_VALUE(CHT.EXECUTIVE_RESOLUTIONS_ACCEPTED)                                         AS ERA
         , ANY_VALUE(CHT.RECORD_TYPE1)                                                           AS RECORD_TYPE1
         , CHT.SOLAR_BILLING_ACCOUNT_NUMBER1
         , ANY_VALUE(DEFAULT_BUCKET)                                                             AS DEFAULT_BUCKET1
         , ANY_VALUE(CASE_WIP_KPI)                                                               AS CASE_WIP_KPI
         , CASE
               WHEN STATUS1 IN ('In Progress') AND DEFAULT_BUCKET1 NOT IN ('MBW', 'Pending Legal')
                   THEN 'P4/P5/DRA Letters'
               WHEN STATUS1 IN ('Pending Corporate Action', 'Pending Customer Action') AND
                    DEFAULT_BUCKET1 NOT IN ('MBW', 'Pending Legal') THEN 'Working with Customer'
        END                                                                                      AS STATUS_BUCKET
         , AVG(CHT.GAP)                                                                          AS AVERAGE_GAP
         , MAX(CHT.COVERAGE)                                                                     AS COVERAGE
         , AVG(CHT.LAST_30_DAY_GAP)                                                              AS LAST_30_DAY_GAP
         , MAX(C.LAST_30_DAY_COVERAGE)                                                           AS LAST_30_DAY_COVERAGE
         , ROW_NUMBER() OVER(
                        PARTITION BY SOLAR_BILLING_ACCOUNT_NUMBER1 ORDER BY RECORD_PRIORITY ASC) AS RN
    FROM CASE_HISTORY_TABLE AS CHT
             LEFT OUTER JOIN
         COVERAGE AS C
         ON C.CASE_NUMBER = CHT.CASE_NUMBER
    GROUP BY SOLAR_BILLING_ACCOUNT_NUMBER1
           , CHT.CASE_NUMBER
    ORDER BY SOLAR_BILLING_ACCOUNT_NUMBER1
)

   , CASE_ACCOUNT_TABLE AS (
    SELECT *
    FROM FULL_CASE
    WHERE RN = 1
    ORDER BY 1
)

   , LD_HISTORY_FLAG AS (
    SELECT BILLING_ACCOUNT
         , NVL(COLLECTION_CODE, 'No Code')                         AS COLLECTION_CODE
         , IFF(COLLECTION_CODE LIKE '%BK%', 'BK', COLLECTION_CODE) AS LD_CODE
         , COLLECTION_DATE
         , MAX(AGE)                                                AS AGE
         , ROW_NUMBER() OVER(PARTITION BY BILLING_ACCOUNT
                        ORDER BY COLLECTION_DATE)
                                                                   AS RN
    FROM LD.T_DAILY_DATA_EXTRACT_HIST AS LDH
    WHERE (
            AS_OF_DATE >= CURRENT_DATE() - 1
            AND COLLECTION_CODE IN ('CORP', 'FORE', 'BK07', 'BK13')
        )
       OR (
            EQUAL_NULL(COLLECTION_CODE, 'FORE')
            OR EQUAL_NULL(COLLECTION_CODE, 'CORP')
            OR EQUAL_NULL(COLLECTION_CODE, 'BK07')
            OR EQUAL_NULL(COLLECTION_CODE, 'BK13')
        )
    GROUP BY BILLING_ACCOUNT
           , COLLECTION_CODE
           , COLLECTION_DATE
    ORDER BY BILLING_ACCOUNT
           , COLLECTION_DATE
)

   , ACTIVE_CODES AS (
    SELECT LD.BILLING_ACCOUNT
         , LD.AGE
         , IFF(LD.COLLECTION_CODE LIKE '%BK%', 'BK', LD.COLLECTION_CODE) AS ACTIVE_LD_CODE
         , LD.COLLECTION_DATE
         , 1                                                             AS WIP_KPI_AC
    FROM LD.T_DAILY_DATA_EXTRACT AS LD
    WHERE LD.COLLECTION_CODE IN ('BK07', 'BK13', 'FORE', 'CORP')
)

   , LD_FULL_HISTORY AS (
    SELECT LDF.BILLING_ACCOUNT                                          AS BILLING_ACCOUNT_H
         , NVL(LDH.COLLECTION_CODE, 'NULL')                             AS COLLECTION_CODE1
         , IFF(LDH.COLLECTION_CODE LIKE '%BK%', 'BK', COLLECTION_CODE1) AS LD_CODE
         , NVL(LDH.COLLECTION_DATE,
               NVL(LAG(LDH.AS_OF_DATE) OVER(PARTITION BY LDF.BILLING_ACCOUNT ORDER BY LDH.COLLECTION_DATE),
                   LDH.AS_OF_DATE))                                     AS CODE_WIP_START1
         , LEAD(LDH.COLLECTION_DATE) OVER(PARTITION BY LDF.BILLING_ACCOUNT
                ORDER BY LDH.COLLECTION_DATE)                           AS CODE_WIP_END1
    FROM LD_HISTORY_FLAG AS LDF
             LEFT JOIN
         LD.T_DAILY_DATA_EXTRACT_HIST AS LDH
         ON LDF.BILLING_ACCOUNT = LDH.BILLING_ACCOUNT
)

   , CODE_MERGE_TABLE AS (
    SELECT AC.BILLING_ACCOUNT
         , FH.BILLING_ACCOUNT_H
         , ANY_VALUE(AC.AGE)                          AS ACTIVE_AGE
         , ANY_VALUE(AC.COLLECTION_DATE)                 ACTIVE_START
         , ANY_VALUE(AC.ACTIVE_LD_CODE)               AS ACTIVE_CODE
         , ANY_VALUE(FH.LD_CODE)                      AS HISTORY_CODE
         , ANY_VALUE(FH.CODE_WIP_START1)              AS WIP_START_H
         , CODE_WIP_END1
         , CASE WHEN CODE_WIP_END1 IS NULL THEN 1 END AS CODE_WIP_KPI
    FROM ACTIVE_CODES AS AC
             FULL JOIN
         LD_FULL_HISTORY AS FH
         ON AC.BILLING_ACCOUNT = FH.BILLING_ACCOUNT_H
    GROUP BY AC.BILLING_ACCOUNT
           , FH.BILLING_ACCOUNT_H
           , CODE_WIP_END1
)

   , LD_CODES AS (
    SELECT BILLING_ACCOUNT_H       AS BILLING_ACCOUNT
         , HISTORY_CODE            AS LD_CODE
         , ANY_VALUE(WIP_START_H)  AS CODE_WIP_START
         , CODE_WIP_END1           AS CODE_WIP_END
         , ANY_VALUE(ACTIVE_AGE)   AS AGE_DAYS
         , ANY_VALUE(CODE_WIP_KPI) AS CODE_WIP_KPI
    FROM CODE_MERGE_TABLE
    WHERE HISTORY_CODE IN ('BK', 'FORE', 'CORP')
      AND NULLIF(WIP_START_H, CODE_WIP_END1) IS NOT NULL
    GROUP BY BILLING_ACCOUNT_H
           , HISTORY_CODE
           , CODE_WIP_END
    ORDER BY 1, 2, 3
)

   , CORP_MERGE_TABLE AS (
    SELECT C.*
         , LD.*
         , CASE
               WHEN CASE_NUMBER IS NULL AND LD_CODE = 'CORP' THEN 'CORP-No Case'
               WHEN C.RECORD_TYPE1 = 'Solar - Customer Default' AND LD.LD_CODE = 'CORP' THEN 'CORP-Default'
               WHEN ((C.RECORD_TYPE1 = 'Solar - Customer Escalation' AND C.ERA IS NOT NULL) OR
                     C.RECORD_TYPE1 = 'Solar - Cancellation Request') AND LD.LD_CODE = 'CORP' THEN 'CORP-ER'
               WHEN C.RECORD_TYPE1 = 'Solar Damage Resolutions' AND LD.LD_CODE = 'CORP' THEN 'CORP-Damage'
               WHEN C.RECORD_TYPE1 = 'Solar - Panel Removal' AND LD.LD_CODE = 'CORP'
                   THEN 'CORP-Removal & Reinstall'
               WHEN (C.RECORD_TYPE1 IN ('Solar - Service', 'Solar - Troubleshooting') OR
                     (C.RECORD_TYPE1 = 'Solar - Customer Escalation' AND C.ERA IS NULL)) AND LD.LD_CODE = 'CORP'
                   THEN 'CORP-CX'
               ELSE LD.LD_CODE
        END                                                                      AS CORP_BUCKET
         , CASE
               WHEN C.STATUS1 = 'New' THEN 'Needs Audit & Assigned'
               WHEN C.STATUS1 = 'Escalated' THEN 'Legal/Lawsuit'
               WHEN C.STATUS1 = 'In Progress' THEN 'Third-Party/Letters'
               WHEN C.STATUS1 = 'Pending Customer Action' THEN 'Working to Cure'
               WHEN C.STATUS1 = 'Pending Corporate Action' THEN 'Cancellation Approval'
               ELSE C.STATUS1
        END                                                                      AS STATUS_NAME
         , NVL(DEFAULT_BUCKET1, CORP_BUCKET)                                     AS MASTER_BUCKET
         , NVL(CODE_WIP_START, CREATED_DATE)                                     AS WIP_START
         , NVL(CODE_WIP_END, CLOSED_DATE)                                        AS WIP_END
         , CASE WHEN C.CASE_WIP_KPI = 1 OR LD.CODE_WIP_KPI = 1 THEN 1 ELSE 0 END AS WIP_KPI
    FROM LD_CODES AS LD
             FULL JOIN
         CASE_ACCOUNT_TABLE AS C
         ON
             C.SOLAR_BILLING_ACCOUNT_NUMBER1 = LD.BILLING_ACCOUNT
    ORDER BY LD.BILLING_ACCOUNT
)

   , SOLUTIONS_WORKBOOK AS (
    SELECT SOLAR_BILLING_ACCOUNT_NUMBER1
         , BILLING_ACCOUNT
         , CODE_WIP_START
         , CREATED_DATE
         , WIP_START
         , CODE_WIP_END
         , CLOSED_DATE
         , WIP_END
         , DATEDIFF('D', CREATED_DATE, NVL(CLOSED_DATE, CURRENT_DATE())) AS CASE_AGE
         , CASE_NUMBER
         , OWNER
         , OWNER_ID
         , STATUS_NAME
         , AVERAGE_GAP
         , COVERAGE
         , LAST_30_DAY_GAP
         , LAST_30_DAY_COVERAGE
         , STATUS1
         , SYSTEM_SIZE
         , SYSTEM_VALUE
         , NVL(AGE_FOR_CASE, AGE_DAYS)                                   AS AGE
         , WIP_KPI
         , RECORD_TYPE1
         , MASTER_BUCKET
    FROM CORP_MERGE_TABLE
    WHERE MASTER_BUCKET IS NOT NULL
)

   , ACTIVE_AGENT_CASES AS (
    SELECT *
    FROM SOLUTIONS_WORKBOOK AS SW
             LEFT OUTER JOIN
         ACTIVE_DEFAULT AS AD
         ON SW.OWNER = AD.FULL_NAME
    WHERE SUPERVISOR IS NOT NULL
)

   , AGENT_IDS AS (
    SELECT DISTINCT OWNER_ID
                  , OWNER
    FROM ACTIVE_AGENT_CASES
)

   , CASE_COMMENTS AS (
    SELECT *
    FROM RPT.V_SF_CASECOMMENT AS CC
             LEFT OUTER JOIN
         AGENT_IDS AS IDS
         ON IDS.OWNER_ID = CC.CREATEDBYID
)

   , ACTIVE_AGENT_COMMENTS AS (
    SELECT *
    FROM CASE_COMMENTS
    WHERE OWNER IS NOT NULL
)

   , AGENT_COMMENTS_KPI AS (
    SELECT OWNER
         , COUNT(CASE WHEN CREATEDATE >= DATEADD('D', -30, CURRENT_DATE()) THEN 1 END) AS TOTAL_COMMENTS
         , ROUND(TOTAL_COMMENTS / 22, 2)                                               AS AVG_DAILY_COMMENTS
    FROM ACTIVE_AGENT_COMMENTS
    GROUP BY OWNER
    ORDER BY OWNER
)

   , AGENT_KPI_TABLE AS (
    SELECT AC.OWNER
         , SUM(AC.WIP_KPI)                                                                 AS ACTIVE_CASES
         , ROUND(AVG(CASE WHEN AC.WIP_KPI > 0 THEN AC.CASE_AGE END), 2)                    AS AVG_OPEN_CASE_AGE
         , ROUND(AVG(CASE WHEN AC.WIP_KPI > 0 THEN AC.LAST_30_DAY_COVERAGE END), 2)        AS OPEN_LAST_30_DAY_COVERAGE
         , COUNT(CASE WHEN AC.CLOSED_DATE >= DATEADD('D', -30, CURRENT_DATE()) THEN 1 END) AS LAST_30_DAY_CLOSED_CASES
         , ANY_VALUE(CC.AVG_DAILY_COMMENTS)                                                AS AVG_DAILY_COMMENTS
    FROM ACTIVE_AGENT_CASES AS AC
             LEFT OUTER JOIN
         AGENT_COMMENTS_KPI AS CC
         ON CC.OWNER = AC.OWNER
    GROUP BY AC.OWNER
)

SELECT *
FROM AGENT_KPI_TABLE