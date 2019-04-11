WITH CASE_TABLE AS (
    -- Collect all the open Default 1,2,4,5 and Escalation Cases in Salesforce --
    SELECT C.CASE_NUMBER
         , ANY_VALUE(C.CASE_ID)                                                                          AS CASE_ID
         , C.PROJECT_ID
         , ANY_VALUE(LDD.AGE)                                                                            AS AGE_FOR_CASE
         , ANY_VALUE(C.SUBJECT)                                                                          AS SUBJECT1
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
),

     CASE_HISTORY_TABLE AS (
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
     ),

     FULL_CASE AS (
         SELECT CASE_NUMBER
              , ANY_VALUE(SUBJECT1)                                                                        AS SUBJECT
              , ANY_VALUE(SYSTEM_SIZE)                                                                     AS SYSTEM_SIZE
              , ANY_VALUE(SYSTEM_VALUE)                                                                    AS SYSTEM_VALUE
              , ANY_VALUE(STATUS2)                                                                         AS STATUS1
              , ANY_VALUE(AGE_FOR_CASE)                                                                    AS AGE_FOR_CASE
              , ANY_VALUE(CREATED_DATE1)                                                                   AS CREATED_DATE
              , ANY_VALUE(RECORD_PRIORITY1)                                                                AS RECORD_PRIORITY
              , ANY_VALUE(CLOSED_DATE1)                                                                    AS CLOSED_DATE
              , ANY_VALUE(EXECUTIVE_RESOLUTIONS_ACCEPTED)                                                  AS ERA
              , ANY_VALUE(RECORD_TYPE1)                                                                    AS RECORD_TYPE1
              , SOLAR_BILLING_ACCOUNT_NUMBER1
              , ANY_VALUE(DEFAULT_BUCKET)                                                                  AS DEFAULT_BUCKET1
              , ANY_VALUE(CASE_WIP_KPI)                                                                    AS CASE_WIP_KPI
              , CASE
                    WHEN STATUS1 IN ('In Progress') AND DEFAULT_BUCKET1 NOT IN ('MBW', 'Pending Legal')
                        THEN 'P4/P5/DRA Letters'
                    WHEN STATUS1 IN ('Pending Corporate Action', 'Pending Customer Action') AND
                         DEFAULT_BUCKET1 NOT IN ('MBW', 'Pending Legal') THEN 'Working with Customer'
             END                                                                                           AS STATUS_BUCKET
              , AVG(GAP)                                                                                   AS AVERAGE_GAP
              , MAX(COVERAGE)                                                                              AS COVERAGE
              , AVG(LAST_30_DAY_GAP)                                                                       AS LAST_30_DAY_GAP
              , SUM(LAST_30_DAY_COVERAGE_TALLY)                                                            AS LAST_30_DAY_COVERAGE
              , ROW_NUMBER() OVER(PARTITION BY SOLAR_BILLING_ACCOUNT_NUMBER1 ORDER BY RECORD_PRIORITY ASC) AS RN
         FROM CASE_HISTORY_TABLE
         GROUP BY SOLAR_BILLING_ACCOUNT_NUMBER1
                , CASE_NUMBER
         ORDER BY SOLAR_BILLING_ACCOUNT_NUMBER1
     ),

     CASE_ACCOUNT_TABLE AS (
         SELECT *
         FROM FULL_CASE
         WHERE RN = 1
         ORDER BY 1
     ),

     LD_HISTORY_FLAG AS (
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
     ),

     ACTIVE_CODES AS (
         SELECT LD.BILLING_ACCOUNT
              , LD.AGE
              , IFF(LD.COLLECTION_CODE LIKE '%BK%', 'BK', LD.COLLECTION_CODE) AS ACTIVE_LD_CODE
              , LD.COLLECTION_DATE
              , 1                                                             AS WIP_KPI_AC
         FROM LD.T_DAILY_DATA_EXTRACT AS LD
         WHERE LD.COLLECTION_CODE IN ('BK07', 'BK13', 'FORE', 'CORP')
     ),

     LD_FULL_HISTORY AS (
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
     ),

     CODE_MERGE_TABLE AS (
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
     ),

     LD_CODES AS (
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
     ),

     CORP_MERGE_TABLE AS (
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
     ),

     SOLUTIONS_WORKBOOK AS (
         SELECT SOLAR_BILLING_ACCOUNT_NUMBER1
              , BILLING_ACCOUNT
              , CODE_WIP_START
              , CREATED_DATE
              , WIP_START
              , CODE_WIP_END
              , CLOSED_DATE
              , WIP_END
              , CASE_NUMBER
              , STATUS_NAME
              , AVERAGE_GAP
              , COVERAGE
              , LAST_30_DAY_GAP
              , LAST_30_DAY_COVERAGE
              , STATUS1
              , SYSTEM_SIZE
              , SYSTEM_VALUE
              , NVL(AGE_FOR_CASE, AGE_DAYS) AS AGE
              , WIP_KPI
              , RECORD_TYPE1
              , MASTER_BUCKET
         FROM CORP_MERGE_TABLE
         WHERE MASTER_BUCKET IS NOT NULL
     ),

     DAILY_WIP AS (
         SELECT D.DT
              , SUM(CASE
                        WHEN TO_DATE(WIP_START) <= D.DT AND (TO_DATE(WIP_END) >= D.DT OR WIP_END IS NULL OR WIP_KPI = 1)
                            THEN 1 END)                                                AS ALL_WIP
              , SUM(CASE
                        WHEN TO_DATE(WIP_START) <= D.DT AND
                             (TO_DATE(WIP_END) >= D.DT OR WIP_END IS NULL OR WIP_KPI = 1) AND
                             SW.MASTER_BUCKET = 'Pending Legal' THEN 1 END)            AS Pending_Legal_WIP
              , SUM(CASE
                        WHEN TO_DATE(WIP_START) <= D.DT AND
                             (TO_DATE(WIP_END) >= D.DT OR WIP_END IS NULL OR WIP_KPI = 1) AND SW.MASTER_BUCKET = 'D1'
                            THEN 1 END)                                                AS D1_WIP
              , SUM(CASE
                        WHEN TO_DATE(WIP_START) <= D.DT AND
                             (TO_DATE(WIP_END) >= D.DT OR WIP_END IS NULL OR WIP_KPI = 1) AND SW.MASTER_BUCKET = 'D2'
                            THEN 1 END)                                                AS D2_WIP
              , SUM(CASE
                        WHEN TO_DATE(WIP_START) <= D.DT AND
                             (TO_DATE(WIP_END) >= D.DT OR WIP_END IS NULL OR WIP_KPI = 1) AND SW.MASTER_BUCKET = 'D4'
                            THEN 1 END)                                                AS D4_WIP
              , SUM(CASE
                        WHEN TO_DATE(WIP_START) <= D.DT AND
                             (TO_DATE(WIP_END) >= D.DT OR WIP_END IS NULL OR WIP_KPI = 1) AND SW.MASTER_BUCKET = 'D5'
                            THEN 1 END)                                                AS D5_WIP
              , SUM(CASE
                        WHEN TO_DATE(WIP_START) <= D.DT AND
                             (TO_DATE(WIP_END) >= D.DT OR WIP_END IS NULL OR WIP_KPI = 1) AND SW.MASTER_BUCKET = 'MBW'
                            THEN 1 END)                                                AS MBW_WIP
              , SUM(CASE
                        WHEN TO_DATE(WIP_START) <= D.DT AND
                             (TO_DATE(WIP_END) >= D.DT OR WIP_END IS NULL OR WIP_KPI = 1) AND SW.MASTER_BUCKET = 'BK'
                            THEN 1 END)                                                AS BK_WIP
              , SUM(CASE
                        WHEN TO_DATE(WIP_START) <= D.DT AND
                             (TO_DATE(WIP_END) >= D.DT OR WIP_END IS NULL OR WIP_KPI = 1) AND SW.MASTER_BUCKET = 'FORE'
                            THEN 1 END)                                                AS FORE_WIP
              , SUM(CASE
                        WHEN TO_DATE(WIP_START) <= D.DT AND
                             (TO_DATE(WIP_END) >= D.DT OR WIP_END IS NULL OR WIP_KPI = 1) AND
                             SW.MASTER_BUCKET = 'CORP-ER' THEN 1 END)                  AS CORP_ER_WIP
              , SUM(CASE
                        WHEN TO_DATE(WIP_START) <= D.DT AND
                             (TO_DATE(WIP_END) >= D.DT OR WIP_END IS NULL OR WIP_KPI = 1) AND
                             SW.MASTER_BUCKET = 'CORP-Default' THEN 1 END)             AS CORP_Default_WIP
              , SUM(CASE
                        WHEN TO_DATE(WIP_START) <= D.DT AND
                             (TO_DATE(WIP_END) >= D.DT OR WIP_END IS NULL OR WIP_KPI = 1) AND
                             SW.MASTER_BUCKET = 'CORP-Damage' THEN 1 END)              AS CORP_Damage_WIP
              , SUM(CASE
                        WHEN TO_DATE(WIP_START) <= D.DT AND
                             (TO_DATE(WIP_END) >= D.DT OR WIP_END IS NULL OR WIP_KPI = 1) AND
                             SW.MASTER_BUCKET = 'CORP-CX' THEN 1 END)                  AS CORP_CX_WIP
              , SUM(CASE
                        WHEN TO_DATE(WIP_START) <= D.DT AND
                             (TO_DATE(WIP_END) >= D.DT OR WIP_END IS NULL OR WIP_KPI = 1) AND
                             SW.MASTER_BUCKET = 'CORP-Removal & Reinstall' THEN 1 END) AS CORP_RR_WIP
              , SUM(CASE
                        WHEN TO_DATE(WIP_START) <= D.DT AND
                             (TO_DATE(WIP_END) >= D.DT OR WIP_END IS NULL OR WIP_KPI = 1) AND
                             SW.MASTER_BUCKET = 'CORP-No Case' THEN 1 END)             AS CORP_NO_CASE_WIP
         FROM SOLUTIONS_WORKBOOK AS SW
            , RPT.T_DATES AS D
         WHERE D.DT BETWEEN DATEADD('y', -1, DATE_TRUNC('MM', CURRENT_DATE())) AND CURRENT_DATE()
         GROUP BY D.DT
         ORDER BY D.DT
     )

SELECT DT
     , D1_WIP + D2_WIP + D4_WIP + NVL(D5_WIP, 0) + CORP_Default_WIP + CORP_No_Case_WIP AS DEFAULT_WIP
     , Pending_Legal_WIP
     , MBW_WIP
     , BK_WIP + FORE_WIP                                                               AS BK_FORE_WIP
     , CORP_ER_WIP + CORP_Damage_WIP + CORP_CX_WIP + CORP_RR_WIP                       AS CORP_OTHER_WIP
     , ALL_WIP
FROM DAILY_WIP AS DW
WHERE DW.DT = LAST_DAY(DATEADD('MM',-1,CURRENT_DATE()))