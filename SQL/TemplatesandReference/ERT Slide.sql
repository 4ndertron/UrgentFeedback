WITH ch1 AS
-- SQL template for looking at the status history of Cases
         (
             SELECT ca.case_id,
                    CASE
                        WHEN
                            ch.caseid IS NOT NULL
                            THEN
                            COALESCE(ch.newvalue,
                                     LEAD(ch.oldvalue) OVER(PARTITION BY ca.case_id ORDER BY ch.createddate),
                                     ca.status)
                        END                                            AS case_hist_status,
                    ch.createddate                                     AS case_hist_status_start,
                    CASE
                        WHEN
                            ch.caseid IS NOT NULL
                            THEN
                            NVL(LEAD(ch.createddate) OVER(PARTITION BY ca.case_id ORDER BY ch.createddate),
                                dateadd('d', 1, CURRENT_TIMESTAMP :: DATETIME))
                        END                                            AS case_hist_status_end,
                    DATE_TRUNC('d', CA.EXECUTIVE_RESOLUTIONS_ACCEPTED) AS ERA,
                    (
                            CA.ORIGIN = 'CEO Promise'
                            OR CA.ORIGIN = 'Executive'
                            OR CA.PRIORITY = '1'
                            OR CA.SUBJECT ILIKE '[CEO]%'
                            OR CA.SUBJECT LIKE '%1%'
                        )                                              AS IS_P1
             FROM rpt.t_case ca
                      INNER JOIN rpt.v_sf_casehistory ch ON
                     ca.case_id = ch.caseid
                     AND ch.field IN ('created', 'Status')
                     -- ↓↓↓ Criteria to narrow down which cases you're looking for goes here ↓↓↓
                     AND RECORD_TYPE = 'Solar - Customer Escalation'
                     AND EXECUTIVE_RESOLUTIONS_ACCEPTED IS NOT NULL
                     AND SUBJECT NOT ILIKE '[NPS]%'
                     AND SUBJECT NOT ILIKE '%VIP%'
                     AND ORIGIN != 'NPS'),
     ch2 AS
-- PREPARE DE-DUPE: Mark each time a new date range is started
         (
             SELECT case_id,
                    case_hist_status_start,
                    case_hist_status_end,
                    IFF(
                                DATEDIFF(
                                        D,
                                        LAG(case_hist_status_end) OVER(PARTITION BY case_id
                                            ORDER BY case_hist_status_start), case_hist_status_start)
                                <= 1,
                                0,
                                1) AS new_date_range,
                    ERA,
                    IS_P1
             FROM ch1
                  -- ↓↓↓ Criteria to include/exclude certain statuses goes here ↓↓↓

             WHERE case_hist_status NOT IN ('In Dispute', 'Closed', 'Closed - No Contact')),
     ch3 AS
-- PREPARE DE-DUPE: Apply unique number each time a new date range is started
         (
             SELECT case_id,
                    case_hist_status_start,
                    case_hist_status_end,
                    SUM(new_date_range) OVER(PARTITION BY case_id
                        ORDER BY
                            case_hist_status_start) AS date_range_count,
                    IS_P1,
                    ERA
             FROM ch2),
     ch4 AS
-- EXECUTE DE-DUPE: Consolidate consecutive rows where there is a good status without skipping any days
         (
             SELECT case_id,
                    date_trunc('d', MIN(case_hist_status_start)) AS created,
                    date_trunc('d', MAX(case_hist_status_end))   AS closed,
                    CASE
                        WHEN date_trunc('d', MIN(case_hist_status_start)) IS NOT NULL
                            THEN 1
                        ELSE 0
                        END                                      AS TOTAL_CASES,
                    any_value(IS_P1)                             AS is_p1,
                    any_value(ERA)                               AS era
             FROM ch3
             GROUP BY case_id,
                      date_range_count),
     t1 AS
--All Wip Cases.
         (
             SELECT *
             FROM ch4
             WHERE (ch4.CLOSED >= DATEADD('y', -1, DATE_TRUNC('d', CURRENT_TIMESTAMP :: TIMESTAMP_NTZ))
                 OR ch4.CLOSED IS NULL)),
     t2 AS (
         SELECT D.DT,
                d.WEEK_DAY_NUM,
                SUM(CASE WHEN T1.ERA <= D.DT AND (T1.CLOSED > D.DT OR T1.CLOSED IS NULL) THEN 1 END) AS ALL_WIP,
                SUM(CASE WHEN T1.CLOSED = D.DT AND T1.CLOSED IS NOT NULL THEN 1 END)                 AS ALL_CLOSED,
                SUM(CASE
                        WHEN T1.CLOSED = D.DT AND
                             DATEDIFF('d', DATE_TRUNC('d', T1.CREATED), DATE_TRUNC('d', T1.CLOSED)) <= 30
                            THEN TOTAL_CASES END)                                                    AS ALL_CLOSED_IN_30,
                ROUND(AVG(CASE
                              WHEN T1.CREATED <= D.DT AND (T1.CLOSED > D.DT OR T1.CLOSED IS NULL)
                                  THEN DATEDIFF('d', T1.CREATED, D.DT) END), 2)                      AS ALL_WIP_AVG_AGE,
                ROUND(MEDIAN(CASE
                                 WHEN T1.CREATED <= D.DT AND (T1.CLOSED > D.DT OR T1.CLOSED IS NULL)
                                     THEN DATEDIFF('d', T1.CREATED, D.DT) END),
                      2)                                                                             AS ALL_WIP_MEDIAN_AGE,
                MAX(CASE
                        WHEN T1.CREATED <= D.DT AND (T1.CLOSED > D.DT OR T1.CLOSED IS NULL)
                            THEN DATEDIFF('d', T1.CREATED, D.DT) END)                                AS ALL_MAX_AGE,
                MIN(CASE
                        WHEN T1.CREATED <= D.DT AND (T1.CLOSED > D.DT OR T1.CLOSED IS NULL)
                            THEN DATEDIFF('d', T1.CREATED, D.DT) END)                                AS ALL_MIN_AGE,
                SUM(CASE
                        WHEN T1.ERA <= D.DT AND (T1.CLOSED > D.DT OR T1.CLOSED IS NULL) AND T1.IS_P1
                            THEN 1 END)                                                              AS P1_WIP,
                SUM(CASE WHEN T1.CLOSED = D.DT AND T1.CLOSED IS NOT NULL AND T1.IS_P1 THEN 1 END)    AS P1_CLOSED,
                SUM(CASE
                        WHEN T1.CLOSED = D.DT AND
                             DATEDIFF('d', DATE_TRUNC('d', T1.CREATED), DATE_TRUNC('d', T1.CLOSED)) <= 30 AND T1.IS_P1
                            THEN TOTAL_CASES END)                                                    AS P1_CLOSED_IN_30,
                ROUND(AVG(CASE
                              WHEN T1.CREATED <= D.DT AND (T1.CLOSED > D.DT OR T1.CLOSED IS NULL) AND T1.IS_P1
                                  THEN DATEDIFF('d', T1.CREATED, D.DT) END), 2)                      AS P1_WIP_AVG_AGE,
                ROUND(MEDIAN(CASE
                                 WHEN T1.CREATED <= D.DT AND (T1.CLOSED > D.DT OR T1.CLOSED IS NULL) AND T1.IS_P1
                                     THEN DATEDIFF('d', T1.CREATED, D.DT) END),
                      2)                                                                             AS P1_WIP_MEDIAN_AGE,
                MAX(CASE
                        WHEN T1.CREATED <= D.DT AND (T1.CLOSED > D.DT OR T1.CLOSED IS NULL) AND T1.IS_P1
                            THEN DATEDIFF('d', T1.CREATED, D.DT) END)                                AS P1_MAX_AGE,
                MIN(CASE
                        WHEN T1.CREATED <= D.DT AND (T1.CLOSED > D.DT OR T1.CLOSED IS NULL) AND T1.IS_P1
                            THEN DATEDIFF('d', T1.CREATED, D.DT) END)                                AS P1_MIN_AGE
         FROM RPT.V_DATES D,
              T1
         WHERE D.DT BETWEEN DATEADD('y', -1, DATE_TRUNC('d', CURRENT_TIMESTAMP :: TIMESTAMP_NTZ))
                   AND DATE_TRUNC('d', CURRENT_TIMESTAMP :: TIMESTAMP_NTZ)
         GROUP BY D.DT,
                  d.WEEK_DAY_NUM
         ORDER BY D.DT)

SELECT t2.dt,
       t2.ALL_WIP,
       t2.ALL_CLOSED,
       t2.ALL_CLOSED_IN_30,
       t2.ALL_WIP_AVG_AGE,
       t2.ALL_WIP_MEDIAN_AGE,
       t2.ALL_MAX_AGE,
       t2.ALL_MIN_AGE,
       t2.P1_WIP,
       t2.P1_CLOSED,
       t2.P1_CLOSED_IN_30,
       t2.P1_WIP_AVG_AGE,
       t2.P1_WIP_MEDIAN_AGE,
       t2.P1_MAX_AGE,
       t2.P1_MIN_AGE
FROM t2
WHERE
   -- Monday Criteria
    (DATE_PART('dw', CURRENT_TIMESTAMP :: TIMESTAMP_NTZ) = 1
        AND t2.WEEK_DAY_NUM = 6)
   OR
   -- Non-Monday Criteria
    (DATE_PART('dw', CURRENT_TIMESTAMP :: TIMESTAMP_NTZ) != 1
        AND t2.WEEK_DAY_NUM = DATE_PART('dw', CURRENT_TIMESTAMP :: TIMESTAMP_NTZ))