WITH RANKING_TABLE AS (
    SELECT DE.FULL_NAME
         , DE.EMPLOYEE_ID
         , DE.EFFECTIVENESS
         , DE.EFFICIENCY
         , DE.QUALITY
         , POW(PERCENT_RANK() OVER (ORDER BY NVL(DE.EFFECTIVENESS, 0) ASC), 1) AS EFFECTIVENESS_RANK
         , POW(PERCENT_RANK() OVER (ORDER BY DE.EFFICIENCY DESC), 1)           AS EFFICIENCY_RANK
         , POW(PERCENT_RANK() OVER (ORDER BY NVL(DE.QUALITY, 0) ASC), 1)       AS QUALITY_RANK
         , (EFFECTIVENESS_RANK + EFFICIENCY_RANK + QUALITY_RANK) / 3           AS WEIGHTED_RANK
    FROM D_POST_INSTALL.V_CX_KPIS_DEFAULT AS DE
)

   , MAIN AS (
    SELECT *
    FROM RANKING_TABLE
    ORDER BY WEIGHTED_RANK DESC
    LIMIT 5
)

   , TEST_CTE AS (
    SELECT *
    FROM RANKING_TABLE
)

SELECT *
FROM MAIN
