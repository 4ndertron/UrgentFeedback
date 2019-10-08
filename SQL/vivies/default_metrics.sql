WITH RANKING_TABLE AS (
    SELECT DE.FULL_NAME
         , DE.EMPLOYEE_ID
         , DE.EFFECTIVENESS
         , DE.EFFICIENCY
         , DE.QUALITY
         , POW(PERCENT_RANK() OVER (ORDER BY NVL(DE.EFFECTIVENESS, 0)), 1) AS EFFECTIVENESS_RANK
         , rank() OVER (ORDER BY NVL(DE.EFFECTIVENESS, 0) DESC)            AS EFFECTIVENESS_INT_RANK
         , 'Not Ranked'                                                    AS EFFICIENCY_RANK
         -- POW(PERCENT_RANK() OVER (ORDER BY DE.EFFICIENCY DESC), 1)
         , 'Not Ranked'                                                    AS EFFICIENCY_INT_RANK
         -- rank() OVER (ORDER BY DE.EFFICIENCY )
         , POW(PERCENT_RANK() OVER (ORDER BY NVL(DE.QUALITY, 0)), 1)       AS QUALITY_RANK
         , rank() OVER (ORDER BY NVL(DE.QUALITY, 0) DESC)                  AS QUALITY_INT_RANK
         , (EFFECTIVENESS_RANK + QUALITY_RANK) / 2                         AS WEIGHTED_RANK
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
