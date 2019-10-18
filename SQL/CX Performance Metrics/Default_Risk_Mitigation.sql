WITH AGENT_TABLE AS (
    SELECT FULL_NAME
         , EMPLOYEE_ID
         , SUM(EFFECTIVENESS)                            AS EFFECTIVENESS
         , TO_CHAR(AVG(EFFICIENCY), '999.0')             AS EFFICIENCY
         , SUM(QUALITY_NUM) / NULLIF(SUM(QUALITY_DENOM), 0) AS QUALITY
    FROM D_POST_INSTALL.V_CX_KPIS_DEFAULT_RISK_MITIGATION AS DD
    WHERE DT >= DATEADD(d, -30, CURRENT_DATE)
    GROUP BY FULL_NAME, EMPLOYEE_ID
)

SELECT *
FROM AGENT_TABLE
