SELECT C.*
     , PERCENT_RANK() OVER (PARTITION BY TEAM_NAME ORDER BY C.CASE_AGE_SECONDS DESC) AS ACA_PERCENT_RANK
     , PERCENT_RANK() OVER (PARTITION BY TEAM_NAME ORDER BY C.ACA_30R DESC)          AS ACA_30R_PERCENT_RANK
     , PERCENT_RANK() OVER (PARTITION BY TEAM_NAME ORDER BY C.ACA_90R DESC)          AS ACA_90R_PERCENT_RANK
     , PERCENT_RANK() OVER (PARTITION BY TEAM_NAME ORDER BY C.CLOSED_CASES DESC)     AS TCC_PERCENT_RANK
     , PERCENT_RANK() OVER (PARTITION BY TEAM_NAME ORDER BY C.CC_30R DESC)           AS TCC_30R_PERCENT_RANK
--      , PERCENT_RANK() OVER (PARTITION BY TEAM_NAME ORDER BY C.CC_90R DESC)          AS TCC_90R_PERCENT_RANK
FROM D_POST_INSTALL.T_CX_KPIS AS C