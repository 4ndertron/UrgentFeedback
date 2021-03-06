WITH AGENTS_QA AS (
    SELECT HR.FIRST_NAME || ' '    AS AGENT
         , ANY_VALUE(HR.LAST_NAME) AS AGENT_LAST
         , MIN(HR.CREATED_DATE)    AS TEAM_START_DATE
         , MAX(HR.EXPIRY_DATE)     AS TEAM_END_DATE
         , AVG(QA.QSCORE)          AS AVG_QA_SCORE
    FROM HR.T_EMPLOYEE AS HR
             LEFT OUTER JOIN
         D_POST_INSTALL.T_NE_AGENT_QSCORE AS QA
         ON QA.AGENT_FIRST_NAME = HR.FIRST_NAME || ' '
    WHERE HR.SUPERVISORY_ORG = 'Default Managers'
      AND QA.EVALUATION_DATE >= DATEADD('D', -30, CURRENT_DATE())
      AND HR.TERMINATED = FALSE
    GROUP BY HR.FIRST_NAME || ' '
    ORDER BY TEAM_START_DATE DESC
)

SELECT *
FROM AGENTS_QA
