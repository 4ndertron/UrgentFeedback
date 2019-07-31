WITH AGENT_TABLE AS (
    SELECT DF.OWNER
    , DF.OWNER_EMPLOYEE_ID
    , DF.CLOSED AS CLOSED_WON
    , DF.AVG_WIP_CYCLE AS CASE_AGE
    , DF.AVG_QA_SCORE AS QA
    FROM D_POST_INSTALL.V_CX_DEFAULT_FORECLOSURE AS DF
)

SELECT *
FROM AGENT_TABLE