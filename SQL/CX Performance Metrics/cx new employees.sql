CREATE OR REPLACE VIEW D_POST_INSTALL.V_CX_EMPLOYEES
            COPY GRANTS
            COMMENT = 'CX employees + logic to determine which team they were on when.'
AS
WITH raw_data AS
         (
             SELECT et.EMPLOYEE_ID
                  , te.FULL_TEAM_NAME                                                AS new_team
                  , GREATEST(et.START_DATE, em.HIRE_DATE)                            AS start_date
                  , LEAST(NVL(DATEADD('day', -1,
                                      LEAD(et.START_DATE) OVER (PARTITION BY et.EMPLOYEE_ID ORDER BY et.START_DATE)),
                              '9999-12-31'), NVL(em.TERMINATION_DATE, '9999-12-31')) AS end_date
                  , em.HIRE_DATE
                  , em.TERMINATION_DATE
                  , CASE
                        WHEN em.BUSINESS_SITE_NAME = 'UT-Work from Home'
                            THEN 'WFH'
                        WHEN em.EMP_COMPENSATION_TYPE = 'Regular'
                            THEN 'Lehi'
                        When em.EMP_COMPENSATION_TYPE = 'Third Party Contractor'
                            Then 'Contractor'
                        ELSE em.EMP_COMPENSATION_TYPE
                 END                                                                 AS location
             FROM D_POST_INSTALL.T_CX_EMPLOYEE_TEAM_HISTORY et
                      INNER JOIN
                  D_POST_INSTALL.T_CX_TEAMS te
                  ON
                          et.department_id = te.DEPARTMENT_ID
                          AND et.team_id = te.TEAM_ID
                      INNER JOIN
                  hr.T_EMPLOYEE em
                  ON
                      et.EMPLOYEE_ID = em.EMPLOYEE_ID
             ORDER BY et.EMPLOYEE_ID
                    , et.START_DATE
         )

SELECT EMPLOYEE_ID
     , NEW_TEAM
     , start_date
     , end_date
     , location
FROM raw_data
WHERE start_date >= HIRE_DATE
  AND end_date <= end_date;

