SELECT FIRST_NAME
     , LAST_NAME
     , FULL_NAME
     , WORK_EMAIL_ADDRESS
     , SUPERVISOR_NAME_1
FROM HR.T_EMPLOYEE AS E
WHERE E.MGR_NAME_4 = 'Chuck Browne'
  AND NOT TERMINATED