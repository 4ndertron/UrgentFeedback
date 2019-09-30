-- 1. Start the transaction
BEGIN;

-- 2. Delete the tasks you want deleted
DELETE
FROM AUTOMATOR.T_AUTO_TASKS
WHERE ID IN ();

-- 3. Make sure no dependencies were broken
--  3a. If there are now blanks in the dependencies_namex column then a dependency was broken
--  3b. If there were dependencies broken either ROLLBACK or investigate broken tasks and delete them as well
WITH dependency_check AS
         (
             SELECT id
                  , NAMEX
                  , OWNERX
                  , OPERATIONAL
                  , TRY_TO_NUMBER(fl.VALUE::STRING) AS dependencies_task_id
             FROM AUTOMATOR.T_AUTO_TASKS
                , LATERAL FLATTEN(INPUT =>SPLIT(DEPENDENCIES, ','), OUTER => TRUE) fl
             WHERE DEPENDENCIES IS NOT NULL
         )

SELECT dc.ID
     , dc.NAMEX
     , dc.OWNERX
     , dc.OPERATIONAL
     , dc.dependencies_task_id
     , aa.NAMEX       AS dependencies_namex
     , aa.OWNERX      AS dependencies_ownerx
     , aa.OPERATIONAL AS dependencies_operational
FROM dependency_check dc
         LEFT JOIN
     AUTOMATOR.T_AUTO_TASKS aa
     ON
         dc.dependencies_task_id = aa.ID
WHERE UPPER(aa.OPERATIONAL) IS DISTINCT FROM 'OPERATIONAL';

-- 4. If everything is good commit, if not rollback
COMMIT;
ROLLBACK;